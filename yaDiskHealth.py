#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 Alexey Sedoykin
# SPDX-License-Identifier: MIT

from __future__ import annotations

import argparse
import csv
import errno
import math
import os
import queue
import random
import re
import sqlite3
import sys
import threading
import time
import webbrowser
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from http.client import IncompleteRead
from pathlib import Path
from typing import Iterator
from urllib.parse import urlsplit

import requests
from urllib3.exceptions import HTTPError as StreamError, ProtocolError


API_ROOT = "https://cloud-api.yandex.net/v1/disk"
PAGE_SIZE = 500
DEFAULT_WORKERS = 6
DEFAULT_PROBE_BYTES = 1024
READ_CHUNK_SIZE = 64 * 1024
API_TIMEOUT = (8, 20)  # connect, read timeout in seconds
CDN_TIMEOUT = (8, 18)
RETRYABLE_CODES = {429} | set(range(500, 600))
MAX_ATTEMPTS = 3
HISTORY_PATH = Path(__file__).with_name("yaDiskHealthHistory.sqlite3")
MODE_LABELS = {"failed": "Только недоступные", "random": "Случайная проверка доступных",
               "full": "Полная проверка", "manual": "Выбранные файлы"}
NETWORK_FAILURE_NOTES = ("Тайм-аут или ошибка сети", "Тайм-аут или ошибка сети при чтении",
                         "Ошибка сети при чтении", "Сеть или API недоступны")
NETWORK_FAILURE_SQL = "note IN (" + ", ".join(f"'{note}'" for note in NETWORK_FAILURE_NOTES) + ")"
# Old versions also stored inconclusive HTTP responses as ok = 0.
CONFIRMED_CHECK_SQL = f"""ok = 1 OR (ok = 0 AND (
    (http_code BETWEEN 400 AND 499 AND http_code NOT IN (408, 416, 429))
    OR ((http_code IS NULL OR http_code IN (200, 206))
        AND note = 'Перенаправление на вход в аккаунт')
    OR {NETWORK_FAILURE_SQL}
))"""
CATALOG_JOIN_SQL = f"""FROM files f LEFT JOIN checks c ON c.id = (
    SELECT id FROM checks WHERE path = f.path AND ({CONFIRMED_CHECK_SQL})
    ORDER BY id DESC LIMIT 1
)"""
CHECK_LOCK = threading.Lock()


@dataclass(frozen=True)
class DiskFile:
    name: str
    path: str
    size: int | None

    @property
    def folder(self) -> str:
        return self.path.rpartition("/")[0] or "disk:/"


@dataclass(frozen=True)
class CheckResult:
    # True = bytes received, False = refusal / network failure, None = inconclusive / stopped.
    ok: bool | None
    note: str
    http_code: int | None = None
    bytes_requested: int | None = None
    bytes_received: int | None = None
    duration_ms: int | None = None
    range_supported: bool | None = None
    failure_reason: str | None = None


@dataclass(frozen=True)
class HistoryEntry:
    path: str
    checked_at: str
    result: CheckResult
    run_id: int | None = None


@dataclass(frozen=True)
class CheckSelection:
    mode: str
    files: list[DiskFile]
    total_candidates: int


@dataclass(frozen=True)
class CheckRun:
    id: int
    started_at: str
    finished_at: str | None
    mode: str
    total_candidates: int
    selected_count: int
    checked_count: int
    success_count: int
    failed_count: int
    unknown_count: int
    status: str | None

    @property
    def duration(self) -> float:
        finish = datetime.fromisoformat(self.finished_at) if self.finished_at else datetime.now(timezone.utc)
        return max(0.0, (finish - datetime.fromisoformat(self.started_at)).total_seconds())


class HistoryStore:
    def __init__(self, path: str | Path):
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS files (
                path TEXT PRIMARY KEY, name TEXT NOT NULL, size INTEGER
            );
            CREATE TABLE IF NOT EXISTS checks (
                id INTEGER PRIMARY KEY, path TEXT NOT NULL,
                checked_at TEXT NOT NULL, ok INTEGER, http_code INTEGER,
                note TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS checks_path ON checks(path, id);
        """)
        with self.connection:
            self.connection.execute("BEGIN IMMEDIATE")
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS check_runs (
                    id INTEGER PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT,
                    mode TEXT NOT NULL, total_candidates INTEGER NOT NULL,
                    selected_count INTEGER NOT NULL, checked_count INTEGER NOT NULL DEFAULT 0,
                    success_count INTEGER NOT NULL DEFAULT 0, failed_count INTEGER NOT NULL DEFAULT 0,
                    unknown_count INTEGER NOT NULL DEFAULT 0,
                    status TEXT CHECK(status IN ('completed', 'stopped', 'error'))
                )
            """)
            columns = {row[1] for row in self.connection.execute("PRAGMA table_info(checks)")}
            for name, kind in (
                ("http_code", "INTEGER"),
                ("bytes_requested", "INTEGER"), ("bytes_received", "INTEGER"),
                ("duration_ms", "INTEGER"), ("range_supported", "INTEGER"), ("failure_reason", "TEXT"),
                ("run_id", "INTEGER REFERENCES check_runs(id)"),
            ):
                if name not in columns:
                    self.connection.execute(f"ALTER TABLE checks ADD COLUMN {name} {kind}")
            self.connection.execute("CREATE INDEX IF NOT EXISTS checks_run ON checks(run_id, id)")
            version = self.connection.execute("PRAGMA user_version").fetchone()[0]
            if version < 1:
                affected_runs = self.connection.execute(f"""SELECT DISTINCT run_id FROM checks
                    WHERE ok IS NOT 1 AND {NETWORK_FAILURE_SQL} AND run_id IS NOT NULL""").fetchall()
                self.connection.execute(f"""UPDATE checks SET ok = 0
                    WHERE ok IS NULL AND {NETWORK_FAILURE_SQL}""")
                for (run_id,) in affected_runs:
                    total, good, failed = self.connection.execute(f"""SELECT COUNT(*),
                        COALESCE(SUM(ok = 1), 0),
                        COALESCE(SUM(ok = 0 AND ({CONFIRMED_CHECK_SQL})), 0)
                        FROM checks WHERE run_id = ?""", (run_id,)).fetchone()
                    self.connection.execute("""UPDATE check_runs SET checked_count = ?,
                        success_count = ?, failed_count = ?, unknown_count = ? WHERE id = ?""",
                        (total, good, failed, total - good - failed, run_id))
                self.connection.execute("DROP INDEX IF EXISTS checks_confirmed")
            self.connection.execute(f"""CREATE INDEX IF NOT EXISTS checks_confirmed
                ON checks(path, id) WHERE {CONFIRMED_CHECK_SQL}""")
            if version < 1:
                self.connection.execute("PRAGMA user_version = 1")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.connection.close()

    def merge_files(self, files: list[DiskFile]) -> None:
        with self.connection:
            self.connection.executemany(
                """INSERT INTO files(path, name, size) VALUES (?, ?, ?)
                   ON CONFLICT(path) DO UPDATE SET name = excluded.name, size = excluded.size""",
                [(file.path, file.name, file.size) for file in files],
            )

    def files(self) -> list[DiskFile]:
        return [DiskFile(name, path, size) for path, name, size in
                self.connection.execute("SELECT path, name, size FROM files ORDER BY path")]

    def catalog_counts(self) -> tuple[int, int, int, int]:
        total, good, failed = self.connection.execute(f"""SELECT COUNT(*),
            COALESCE(SUM(c.ok = 1), 0), COALESCE(SUM(c.ok = 0), 0) {CATALOG_JOIN_SQL}""").fetchone()
        return total, good, failed, total - good - failed

    def select_files(self, mode: str | None = None, sample_percent: float | None = None,
                     sample_count: int | None = None, seed: int | None = None,
                     paths: list[str] | None = None) -> CheckSelection:
        if paths:
            if mode is not None or sample_percent is not None or sample_count is not None or seed is not None:
                raise ValueError("--path используется отдельно от --mode и параметров выборки")
            files = files_for_check(self.files(), paths)
            return CheckSelection("manual", files, len(files))
        mode = mode or "full"
        if mode not in ("failed", "random", "full"):
            raise ValueError("Неизвестный режим проверки")
        if mode != "random" and any(value is not None for value in (sample_percent, sample_count, seed)):
            raise ValueError("Параметры выборки используются только с --mode random")
        if mode == "full":
            files = self.files()
        else:
            files = [DiskFile(name, path, size) for path, name, size in self.connection.execute(
                f"SELECT f.path, f.name, f.size {CATALOG_JOIN_SQL} WHERE c.ok = ? ORDER BY f.path",
                (mode == "random",),
            )]
        candidates = len(files)
        if mode == "random":
            count = sample_size(candidates, sample_percent, sample_count)
            files = random.Random(seed).sample(files, count)
        return CheckSelection(mode, files, candidates)

    def start_run(self, mode: str, candidates: int, selected: int) -> int:
        with self.connection:
            cursor = self.connection.execute("""INSERT INTO check_runs
                (started_at, mode, total_candidates, selected_count) VALUES (?, ?, ?, ?)""",
                (datetime.now(timezone.utc).isoformat(timespec="microseconds"), mode, candidates, selected))
        return cursor.lastrowid

    def finish_run(self, run_id: int, status: str) -> CheckRun:
        with self.connection:
            self.connection.execute("UPDATE check_runs SET finished_at = ?, status = ? WHERE id = ?",
                (datetime.now(timezone.utc).isoformat(timespec="microseconds"), status, run_id))
        return self.runs(run_id)[0]

    def runs(self, run_id: int | None = None) -> list[CheckRun]:
        query = """SELECT id, started_at, finished_at, mode, total_candidates, selected_count,
            checked_count, success_count, failed_count, unknown_count, status FROM check_runs"""
        if run_id is not None:
            query += " WHERE id = ?"
        return [CheckRun(*row) for row in self.connection.execute(
            query + " ORDER BY id", () if run_id is None else (run_id,))]

    def record(self, path: str, result: CheckResult, run_id: int | None = None) -> HistoryEntry:
        checked_at = datetime.now(timezone.utc).isoformat(timespec="microseconds")
        with self.connection:
            self.connection.execute(
                """INSERT INTO checks(path, checked_at, ok, http_code, note, bytes_requested,
                                      bytes_received, duration_ms, range_supported, failure_reason, run_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (path, checked_at, result.ok, result.http_code, result.note, result.bytes_requested,
                 result.bytes_received, result.duration_ms, result.range_supported, result.failure_reason, run_id),
            )
            if run_id is not None:
                self.connection.execute("""UPDATE check_runs SET checked_count = checked_count + 1,
                    success_count = success_count + ?, failed_count = failed_count + ?,
                    unknown_count = unknown_count + ? WHERE id = ?""",
                    (result.ok is True, result.ok is False, result.ok is None, run_id))
        return HistoryEntry(path, checked_at, result, run_id)

    def entries(self, paths: list[str] | None = None, latest: bool = False,
                run_id: int | None = None) -> list[HistoryEntry]:
        query = f"""SELECT path, checked_at, CASE WHEN {CONFIRMED_CHECK_SQL} THEN ok END,
                          note, http_code, bytes_requested, bytes_received,
                          duration_ms, range_supported, failure_reason, run_id FROM checks"""
        conditions = []
        parameters = list(paths or [])
        if paths:
            conditions.append("path IN (" + ",".join("?" for _ in paths) + ")")
        if latest:
            conditions.append("id IN (SELECT MAX(id) FROM checks GROUP BY path)")
        if run_id is not None:
            conditions.append("run_id = ?")
            parameters.append(run_id)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        rows = self.connection.execute(query + " ORDER BY id", parameters)
        return [HistoryEntry(path, checked_at, CheckResult(
                    None if ok is None else bool(ok), note, code, requested, received, duration,
                    None if ranged is None else bool(ranged), reason), run_id)
                for path, checked_at, ok, note, code, requested, received, duration, ranged, reason, run_id in rows]


def sample_size(candidates: int, percent: float | None = None, count: int | None = None) -> int:
    if percent is not None and count is not None:
        raise ValueError("Процент и количество файлов нельзя указать вместе")
    if count is not None:
        if count < 1:
            raise ValueError("Количество файлов должно быть больше нуля")
        return min(candidates, count)
    percent = 10 if percent is None else percent
    if not math.isfinite(percent) or not 1 <= percent <= 100:
        raise ValueError("Процент должен быть от 1 до 100")
    return math.ceil(candidates * percent / 100)


def run_report(run: CheckRun) -> str:
    return (f"Запуск #{run.id}; режим: {MODE_LABELS.get(run.mode, run.mode)}; "
            f"кандидатов: {run.total_candidates}; выбрано: {run.selected_count}; "
            f"проверено: {run.checked_count}; доступно: {run.success_count}; "
            f"недоступно: {run.failed_count}; неопределённо: {run.unknown_count}; "
            f"длительность: {run.duration:.2f} с; статус: {run.status or 'выполняется'}")


def files_for_check(files: list[DiskFile], paths: list[str]) -> list[DiskFile]:
    if not paths:
        return list(files)
    selected = set(paths)
    missing = selected.difference(file.path for file in files)
    if missing:
        raise ValueError("Выбранный путь отсутствует в загруженном списке: " + sorted(missing)[0])
    return [file for file in files if file.path in selected]


def local_time(value: str) -> str:
    return datetime.fromisoformat(value).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def history_rows(entries: list[HistoryEntry]) -> Iterator[tuple]:
    previous: dict[str, bool] = {}
    for entry in entries:
        result = entry.result
        status = "Доступен" if result.ok is True else "Ошибка" if result.ok is False else (
            "Остановлено" if result.note == "Остановлено" else "Не подтверждён")
        change = ""
        if result.ok is not None:
            if entry.path in previous and previous[entry.path] != result.ok:
                change = "Доступ появился" if result.ok else "Доступ не подтверждён"
            previous[entry.path] = result.ok
        yield (local_time(entry.checked_at), entry.path, status, change,
               result.http_code or "", result.note,
               result.bytes_requested if result.bytes_requested is not None else "",
               result.bytes_received if result.bytes_received is not None else "",
               result.duration_ms if result.duration_ms is not None else "",
               "" if result.range_supported is None else "Да" if result.range_supported else "Нет",
               result.failure_reason or "", entry.run_id if entry.run_id is not None else "")


class ScanStopped(Exception):
    pass


class CheckBusy(Exception):
    pass


@contextmanager
def check_operation(history_path: str | Path):
    """Use a separate lock file so SQLite can keep its own database locks."""
    if not CHECK_LOCK.acquire(blocking=False):
        raise CheckBusy()
    try:
        with open(str(Path(history_path).resolve()) + ".lock", "a+b") as lock_file:
            try:
                if os.name == "nt":
                    import msvcrt
                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as error:
                if error.errno in (errno.EACCES, errno.EAGAIN):
                    raise CheckBusy() from error
                raise
            try:
                yield
            finally:
                if os.name == "nt":
                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(lock_file, fcntl.LOCK_UN)
    finally:
        CHECK_LOCK.release()


class DiskAPIError(Exception):
    def __init__(self, code: int | None, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class AdaptiveLimiter:
    def __init__(self, maximum: int, stop: threading.Event):
        self.maximum = maximum
        self.limit = min(DEFAULT_WORKERS, maximum)
        self._stop = stop
        self._condition = threading.Condition()
        self._active = 0
        self._blocked_until = 0.0
        self._stable_since = time.monotonic()
        self._successes = 0
        self._generation = 0

    def acquire(self) -> int:
        with self._condition:
            while True:
                if self._stop.is_set():
                    raise ScanStopped()
                delay = self._blocked_until - time.monotonic()
                if self._active < self.limit and delay <= 0:
                    self._active += 1
                    return self._generation
                # Event.set() cannot notify a Condition; bound the stop latency.
                self._condition.wait(min(0.1, delay) if delay > 0 else 0.1)

    def release(self) -> None:
        with self._condition:
            self._active -= 1
            self._condition.notify_all()

    def feedback(self, code: int, delay: float, generation: int) -> None:
        with self._condition:
            now = time.monotonic()
            if code == 429:
                self.limit = max(1, self.limit // 2)
                self._generation += 1
                self._blocked_until = max(self._blocked_until, now + delay)
                self._stable_since = self._blocked_until
                self._successes = 0
                self._condition.notify_all()
            elif generation == self._generation:
                if 200 <= code < 400:
                    self._successes += 1
                    if (self.limit < self.maximum and now - self._stable_since >= 5.0
                            and self._successes >= self.limit):
                        self.limit += 1
                        self._stable_since = now
                        self._successes = 0
                        self._condition.notify_all()
                else:
                    self._stable_since = max(now, self._blocked_until)
                    self._successes = 0


class DiskProbe:
    def __init__(self, token: str, stop: threading.Event | None = None, workers: int = DEFAULT_WORKERS,
                 probe_bytes: int = DEFAULT_PROBE_BYTES):
        self.token = token.strip()
        if not self.token:
            raise ValueError("OAuth-токен не указан")
        if probe_bytes <= 0:
            raise ValueError("Объём проверки должен быть больше нуля")
        self.probe_bytes = probe_bytes
        self.stop = stop or threading.Event()
        self._thread_local = threading.local()
        self._limiter = AdaptiveLimiter(workers, self.stop)

    def _check_stop(self) -> None:
        if self.stop.is_set():
            raise ScanStopped()

    def _sleep(self, seconds: float) -> None:
        if self.stop.wait(seconds):
            raise ScanStopped()

    @staticmethod
    def _delay(attempt: int, retry_after: str | None = None) -> float:
        if retry_after:
            try:
                delay = float(retry_after)
            except ValueError:
                try:
                    date = parsedate_to_datetime(retry_after)
                    if date.tzinfo is None:
                        date = date.replace(tzinfo=timezone.utc)
                    return max(0.0, (date - datetime.now(timezone.utc)).total_seconds())
                except (ValueError, TypeError, OverflowError):
                    pass
            else:
                if math.isfinite(delay):
                    return max(0.0, delay)
        delay = min(12.0, 0.75 * (2 ** attempt))
        return random.uniform(delay / 2, delay)

    @contextmanager
    def _get(self, api: bool, url: str, attempt: int, **kwargs):
        generation = self._limiter.acquire()
        try:
            with self._session(api=api).get(url, **kwargs) as response:
                self._thread_local.http_code = response.status_code
                delay = self._delay(attempt, response.headers.get("Retry-After")) if response.status_code == 429 else 0.0
                self._limiter.feedback(response.status_code, delay, generation)
                yield response
        except requests.RequestException:
            self._limiter.feedback(0, 0.0, generation)
            raise
        finally:
            self._limiter.release()

    def _session(self, api: bool) -> requests.Session:
        # requests.Session is not guaranteed to be thread-safe.
        # Each worker owns independent API and CDN sessions.
        key = "api_session" if api else "cdn_session"
        session = getattr(self._thread_local, key, None)
        if session is None:
            session = requests.Session()
            session.headers.update({"User-Agent": "YaDiskDownloadCheck/1.0"})
            if api:
                session.headers.update({
                    "Authorization": f"OAuth {self.token}",
                    "Accept": "application/json",
                })
            setattr(self._thread_local, key, session)
        return session

    @staticmethod
    def _api_error(response: requests.Response) -> DiskAPIError:
        if response.status_code == 401:
            message = "Неверный или просроченный OAuth-токен"
        elif response.status_code == 403:
            message = "Недостаточно прав для чтения Диска"
        elif response.status_code == 429:
            message = "Превышен лимит запросов API"
        else:
            try:
                details = response.json()
                if isinstance(details, dict):
                    message = str(
                        details.get("message")
                        or details.get("description")
                        or "Ошибка сервера"
                    )[:160]
                else:
                    message = "Ошибка сервера"
            except (ValueError, requests.RequestException):
                message = "Ошибка сервера"
        return DiskAPIError(response.status_code, message)

    def _api_get(self, route: str, params: dict) -> dict:
        url = f"{API_ROOT}{route}"
        for attempt in range(MAX_ATTEMPTS):
            self._check_stop()
            delay = None
            try:
                with self._get(
                    True, url, attempt, params=params, timeout=API_TIMEOUT
                ) as response:
                    if response.status_code in RETRYABLE_CODES and attempt < MAX_ATTEMPTS - 1:
                        if response.status_code != 429:
                            delay = self._delay(attempt, response.headers.get("Retry-After"))
                    elif not response.ok:
                        raise self._api_error(response)
                    else:
                        try:
                            result = response.json()
                        except ValueError as error:
                            raise DiskAPIError(response.status_code, "API вернул не JSON") from error
                        if not isinstance(result, dict):
                            raise DiskAPIError(response.status_code, "Неверный формат ответа API")
                        return result
            except (requests.Timeout, requests.ConnectionError) as error:
                if attempt == MAX_ATTEMPTS - 1:
                    raise DiskAPIError(None, "Сеть или API недоступны") from error
                delay = self._delay(attempt)
            if delay is not None:
                self._sleep(delay)
        raise DiskAPIError(None, "Не удалось получить ответ API")

    def iter_files(self) -> Iterator[DiskFile]:
        """Iterate the flat, paginated list; excludes trash and directories."""
        offset = 0
        seen: set[str] = set()
        while True:
            self._check_stop()
            data = self._api_get(
                "/resources/files", {"limit": PAGE_SIZE, "offset": offset}
            )
            items = data.get("items")
            if not isinstance(items, list):
                raise DiskAPIError(None, "API не вернул список items")
            if not items:
                return

            for item in items:
                if not isinstance(item, dict):
                    continue
                path = item.get("path")
                if not isinstance(path, str) or not path or path in seen:
                    continue
                seen.add(path)
                size = item.get("size")
                if not isinstance(size, int) or size < 0:
                    size = None
                name = item.get("name")
                if not isinstance(name, str) or not name:
                    name = path.rpartition("/")[2] or path
                yield DiskFile(name=name, path=path, size=size)

            offset += len(items)
            total = data.get("total")
            if isinstance(total, int):
                if offset >= total:
                    return
            elif len(items) < PAGE_SIZE:
                return

    @staticmethod
    def _safe_download_url(href: object) -> bool:
        if not isinstance(href, str):
            return False
        try:
            url = urlsplit(href)
        except ValueError:
            return False
        return url.scheme == "https" and bool(url.hostname) and not url.username and not url.password

    def download_url(self, file: DiskFile) -> str:
        self._check_stop()
        data = self._api_get("/resources/download", {"path": file.path})
        self._check_stop()
        href = data.get("href")
        if not self._safe_download_url(href):
            raise ValueError("API не выдал корректную HTTPS-ссылку")
        return href

    def _read_probe(self, response: requests.Response, size: int | None) -> tuple[CheckResult, bool]:
        """Validate the response and discard each bounded block after counting it."""
        code = response.status_code
        received = 0
        expected = min(size, self.probe_bytes) if size is not None else None

        def result(ok: bool | None, note: str, retry: bool = False) -> tuple[CheckResult, bool]:
            return CheckResult(ok, note, code,
                               bytes_requested=expected if expected is not None else self.probe_bytes,
                               bytes_received=received, range_supported=code == 206), retry

        host = (urlsplit(response.url).hostname or "").lower()
        if host in ("oauth.yandex.ru", "passport.yandex.ru"):
            return result(False, "Перенаправление на вход в аккаунт")
        if response.headers.get("Content-Encoding", "identity").strip().lower() != "identity":
            return result(None, "Сервер вернул сжатое содержимое вместо identity")
        length = response.headers.get("Content-Length")
        if length is not None:
            try:
                if not re.fullmatch(r"[0-9]+", length.strip()):
                    raise ValueError()
                length = int(length)
            except ValueError:
                return result(None, "Некорректный Content-Length")

        if code == 206:
            match = re.fullmatch(r"bytes ([0-9]+)-([0-9]+)/([0-9]+|\*)",
                                 response.headers.get("Content-Range", "").strip(), re.IGNORECASE)
            if not match:
                return result(None, "Отсутствует или некорректен Content-Range")
            try:
                start, end = int(match[1]), int(match[2])
                total = None if match[3] == "*" else int(match[3])
            except ValueError:
                return result(None, "Некорректный Content-Range")
            if start != 0 or end >= self.probe_bytes or (total is not None and end >= total):
                return result(None, "Некорректные границы Content-Range")
            if size is not None and total is not None and total != size:
                return result(None, "Размер Content-Range не совпадает с размером из SQLite")
            if expected is None:
                expected = min(total, self.probe_bytes) if total is not None else self.probe_bytes
            if end + 1 != expected or (length is not None and length != expected):
                return result(None, "Content-Range или Content-Length не подтверждает нужный объём")
        else:
            if "Content-Range" in response.headers:
                return result(None, "Неожиданный Content-Range при HTTP 200")
            if length is not None:
                if size is not None and length != size:
                    return result(None, "Content-Length не совпадает с размером из SQLite")
                expected = min(length, self.probe_bytes)

        # Without Content-Length, a short body needs a clean EOF.
        need_eof = length is None and (expected is None or expected < self.probe_bytes)
        target = self.probe_bytes if expected is None else expected
        try:
            while True:
                self._check_stop()
                remaining = target - received
                if remaining == 0 and target > 0 and not need_eof:
                    return result(True, "Требуемый объём данных получен")
                # One byte detects a nonempty 'empty file' or excess data before EOF.
                amount = min(READ_CHUNK_SIZE, remaining if remaining > 0 else 1)
                chunk = response.raw.read(amount, decode_content=False)
                count = len(chunk)
                received += count
                del chunk  # Retain no payload between reads.
                self._check_stop()
                if received > target:
                    return result(None, "Получено больше байтов, чем ожидается")
                if received == target and target == self.probe_bytes:
                    return result(True, "Требуемый объём данных получен")
                # raw.read() can return less than requested; only an empty read is EOF.
                if count == 0:
                    if expected is None:
                        return result(None, "Недостаточно данных: размер файла неизвестен, длина не указана")
                    if received != expected:
                        return result(None, "Преждевременное завершение передачи")
                    return result(True, "Содержимое файла получено полностью")
        except ScanStopped:
            return result(None, "Остановлено")
        except (IncompleteRead, requests.exceptions.ChunkedEncodingError) as error:
            if isinstance(error, IncompleteRead) and isinstance(error.partial, bytes):
                received += len(error.partial)
            return result(None, "Преждевременное завершение передачи")
        except ProtocolError as error:
            for detail in error.args:
                if isinstance(detail, IncompleteRead):
                    if isinstance(detail.partial, bytes):
                        received += len(detail.partial)
                    return result(None, "Преждевременное завершение передачи")
            return result(False, "Ошибка сети при чтении", retry=True)
        except (requests.RequestException, StreamError, OSError):
            return result(False, "Тайм-аут или ошибка сети при чтении", retry=True)

    def _probe_cdn(self, href: str, size: int | None, attempt: int = 0) -> tuple[CheckResult, str | None, bool]:
        headers = {"Range": f"bytes=0-{self.probe_bytes - 1}", "Accept-Encoding": "identity"}
        for ranged in (True, False):
            with self._get(
                False, href, attempt,  # No OAuth token sent to the CDN.
                headers=headers, stream=True, allow_redirects=True, timeout=CDN_TIMEOUT,
            ) as response:
                code = response.status_code
                retry_after = response.headers.get("Retry-After")
                if code in (200, 206):
                    result, retry = self._read_probe(response, size)
                    return result, retry_after, retry
                empty_range = re.fullmatch(r"bytes \*/0", response.headers.get("Content-Range", "").strip(),
                                           re.IGNORECASE)
                if code != 416 or not ranged or size not in (0, None) or not empty_range:
                    break
                size = 0
            # Close the 416 response and release its permit before retrying.
            headers = {"Accept-Encoding": "identity"}
        if code == 429:
            return CheckResult(None, "HTTP 429 — проверка временно не подтверждена", code), retry_after, True
        if code == 416:
            return CheckResult(None, "HTTP 416 — диапазон или размер файла не подтверждён", code), retry_after, False
        ok = False if 400 <= code < 500 and code != 408 else None
        return CheckResult(ok, f"HTTP {code} — выдача файла не подтверждена", code), retry_after, code in RETRYABLE_CODES

    def check_file(self, file: DiskFile) -> CheckResult:
        """Generate a fresh download URL and probe it, with bounded retries."""
        started = time.monotonic()
        self._thread_local.http_code = None
        result = CheckResult(None, "Остановлено")
        for attempt in range(MAX_ATTEMPTS):
            retry_after = None
            try:
                self._check_stop()
                href = self.download_url(file)
                result, retry_after, retry = self._probe_cdn(href, file.size, attempt)
                if self.stop.is_set() or attempt == MAX_ATTEMPTS - 1 or not retry:
                    break
                if result.http_code == 429:
                    continue  # The shared limiter applies the retry delay.
            except ScanStopped:
                result = replace(result, ok=None, note="Остановлено")
                break
            except ValueError:
                result = CheckResult(None, "API не выдал корректную HTTPS-ссылку")
                break
            except DiskAPIError as error:
                if error.code == 429:
                    result = CheckResult(None, "HTTP 429 — проверка временно не подтверждена", 429)
                elif error.code is None:
                    result = CheckResult(False, "Сеть или API недоступны")
                else:
                    # Server messages can contain secrets; persist only our own text.
                    ok = False if 400 <= error.code < 500 and error.code not in (408, 416) else None
                    result = CheckResult(ok, f"API HTTP {error.code} — не удалось получить ссылку", error.code)
                break
            except (requests.RequestException, StreamError):
                # Do not print the exception: it may contain the signed CDN URL.
                result = CheckResult(False, "Тайм-аут или ошибка сети")
            if attempt == MAX_ATTEMPTS - 1:
                break
            try:
                self._sleep(self._delay(attempt, retry_after))
            except ScanStopped:
                result = replace(result, ok=None, note="Остановлено")
                break
        if self.stop.is_set() and result.note in NETWORK_FAILURE_NOTES:
            result = replace(result, ok=None, note="Остановлено")
        requested = min(file.size, self.probe_bytes) if file.size is not None else self.probe_bytes
        return replace(result,
                       http_code=self._thread_local.http_code if self._thread_local.http_code is not None else result.http_code,
                       bytes_requested=result.bytes_requested if result.bytes_requested is not None else requested,
                       bytes_received=result.bytes_received or 0,
                       duration_ms=round((time.monotonic() - started) * 1000),
                       failure_reason=result.note if result.ok is not True else None)


def prepare_download(token: str, file: DiskFile, events: "queue.Queue[tuple[str, object]]",
                     stop: threading.Event) -> None:
    href, problem = None, None
    try:
        href = DiskProbe(token, stop).download_url(file)
    except ScanStopped:
        pass
    except DiskAPIError as error:
        problem = ("Сеть или API недоступны" if error.code is None else
                   f"HTTP {error.code} — не удалось получить ссылку для скачивания")
    except ValueError:
        problem = "API не выдал корректную HTTPS-ссылку"
    except (requests.RequestException, StreamError, OSError):
        problem = "Тайм-аут или ошибка сети"
    except Exception:
        problem = "Не удалось получить ссылку для скачивания"
    events.put(("download", (href, problem)))


def load_files(
    token: str,
    events: "queue.Queue[tuple[str, object]]",
    stop: threading.Event,
    history_path: str | Path = HISTORY_PATH,
) -> None:
    """Load metadata only; merge into the saved list after a complete listing."""
    problem = None
    try:
        client = DiskProbe(token, stop)
        files = []
        for file in client.iter_files():
            files.append(file)
            events.put(("file", file))
        client._check_stop()
        with HistoryStore(history_path) as history:
            history.merge_files(files)
    except ScanStopped:
        pass
    except DiskAPIError as error:
        problem = f"Список файлов: HTTP {error.code or '—'} — {error.message}"
    except (sqlite3.Error, OSError):
        problem = "Не удалось сохранить список файлов"
    except Exception:
        problem = "Не удалось получить список файлов"
    events.put(("finished", problem))


def scan(
    token: str,
    files: list[DiskFile],
    events: "queue.Queue[tuple[str, object]]",
    stop: threading.Event,
    workers: int = DEFAULT_WORKERS,
    history_path: str | Path = HISTORY_PATH,
    probe_bytes: int = DEFAULT_PROBE_BYTES,
    mode: str = "full",
    total_candidates: int | None = None,
) -> None:
    """Check the supplied files; commit each result before notifying the UI."""
    problem = None
    try:
        with check_operation(history_path), HistoryStore(history_path) as history:
            files = list({file.path: file for file in files}.values())
            run_id = history.start_run(mode, len(files) if total_candidates is None else total_candidates, len(files))
            try:
                if files and not stop.is_set():
                    client = DiskProbe(token, stop, workers, probe_bytes)
                    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="disk-probe") as executor:
                        running = {}
                        remaining = iter(files)
                        try:
                            while True:
                                while len(running) < workers and not stop.is_set():
                                    file = next(remaining, None)
                                    if file is None:
                                        break
                                    running[executor.submit(client.check_file, file)] = file
                                if not running:
                                    break
                                done, _ = wait(running, return_when=FIRST_COMPLETED)
                                for future in done:
                                    file = running.pop(future)
                                    if future.cancelled():
                                        continue
                                    try:
                                        result = future.result()
                                    except Exception:
                                        problem = "Непредвиденная ошибка проверки"
                                        result = CheckResult(None, problem, failure_reason=problem)
                                    entry = history.record(file.path, result, run_id)
                                    events.put(("result", entry))
                                if stop.is_set():
                                    for future in running:
                                        future.cancel()
                        except Exception:
                            stop.set()
                            for future in running:
                                future.cancel()
                            raise
            except (sqlite3.Error, OSError):
                problem = "Не удалось сохранить историю проверок"
            except Exception:
                problem = "Не удалось выполнить проверку"
            status = "error" if problem else "stopped" if stop.is_set() else "completed"
            events.put(("run", history.finish_run(run_id, status)))
    except CheckBusy:
        problem = "Другая проверка уже выполняется"
    except (sqlite3.Error, OSError):
        problem = "Не удалось сохранить историю проверок"
    except Exception:
        problem = "Не удалось выполнить проверку"
    events.put(("finished", problem))


def readable_size(size: int | None) -> str:
    if size is None:
        return "—"
    value = float(size)
    for unit in ("Б", "КБ", "МБ", "ГБ", "ТБ", "ПБ"):
        if value < 1024 or unit == "ПБ":
            return f"{int(value)} {unit}" if unit == "Б" else f"{value:.1f} {unit}"
        value /= 1024
    return str(size)


def csv_safe(value: str) -> str:
    """Avoid spreadsheet formula execution for unusual user file names."""
    if value and value[0] in "=+-@\t\r":
        return "'" + value
    return value


def run_cli(
    token: str,
    workers: int,
    command: str = "list",
    paths: list[str] | None = None,
    history_path: str | Path = HISTORY_PATH,
    probe_bytes: int = DEFAULT_PROBE_BYTES,
    mode: str | None = None,
    sample_percent: float | None = None,
    sample_count: int | None = None,
    seed: int | None = None,
) -> int:
    paths = paths or []
    try:
        with HistoryStore(history_path) as history:
            if command == "history":
                for run in history.runs():
                    print(run_report(run))
                for row in history_rows(history.entries(paths)):
                    print("\t".join(str(value) for value in row))
                return 0
            if command == "check":
                selection = history.select_files(mode, sample_percent, sample_count, seed, paths)
                counts = history.catalog_counts()
                files = selection.files
            else:
                files = []
    except (sqlite3.Error, OSError):
        print("Не удалось открыть историю проверок.", file=sys.stderr)
        return 2
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2
    if command == "check" and not counts[0]:
        print("Сначала загрузите список: --cli list", file=sys.stderr)
        return 2
    events: queue.Queue[tuple[str, object]] = queue.Queue()
    stop = threading.Event()
    if command == "list":
        target, arguments = load_files, (token, events, stop, history_path)
    else:
        print(f"Всего в БД: {counts[0]}; ранее доступных: {counts[1]}; ранее недоступных: {counts[2]}; "
              f"без достоверного результата: {counts[3]}; выбрано: {len(files)}", flush=True)
        target, arguments = scan, (token, files, events, stop, workers, history_path, probe_bytes,
                                   selection.mode, selection.total_candidates)
    thread = threading.Thread(target=target, args=arguments)
    thread.start()
    results: dict[str, CheckResult] = {}
    found = 0
    error = None
    report = None
    while True:
        try:
            kind, payload = events.get()
            if kind == "file":
                found += 1
                print(payload.path, flush=True)
            elif kind == "result":
                result = payload.result
                results[payload.path] = result
                bubble = "\033[32m●\033[0m" if result.ok is True else (
                    "\033[31m●\033[0m" if result.ok is False else "\033[90m●\033[0m"
                )
                if not sys.stdout.isatty():
                    bubble = "[OK]" if result.ok is True else "[FAIL]" if result.ok is False else (
                        "[STOP]" if result.note == "Остановлено" else "[UNKNOWN]"
                    )
                received = result.bytes_received if result.bytes_received is not None else "—"
                requested = result.bytes_requested if result.bytes_requested is not None else "—"
                duration = result.duration_ms if result.duration_ms is not None else "—"
                code = result.http_code if result.http_code is not None else "—"
                print(f"{bubble} {payload.path} — {result.failure_reason or result.note}; "
                      f"получено: {received} / {requested} байт; время: {duration} мс; HTTP: {code}", flush=True)
            elif kind == "run":
                report = payload
            elif kind == "finished":
                error = payload
                break
        except KeyboardInterrupt:
            stop.set()
    thread.join(timeout=1)
    ok = sum(1 for result in results.values() if result.ok is True)
    failed = sum(1 for result in results.values() if result.ok is False)
    if report is not None:
        print(run_report(report))
    if error:
        print(error, file=sys.stderr)
        return 2
    if command == "list":
        print(f"Список загружен: {found}. Проверка: --cli check")
    elif report is None:
        unknown = sum(result.ok is None for result in results.values())
        print(f"Выбрано: {len(files)}; доступно: {ok}; ошибки: {failed}; не определено: {unknown}")
    return 1 if failed else 0


def run_gui(workers: int, history_path: str | Path = HISTORY_PATH,
            probe_bytes: int = DEFAULT_PROBE_BYTES) -> int:
    try:
        import tkinter as tk
        from tkinter import filedialog, font, messagebox, ttk
    except ImportError:
        print("Tkinter отсутствует. Установите python3-tk или используйте --cli.", file=sys.stderr)
        return 2

    class DiskCheckWindow:
        def __init__(self, root: "tk.Tk") -> None:
            self.root = root
            root.title("Яндекс Диск — проверка доступности файлов")
            root.geometry("1150x690")
            root.minsize(780, 420)

            self.token_var = tk.StringVar(value=os.environ.get("YANDEX_DISK_TOKEN", ""))
            self.overview_var = tk.StringVar(value="Найдено: 0    Доступно: 0    Ошибки: 0    Ожидают: 0")
            self.state_var = tk.StringVar(value="Введите OAuth-токен с правом cloud_api:disk.read")
            self.mode_var = tk.StringVar(value=MODE_LABELS["full"])
            self.sample_unit_var = tk.StringVar(value="Процент")
            self.sample_var = tk.StringVar(value="10")
            self.preview_var = tk.StringVar()
            self.catalog_counts = (0, 0, 0, 0)
            self.last_run: CheckRun | None = None
            self.events: queue.Queue[tuple[str, object]] = queue.Queue()
            self.stop_event = threading.Event()
            self.worker: threading.Thread | None = None
            self.files: dict[str, DiskFile] = {}
            self.results: dict[str, CheckResult] = {}
            self.checked_at: dict[str, str] = {}
            self.ids: dict[str, str] = {}
            self.running = False
            self.closing = False
            self.operation = ""

            frame = ttk.Frame(root, padding=(14, 12))
            frame.pack(fill="both", expand=True)
            toolbar = ttk.Frame(frame)
            toolbar.pack(fill="x")
            self.token_link_font = font.Font(root=root, font="TkDefaultFont")
            self.token_link_font.configure(underline=True)
            token_link = ttk.Label(toolbar, text="OAuth-токен:", foreground="#2563EB",
                                   cursor="hand2", font=self.token_link_font)
            token_link.pack(side="left")
            token_link.bind("<Button-1>", lambda _event: webbrowser.open_new_tab(
                "https://yandex.ru/dev/disk/poligon#access_token"))
            self.token_entry = ttk.Entry(toolbar, textvariable=self.token_var, show="•", width=43)
            self.token_entry.pack(side="left", padx=(9, 10), fill="x", expand=True)
            self.load_btn = ttk.Button(toolbar, text="Загрузить список", command=self.load)
            self.load_btn.pack(side="left", padx=(0, 6))
            self.start_btn = ttk.Button(toolbar, text="Проверить", command=self.start, state="disabled")
            self.start_btn.pack(side="left", padx=(0, 6))
            self.stop_btn = ttk.Button(toolbar, text="Остановить", command=self.cancel, state="disabled")
            self.stop_btn.pack(side="left", padx=(0, 6))
            actions = ttk.Frame(frame)
            actions.pack(fill="x", pady=(8, 0))
            self.history_btn = ttk.Button(actions, text="История", command=self.show_history)
            self.history_btn.pack(side="left", padx=(0, 6))
            self.runs_btn = ttk.Button(actions, text="Запуски", command=self.show_runs)
            self.runs_btn.pack(side="left", padx=(0, 6))
            self.export_btn = ttk.Button(actions, text="CSV…", command=self.save_csv, state="disabled")
            self.export_btn.pack(side="left", padx=(0, 6))
            self.manual_btn = ttk.Button(actions, text="Проверить выбранные",
                                         command=lambda: self.start(manual=True), state="disabled")
            self.manual_btn.pack(side="left", padx=(0, 6))
            ttk.Button(actions, text="Снять выбор", command=self.clear_selection).pack(side="left")
            self.selection_var = tk.StringVar(value="Строки не выбраны")
            ttk.Label(actions, textvariable=self.selection_var).pack(side="left", padx=10)

            modes = ttk.Frame(frame)
            modes.pack(fill="x", pady=(8, 0))
            ttk.Label(modes, text="Режим:").pack(side="left")
            self.mode_combo = ttk.Combobox(modes, textvariable=self.mode_var, state="readonly", width=33,
                values=tuple(MODE_LABELS[mode] for mode in ("failed", "random", "full")))
            self.mode_combo.pack(side="left", padx=(6, 12))
            self.sample_unit_combo = ttk.Combobox(modes, textvariable=self.sample_unit_var,
                values=("Процент", "Количество"), width=12, state="disabled")
            self.sample_unit_combo.pack(side="left", padx=(0, 6))
            self.sample_entry = ttk.Entry(modes, textvariable=self.sample_var, width=8, state="disabled")
            self.sample_entry.pack(side="left")

            summary = ttk.Frame(frame)
            summary.pack(fill="x", pady=(13, 9))
            ttk.Label(summary, textvariable=self.preview_var, wraplength=1040).pack(anchor="w")
            ttk.Label(summary, textvariable=self.overview_var).pack(anchor="w")
            ttk.Label(summary, textvariable=self.state_var, wraplength=980).pack(anchor="w", pady=(4, 0))

            table_frame = ttk.Frame(frame)
            table_frame.pack(fill="both", expand=True)
            self.icons = {
                "pending": self.make_bubble("#9CA3AF", "#77818F"),
                "good": self.make_bubble("#21B15A", "#14783C"),
                "bad": self.make_bubble("#EB4747", "#B91C1C"),
            }
            self.tree = ttk.Treeview(
                table_frame,
                columns=("name", "folder", "size", "checked", "received", "duration", "http", "note"),
                displaycolumns=("name", "folder", "size", "checked", "duration"),
                show="tree headings",
                selectmode="extended",
            )
            self.tree.heading("#0", text="●")
            self.tree.column("#0", width=45, minwidth=45, stretch=False, anchor="center")
            for column, heading, width, anchor in (
                ("name", "Файл", 260, "w"),
                ("folder", "Папка", 310, "w"),
                ("size", "Размер", 100, "e"),
                ("checked", "Последняя проверка", 155, "w"),
                ("received", "Получено / нужно, байт", 170, "e"),
                ("duration", "Время, мс", 90, "e"),
                ("http", "HTTP", 65, "e"),
                ("note", "Результат / причина ошибки", 310, "w"),
            ):
                self.tree.heading(column, text=heading)
                self.tree.column(column, width=width, minwidth=80, anchor=anchor)
            scroll_y = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
            scroll_x = ttk.Scrollbar(table_frame, orient="horizontal", command=self.tree.xview)
            self.tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
            self.tree.grid(row=0, column=0, sticky="nsew")
            self.tree.bind("<<TreeviewSelect>>", self.selection_changed)
            self.tree.bind("<Escape>", self.clear_selection)
            self.file_menu = tk.Menu(self.tree, tearoff=False)
            self.tree.bind("<Button-3>", self.show_download_menu)
            if sys.platform == "darwin":
                self.tree.bind("<Button-2>", self.show_download_menu)
                self.tree.bind("<Control-Button-1>", self.show_download_menu)
            scroll_y.grid(row=0, column=1, sticky="ns")
            scroll_x.grid(row=1, column=0, sticky="ew")
            table_frame.rowconfigure(0, weight=1)
            table_frame.columnconfigure(0, weight=1)

            for variable in (self.mode_var, self.sample_unit_var, self.sample_var):
                variable.trace_add("write", self.preview_selection)
            self.restore_catalog()
            self.root.protocol("WM_DELETE_WINDOW", self.close)
            self.root.after(75, self.consume_events)

        @staticmethod
        def make_bubble(fill: str, border: str) -> "tk.PhotoImage":
            img = tk.PhotoImage(width=18, height=18)
            for y in range(18):
                for x in range(18):
                    dist_sq = (x - 8.5) ** 2 + (y - 8.5) ** 2
                    if dist_sq <= 7.25 ** 2:
                        img.put(fill if dist_sq < 6.1 ** 2 else border, (x, y))
            return img

        def get_token(self) -> str:
            token = self.token_var.get().strip()
            if not token:
                messagebox.showerror("Нет токена", "Укажите OAuth-токен с правом чтения Диска.")
            return token

        def clear_selection(self, _event=None) -> None:
            self.tree.selection_remove(*self.tree.selection())

        def selection_changed(self, _event=None) -> None:
            count = len(self.tree.selection())
            self.selection_var.set(f"Выбрано строк: {count}" if count else "Строки не выбраны")
            self.manual_btn.configure(state="normal" if count and not self.running else "disabled")

        def show_download_menu(self, event) -> str:
            iid = self.tree.identify_row(event.y)
            if not iid or self.running:
                return "break"
            file = next(self.files[path] for path, row in self.ids.items() if row == iid)
            self.file_menu.delete(0, "end")
            self.file_menu.add_command(label="Скачать", command=lambda: self.download(file))
            try:
                self.file_menu.tk_popup(event.x_root, event.y_root)
            finally:
                self.file_menu.grab_release()
            return "break"

        def download(self, file: DiskFile) -> None:
            if self.running:
                return
            token = self.get_token()
            if not token:
                return
            self.state_var.set(f"Получение ссылки для скачивания: {file.name}…")
            self.launch("download", token, file=file)

        def selected_mode(self) -> str:
            return next(mode for mode, label in MODE_LABELS.items() if label == self.mode_var.get())

        def sample_options(self) -> dict:
            if self.selected_mode() != "random":
                return {}
            try:
                if self.sample_unit_var.get() == "Количество":
                    options = {"sample_count": int(self.sample_var.get())}
                else:
                    options = {"sample_percent": float(self.sample_var.get())}
            except ValueError:
                raise ValueError("Укажите число для объёма случайной выборки") from None
            sample_size(self.catalog_counts[1], options.get("sample_percent"), options.get("sample_count"))
            return options

        def preview_selection(self, *_args) -> None:
            total, good, failed, unknown = self.catalog_counts
            mode = self.selected_mode()
            valid = True
            try:
                options = self.sample_options()
                selected = (sample_size(good, options.get("sample_percent"), options.get("sample_count"))
                            if mode == "random" else failed if mode == "failed" else total)
            except ValueError:
                selected = "— (проверьте объём выборки)"
                valid = False
            self.preview_var.set(f"Всего в БД: {total}    Ранее доступных: {good}    Ранее недоступных: {failed}    "
                                 f"Без достоверного результата: {unknown}    Выбрано для проверки: {selected}")
            if not self.running:
                self.start_btn.configure(state="normal" if self.files and valid else "disabled")
                self.mode_combo.configure(state="readonly")
                self.sample_unit_combo.configure(state="readonly" if mode == "random" else "disabled")
                self.sample_entry.configure(state="normal" if mode == "random" else "disabled")

        def selected_paths(self) -> list[str]:
            selected = set(self.tree.selection())
            return [path for path, iid in self.ids.items() if iid in selected]

        def add_file(self, file: DiskFile) -> None:
            self.files[file.path] = file
            self.ids[file.path] = self.tree.insert(
                "", "end", image=self.icons["pending"],
                values=(file.name, file.folder, readable_size(file.size), "—", "—", "—", "—", "Не проверено"),
            )

        def display_result(self, entry: HistoryEntry) -> None:
            iid = self.ids.get(entry.path)
            if iid is None:
                return
            result = entry.result
            self.results[entry.path] = result
            self.checked_at[entry.path] = entry.checked_at
            icon = "good" if result.ok is True else "bad" if result.ok is False else "pending"
            self.tree.item(iid, image=self.icons[icon])
            self.tree.set(iid, "note", result.failure_reason or result.note)
            self.tree.set(iid, "checked", local_time(entry.checked_at))
            received = result.bytes_received if result.bytes_received is not None else "—"
            requested = result.bytes_requested if result.bytes_requested is not None else "—"
            self.tree.set(iid, "received", f"{received} / {requested}")
            self.tree.set(iid, "duration", result.duration_ms if result.duration_ms is not None else "—")
            self.tree.set(iid, "http", result.http_code if result.http_code is not None else "—")

        def clear_files(self) -> None:
            self.files.clear()
            self.results.clear()
            self.checked_at.clear()
            self.ids.clear()
            self.tree.delete(*self.tree.get_children())

        def restore_catalog(self) -> None:
            with HistoryStore(history_path) as history:
                files = history.files()
                entries = history.entries(latest=True)
                self.catalog_counts = history.catalog_counts()
            self.clear_files()
            for file in files:
                self.add_file(file)
            for entry in entries:
                self.display_result(entry)
            self.start_btn.configure(state="normal" if files else "disabled")
            self.export_btn.configure(state="normal" if files else "disabled")
            self.update_counts()
            if files:
                self.state_var.set("Сохранённый список загружен. Обновите список или запустите проверку.")

        def launch(self, operation: str, token: str, selection: CheckSelection | None = None,
                   file: DiskFile | None = None) -> None:
            self.operation = operation
            self.stop_event = threading.Event()
            self.running = True
            self.last_run = None
            self.load_btn.configure(state="disabled")
            self.start_btn.configure(state="disabled")
            self.stop_btn.configure(state="normal")
            self.export_btn.configure(state="disabled")
            self.history_btn.configure(state="disabled")
            self.runs_btn.configure(state="disabled")
            self.manual_btn.configure(state="disabled")
            self.mode_combo.configure(state="disabled")
            self.sample_unit_combo.configure(state="disabled")
            self.sample_entry.configure(state="disabled")
            self.token_entry.configure(state="disabled")
            if operation == "load":
                target, arguments = load_files, (token, self.events, self.stop_event, history_path)
            elif operation == "download":
                target, arguments = prepare_download, (token, file, self.events, self.stop_event)
            else:
                target, arguments = scan, (token, selection.files, self.events, self.stop_event, workers,
                                           history_path, probe_bytes, selection.mode, selection.total_candidates)
            self.worker = threading.Thread(
                target=target,
                args=arguments,
            )
            self.worker.start()

        def load(self) -> None:
            if self.running:
                return
            token = self.get_token()
            if not token:
                return
            self.clear_files()
            self.update_counts()
            self.state_var.set("Получение полного списка файлов…")
            self.launch("load", token)

        def start(self, manual: bool = False) -> None:
            if self.running or not self.files:
                return
            if manual and not self.selected_paths():
                return
            try:
                with HistoryStore(history_path) as history:
                    selection = (history.select_files(paths=self.selected_paths()) if manual else
                                 history.select_files(self.selected_mode(), **self.sample_options()))
                    self.catalog_counts = history.catalog_counts()
            except (ValueError, sqlite3.Error, OSError) as error:
                messagebox.showerror("Проверка", str(error))
                return
            files = selection.files
            token = self.get_token() if files else self.token_var.get().strip()
            if files and not token:
                return
            for file in files:
                self.results.pop(file.path, None)
                self.tree.item(self.ids[file.path], image=self.icons["pending"])
                self.tree.set(self.ids[file.path], "note", "Ожидает проверки")
                self.tree.set(self.ids[file.path], "received", "—")
                self.tree.set(self.ids[file.path], "duration", "—")
                self.tree.set(self.ids[file.path], "http", "—")
            self.update_counts()
            self.state_var.set(f"Проверка файлов: {len(files)}…")
            self.launch("check", token, selection)

        def cancel(self) -> None:
            if self.running:
                self.stop_event.set()
                self.stop_btn.configure(state="disabled")
                self.state_var.set("Отмена скачивания…" if self.operation == "download" else "Остановка проверок…")

        def update_counts(self) -> None:
            ok = sum(result.ok is True for result in self.results.values())
            fail = sum(result.ok is False for result in self.results.values())
            self.overview_var.set(
                f"Найдено: {len(self.files)}    Доступно: {ok}    "
                f"Ошибки: {fail}    Не проверено: {len(self.files) - ok - fail}"
            )
            self.preview_selection()

        def consume_events(self) -> None:
            changed = False
            for _ in range(350):
                try:
                    kind, payload = self.events.get_nowait()
                except queue.Empty:
                    break
                if kind == "download":
                    href, problem = payload
                    if self.closing or self.stop_event.is_set():
                        problem = None
                    elif not problem:
                        try:
                            if not webbrowser.open_new_tab(href):
                                problem = "Не удалось открыть браузер для скачивания"
                        except (webbrowser.Error, OSError):
                            problem = "Не удалось открыть браузер для скачивания"
                    kind, payload = "finished", problem
                if kind == "file":
                    file = payload
                    if file.path in self.ids:
                        continue
                    self.add_file(file)
                    changed = True
                elif kind == "result":
                    self.display_result(payload)
                    changed = True
                elif kind == "run":
                    self.last_run = payload
                elif kind == "finished":
                    self.running = False
                    try:
                        if self.operation == "load":
                            self.restore_catalog()
                        elif self.operation == "check":
                            with HistoryStore(history_path) as history:
                                entries = history.entries(latest=True)
                                self.catalog_counts = history.catalog_counts()
                            for entry in entries:
                                self.display_result(entry)
                            for path, iid in self.ids.items():
                                if path not in self.results:
                                    self.tree.set(iid, "note", "Не проверено")
                    except (sqlite3.Error, OSError):
                        if self.operation == "load":
                            self.clear_files()
                        payload = "Не удалось прочитать сохранённые результаты"
                    self.load_btn.configure(state="normal")
                    self.start_btn.configure(state="normal" if self.files else "disabled")
                    self.stop_btn.configure(state="disabled")
                    self.export_btn.configure(state="normal" if self.files else "disabled")
                    self.history_btn.configure(state="normal")
                    self.runs_btn.configure(state="normal")
                    self.token_entry.configure(state="normal")
                    self.selection_changed()
                    if payload:
                        self.state_var.set(f"Ошибка: {payload}" +
                                           (f". {run_report(self.last_run)}" if self.last_run is not None else ""))
                        if not self.closing:
                            if self.operation == "download":
                                messagebox.showerror("Скачивание", str(payload))
                            else:
                                messagebox.showwarning("Проверка завершена с ошибкой", str(payload))
                    elif self.last_run is not None:
                        self.state_var.set(run_report(self.last_run))
                    elif self.operation == "download":
                        self.state_var.set("Скачивание отменено" if self.stop_event.is_set() else
                                           "Ссылка открыта в браузере")
                    elif self.stop_event.is_set():
                        self.state_var.set("Операция остановлена. Предыдущий полный список сохранён.")
                    elif self.operation == "load":
                        self.state_var.set("Полный список загружен. Выберите файлы или проверьте весь список.")
                    else:
                        self.state_var.set("Проверка завершена")
                    changed = True
                    if self.closing:
                        self.root.destroy()
                        return
            if changed:
                self.update_counts()
            self.root.after(75, self.consume_events)

        def show_history(self, run_id: int | None = None) -> None:
            try:
                with HistoryStore(history_path) as history:
                    entries = history.entries(self.selected_paths() if run_id is None else None, run_id=run_id)
            except (sqlite3.Error, OSError):
                messagebox.showerror("История", "Не удалось прочитать историю проверок.")
                return
            window = tk.Toplevel(self.root)
            window.title(f"Результаты запуска #{run_id}" if run_id is not None else
                         "История проверок — выбранные файлы или весь Диск")
            window.geometry("1150x520")
            ttk.Label(window, text="Сетевые сбои учитываются как ошибки проверки. Время указано в местном часовом поясе.").pack(
                anchor="w", padx=10, pady=8
            )
            frame = ttk.Frame(window)
            frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))
            columns = ("date", "path", "status", "change", "http", "note",
                       "requested", "received", "duration", "range", "failure", "run")
            tree = ttk.Treeview(frame, columns=columns, show="headings",
                                displaycolumns=("date", "path", "status", "change", "duration", "run"))
            tree.tag_configure("bad", foreground="#B91C1C")
            for column, heading, width in zip(
                columns, ("Дата и время", "Путь", "Результат", "Изменение", "HTTP", "Описание",
                          "Запрошено, байт", "Получено, байт", "Время, мс", "HTTP Range", "Причина ошибки", "Запуск"),
                (160, 340, 140, 175, 60, 300, 130, 120, 90, 100, 300, 70),
            ):
                tree.heading(column, text=heading)
                tree.column(column, width=width, minwidth=60)
            for row in history_rows(entries):
                tree.insert("", 0, values=row, tags=("bad",) if row[2] == "Ошибка" else ())
            vertical = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
            horizontal = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
            tree.configure(yscrollcommand=vertical.set, xscrollcommand=horizontal.set)
            tree.grid(row=0, column=0, sticky="nsew")
            vertical.grid(row=0, column=1, sticky="ns")
            horizontal.grid(row=1, column=0, sticky="ew")
            frame.rowconfigure(0, weight=1)
            frame.columnconfigure(0, weight=1)

        def show_runs(self) -> None:
            try:
                with HistoryStore(history_path) as history:
                    runs = history.runs()
            except (sqlite3.Error, OSError):
                messagebox.showerror("Запуски", "Не удалось прочитать историю запусков.")
                return
            window = tk.Toplevel(self.root)
            window.title("Запуски проверок")
            window.geometry("1150x420")
            ttk.Label(window, text="Двойной щелчок по запуску открывает его результаты.").pack(anchor="w", padx=10, pady=8)
            frame = ttk.Frame(window, padding=(10, 0, 10, 10))
            frame.pack(fill="both", expand=True)
            columns = ("id", "started", "finished", "mode", "candidates", "selected", "checked",
                       "success", "failed", "unknown", "duration", "status")
            tree = ttk.Treeview(frame, columns=columns, show="headings")
            for column, label in zip(columns, ("№", "Начало", "Завершение", "Режим", "Кандидатов", "Выбрано",
                    "Проверено", "Доступно", "Недоступно", "Неопределённо", "Длительность, с", "Статус")):
                tree.heading(column, text=label)
                tree.column(column, width=190 if column in ("started", "finished", "mode") else 110, minwidth=60)
            for run in reversed(runs):
                tree.insert("", "end", iid=str(run.id), values=(run.id, local_time(run.started_at),
                    local_time(run.finished_at) if run.finished_at else "—", MODE_LABELS.get(run.mode, run.mode),
                    run.total_candidates, run.selected_count, run.checked_count, run.success_count,
                    run.failed_count, run.unknown_count, f"{run.duration:.2f}", run.status or "выполняется"))
            tree.bind("<Double-1>", lambda _event: self.show_history(int(tree.selection()[0])) if tree.selection() else None)
            vertical = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
            horizontal = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
            tree.configure(yscrollcommand=vertical.set, xscrollcommand=horizontal.set)
            tree.grid(row=0, column=0, sticky="nsew")
            vertical.grid(row=0, column=1, sticky="ns")
            horizontal.grid(row=1, column=0, sticky="ew")
            frame.rowconfigure(0, weight=1)
            frame.columnconfigure(0, weight=1)

        def close(self) -> None:
            if self.running:
                self.closing = True
                self.cancel()
            else:
                self.root.destroy()

        def save_csv(self) -> None:
            if not self.files:
                return
            target = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV", "*.csv")],
                initialfile="yadisk_download_check.csv",
            )
            if not target:
                return
            try:
                with open(target, "w", newline="", encoding="utf-8-sig") as output:
                    writer = csv.writer(output, delimiter=";")
                    writer.writerow(("Имя", "Путь", "Размер, байт", "Доступно", "HTTP", "Описание", "Последняя проверка",
                                     "Запрошено, байт", "Получено, байт", "Время, мс", "HTTP Range", "Причина ошибки"))
                    for file in self.files.values():
                        result = self.results.get(file.path)
                        status = "Да" if result and result.ok is True else (
                            "Нет" if result and result.ok is False else (
                                "Не подтверждён" if result else "Не проверено"
                            )
                        )
                        writer.writerow((
                            csv_safe(file.name), csv_safe(file.path),
                            file.size if file.size is not None else "",
                            status, result.http_code if result and result.http_code else "",
                            csv_safe(result.note if result else "Ожидает проверки"),
                            local_time(self.checked_at[file.path]) if file.path in self.checked_at else "",
                            result.bytes_requested if result and result.bytes_requested is not None else "",
                            result.bytes_received if result and result.bytes_received is not None else "",
                            result.duration_ms if result and result.duration_ms is not None else "",
                            "" if not result or result.range_supported is None else "Да" if result.range_supported else "Нет",
                            csv_safe(result.failure_reason or "") if result else "",
                        ))
            except OSError as error:
                messagebox.showerror("Ошибка сохранения", str(error))

    try:
        root = tk.Tk()
    except tk.TclError as error:
        print(f"Не удалось открыть окно: {error}. Используйте --cli.", file=sys.stderr)
        return 2
    try:
        DiskCheckWindow(root)
    except (sqlite3.Error, OSError):
        print("Не удалось открыть историю проверок.", file=sys.stderr)
        root.destroy()
        return 2
    root.mainloop()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Проверка доступности файлов Яндекс Диска чтением начала файла без сохранения"
    )
    parser.add_argument("--cli", action="store_true", help="Вывод в терминал вместо окна")
    parser.add_argument("command", nargs="?", choices=("list", "check", "history"),
                        help="Загрузить список, проверить файлы или показать историю")
    parser.add_argument("--path", action="append", default=[], help="Путь выбранного файла; можно указать несколько раз")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Одновременные проверки (1–12)")
    parser.add_argument("--probe-bytes", type=int, default=DEFAULT_PROBE_BYTES,
                        help=f"Лимит чтения файла в байтах (по умолчанию {DEFAULT_PROBE_BYTES})")
    parser.add_argument("--mode", choices=("failed", "random", "full"),
                        help="Режим проверки; без параметра — full")
    sample = parser.add_mutually_exclusive_group()
    sample.add_argument("--sample-percent", type=float, help="Процент доступных файлов (1–100; по умолчанию 10)")
    sample.add_argument("--sample-count", type=int, help="Количество доступных файлов (больше нуля)")
    parser.add_argument("--seed", type=int, help="Начальное значение генератора случайной выборки")
    args = parser.parse_args()
    if not 1 <= args.workers <= 12:
        parser.error("--workers: ожидается число от 1 до 12")
    if args.probe_bytes <= 0:
        parser.error("--probe-bytes: ожидается число больше нуля")
    if not args.cli and (args.command or args.path):
        parser.error("command и --path используются вместе с --cli")
    check_options = any(value is not None for value in (args.mode, args.sample_percent, args.sample_count, args.seed))
    if check_options and (not args.cli or args.command != "check"):
        parser.error("--mode и параметры выборки используются с --cli check")
    if args.path and check_options:
        parser.error("--path используется отдельно от --mode и параметров выборки")
    if args.mode != "random" and any(value is not None for value in (args.sample_percent, args.sample_count, args.seed)):
        parser.error("Параметры выборки используются только с --mode random")
    if args.mode == "random":
        try:
            sample_size(0, args.sample_percent, args.sample_count)
        except ValueError as error:
            parser.error(str(error))
    if args.cli:
        command = args.command or "list"
        if args.path and command == "list":
            parser.error("--path используется с check или history")
        token = os.environ.get("YANDEX_DISK_TOKEN", "").strip()
        if not token and command != "history":
            print("Укажите OAuth-токен через переменную YANDEX_DISK_TOKEN.", file=sys.stderr)
            return 2
        return run_cli(token, args.workers, command, args.path, probe_bytes=args.probe_bytes,
                       mode=args.mode, sample_percent=args.sample_percent, sample_count=args.sample_count, seed=args.seed)
    return run_gui(args.workers, probe_bytes=args.probe_bytes)


if __name__ == "__main__":
    raise SystemExit(main())
