#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 Alexey Sedoykin
# SPDX-License-Identifier: MIT

from __future__ import annotations

import argparse
import csv
import os
import queue
import sqlite3
import sys
import threading
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator
from urllib.parse import urlsplit

import requests


API_ROOT = "https://cloud-api.yandex.net/v1/disk"
PAGE_SIZE = 500
DEFAULT_WORKERS = 3
API_TIMEOUT = (8, 20)  # connect, read timeout in seconds
CDN_TIMEOUT = (8, 18)
RETRYABLE_CODES = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 3
HISTORY_PATH = Path(__file__).with_name("yaDiskHealthHistory.sqlite3")


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
    # True = accessible, False = failed check, None = not checked / stopped.
    ok: bool | None
    note: str
    http_code: int | None = None


@dataclass(frozen=True)
class HistoryEntry:
    path: str
    checked_at: str
    result: CheckResult


class HistoryStore:
    def __init__(self, path: str | Path):
        self.connection = sqlite3.connect(path)
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

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.connection.close()

    def replace_files(self, files: list[DiskFile]) -> None:
        with self.connection:
            self.connection.execute("DELETE FROM files")
            self.connection.executemany(
                "INSERT INTO files(path, name, size) VALUES (?, ?, ?)",
                [(file.path, file.name, file.size) for file in files],
            )

    def files(self) -> list[DiskFile]:
        return [DiskFile(name, path, size) for path, name, size in
                self.connection.execute("SELECT path, name, size FROM files ORDER BY path")]

    def record(self, path: str, result: CheckResult) -> HistoryEntry:
        checked_at = datetime.now(timezone.utc).isoformat(timespec="microseconds")
        with self.connection:
            self.connection.execute(
                "INSERT INTO checks(path, checked_at, ok, http_code, note) VALUES (?, ?, ?, ?, ?)",
                (path, checked_at, result.ok, result.http_code, result.note),
            )
        return HistoryEntry(path, checked_at, result)

    def entries(self, paths: list[str] | None = None, latest: bool = False) -> list[HistoryEntry]:
        query = "SELECT path, checked_at, ok, note, http_code FROM checks"
        conditions = []
        if paths:
            conditions.append("path IN (" + ",".join("?" for _ in paths) + ")")
        if latest:
            conditions.append("id IN (SELECT MAX(id) FROM checks GROUP BY path)")
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        rows = self.connection.execute(query + " ORDER BY id", paths or [])
        return [HistoryEntry(path, checked_at, CheckResult(None if ok is None else bool(ok), note, code))
                for path, checked_at, ok, note, code in rows]


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
        status = "Доступен" if result.ok is True else (
            "Не подтверждён" if result.ok is False else "Остановлено"
        )
        change = ""
        if result.ok is not None:
            if entry.path in previous and previous[entry.path] != result.ok:
                change = "Доступ появился" if result.ok else "Доступ не подтверждён"
            previous[entry.path] = result.ok
        yield (local_time(entry.checked_at), entry.path, status, change,
               result.http_code or "", result.note)


class ScanStopped(Exception):
    pass


class DiskAPIError(Exception):
    def __init__(self, code: int | None, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class DiskProbe:
    def __init__(self, token: str, stop: threading.Event | None = None):
        self.token = token.strip()
        if not self.token:
            raise ValueError("OAuth-токен не указан")
        self.stop = stop or threading.Event()
        self._thread_local = threading.local()

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
                return min(30.0, max(0.0, float(retry_after)))
            except ValueError:
                pass
        return min(12.0, 0.75 * (2 ** attempt))

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
                with self._session(api=True).get(
                    url, params=params, timeout=API_TIMEOUT
                ) as response:
                    if response.status_code in RETRYABLE_CODES and attempt < MAX_ATTEMPTS - 1:
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
        url = urlsplit(href)
        return url.scheme == "https" and bool(url.hostname) and not url.username and not url.password

    def _probe_cdn(self, href: str, size: int | None) -> CheckResult:
        """Send a GET but NEVER read response.content, response.text or iter_content.

        For a normal file, request only byte 0; servers that ignore Range can
        reply 200, but the stream is closed immediately without reading it.
        For a zero-byte file, omit the Range header (which would produce 416).
        """
        session = self._session(api=False)  # No OAuth token sent to the CDN.
        headers = {"Range": "bytes=0-0"} if size != 0 else {}
        with session.get(
            href,
            headers=headers,
            stream=True,
            allow_redirects=True,
            timeout=CDN_TIMEOUT,
        ) as response:
            code = response.status_code
            if code in (200, 206):
                # A redirect to a login page is not a valid CDN download.
                host = (urlsplit(response.url).hostname or "").lower()
                if host in ("oauth.yandex.ru", "passport.yandex.ru"):
                    return CheckResult(False, "Перенаправление на вход в аккаунт", code)
                if code == 206:
                    return CheckResult(True, "HTTP 206 — выдача части файла разрешена", code)
                return CheckResult(True, "HTTP 200 — выдача файла разрешена", code)

            if code == 416 and headers:
                # Metadata can be stale. Retry without a range in case the
                # current file is empty or the backend rejects this range.
                with session.get(
                    href,
                    stream=True,
                    allow_redirects=True,
                    timeout=CDN_TIMEOUT,
                ) as plain:
                    if plain.status_code == 200:
                        if urlsplit(plain.url).hostname in ("oauth.yandex.ru", "passport.yandex.ru"):
                            return CheckResult(False, "Перенаправление на вход в аккаунт", 200)
                        return CheckResult(True, "HTTP 200 — выдача без Range разрешена", 200)
                    code = plain.status_code
            if code == 401:
                return CheckResult(False, "HTTP 401 — нет авторизации на сервере файлов", code)
            if code == 403:
                return CheckResult(False, "HTTP 403 — сервер отказал в доступе", code)
            if code == 404:
                return CheckResult(False, "HTTP 404 — файл недоступен на сервере", code)
            if code == 429:
                return CheckResult(False, "HTTP 429 — ограничение частоты запросов", code)
            return CheckResult(False, f"HTTP {code} — выдача файла не подтверждена", code)

    def check_file(self, file: DiskFile) -> CheckResult:
        """Generate a fresh download URL and probe it, with bounded retries."""
        for attempt in range(MAX_ATTEMPTS):
            try:
                self._check_stop()
                data = self._api_get("/resources/download", {"path": file.path})
                href = data.get("href")
                if not self._safe_download_url(href):
                    return CheckResult(False, "API не выдал корректную HTTPS-ссылку")
                result = self._probe_cdn(href, file.size)
                if result.http_code not in RETRYABLE_CODES or attempt == MAX_ATTEMPTS - 1:
                    return result
            except ScanStopped:
                return CheckResult(None, "Остановлено")
            except DiskAPIError as error:
                return CheckResult(False, f"API HTTP {error.code or '—'} — {error.message}", error.code)
            except (requests.Timeout, requests.ConnectionError):
                if attempt == MAX_ATTEMPTS - 1:
                    return CheckResult(False, "Тайм-аут или ошибка сети")
            except requests.RequestException:
                # Do not print the exception: it may contain the signed CDN URL.
                return CheckResult(False, "Ошибка HTTP-клиента")
            try:
                self._sleep(self._delay(attempt))
            except ScanStopped:
                return CheckResult(None, "Остановлено")
        return CheckResult(False, "Проверка не выполнена")


def load_files(
    token: str,
    events: "queue.Queue[tuple[str, object]]",
    stop: threading.Event,
    history_path: str | Path = HISTORY_PATH,
) -> None:
    """Load metadata only; replace the saved list after a complete listing."""
    problem = None
    try:
        client = DiskProbe(token, stop)
        files = []
        for file in client.iter_files():
            files.append(file)
            events.put(("file", file))
        client._check_stop()
        with HistoryStore(history_path) as history:
            history.replace_files(files)
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
) -> None:
    """Check the supplied files; commit each result before notifying the UI."""
    problem = None
    try:
        client = DiskProbe(token, stop)
        with HistoryStore(history_path) as history, ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="disk-probe"
        ) as executor:
            running = set()
            future_to_file = {}

            def report_completed(done) -> None:
                for future in done:
                    if future.cancelled():
                        continue
                    file = future_to_file.pop(future)
                    try:
                        result = future.result()
                    except Exception:
                        result = CheckResult(False, "Непредвиденная ошибка проверки")
                    entry = history.record(file.path, result)
                    events.put(("result", entry))

            for file in files:
                if stop.is_set():
                    break
                future = executor.submit(client.check_file, file)
                future_to_file[future] = file
                running.add(future)
                # Keep the queue bounded for large disks.
                if len(running) >= workers * 4:
                    done, running = wait(running, return_when=FIRST_COMPLETED)
                    report_completed(done)

            if stop.is_set():
                for future in running:
                    future.cancel()
            for future in as_completed(running):
                report_completed([future])
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
) -> int:
    paths = paths or []
    try:
        with HistoryStore(history_path) as history:
            if command == "history":
                for row in history_rows(history.entries(paths)):
                    print("\t".join(str(value) for value in row))
                return 0
            files = files_for_check(history.files(), paths) if command == "check" else []
    except (sqlite3.Error, OSError):
        print("Не удалось открыть историю проверок.", file=sys.stderr)
        return 2
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2
    if command == "check" and not files:
        print("Сначала загрузите список: --cli list", file=sys.stderr)
        return 2
    events: queue.Queue[tuple[str, object]] = queue.Queue()
    stop = threading.Event()
    if command == "list":
        target, arguments = load_files, (token, events, stop, history_path)
    else:
        target, arguments = scan, (token, files, events, stop, workers, history_path)
    thread = threading.Thread(target=target, args=arguments, daemon=True)
    thread.start()
    results: dict[str, CheckResult] = {}
    found = 0
    error = None
    while True:
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
                bubble = "[OK]" if result.ok is True else "[FAIL]" if result.ok is False else "[STOP]"
            print(f"{bubble} {payload.path} — {result.note}", flush=True)
        elif kind == "finished":
            error = payload
            break
    thread.join(timeout=1)
    ok = sum(1 for result in results.values() if result.ok is True)
    failed = sum(1 for result in results.values() if result.ok is False)
    if error:
        print(error, file=sys.stderr)
        return 2
    if command == "list":
        print(f"Список загружен: {found}. Проверка: --cli check")
    else:
        print(f"Выбрано: {len(files)}; доступно: {ok}; ошибки: {failed}")
    return 1 if failed else 0


def run_gui(workers: int, history_path: str | Path = HISTORY_PATH) -> int:
    try:
        import tkinter as tk
        from tkinter import filedialog, messagebox, ttk
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
            self.events: queue.Queue[tuple[str, object]] = queue.Queue()
            self.stop_event = threading.Event()
            self.worker: threading.Thread | None = None
            self.files: dict[str, DiskFile] = {}
            self.results: dict[str, CheckResult] = {}
            self.checked_at: dict[str, str] = {}
            self.ids: dict[str, str] = {}
            self.running = False
            self.operation = ""

            frame = ttk.Frame(root, padding=(14, 12))
            frame.pack(fill="both", expand=True)
            toolbar = ttk.Frame(frame)
            toolbar.pack(fill="x")
            ttk.Label(toolbar, text="OAuth-токен:").pack(side="left")
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
            self.export_btn = ttk.Button(actions, text="CSV…", command=self.save_csv, state="disabled")
            self.export_btn.pack(side="left", padx=(0, 6))
            ttk.Button(actions, text="Снять выбор", command=self.clear_selection).pack(side="left")
            self.selection_var = tk.StringVar(value="Без выбора: проверяются все файлы")
            ttk.Label(actions, textvariable=self.selection_var).pack(side="left", padx=10)

            summary = ttk.Frame(frame)
            summary.pack(fill="x", pady=(13, 9))
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
                columns=("name", "folder", "size", "checked", "note"),
                show="tree headings",
                selectmode="extended",
            )
            self.tree.heading("#0", text="●")
            self.tree.column("#0", width=45, minwidth=45, stretch=False, anchor="center")
            for column, heading, width, anchor in (
                ("name", "Имя файла", 260, "w"),
                ("folder", "Папка", 310, "w"),
                ("size", "Размер", 100, "e"),
                ("checked", "Последняя проверка", 155, "w"),
                ("note", "Результат HTTP-проверки", 310, "w"),
            ):
                self.tree.heading(column, text=heading)
                self.tree.column(column, width=width, minwidth=80, anchor=anchor)
            scroll_y = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
            scroll_x = ttk.Scrollbar(table_frame, orient="horizontal", command=self.tree.xview)
            self.tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
            self.tree.grid(row=0, column=0, sticky="nsew")
            self.tree.bind("<<TreeviewSelect>>", self.selection_changed)
            self.tree.bind("<Escape>", self.clear_selection)
            scroll_y.grid(row=0, column=1, sticky="ns")
            scroll_x.grid(row=1, column=0, sticky="ew")
            table_frame.rowconfigure(0, weight=1)
            table_frame.columnconfigure(0, weight=1)

            ttk.Label(
                frame,
                text="Зелёный — сервер разрешил начало выдачи; красный — проверка не прошла; серый — не проверено. "
                     "Ctrl/Shift — выбор группы. Без выбора проверяются все файлы. Содержимое не загружается.",
                wraplength=1040,
            ).pack(anchor="w", pady=(10, 0))
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
            self.selection_var.set(f"Выбрано: {count}" if count else "Без выбора: проверяются все файлы")

        def selected_paths(self) -> list[str]:
            selected = set(self.tree.selection())
            return [path for path, iid in self.ids.items() if iid in selected]

        def add_file(self, file: DiskFile) -> None:
            self.files[file.path] = file
            self.ids[file.path] = self.tree.insert(
                "", "end", image=self.icons["pending"],
                values=(file.name, file.folder, readable_size(file.size), "—", "Не проверено"),
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
            self.tree.set(iid, "note", result.note)
            self.tree.set(iid, "checked", local_time(entry.checked_at))

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

        def launch(self, operation: str, token: str, files: list[DiskFile] | None = None) -> None:
            self.operation = operation
            self.stop_event = threading.Event()
            self.running = True
            self.load_btn.configure(state="disabled")
            self.start_btn.configure(state="disabled")
            self.stop_btn.configure(state="normal")
            self.export_btn.configure(state="disabled")
            self.history_btn.configure(state="disabled")
            self.token_entry.configure(state="disabled")
            if operation == "load":
                target, arguments = load_files, (token, self.events, self.stop_event, history_path)
            else:
                target, arguments = scan, (token, files, self.events, self.stop_event, workers, history_path)
            self.worker = threading.Thread(
                target=target,
                args=arguments,
                daemon=True,
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

        def start(self) -> None:
            if self.running or not self.files:
                return
            token = self.get_token()
            if not token:
                return
            files = files_for_check(list(self.files.values()), self.selected_paths())
            for file in files:
                self.results.pop(file.path, None)
                self.tree.item(self.ids[file.path], image=self.icons["pending"])
                self.tree.set(self.ids[file.path], "note", "Ожидает проверки")
            self.update_counts()
            self.state_var.set(f"Проверка файлов: {len(files)}…")
            self.launch("check", token, files)

        def cancel(self) -> None:
            if self.running:
                self.stop_event.set()
                self.stop_btn.configure(state="disabled")
                self.state_var.set("Остановка проверок…")

        def update_counts(self) -> None:
            ok = sum(result.ok is True for result in self.results.values())
            fail = sum(result.ok is False for result in self.results.values())
            self.overview_var.set(
                f"Найдено: {len(self.files)}    Доступно: {ok}    "
                f"Ошибки: {fail}    Не проверено: {len(self.files) - ok - fail}"
            )

        def consume_events(self) -> None:
            changed = False
            for _ in range(350):
                try:
                    kind, payload = self.events.get_nowait()
                except queue.Empty:
                    break
                if kind == "file":
                    file = payload
                    if file.path in self.ids:
                        continue
                    self.add_file(file)
                    changed = True
                elif kind == "result":
                    self.display_result(payload)
                    changed = True
                elif kind == "finished":
                    self.running = False
                    try:
                        if self.operation == "load":
                            self.restore_catalog()
                        else:
                            with HistoryStore(history_path) as history:
                                entries = history.entries(latest=True)
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
                    self.token_entry.configure(state="normal")
                    if payload:
                        self.state_var.set(f"Ошибка: {payload}")
                        messagebox.showwarning("Проверка завершена с ошибкой", str(payload))
                    elif self.stop_event.is_set():
                        self.state_var.set("Операция остановлена. Предыдущий полный список сохранён.")
                    elif self.operation == "load":
                        self.state_var.set("Полный список загружен. Выберите файлы или проверьте весь список.")
                    else:
                        self.state_var.set("Проверка завершена")
                    changed = True
            if changed:
                self.update_counts()
            self.root.after(75, self.consume_events)

        def show_history(self) -> None:
            try:
                with HistoryStore(history_path) as history:
                    entries = history.entries(self.selected_paths())
            except (sqlite3.Error, OSError):
                messagebox.showerror("История", "Не удалось прочитать историю проверок.")
                return
            window = tk.Toplevel(self.root)
            window.title("История проверок — выбранные файлы или весь Диск")
            window.geometry("1150x520")
            ttk.Label(window, text="Ошибка сети не доказывает потерю доступа. Время указано в местном часовом поясе.").pack(
                anchor="w", padx=10, pady=8
            )
            frame = ttk.Frame(window)
            frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))
            columns = ("date", "path", "status", "change", "http", "note")
            tree = ttk.Treeview(frame, columns=columns, show="headings")
            for column, heading, width in zip(
                columns, ("Дата и время", "Путь", "Результат", "Изменение", "HTTP", "Причина"),
                (160, 340, 140, 175, 60, 300),
            ):
                tree.heading(column, text=heading)
                tree.column(column, width=width, minwidth=60)
            for row in history_rows(entries):
                tree.insert("", 0, values=row)
            vertical = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
            horizontal = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
            tree.configure(yscrollcommand=vertical.set, xscrollcommand=horizontal.set)
            tree.grid(row=0, column=0, sticky="nsew")
            vertical.grid(row=0, column=1, sticky="ns")
            horizontal.grid(row=1, column=0, sticky="ew")
            frame.rowconfigure(0, weight=1)
            frame.columnconfigure(0, weight=1)

        def close(self) -> None:
            self.stop_event.set()
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
                    writer.writerow(("Имя", "Путь", "Размер, байт", "Доступно", "HTTP", "Описание", "Последняя проверка"))
                    for file in self.files.values():
                        result = self.results.get(file.path)
                        status = "Да" if result and result.ok is True else (
                            "Нет" if result and result.ok is False else "Не проверено"
                        )
                        writer.writerow((
                            csv_safe(file.name), csv_safe(file.path),
                            file.size if file.size is not None else "",
                            status, result.http_code if result and result.http_code else "",
                            csv_safe(result.note if result else "Ожидает проверки"),
                            local_time(self.checked_at[file.path]) if file.path in self.checked_at else "",
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
        description="Проверка доступности скачивания всех файлов Яндекс Диска без загрузки содержимого"
    )
    parser.add_argument("--cli", action="store_true", help="Вывод в терминал вместо окна")
    parser.add_argument("command", nargs="?", choices=("list", "check", "history"),
                        help="Загрузить список, проверить файлы или показать историю")
    parser.add_argument("--path", action="append", default=[], help="Путь выбранного файла; можно указать несколько раз")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Одновременные проверки (1–12)")
    args = parser.parse_args()
    if not 1 <= args.workers <= 12:
        parser.error("--workers: ожидается число от 1 до 12")
    if not args.cli and (args.command or args.path):
        parser.error("command и --path используются вместе с --cli")
    if args.cli:
        command = args.command or "list"
        if args.path and command == "list":
            parser.error("--path используется с check или history")
        token = os.environ.get("YANDEX_DISK_TOKEN", "").strip()
        if not token and command != "history":
            print("Укажите OAuth-токен через переменную YANDEX_DISK_TOKEN.", file=sys.stderr)
            return 2
        return run_cli(token, args.workers, command, args.path)
    return run_gui(args.workers)


if __name__ == "__main__":
    raise SystemExit(main())
