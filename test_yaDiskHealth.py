# SPDX-FileCopyrightText: Copyright (c) 2026 Alexey Sedoykin
# SPDX-License-Identifier: MIT

import contextlib
import io
import queue
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import yadisk_download_check as mod


class Response:
    def __init__(self, status_code, payload=None, url="https://downloader.disk.yandex.ru/file", headers=None):
        self.status_code = status_code
        self.payload = payload
        self.url = url
        self.headers = headers or {}

    @property
    def ok(self):
        return self.status_code < 400

    @property
    def content(self):
        raise AssertionError("Content MUST NOT be consumed")

    @property
    def text(self):
        raise AssertionError("Text MUST NOT be consumed")

    def json(self):
        return self.payload

    def iter_content(self, *args, **kwargs):
        raise AssertionError("Download body MUST NOT be consumed")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def get(self, *args, **kwargs):
        self.requests.append((args, kwargs))
        assert self.responses, "No more fake responses"
        return self.responses.pop(0)


class DiskProbeTests(unittest.TestCase):
    def setUp(self):
        self.probe = mod.DiskProbe("fictional-test-token")

    def test_flat_list_offset_and_nested_paths(self):
        a = {"name": "first.jpg", "path": "disk:/first.jpg", "size": 13}
        b = {"name": "notes.pdf", "path": "disk:/folder/notes.pdf", "size": 1234}
        c = {"name": "empty.txt", "path": "disk:/empty.txt", "size": 0}
        fake = FakeSession([Response(200, {"items": [a, b], "total": 3}),
                            Response(200, {"items": [c], "total": 3})])
        self.probe._session = lambda api: fake
        with patch.object(mod, "PAGE_SIZE", 2):
            files = list(self.probe.iter_files())
        self.assertEqual([x.path for x in files], [a["path"], b["path"], c["path"]])
        self.assertEqual(fake.requests[1][1]["params"]["offset"], 2)

    def test_probe_206_uses_range_stream_no_body(self):
        api = FakeSession([Response(200, {"href": "https://downloader.disk.yandex.ru/my-file"})])
        cdn = FakeSession([Response(206)])
        self.probe._session = lambda **kw: api if kw["api"] else cdn
        result = self.probe.check_file(mod.DiskFile("x", "disk:/x", 500))
        self.assertIs(result.ok, True)
        self.assertEqual(result.http_code, 206)
        self.assertEqual(cdn.requests[0][1]["headers"], {"Range": "bytes=0-0"})
        self.assertIs(cdn.requests[0][1]["stream"], True)

    def test_zero_byte_file_has_no_range(self):
        api = FakeSession([Response(200, {"href": "https://downloader.disk.yandex.ru/empty"})])
        cdn = FakeSession([Response(200)])
        self.probe._session = lambda **kw: api if kw["api"] else cdn
        result = self.probe.check_file(mod.DiskFile("empty", "disk:/empty", 0))
        self.assertIs(result.ok, True)
        self.assertEqual(cdn.requests[0][1]["headers"], {})

    def test_404_is_red(self):
        api = FakeSession([Response(200, {"href": "https://downloader.disk.yandex.ru/x"})])
        cdn = FakeSession([Response(404)])
        self.probe._session = lambda **kw: api if kw["api"] else cdn
        result = self.probe.check_file(mod.DiskFile("x", "disk:/x", 100))
        self.assertIs(result.ok, False)
        self.assertIn("404", result.note)

    def test_range_416_falls_back_to_unranged_get(self):
        api = FakeSession([Response(200, {"href": "https://downloader.disk.yandex.ru/x"})])
        cdn = FakeSession([Response(416), Response(200)])
        self.probe._session = lambda **kw: api if kw["api"] else cdn
        result = self.probe.check_file(mod.DiskFile("x", "disk:/x", 1))
        self.assertIs(result.ok, True)
        self.assertEqual(len(cdn.requests), 2)
        self.assertNotIn("headers", cdn.requests[1][1])

    def test_range_fallback_rejects_login_page(self):
        api = FakeSession([Response(200, {"href": "https://downloader.disk.yandex.ru/x"})])
        cdn = FakeSession([Response(416), Response(200, url="https://passport.yandex.ru/auth")])
        self.probe._session = lambda **kw: api if kw["api"] else cdn
        result = self.probe.check_file(mod.DiskFile("x", "disk:/x", 1))
        self.assertIs(result.ok, False)

    def test_invalid_link_is_not_contacted(self):
        api = FakeSession([Response(200, {"href": "http://insecure.invalid/file"})])
        cdn = FakeSession([])
        self.probe._session = lambda **kw: api if kw["api"] else cdn
        result = self.probe.check_file(mod.DiskFile("x", "disk:/x", 12))
        self.assertIs(result.ok, False)
        self.assertEqual(len(cdn.requests), 0)

    def test_oauth_header_not_sent_to_cdn(self):
        self.assertIn("Authorization", self.probe._session(api=True).headers)
        self.assertNotIn("Authorization", self.probe._session(api=False).headers)

    def test_oauth_error(self):
        api = FakeSession([Response(401, {"message": "auth failure"})])
        self.probe._session = lambda **kw: api
        with self.assertRaises(mod.DiskAPIError) as raised:
            list(self.probe.iter_files())
        self.assertEqual(raised.exception.code, 401)


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database = Path(self.temp.name) / "history.sqlite3"
        self.files = [mod.DiskFile("a", "disk:/a", 10), mod.DiskFile("b", "disk:/folder/b", 20)]
        self.events = queue.Queue()
        self.stop = threading.Event()
        self.good = mod.CheckResult(True, "HTTP 206", 206)
        with mod.HistoryStore(self.database) as history:
            history.replace_files(self.files)

    def drain(self):
        events = []
        while not self.events.empty():
            events.append(self.events.get_nowait())
        return events

    def test_loading_only_lists_files(self):
        with patch.object(mod.DiskProbe, "iter_files", return_value=iter(self.files)), \
                patch.object(mod.DiskProbe, "check_file") as check:
            mod.load_files("fictional-test-token", self.events, self.stop, self.database)
        check.assert_not_called()
        self.assertEqual([kind for kind, _ in self.drain()], ["file", "file", "finished"])
        with mod.HistoryStore(self.database) as history:
            self.assertEqual(history.files(), self.files)
            self.assertEqual(history.entries(), [])

    def test_incomplete_listing_keeps_previous_catalog_and_history(self):
        with mod.HistoryStore(self.database) as history:
            history.record(self.files[0].path, self.good)

        def broken_listing(_client):
            yield mod.DiskFile("partial", "disk:/partial", 1)
            raise mod.DiskAPIError(500, "temporary failure")

        with patch.object(mod.DiskProbe, "iter_files", broken_listing):
            mod.load_files("fictional-test-token", self.events, self.stop, self.database)
        self.assertIn("500", self.drain()[-1][1])
        with mod.HistoryStore(self.database) as history:
            self.assertEqual(history.files(), self.files)
            self.assertEqual(len(history.entries()), 1)

    def test_cancelled_listing_keeps_previous_catalog(self):
        def cancelled_listing(_client):
            yield mod.DiskFile("partial", "disk:/partial", 1)
            self.stop.set()

        with patch.object(mod.DiskProbe, "iter_files", cancelled_listing):
            mod.load_files("fictional-test-token", self.events, self.stop, self.database)
        with mod.HistoryStore(self.database) as history:
            self.assertEqual(history.files(), self.files)

    def test_complete_empty_listing_clears_catalog_but_keeps_history(self):
        with mod.HistoryStore(self.database) as history:
            history.record(self.files[0].path, self.good)
        with patch.object(mod.DiskProbe, "iter_files", return_value=iter([])):
            mod.load_files("fictional-test-token", self.events, self.stop, self.database)
        with mod.HistoryStore(self.database) as history:
            self.assertEqual(history.files(), [])
            self.assertEqual(len(history.entries()), 1)

    def test_selection_one_many_all_and_unknown_path(self):
        self.assertEqual(mod.files_for_check(self.files, []), self.files)
        self.assertEqual(mod.files_for_check(self.files, ["disk:/a"]), self.files[:1])
        self.assertEqual(mod.files_for_check(self.files, [file.path for file in self.files]), self.files)
        with self.assertRaises(ValueError):
            mod.files_for_check(self.files, ["disk:/missing"])

    def test_check_uses_only_supplied_files_and_persists_results(self):
        with patch.object(mod.DiskProbe, "iter_files") as listing, \
                patch.object(mod.DiskProbe, "check_file", return_value=self.good) as check:
            mod.scan("fictional-test-token", self.files[:1], self.events, self.stop, 1, self.database)
        listing.assert_not_called()
        check.assert_called_once_with(self.files[0])
        events = self.drain()
        self.assertEqual([kind for kind, _ in events], ["result", "finished"])
        with mod.HistoryStore(self.database) as history:
            self.assertEqual(history.entries(), [events[0][1]])
        self.assertNotIn(b"fictional-test-token", self.database.read_bytes())

    def test_stopped_check_does_not_start_requests_or_erase_history(self):
        with mod.HistoryStore(self.database) as history:
            history.record(self.files[0].path, self.good)
        self.stop.set()
        with patch.object(mod.DiskProbe, "check_file") as check:
            mod.scan("fictional-test-token", self.files, self.events, self.stop, 1, self.database)
        check.assert_not_called()
        with mod.HistoryStore(self.database) as history:
            self.assertEqual(len(history.entries()), 1)

    def test_history_survives_restart_and_tracks_changes_across_days(self):
        failed = mod.CheckResult(False, "HTTP 403", 403)
        with mod.HistoryStore(self.database) as history:
            with patch.object(mod, "datetime") as clock:
                clock.now.side_effect = [datetime(2026, 10, day, tzinfo=timezone.utc) for day in range(18, 22)]
                for result in (failed, failed, self.good, failed):
                    history.record(self.files[0].path, result)
            history.record(self.files[1].path, self.good)
        with mod.HistoryStore(self.database) as reopened:
            entries = reopened.entries([self.files[0].path])
            self.assertEqual([entry.result.ok for entry in entries], [False, False, True, False])
            self.assertEqual([entry.checked_at[:10] for entry in entries],
                             ["2026-10-18", "2026-10-19", "2026-10-20", "2026-10-21"])
            rows = list(mod.history_rows(entries))
            self.assertEqual(rows[1][3], "")
            self.assertEqual(rows[2][3], "Доступ появился")
            self.assertEqual(rows[3][3], "Доступ не подтверждён")
            latest = {entry.path: entry for entry in reopened.entries(latest=True)}
            self.assertEqual(latest[self.files[0].path].result, failed)
            self.assertEqual(latest[self.files[1].path].result, self.good)

    def test_stopped_attempt_does_not_create_access_transition(self):
        with mod.HistoryStore(self.database) as history:
            for result in (self.good, mod.CheckResult(None, "Остановлено"), self.good):
                history.record(self.files[0].path, result)
            rows = list(mod.history_rows(history.entries()))
        self.assertEqual([row[3] for row in rows], ["", "", ""])

    def test_refresh_does_not_erase_history_for_removed_files(self):
        with mod.HistoryStore(self.database) as history:
            history.record(self.files[0].path, self.good)
            history.replace_files(self.files[1:])
            self.assertEqual(len(history.entries()), 1)
            self.assertEqual(history.files(), self.files[1:])

    def test_cli_list_then_check_selection_then_check_all(self):
        with contextlib.redirect_stdout(io.StringIO()):
            with patch.object(mod.DiskProbe, "iter_files", return_value=iter(self.files)), \
                    patch.object(mod.DiskProbe, "check_file", return_value=self.good) as check:
                self.assertEqual(mod.run_cli("fictional-test-token", 1, "list", history_path=self.database), 0)
                check.assert_not_called()
                self.assertEqual(mod.run_cli("fictional-test-token", 1, "check", ["disk:/a"], self.database), 0)
                check.assert_called_once_with(self.files[0])
                check.reset_mock()
                self.assertEqual(mod.run_cli("fictional-test-token", 1, "check", history_path=self.database), 0)
                self.assertCountEqual([call.args[0] for call in check.call_args_list], self.files)
        with mod.HistoryStore(self.database) as history:
            self.assertEqual(len(history.entries()), 3)

    def test_cli_history_needs_no_token_or_network(self):
        with mod.HistoryStore(self.database) as history:
            history.record(self.files[0].path, self.good)
        with contextlib.redirect_stdout(io.StringIO()) as output, patch.object(mod, "DiskProbe") as probe:
            self.assertEqual(mod.run_cli("", 1, "history", history_path=self.database), 0)
        probe.assert_not_called()
        self.assertIn("disk:/a", output.getvalue())

    def test_cli_refuses_check_before_list_or_with_unknown_selection(self):
        with mod.HistoryStore(self.database) as history:
            history.replace_files([])
        with contextlib.redirect_stderr(io.StringIO()), patch.object(mod.DiskProbe, "check_file") as check:
            self.assertEqual(mod.run_cli("fictional-test-token", 1, "check", history_path=self.database), 2)
            self.assertEqual(mod.run_cli("fictional-test-token", 1, "check", ["disk:/missing"], self.database), 2)
        check.assert_not_called()


class GuiWorkflowTests(unittest.TestCase):
    def test_buttons_selection_history_and_restart(self):
        try:
            import tkinter as tk
            from tkinter import ttk
        except ImportError:
            self.skipTest("Tkinter unavailable")
        try:
            root = tk.Tk()
        except tk.TclError as error:
            self.skipTest(f"Tk window unavailable: {type(error).__name__}")
        root.withdraw()
        self.addCleanup(lambda: root.destroy() if root.winfo_exists() else None)

        with tempfile.TemporaryDirectory() as folder:
            database = Path(folder) / "history.sqlite3"
            files = [mod.DiskFile("a", "disk:/a", 10), mod.DiskFile("b", "disk:/b", 20)]

            def widgets(parent):
                for child in parent.winfo_children():
                    yield child
                    yield from widgets(child)

            def wait_until(predicate):
                deadline = time.monotonic() + 5
                while not predicate() and time.monotonic() < deadline:
                    root.update()
                    time.sleep(0.005)
                self.assertTrue(predicate())

            def drive():
                controls = list(widgets(root))
                buttons = {widget.cget("text"): widget for widget in controls if isinstance(widget, ttk.Button)}
                tree = next(widget for widget in controls if isinstance(widget, ttk.Treeview))
                token = next(widget for widget in controls if isinstance(widget, ttk.Entry))
                token.insert(0, "fictional-test-token")
                self.assertIn("disabled", buttons["Проверить"].state())
                buttons["Загрузить список"].invoke()
                self.assertIn("disabled", buttons["Проверить"].state())
                wait_until(lambda: "disabled" not in buttons["Проверить"].state())
                check.assert_not_called()
                self.assertEqual(len(tree.get_children()), 2)

                tree.selection_set(tree.get_children()[0])
                buttons["Проверить"].invoke()
                wait_until(lambda: "disabled" not in buttons["Проверить"].state())
                check.assert_called_once_with(files[0])
                self.assertTrue(tree.set(tree.get_children()[0], "checked"))

                buttons["Снять выбор"].invoke()
                buttons["Проверить"].invoke()
                wait_until(lambda: "disabled" not in buttons["Проверить"].state())
                self.assertEqual(check.call_count, 3)

                buttons["История"].invoke()
                history_tree = [widget for widget in widgets(root) if isinstance(widget, ttk.Treeview)][1]
                self.assertEqual(len(history_tree.get_children()), 3)
                for window in root.winfo_children():
                    if isinstance(window, tk.Toplevel):
                        window.destroy()

                def stop_before_requests(_token, _files, events, stop, *_args):
                    stop.set()
                    events.put(("finished", None))

                with patch.object(mod, "scan", side_effect=stop_before_requests):
                    buttons["Проверить"].invoke()
                    wait_until(lambda: "disabled" not in buttons["Проверить"].state())
                self.assertTrue(all(tree.set(iid, "note") == "HTTP 206" for iid in tree.get_children()))
                with mod.HistoryStore(database) as history:
                    self.assertEqual(len(history.entries()), 3)

            root.mainloop = drive
            with patch.object(tk, "Tk", return_value=root), \
                    patch.object(mod.DiskProbe, "iter_files", return_value=iter(files)), \
                    patch.object(mod.DiskProbe, "check_file", return_value=mod.CheckResult(True, "HTTP 206", 206)) as check:
                self.assertEqual(mod.run_gui(1, database), 0)

            # Reopen the saved catalog in another window; no network is needed.
            second = tk.Toplevel(root)
            second.withdraw()
            self.addCleanup(second.destroy)

            def reopened():
                tree = next(widget for widget in widgets(second) if isinstance(widget, ttk.Treeview))
                self.assertEqual(len(tree.get_children()), 2)
                self.assertTrue(all(tree.set(iid, "checked") != "—" for iid in tree.get_children()))

            second.mainloop = reopened
            with patch.object(tk, "Tk", return_value=second), patch.object(mod, "DiskProbe") as probe:
                self.assertEqual(mod.run_gui(1, database), 0)
            probe.assert_not_called()


if __name__ == "__main__":
    unittest.main()
