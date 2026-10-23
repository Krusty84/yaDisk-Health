# SPDX-FileCopyrightText: Copyright (c) 2026 Alexey Sedoykin
# SPDX-License-Identifier: MIT

import contextlib
import csv
import io
import queue
import sqlite3
import subprocess
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import yaDiskHealth as mod
from urllib3.exceptions import IncompleteRead, ProtocolError, ReadTimeoutError
from urllib3.response import HTTPResponse


class Stream:
    def __init__(self, size=0, fail_after=None, error=None, on_read=None, max_chunk_size=None):
        self.size = size
        self.position = 0
        self.read_sizes = []
        self.closed = False
        self.fail_after = fail_after
        self.error = error
        self.on_read = on_read
        self.max_chunk_size = max_chunk_size

    def read(self, amount, decode_content=False):
        assert not self.closed, "Read after close"
        assert 0 < amount <= 65536, "Unbounded stream read"
        assert decode_content is False, "Body must not be decoded"
        self.read_sizes.append(amount)
        if self.fail_after is not None and self.position >= self.fail_after:
            raise self.error
        available = self.size - self.position
        if self.fail_after is not None:
            available = min(available, self.fail_after - self.position)
        count = min(amount, available)
        if self.max_chunk_size is not None:
            count = min(count, self.max_chunk_size)
        self.position += count
        if self.on_read:
            self.on_read()
        return b"x" * count

    def close(self):
        self.closed = True


class Response:
    def __init__(self, status_code, payload=None, url="https://downloader.disk.yandex.ru/file", headers=None,
                 body_size=0, **stream_options):
        self.status_code = status_code
        self.payload = payload
        self.url = url
        self.headers = mod.requests.structures.CaseInsensitiveDict(headers or {})
        self.raw = Stream(body_size, **stream_options)

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
        self.raw.close()
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

    def test_probe_206_reads_1024_bytes_and_closes(self):
        api = FakeSession([Response(200, {"href": "https://downloader.disk.yandex.ru/my-file"})])
        response = Response(206, headers={"Content-Range": "bytes 0-1023/900000", "Content-Length": "1024"},
                            body_size=1024)
        cdn = FakeSession([response])
        self.probe._session = lambda **kw: api if kw["api"] else cdn
        with patch("builtins.open", side_effect=AssertionError("File body must not be saved")):
            result = self.probe.check_file(mod.DiskFile("x", "disk:/x", 900000))
        self.assertIs(result.ok, True)
        self.assertEqual(result.http_code, 206)
        self.assertEqual(cdn.requests[0][1]["headers"],
                         {"Range": "bytes=0-1023", "Accept-Encoding": "identity"})
        self.assertIs(cdn.requests[0][1]["stream"], True)
        self.assertEqual(result.bytes_requested, 1024)
        self.assertEqual(result.bytes_received, 1024)
        self.assertIs(result.range_supported, True)
        self.assertIsNone(result.failure_reason)
        self.assertGreaterEqual(result.duration_ms, 0)
        self.assertEqual(response.raw.position, 1024)
        self.assertEqual(response.raw.read_sizes, [1024])
        self.assertTrue(response.raw.closed)

    def test_zero_byte_file_reads_empty_response(self):
        api = FakeSession([Response(200, {"href": "https://downloader.disk.yandex.ru/empty"})])
        response = Response(200, headers={"Content-Length": "0"})
        cdn = FakeSession([response])
        self.probe._session = lambda **kw: api if kw["api"] else cdn
        result = self.probe.check_file(mod.DiskFile("empty", "disk:/empty", 0))
        self.assertIs(result.ok, True)
        self.assertEqual(cdn.requests[0][1]["headers"]["Range"], "bytes=0-1023")
        self.assertEqual(result.bytes_received, 0)
        self.assertEqual(result.bytes_requested, 0)
        self.assertTrue(response.raw.read_sizes)
        self.assertTrue(response.raw.closed)

    def run_probe(self, responses, size=900000, probe_bytes=1024):
        self.probe = mod.DiskProbe("fictional-test-token", probe_bytes=probe_bytes)
        api = FakeSession([Response(200, {"href": f"https://downloader.disk.yandex.ru/x?attempt={i}"})
                           for i in range(len(responses))])
        cdn = FakeSession(responses)
        self.probe._session = lambda **kw: api if kw["api"] else cdn
        with patch.object(self.probe, "_sleep"):
            result = self.probe.check_file(mod.DiskFile("x", "disk:/x", size))
        self.assertTrue(all(response.raw.closed for response in responses[:len(cdn.requests)]))
        return result, api, cdn

    def test_small_file_206_and_200(self):
        for code, headers in ((206, {"Content-Range": "bytes 0-999/1000", "Content-Length": "1000"}),
                              (200, {"Content-Length": "1000"}), (200, {})):
            with self.subTest(code=code, headers=headers):
                response = Response(code, headers=headers, body_size=1000)
                result, _, _ = self.run_probe([response], size=1000)
                self.assertIs(result.ok, True)
                self.assertEqual(result.bytes_received, 1000)
                self.assertEqual(result.bytes_requested, 1000)
                self.assertEqual(response.raw.position, 1000)
                self.assertIs(result.range_supported, code == 206)

    def test_200_ignoring_range_never_reads_past_limit(self):
        response = Response(200, headers={"Content-Length": "900000"}, body_size=900000)
        result, _, _ = self.run_probe([response])
        self.assertIs(result.ok, True)
        self.assertIs(result.range_supported, False)
        self.assertEqual(result.bytes_received, 1024)
        self.assertEqual(response.raw.position, 1024)
        self.assertEqual(response.raw.read_sizes, [1024])

    def test_unknown_size_uses_headers_or_receives_full_probe(self):
        for code, headers, body_size in (
            (206, {"Content-Range": "bytes 0-999/1000"}, 1000),
            (206, {"Content-Range": "bytes 0-1023/*"}, 1024),
            (200, {"Content-Length": "1000"}, 1000),
            (200, {}, 900000),
        ):
            with self.subTest(code=code, headers=headers, body_size=body_size):
                response = Response(code, headers=headers, body_size=body_size)
                result, _, _ = self.run_probe([response], size=None)
                self.assertIs(result.ok, True)
                self.assertEqual(result.bytes_received, min(body_size, 1024))
                self.assertLessEqual(response.raw.position, 1024)

    def test_unknown_size_without_length_and_short_eof_is_gray(self):
        for body_size in (0, 500, 1000):
            with self.subTest(body_size=body_size):
                result, _, _ = self.run_probe([Response(200, body_size=body_size)], size=None)
                self.assertIsNone(result.ok)
                self.assertEqual(result.bytes_received, body_size)
                self.assertEqual(result.bytes_requested, 1024)
                self.assertTrue(result.failure_reason)

    def test_unknown_empty_file_requires_zero_length_header(self):
        result, _, _ = self.run_probe([Response(200, headers={"Content-Length": "0"})], size=None)
        self.assertIs(result.ok, True)
        self.assertEqual(result.bytes_requested, 0)
        self.assertEqual(result.bytes_received, 0)

    def test_short_reads_are_combined_without_exceeding_probe_limit(self):
        response = Response(206, headers={"Content-Range": "bytes 0-1023/900000"},
                            body_size=1024, max_chunk_size=100)
        result, _, _ = self.run_probe([response])
        self.assertIs(result.ok, True)
        self.assertEqual(result.bytes_received, 1024)
        self.assertEqual(response.raw.position, 1024)
        self.assertEqual(len(response.raw.read_sizes), 11)
        self.assertEqual(response.raw.read_sizes[-1], 24)

    def test_invalid_range_headers_are_gray_without_body_read(self):
        for headers in (
            {}, {"Content-Range": "not a range"}, {"Content-Range": "bytes 1-1023/900000"},
            {"Content-Range": "bytes 0-1024/900000"}, {"Content-Range": "bytes 0-1023/1023"},
            {"Content-Range": "bytes 0-499/900000"}, {"Content-Range": "bytes 0-1023/800000"},
            {"Content-Range": "bytes 0-1023/900000", "Content-Length": "1023"},
            {"Content-Range": "bytes 0-1023/900000", "Content-Length": "-1"},
        ):
            with self.subTest(headers=headers):
                response = Response(206, headers=headers, body_size=1024)
                result, _, _ = self.run_probe([response])
                self.assertIsNone(result.ok)
                self.assertTrue(result.failure_reason)
                self.assertEqual(response.raw.position, 0)

    def test_unknown_short_206_cannot_confirm_entire_file(self):
        result, _, _ = self.run_probe([Response(206, headers={"Content-Range": "bytes 0-999/*"},
                                               body_size=1000)], size=None)
        self.assertIsNone(result.ok)

    def test_short_206_without_content_length_rejects_extra_body_bytes(self):
        response = Response(206, headers={"Content-Range": "bytes 0-999/1000"}, body_size=1001)
        result, _, _ = self.run_probe([response], size=1000)
        self.assertIsNone(result.ok)
        self.assertEqual(result.bytes_received, 1001)

    def test_real_urllib3_stream_reports_incomplete_content_length(self):
        response = Response(206, headers={"Content-Range": "bytes 0-1023/900000", "Content-Length": "1024"})
        response.raw = HTTPResponse(body=io.BytesIO(b"x" * 500), headers=response.headers,
                                    preload_content=False, enforce_content_length=True)
        result, _, _ = self.run_probe([response])
        self.assertIsNone(result.ok)
        self.assertEqual(result.bytes_received, 500)

    def test_invalid_length_encoding_and_nonempty_zero_file_are_gray(self):
        for size, headers, body_size in (
            (1000, {"Content-Length": "900"}, 900), (1000, {"Content-Length": "invalid"}, 1000),
            (1000, {}, 1001), (0, {"Content-Length": "0"}, 1),
            (None, {"Content-Length": "0"}, 1),
            (900000, {"Content-Range": "bytes 0-1023/900000"}, 1024),
            (900000, {"Content-Encoding": "gzip"}, 1024),
        ):
            with self.subTest(size=size, headers=headers):
                result, _, _ = self.run_probe([Response(200, headers=headers, body_size=body_size)], size=size)
                self.assertIsNone(result.ok)
                self.assertTrue(result.failure_reason)

    def test_early_eof_after_500_bytes_is_gray(self):
        for code, headers in ((206, {"Content-Range": "bytes 0-1023/900000"}),
                              (200, {"Content-Length": "900000"})):
            with self.subTest(code=code):
                response = Response(code, headers=headers, body_size=500)
                result, _, _ = self.run_probe([response])
                self.assertIsNone(result.ok)
                self.assertEqual(result.bytes_received, 500)
                self.assertTrue(result.failure_reason)

    def test_incomplete_http_framing_is_gray(self):
        for error in (ProtocolError("broken", IncompleteRead(500, 524)),
                      mod.requests.exceptions.ChunkedEncodingError("broken")):
            with self.subTest(error=type(error).__name__):
                response = Response(206, headers={"Content-Range": "bytes 0-1023/900000"},
                                    body_size=1024, fail_after=500, error=error)
                result, _, _ = self.run_probe([response])
                self.assertIsNone(result.ok)
                self.assertEqual(result.bytes_received, 500)

    def test_partial_bytes_in_read_exception_are_counted(self):
        for error in (mod.IncompleteRead(b"x" * 500, 524),
                      ProtocolError("broken", mod.IncompleteRead(b"x" * 500, 524))):
            with self.subTest(error=type(error).__name__):
                response = Response(206, headers={"Content-Range": "bytes 0-1023/900000"},
                                    fail_after=0, error=error)
                result, _, _ = self.run_probe([response])
                self.assertIsNone(result.ok)
                self.assertEqual(result.bytes_received, 500)
                self.assertEqual(result.bytes_requested, 1024)
                self.assertTrue(result.failure_reason)

    def test_timeouts_and_network_errors_retry_then_fail(self):
        for error in (mod.requests.Timeout("signed-url?token=secret"),
                      mod.requests.ConnectionError("signed-url?token=secret"),
                      ReadTimeoutError(None, "signed-url?token=secret", "timeout")):
            with self.subTest(error=type(error).__name__):
                responses = [Response(206, headers={"Content-Range": "bytes 0-1023/900000"},
                                      body_size=1024, fail_after=500, error=error)
                             for _ in range(mod.MAX_ATTEMPTS)]
                result, api, cdn = self.run_probe(responses)
                self.assertIs(result.ok, False)
                self.assertEqual(result.http_code, 206)
                self.assertEqual(result.bytes_received, 500)
                self.assertEqual(len(api.requests), mod.MAX_ATTEMPTS)
                self.assertEqual(len(cdn.requests), mod.MAX_ATTEMPTS)
                self.assertNotIn("secret", result.note)
                self.assertNotIn("signed-url", result.failure_reason)

    def test_network_retry_restarts_from_zero_with_a_fresh_url(self):
        responses = [Response(206, headers={"Content-Range": "bytes 0-1023/900000"},
                              body_size=1024, fail_after=500, error=mod.requests.ConnectionError()),
                     Response(206, headers={"Content-Range": "bytes 0-1023/900000"}, body_size=1024)]
        result, api, cdn = self.run_probe(responses)
        self.assertIs(result.ok, True)
        self.assertEqual(result.bytes_received, 1024)
        self.assertEqual(len(api.requests), 2)
        self.assertNotEqual(cdn.requests[0][0][0], cdn.requests[1][0][0])
        self.assertTrue(all(request[1]["headers"]["Range"] == "bytes=0-1023" for request in cdn.requests))

    def test_download_url_requests_a_fresh_link_without_reading_the_file(self):
        file = mod.DiskFile("имя.txt", "disk:/папка/имя.txt", 10)
        links = ["https://downloader.disk.yandex.ru/x?attempt=1",
                 "https://downloader.disk.yandex.ru/x?attempt=2"]
        with patch.object(self.probe, "_api_get", side_effect=[{"href": href} for href in links]) as get, \
                patch.object(self.probe, "_probe_cdn") as cdn:
            self.assertEqual([self.probe.download_url(file) for _ in links], links)
        self.assertEqual([call.args for call in get.call_args_list],
                         [("/resources/download", {"path": file.path})] * 2)
        cdn.assert_not_called()

    def test_download_url_rejects_invalid_links_and_honors_stop(self):
        file = mod.DiskFile("x", "disk:/x", 1)
        for href in (None, "http://example.org/x", "https://user:secret@example.org/x", "https://["):
            with self.subTest(href=href), patch.object(self.probe, "_api_get", return_value={"href": href}):
                with self.assertRaisesRegex(ValueError, "корректную HTTPS-ссылку"):
                    self.probe.download_url(file)
        self.probe.stop.set()
        with patch.object(self.probe, "_api_get") as get, self.assertRaises(mod.ScanStopped):
            self.probe.download_url(file)
        get.assert_not_called()

    def test_api_connection_failures_retry_then_fail(self):
        for error in (mod.requests.Timeout("secret-url"), mod.requests.ConnectionError("secret-url")):
            with self.subTest(error=type(error).__name__), \
                    patch.object(mod.requests.Session, "get", side_effect=error) as get, \
                    patch.object(self.probe, "_sleep"):
                result = self.probe.check_file(mod.DiskFile("x", "disk:/x", 1))
            self.assertIs(result.ok, False)
            self.assertEqual(result.note, "Сеть или API недоступны")
            self.assertEqual(get.call_count, mod.MAX_ATTEMPTS)

    def test_cancel_during_final_network_attempt_stays_stopped(self):
        for api_failure in (True, False):
            self.probe.stop.clear()

            def get(url, **kwargs):
                if not api_failure and url.endswith("/resources/download"):
                    return Response(200, {"href": "https://downloader.disk.yandex.ru/x"})
                self.probe.stop.set()
                raise mod.requests.Timeout("secret-url")

            with self.subTest(api=api_failure), patch.object(mod, "MAX_ATTEMPTS", 1), \
                    patch.object(mod.requests.Session, "get", side_effect=get):
                result = self.probe.check_file(mod.DiskFile("x", "disk:/x", 10))
            self.assertIsNone(result.ok)
            self.assertEqual(result.note, "Остановлено")

    def test_stop_during_read_preserves_partial_statistics(self):
        api = FakeSession([Response(200, {"href": "https://downloader.disk.yandex.ru/x"})])
        response = Response(206, headers={"Content-Range": "bytes 0-1023/900000"}, body_size=1024,
                            on_read=self.probe.stop.set, max_chunk_size=500)
        cdn = FakeSession([response])
        self.probe._session = lambda **kw: api if kw["api"] else cdn
        result = self.probe.check_file(mod.DiskFile("x", "disk:/x", 900000))
        self.assertIsNone(result.ok)
        self.assertEqual(result.bytes_received, 500)
        self.assertEqual(result.failure_reason, "Остановлено")
        self.assertEqual(len(response.raw.read_sizes), 1)
        self.assertTrue(response.raw.closed)
        self.assertEqual(self.probe._limiter._active, 0)

    def test_http_refusals_are_red_and_server_failures_are_gray(self):
        for code in (403, 404, 500, 501, 502, 503, 504):
            with self.subTest(code=code):
                attempts = mod.MAX_ATTEMPTS if code >= 500 else 1
                result, api, _ = self.run_probe([Response(code) for _ in range(attempts)])
                self.assertIs(result.ok, None if code >= 500 else False)
                self.assertEqual(result.http_code, code)
                self.assertEqual(result.bytes_received, 0)
                self.assertIn(str(code), result.failure_reason)
                self.assertEqual(len(api.requests), attempts)

    def test_api_refusals_and_server_failures_keep_their_http_code(self):
        for code in (403, 404, 416, 500, 501, 503):
            with self.subTest(code=code):
                attempts = mod.MAX_ATTEMPTS if code >= 500 else 1
                responses = [Response(code, {"message": "secret-signed-url"}, headers={"Retry-After": "0"})
                             for _ in range(attempts)]
                api = FakeSession(responses)
                cdn = FakeSession([])
                self.probe._session = lambda **kw: api if kw["api"] else cdn
                with patch.object(self.probe, "_sleep"):
                    result = self.probe.check_file(mod.DiskFile("x", "disk:/x", 900000))
                self.assertIs(result.ok, False if code in (403, 404) else None)
                self.assertEqual(result.http_code, code)
                self.assertEqual(result.bytes_requested, 1024)
                self.assertEqual(result.bytes_received, 0)
                self.assertIn(str(code), result.failure_reason)
                self.assertNotIn("secret-signed-url", result.note)
                self.assertTrue(all(response.raw.closed for response in responses))
                self.assertEqual(len(api.requests), attempts)
                self.assertEqual(cdn.requests, [])

    def test_cdn_500_retry_after_is_used_after_response_is_closed(self):
        responses = [Response(500, headers={"Retry-After": "90"}),
                     Response(200, headers={"Content-Length": "1000"}, body_size=1000)]
        api = FakeSession([Response(200, {"href": "https://downloader.disk.yandex.ru/x"}) for _ in responses])
        cdn = FakeSession(responses)
        self.probe._session = lambda **kw: api if kw["api"] else cdn

        def sleep(seconds):
            self.assertEqual(seconds, 90)
            self.assertTrue(responses[0].raw.closed)
            self.assertEqual(self.probe._limiter._active, 0)

        with patch.object(self.probe, "_sleep", side_effect=sleep) as pause:
            result = self.probe.check_file(mod.DiskFile("x", "disk:/x", 1000))
        self.assertIs(result.ok, True)
        pause.assert_called_once_with(90)

    def test_api_network_failure_is_error_and_server_text_is_not_saved(self):
        for error, ok in ((mod.DiskAPIError(None, "fictional-test-token signed-url"), False),
                          (mod.DiskAPIError(500, "fictional-test-token signed-url"), None)):
            with self.subTest(code=error.code), patch.object(self.probe, "_api_get", side_effect=error):
                result = self.probe.check_file(mod.DiskFile("x", "disk:/x", 1))
                self.assertIs(result.ok, ok)
                self.assertNotIn("fictional-test-token", result.failure_reason)
                self.assertNotIn("signed-url", result.note)

    def test_custom_probe_limit_controls_request_and_read_size(self):
        for limit in (1, 1000, 65537):
            with self.subTest(limit=limit):
                response = Response(200, body_size=900000)
                result, _, cdn = self.run_probe([response], probe_bytes=limit)
                self.assertIs(result.ok, True)
                self.assertEqual(result.bytes_requested, limit)
                self.assertEqual(result.bytes_received, limit)
                self.assertEqual(response.raw.position, limit)
                self.assertEqual(cdn.requests[0][1]["headers"]["Range"], f"bytes=0-{limit - 1}")

    def test_empty_file_416_fallback_closes_both_responses(self):
        for size in (0, None):
            with self.subTest(size=size):
                responses = [Response(416, headers={"Content-Range": "bytes */0"}),
                             Response(200, headers={"Content-Length": "0"})]
                result, api, cdn = self.run_probe(responses, size=size)
                self.assertIs(result.ok, True)
                self.assertEqual(result.bytes_received, 0)
                self.assertEqual(len(api.requests), 1)
                self.assertEqual(len(cdn.requests), 2)
                self.assertEqual(responses[0].raw.read_sizes, [])
                self.assertTrue(responses[1].raw.read_sizes)
                self.assertEqual(cdn.requests[1][1]["headers"], {"Accept-Encoding": "identity"})
                self.assertEqual(result.bytes_requested, 0)

    def test_empty_416_cannot_hide_nonempty_fallback_body(self):
        result, _, _ = self.run_probe([
            Response(416, headers={"Content-Range": "bytes */0"}),
            Response(200, body_size=1),
        ], size=None)
        self.assertIsNone(result.ok)
        self.assertEqual(result.bytes_requested, 0)
        self.assertEqual(result.bytes_received, 1)

    def test_duration_covers_link_request_and_transfer(self):
        now = [10.0]
        response = Response(200, headers={"Content-Length": "1000"}, body_size=1000,
                            on_read=lambda: now.__setitem__(0, 10.125))
        api = FakeSession([Response(200, {"href": "https://downloader.disk.yandex.ru/x"})])
        cdn = FakeSession([response])
        self.probe._session = lambda **kw: api if kw["api"] else cdn
        with patch.object(mod.time, "monotonic", side_effect=lambda: now[0]):
            result = self.probe.check_file(mod.DiskFile("x", "disk:/x", 1000))
        self.assertEqual(result.duration_ms, 125)

    def test_404_is_red(self):
        api = FakeSession([Response(200, {"href": "https://downloader.disk.yandex.ru/x"})])
        cdn = FakeSession([Response(404)])
        self.probe._session = lambda **kw: api if kw["api"] else cdn
        result = self.probe.check_file(mod.DiskFile("x", "disk:/x", 100))
        self.assertIs(result.ok, False)
        self.assertIn("404", result.note)

    def test_invalid_or_nonempty_416_is_gray_without_fallback(self):
        for size, content_range in ((0, None), (0, "invalid"), (0, "bytes */10"),
                                    (None, "bytes */10"), (10, "bytes */0"), (10, "bytes */10")):
            with self.subTest(size=size, content_range=content_range):
                headers = {} if content_range is None else {"Content-Range": content_range}
                result, _, cdn = self.run_probe([Response(416, headers=headers)], size=size)
                self.assertIsNone(result.ok)
                self.assertEqual(result.http_code, 416)
                self.assertEqual(result.bytes_received, 0)
                self.assertIn("416", result.failure_reason)
                self.assertEqual(len(cdn.requests), 1)

    def test_range_fallback_rejects_login_page(self):
        api = FakeSession([Response(200, {"href": "https://downloader.disk.yandex.ru/x"})])
        cdn = FakeSession([Response(416, headers={"Content-Range": "bytes */0"}),
                           Response(200, url="https://passport.yandex.ru/auth")])
        self.probe._session = lambda **kw: api if kw["api"] else cdn
        result = self.probe.check_file(mod.DiskFile("x", "disk:/x", 0))
        self.assertIs(result.ok, False)

    def test_invalid_link_is_not_contacted(self):
        api = FakeSession([Response(200, {"href": "http://insecure.invalid/file"})])
        cdn = FakeSession([])
        self.probe._session = lambda **kw: api if kw["api"] else cdn
        result = self.probe.check_file(mod.DiskFile("x", "disk:/x", 12))
        self.assertIsNone(result.ok)
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

    def test_retry_after_seconds_and_http_date_are_not_capped(self):
        with patch.object(mod, "datetime") as clock, patch.object(mod.random, "uniform") as jitter:
            clock.now.return_value = datetime(2026, 10, 22, 12, tzinfo=timezone.utc)
            self.assertEqual(self.probe._delay(0, "90"), 90)
            self.assertEqual(self.probe._delay(0, "Thu, 22 Oct 2026 12:02:00 GMT"), 120)
            self.assertEqual(self.probe._delay(0, "Thu, 22 Oct 2026 11:59:00 GMT"), 0)
            jitter.assert_not_called()

    def test_backoff_grows_with_jitter_for_missing_or_invalid_header(self):
        with patch.object(mod.random, "uniform", side_effect=lambda low, high: high) as jitter:
            self.assertEqual([self.probe._delay(attempt) for attempt in range(3)], [0.75, 1.5, 3])
            for header in ("invalid", "NaN", "inf"):
                self.assertEqual(self.probe._delay(0, header), 0.75)
        self.assertEqual(jitter.call_args_list[0].args, (0.375, 0.75))
        self.assertEqual(jitter.call_args_list[1].args, (0.75, 1.5))

    def test_api_and_cdn_429_retries_are_bounded_and_inconclusive(self):
        for endpoint in ("api", "cdn", "range fallback"):
            with self.subTest(endpoint=endpoint):
                probe = mod.DiskProbe("fictional-test-token")
                limited = lambda: Response(429, headers={"Retry-After": "0"})
                api = FakeSession([limited() for _ in range(mod.MAX_ATTEMPTS)] if endpoint == "api" else
                                  [Response(200, {"href": "https://downloader.disk.yandex.ru/x"})
                                   for _ in range(mod.MAX_ATTEMPTS)])
                cdn = FakeSession([response for _ in range(mod.MAX_ATTEMPTS)
                                   for response in ([Response(416, headers={"Content-Range": "bytes */0"}), limited()] if endpoint == "range fallback"
                                                    else [limited()])])
                probe._session = lambda **kw: api if kw["api"] else cdn
                result = probe.check_file(mod.DiskFile("x", "disk:/x", 0 if endpoint == "range fallback" else 1))
                self.assertIsNone(result.ok)
                self.assertEqual(result.http_code, 429)
                self.assertIn("HTTP 429", result.note)
                self.assertEqual(len(api.requests), mod.MAX_ATTEMPTS)
                if endpoint != "api":
                    self.assertEqual(len(cdn.requests), mod.MAX_ATTEMPTS * (2 if endpoint == "range fallback" else 1))

    def test_simultaneous_429_reduce_shared_limit_to_one(self):
        barrier = threading.Barrier(6)
        lock = threading.Lock()
        api_calls = 0

        def get(url, **kwargs):
            nonlocal api_calls
            if url.endswith("/resources/download"):
                with lock:
                    api_calls += 1
                    number = api_calls
                if number <= 6:
                    barrier.wait(timeout=3)
                    return Response(429, headers={"Retry-After": "0"})
                self.assertLessEqual(self.probe._limiter._active, self.probe._limiter.limit)
                return Response(200, {"href": "https://downloader.disk.yandex.ru/x"})
            return Response(206, headers={"Content-Range": "bytes 0-0/1"}, body_size=1)

        with patch.object(mod.requests.Session, "get", side_effect=get), \
                ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(self.probe.check_file, [mod.DiskFile("x", f"disk:/{i}", 1) for i in range(6)]))
        self.assertTrue(all(result.ok is True for result in results))
        self.assertEqual(api_calls, 12)
        self.assertEqual(self.probe._limiter.limit, 1)

    def test_stop_interrupts_shared_retry_after_wait(self):
        waiting = threading.Event()
        original_wait = self.probe._limiter._condition.wait

        def wait(timeout):
            waiting.set()
            return original_wait(timeout)

        with patch.object(mod.requests.Session, "get", return_value=Response(429, headers={"Retry-After": "3600"})) as get, \
                patch.object(self.probe._limiter._condition, "wait", side_effect=wait), \
                ThreadPoolExecutor(max_workers=6) as pool:
            futures = [pool.submit(self.probe.check_file, mod.DiskFile("x", f"disk:/{i}", 1)) for i in range(6)]
            try:
                self.assertTrue(waiting.wait(3))
            finally:
                self.probe.stop.set()
            results = [future.result(timeout=1) for future in futures]
            calls = get.call_count
        self.assertTrue(all(result.ok is None and result.note == "Остановлено" for result in results))
        self.assertLessEqual(calls, 6)


class AdaptiveLimiterTests(unittest.TestCase):
    def test_initial_limit_respects_user_maximum(self):
        self.assertEqual(mod.DEFAULT_WORKERS, 6)
        for maximum in (1, 6, 12):
            limiter = mod.AdaptiveLimiter(maximum, threading.Event())
            self.assertEqual(limiter.limit, min(6, maximum))
            self.assertEqual(limiter.maximum, maximum)

    def test_cooldown_is_shared_and_cannot_be_shortened(self):
        with patch.object(mod.time, "monotonic", return_value=100) as clock:
            limiter = mod.AdaptiveLimiter(6, threading.Event())
            limiter.feedback(429, 90, 0)
            clock.return_value = 101
            limiter.feedback(429, 1, 0)
            self.assertEqual(limiter._blocked_until, 190)
            self.assertEqual(limiter.limit, 1)

    def test_recovery_needs_stable_time_and_successes_and_stops_at_maximum(self):
        with patch.object(mod.time, "monotonic", return_value=100) as clock:
            limiter = mod.AdaptiveLimiter(6, threading.Event())
            old_generation = limiter.acquire()
            limiter.feedback(429, 0, old_generation)
            limiter.release()
            for _ in range(3):
                generation = limiter.acquire()
                limiter.feedback(200, 0, generation)
                limiter.release()
            self.assertEqual(limiter.limit, 3)
            clock.return_value = 105
            limiter.feedback(206, 0, old_generation)
            self.assertEqual(limiter.limit, 3)
            limiter.feedback(206, 0, generation)
            self.assertEqual(limiter.limit, 4)
            clock.return_value = 110
            for _ in range(3):
                limiter.feedback(200, 0, generation)
            self.assertEqual(limiter.limit, 4)
            limiter.feedback(200, 0, generation)
            self.assertEqual(limiter.limit, 5)
            for now in (115, 120):
                clock.return_value = now
                for _ in range(6):
                    limiter.feedback(200, 0, generation)
            self.assertEqual(limiter.limit, 6)

    def test_stop_interrupts_capacity_wait(self):
        stop = threading.Event()
        limiter = mod.AdaptiveLimiter(1, stop)
        limiter.acquire()
        waiting = threading.Event()
        original_wait = limiter._condition.wait

        def wait(timeout):
            waiting.set()
            return original_wait(timeout)

        with patch.object(limiter._condition, "wait", side_effect=wait), ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(limiter.acquire)
            try:
                self.assertTrue(waiting.wait(3))
            finally:
                stop.set()
            with self.assertRaises(mod.ScanStopped):
                future.result(timeout=1)
        limiter.release()


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
            history.merge_files(self.files)

    def drain(self):
        events = []
        while not self.events.empty():
            events.append(self.events.get_nowait())
        return events

    def record_run(self):
        with mod.HistoryStore(self.database) as history:
            run_id = history.start_run("full", 2, 2)
            history.record(self.files[0].path, mod.CheckResult(True, "HTTP 206", 206,
                           10, 10, 125, True), run_id)
            history.record(self.files[1].path, mod.CheckResult(False, "HTTP 403", 403,
                           20, 0, 250, None, "HTTP 403"), run_id)
            history.finish_run(run_id, "completed")

    def saved_rows(self):
        with contextlib.closing(sqlite3.connect(self.database)) as connection:
            return {table: list(connection.execute(f"SELECT * FROM {table} ORDER BY 1"))
                    for table in ("files", "checks", "check_runs")}

    def test_cli_and_gui_default_to_six_and_keep_worker_overrides(self):
        with patch.dict(mod.os.environ, {"YANDEX_DISK_TOKEN": "fictional-test-token"}), \
                patch.object(mod, "run_cli", return_value=0) as cli, \
                patch.object(mod, "run_gui", return_value=0) as gui:
            for arguments, workers in (([], 6), (["--workers", "1"], 1), (["--workers", "12"], 12)):
                with patch.object(mod.sys, "argv", ["yaDiskHealth.py", *arguments]):
                    self.assertEqual(mod.main(), 0)
                gui.assert_called_with(workers, probe_bytes=1024)
                with patch.object(mod.sys, "argv", ["yaDiskHealth.py", "--cli", "check", *arguments]):
                    self.assertEqual(mod.main(), 0)
                cli.assert_called_with("fictional-test-token", workers, "check", [], probe_bytes=1024,
                                       mode=None, sample_percent=None, sample_count=None, seed=None)
            for workers in (0, 13):
                with patch.object(mod.sys, "argv", ["yaDiskHealth.py", "--workers", str(workers)]), \
                        contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
                    mod.main()
                self.assertEqual(raised.exception.code, 2)

    def test_probe_bytes_cli_option_is_forwarded_and_validated(self):
        with patch.dict(mod.os.environ, {"YANDEX_DISK_TOKEN": "fictional-test-token"}), \
                patch.object(mod, "run_cli", return_value=0) as cli:
            with patch.object(mod.sys, "argv", ["yaDiskHealth.py", "--cli", "check", "--probe-bytes", "1234"]):
                self.assertEqual(mod.main(), 0)
            cli.assert_called_once_with("fictional-test-token", 6, "check", [], probe_bytes=1234,
                                        mode=None, sample_percent=None, sample_count=None, seed=None)
            for value in ("0", "-1", "invalid"):
                with patch.object(mod.sys, "argv", ["yaDiskHealth.py", "--cli", "check", "--probe-bytes", value]), \
                        contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
                    mod.main()
                self.assertEqual(raised.exception.code, 2)

    def test_default_scan_has_six_parallel_requests_without_reindexing(self):
        files = [mod.DiskFile(str(i), f"disk:/{i}", 1) for i in range(18)]
        release = threading.Event()
        started = threading.Event()
        lock = threading.Lock()
        active = peak = 0

        def get(url, **kwargs):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
                if active == 6:
                    started.set()
            try:
                if not release.wait(3):
                    raise AssertionError("Requests were not released")
                if url.endswith("/resources/download"):
                    return Response(200, {"href": "https://downloader.disk.yandex.ru/x"})
                return Response(206, headers={"Content-Range": "bytes 0-0/1"}, body_size=1)
            finally:
                with lock:
                    active -= 1

        with patch.object(mod.requests.Session, "get", side_effect=get), \
                patch.object(mod.DiskProbe, "iter_files") as listing:
            thread = threading.Thread(target=mod.scan, args=("fictional-test-token", files, self.events, self.stop),
                                      kwargs={"history_path": self.database})
            thread.start()
            try:
                self.assertTrue(started.wait(3))
                self.assertEqual(peak, 6)
            finally:
                release.set()
                thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
        listing.assert_not_called()
        self.assertEqual(peak, 6)
        with mod.HistoryStore(self.database) as history:
            self.assertEqual(len(history.entries()), len(files))
            self.assertTrue(all(entry.result.ok is True for entry in history.entries()))

    def test_cli_429_keeps_history_catalog_and_unknown_status(self):
        with mod.HistoryStore(self.database) as history:
            previous = history.record(self.files[0].path, self.good)
        with patch.object(mod.requests.Session, "get", side_effect=lambda *args, **kwargs:
                          Response(429, headers={"Retry-After": "0"})) as get, \
                patch.object(mod.DiskProbe, "iter_files") as listing, \
                contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(mod.run_cli("fictional-test-token", 6, "check", ["disk:/a"], self.database), 0)
        listing.assert_not_called()
        self.assertEqual(get.call_count, mod.MAX_ATTEMPTS)
        self.assertIn("[UNKNOWN]", output.getvalue())
        self.assertIn("HTTP 429", output.getvalue())
        with mod.HistoryStore(self.database) as history:
            entries = history.entries()
            self.assertEqual(history.files(), self.files)
            self.assertEqual(len(entries), 2)
            self.assertEqual(entries[0], previous)
            self.assertIsNone(entries[1].result.ok)
            self.assertEqual(entries[1].result.http_code, 429)
            self.assertEqual(entries[1].result.bytes_requested, 10)
            self.assertEqual(entries[1].result.bytes_received, 0)
            self.assertGreaterEqual(entries[1].result.duration_ms, 0)
            self.assertTrue(datetime.fromisoformat(entries[1].checked_at).tzinfo)
            history.record(self.files[0].path, self.good)
            rows = list(mod.history_rows(history.entries()))
            self.assertEqual(rows[1][2], "Не подтверждён")
            self.assertEqual([row[3] for row in rows], ["", "", ""])

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
        self.record_run()
        before = self.saved_rows()

        def broken_listing(_client):
            yield mod.DiskFile("partial", "disk:/partial", 1)
            raise mod.DiskAPIError(500, "temporary failure")

        with patch.object(mod.DiskProbe, "iter_files", broken_listing):
            mod.load_files("fictional-test-token", self.events, self.stop, self.database)
        self.assertIn("500", self.drain()[-1][1])
        self.assertEqual(self.saved_rows(), before)

    def test_cancelled_listing_keeps_previous_catalog(self):
        self.record_run()
        before = self.saved_rows()

        def cancelled_listing(_client):
            yield mod.DiskFile("partial", "disk:/partial", 1)
            self.stop.set()

        with patch.object(mod.DiskProbe, "iter_files", cancelled_listing):
            mod.load_files("fictional-test-token", self.events, self.stop, self.database)
        self.assertEqual(self.drain()[-1], ("finished", None))
        self.assertEqual(self.saved_rows(), before)

    def test_complete_empty_listing_preserves_catalog_history_and_runs(self):
        self.record_run()
        before = self.saved_rows()
        with patch.object(mod.DiskProbe, "iter_files", return_value=iter([])):
            mod.load_files("fictional-test-token", self.events, self.stop, self.database)
        self.assertEqual(self.drain(), [("finished", None)])
        self.assertEqual(self.saved_rows(), before)

    def test_repeated_listing_merges_metadata_and_preserves_history_and_runs(self):
        self.record_run()
        self.record_run()
        before = self.saved_rows()
        updated = mod.DiskFile("renamed", self.files[0].path, 100)
        added = mod.DiskFile("new", "disk:/new", 30)
        for _ in range(2):
            with patch.object(mod.DiskProbe, "iter_files", return_value=iter([updated, added])):
                mod.load_files("fictional-test-token", self.events, self.stop, self.database)
            self.assertEqual(self.drain()[-1], ("finished", None))
            with mod.HistoryStore(self.database) as history:
                self.assertEqual(history.files(), [updated, self.files[1], added])
                self.assertEqual(history.select_files("full").files, history.files())
                self.assertEqual(history.select_files(paths=[self.files[1].path]).files, self.files[1:])
                self.assertEqual(history.catalog_counts(), (3, 1, 1, 1))
            after = self.saved_rows()
            self.assertEqual(after["checks"], before["checks"])
            self.assertEqual(after["check_runs"], before["check_runs"])

    def test_listing_write_failure_rolls_back_metadata_and_new_files(self):
        self.record_run()
        with mod.HistoryStore(self.database) as history:
            history.connection.executescript("""CREATE TRIGGER reject_file BEFORE INSERT ON files
                WHEN NEW.path = 'disk:/rejected' BEGIN
                    SELECT RAISE(ABORT, 'test write failure');
                END;""")
        before = self.saved_rows()
        files = [mod.DiskFile("updated", self.files[0].path, 100),
                 mod.DiskFile("new", "disk:/new", 30),
                 mod.DiskFile("rejected", "disk:/rejected", 40)]
        with patch.object(mod.DiskProbe, "iter_files", return_value=iter(files)):
            mod.load_files("fictional-test-token", self.events, self.stop, self.database)
        self.assertEqual(self.drain()[-1], ("finished", "Не удалось сохранить список файлов"))
        self.assertEqual(self.saved_rows(), before)

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
        self.assertEqual([kind for kind, _ in events], ["result", "run", "finished"])
        with mod.HistoryStore(self.database) as history:
            self.assertEqual(history.entries(), [events[0][1]])
        self.assertNotIn(b"fictional-test-token", self.database.read_bytes())

    def test_old_database_migration_preserves_ids_catalog_and_history(self):
        legacy = Path(self.temp.name) / "legacy.sqlite3"
        with contextlib.closing(sqlite3.connect(legacy)) as connection:
            connection.executescript("""
                CREATE TABLE files (path TEXT PRIMARY KEY, name TEXT NOT NULL, size INTEGER);
                CREATE TABLE checks (id INTEGER PRIMARY KEY, path TEXT NOT NULL,
                    checked_at TEXT NOT NULL, ok INTEGER, http_code INTEGER, note TEXT NOT NULL);
                INSERT INTO files VALUES ('disk:/old', 'old', 1000);
                INSERT INTO checks VALUES (17, 'disk:/old', '2026-10-18T10:00:00+00:00', 1, 206, 'Old check');
            """)
        new_result = mod.CheckResult(None, "Преждевременное завершение передачи", 206,
                                     1024, 500, 123, True, "Преждевременное завершение передачи")
        with mod.HistoryStore(legacy) as history:
            self.assertEqual(history.files(), [mod.DiskFile("old", "disk:/old", 1000)])
            previous = history.entries()[0]
            self.assertEqual(previous.result, mod.CheckResult(True, "Old check", 206))
            self.assertIsNone(previous.result.bytes_received)
            history.record("disk:/old", new_result)
            rows = list(history.connection.execute("SELECT id, bytes_received, duration_ms FROM checks ORDER BY id"))
            self.assertEqual(rows, [(17, None, None), (18, 500, 123)])
            columns = {row[1] for row in history.connection.execute("PRAGMA table_info(checks)")}
            self.assertTrue({"bytes_requested", "bytes_received", "duration_ms", "http_code",
                             "range_supported", "failure_reason"}.issubset(columns))
        with mod.HistoryStore(legacy) as history:
            self.assertEqual(history.entries()[0], previous)
            self.assertEqual(history.entries()[1].result, new_result)
            self.assertEqual(history.entries(latest=True)[0].result, new_result)

    def test_partial_schema_migration_keeps_existing_probe_statistics(self):
        legacy = Path(self.temp.name) / "partial.sqlite3"
        with contextlib.closing(sqlite3.connect(legacy)) as connection:
            connection.executescript("""
                CREATE TABLE checks (id INTEGER PRIMARY KEY, path TEXT NOT NULL,
                    checked_at TEXT NOT NULL, ok INTEGER, note TEXT NOT NULL,
                    bytes_requested INTEGER, bytes_received INTEGER);
                INSERT INTO checks VALUES
                    (17, 'disk:/old', '2026-10-18T10:00:00+00:00', 1, 'Old probe', 500000, 500000);
            """)
        for _ in range(2):
            with mod.HistoryStore(legacy) as history:
                entry = history.entries()[0]
                self.assertIs(entry.result.ok, True)
                self.assertEqual(entry.result.bytes_requested, 500000)
                self.assertEqual(entry.result.bytes_received, 500000)
                self.assertIsNone(entry.result.http_code)
                self.assertIsNone(entry.result.duration_ms)
                self.assertEqual(history.connection.execute("SELECT id FROM checks").fetchone(), (17,))

    def test_failed_schema_migration_rolls_back_added_columns(self):
        legacy = Path(self.temp.name) / "interrupted.sqlite3"
        with contextlib.closing(sqlite3.connect(legacy)) as connection:
            connection.executescript("""
                CREATE TABLE checks (id INTEGER PRIMARY KEY, path TEXT NOT NULL,
                    checked_at TEXT NOT NULL, ok INTEGER, http_code INTEGER, note TEXT NOT NULL);
                INSERT INTO checks VALUES (17, 'disk:/old', '2026-10-18T10:00:00+00:00', 1, 206, 'Old check');
            """)

        class InterruptedMigration(sqlite3.Connection):
            def execute(self, sql, *args):
                if sql == "ALTER TABLE checks ADD COLUMN duration_ms INTEGER":
                    raise sqlite3.OperationalError("Migration interrupted")
                return super().execute(sql, *args)

        connection = sqlite3.connect(legacy, factory=InterruptedMigration)
        try:
            with patch.object(mod.sqlite3, "connect", return_value=connection), \
                    self.assertRaises(sqlite3.OperationalError):
                mod.HistoryStore(legacy)
            columns = {row[1] for row in connection.execute("PRAGMA table_info(checks)")}
            self.assertNotIn("bytes_requested", columns)
            self.assertNotIn("bytes_received", columns)
            self.assertEqual(connection.execute("SELECT id, note FROM checks").fetchone(), (17, "Old check"))
        finally:
            connection.close()
        with mod.HistoryStore(legacy) as history:
            self.assertEqual(history.entries()[0].result, mod.CheckResult(True, "Old check", 206))

    def test_network_migration_preserves_details_and_recounts_affected_runs_once(self):
        notes = ("Тайм-аут или ошибка сети", "Тайм-аут или ошибка сети при чтении",
                 "Ошибка сети при чтении", "Сеть или API недоступны")
        with mod.HistoryStore(self.database) as history:
            run_id = history.start_run("full", 6, 6)
            history.record(self.files[0].path, self.good, run_id)
            for index, note in enumerate(notes):
                history.record(self.files[0].path,
                               mod.CheckResult(None if index % 2 else False, note, 206,
                                               10, index, 125, True, note), run_id)
            history.record(self.files[1].path, mod.CheckResult(None, "HTTP 429", 429), run_id)
            history.finish_run(run_id, "completed")
            other_run = history.start_run("full", 0, 0)
            unrelated = history.finish_run(other_run, "stopped")
            with history.connection:
                history.connection.execute("PRAGMA user_version = 0")
                history.connection.execute("DROP INDEX checks_confirmed")
                history.connection.execute("CREATE INDEX checks_confirmed ON checks(path, id) WHERE ok = 1")
            before = history.connection.execute("SELECT * FROM checks ORDER BY id").fetchall()
            columns = [row[1] for row in history.connection.execute("PRAGMA table_info(checks)")]

        with mod.HistoryStore(self.database) as history:
            after = history.connection.execute("SELECT * FROM checks ORDER BY id").fetchall()
            for index, name in enumerate(columns):
                if name != "ok":
                    self.assertEqual([row[index] for row in after], [row[index] for row in before], name)
            self.assertEqual([entry.result.ok for entry in history.entries()],
                             [True, False, False, False, False, None])
            self.assertEqual(history.connection.execute("PRAGMA user_version").fetchone(), (1,))
            run = history.runs(run_id)[0]
            self.assertEqual((run.checked_count, run.success_count, run.failed_count, run.unknown_count),
                             (6, 1, 4, 1))
            self.assertEqual(history.runs(other_run)[0], unrelated)
            self.assertEqual(history.catalog_counts(), (2, 0, 1, 1))
            self.assertEqual(history.select_files("failed").files, self.files[:1])
            self.assertEqual(history.select_files("random").files, [])
            index_sql = history.connection.execute(
                "SELECT sql FROM sqlite_master WHERE name = 'checks_confirmed'").fetchone()[0]
            self.assertIn("Тайм-аут или ошибка сети", index_sql)
            self.assertEqual(list(mod.history_rows(history.entries()))[-2][2], "Ошибка")
            runs = history.runs()

        with mod.HistoryStore(self.database) as history:
            self.assertEqual(history.connection.execute("SELECT * FROM checks ORDER BY id").fetchall(), after)
            self.assertEqual(history.runs(), runs)
            self.assertEqual(history.connection.execute(
                "SELECT sql FROM sqlite_master WHERE name = 'checks_confirmed'").fetchone()[0], index_sql)

    def test_interrupted_network_migration_rolls_back_status_counts_and_version(self):
        with mod.HistoryStore(self.database) as history:
            run_id = history.start_run("full", 1, 1)
            history.record(self.files[0].path, mod.CheckResult(None, "Тайм-аут или ошибка сети"), run_id)
            history.finish_run(run_id, "completed")
            history.connection.execute("PRAGMA user_version = 0")
            with history.connection:
                history.connection.execute("DROP INDEX checks_confirmed")
                history.connection.execute("CREATE INDEX checks_confirmed ON checks(path, id) WHERE ok = 1")
            previous_index = history.connection.execute(
                "SELECT sql FROM sqlite_master WHERE name = 'checks_confirmed'").fetchone()

        class InterruptedMigration(sqlite3.Connection):
            def execute(self, sql, *args):
                if sql == "PRAGMA user_version = 1":
                    raise sqlite3.OperationalError("Migration interrupted")
                return super().execute(sql, *args)

        connection = sqlite3.connect(self.database, factory=InterruptedMigration)
        try:
            with patch.object(mod.sqlite3, "connect", return_value=connection), \
                    self.assertRaises(sqlite3.OperationalError):
                mod.HistoryStore(self.database)
            self.assertEqual(connection.execute("SELECT ok FROM checks").fetchone(), (None,))
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone(), (0,))
            self.assertEqual(connection.execute(
                "SELECT failed_count, unknown_count FROM check_runs").fetchone(), (0, 1))
            self.assertEqual(connection.execute(
                "SELECT sql FROM sqlite_master WHERE name = 'checks_confirmed'").fetchone(), previous_index)
        finally:
            connection.close()
        with mod.HistoryStore(self.database) as history:
            self.assertIs(history.entries()[0].result.ok, False)
            self.assertEqual(history.runs()[0].failed_count, 1)

    def test_all_http_outcomes_are_saved_without_catalog_requests(self):
        self.database = Path(self.temp.name) / "outcomes.sqlite3"
        cases = {
            "good": (900000, 206, True, 1024), "small": (17, 200, True, 17),
            "empty": (0, 200, True, 0), "denied": (900000, 403, False, 0),
            "missing": (900000, 404, False, 0), "range": (900000, 416, None, 0),
            "limited": (900000, 429, None, 0), "server": (900000, 500, None, 0),
            "partial": (900000, 206, None, 500), "timeout": (900000, 206, False, 500),
            "unknown": (None, 200, None, 500),
        }
        files = [mod.DiskFile(name, f"disk:/{name}", case[0]) for name, case in cases.items()]
        with mod.HistoryStore(self.database) as history:
            history.merge_files(files)

        def get(url, **kwargs):
            if url.endswith("/resources/download"):
                name = kwargs["params"]["path"].removeprefix("disk:/")
                return Response(200, {"href": "https://downloader.disk.yandex.ru/" + name})
            self.assertEqual(kwargs["headers"], {"Range": "bytes=0-1023", "Accept-Encoding": "identity"})
            self.assertIs(kwargs["stream"], True)
            name = url.rpartition("/")[2]
            size, code, _, received = cases[name]
            headers = {"Retry-After": "0"}
            if code == 206:
                headers["Content-Range"] = f"bytes 0-1023/{size}"
            elif code == 200 and size is not None:
                headers["Content-Length"] = str(size)
            options = {"fail_after": 500, "error": ReadTimeoutError(None, "secret-url", "timeout")} if name == "timeout" else {}
            return Response(code, headers=headers, body_size=1024 if name == "timeout" else received, **options)

        with patch.object(mod.requests.Session, "get", side_effect=get), \
                patch.object(mod.DiskProbe, "_sleep"), patch.object(mod.DiskProbe, "iter_files") as listing:
            mod.scan("fictional-test-token", files, self.events, self.stop, history_path=self.database)
        listing.assert_not_called()
        events = self.drain()
        self.assertEqual(events[-1], ("finished", None))
        self.assertEqual(len(events), len(files) + 2)
        with mod.HistoryStore(self.database) as history:
            self.assertEqual(history.files(), sorted(files, key=lambda file: file.path))
            entries = history.entries()
            self.assertEqual(len(entries), len(files))
            for entry in entries:
                name = entry.path.removeprefix("disk:/")
                size, code, ok, received = cases[name]
                result = entry.result
                self.assertIs(result.ok, ok, name)
                self.assertEqual(result.http_code, code, name)
                self.assertEqual(result.bytes_requested, min(size, 1024) if size is not None else 1024, name)
                self.assertEqual(result.bytes_received, received, name)
                self.assertGreaterEqual(result.duration_ms, 0)
                self.assertEqual(result.failure_reason is None, ok is True, name)
        saved = self.database.read_bytes()
        self.assertNotIn(b"fictional-test-token", saved)
        self.assertNotIn(b"https://downloader", saved)
        self.assertNotIn(b"secret-url", saved)

    def test_stop_during_transfer_is_saved_with_partial_bytes(self):
        file = mod.DiskFile("a", "disk:/a", 900000)
        response = Response(206, headers={"Content-Range": "bytes 0-1023/900000"}, body_size=1024,
                            max_chunk_size=500, on_read=self.stop.set)
        with patch.object(mod.requests.Session, "get", side_effect=[
            Response(200, {"href": "https://downloader.disk.yandex.ru/a"}), response,
        ]):
            mod.scan("fictional-test-token", [file], self.events, self.stop, 1, self.database)
        self.assertTrue(response.raw.closed)
        with mod.HistoryStore(self.database) as history:
            entry = history.entries()[0]
            self.assertIsNone(entry.result.ok)
            self.assertEqual(entry.result.bytes_requested, 1024)
            self.assertEqual(entry.result.bytes_received, 500)
            self.assertEqual(entry.result.http_code, 206)
            self.assertGreaterEqual(entry.result.duration_ms, 0)
            self.assertEqual(entry.result.failure_reason, "Остановлено")

    def test_stream_results_are_independent_and_statistics_survive_restart(self):
        files = [mod.DiskFile("a", "disk:/a", 900000), mod.DiskFile("b", "disk:/folder/b", 1000)]
        with mod.HistoryStore(self.database) as history:
            history.merge_files(files)
            previous = history.record(files[0].path, self.good)

        def get(url, **kwargs):
            if url.endswith("/resources/download"):
                self.assertIn(kwargs["params"]["path"], {file.path for file in files})
                return Response(200, {"href": "https://downloader.disk.yandex.ru/" + kwargs["params"]["path"][-1]})
            if url.endswith("/a"):
                return Response(206, headers={"Content-Range": "bytes 0-1023/900000"}, body_size=500)
            return Response(200, headers={"Content-Length": "1000"}, body_size=1000)

        with patch.object(mod.requests.Session, "get", side_effect=get), \
                patch.object(mod.DiskProbe, "iter_files") as listing, contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(mod.run_cli("fictional-test-token", 1, "check", history_path=self.database), 0)
        listing.assert_not_called()
        self.assertIn("получено: 500 / 1024 байт", output.getvalue())
        self.assertIn("получено: 1000 / 1000 байт", output.getvalue())
        self.assertIn("время:", output.getvalue())
        with mod.HistoryStore(self.database) as history:
            self.assertEqual(history.files(), files)
            entries = history.entries()
            self.assertEqual(entries[0], previous)
            self.assertEqual(len(entries), 3)
            results = {entry.path: entry.result for entry in entries[1:]}
            self.assertIsNone(results[files[0].path].ok)
            self.assertIs(results[files[1].path].ok, True)
            self.assertEqual(results[files[0].path].bytes_received, 500)
            self.assertEqual(results[files[1].path].bytes_received, 1000)
            self.assertEqual(results[files[0].path].bytes_requested, 1024)
            self.assertEqual(results[files[1].path].bytes_requested, 1000)
            self.assertTrue(all(isinstance(result.duration_ms, int) for result in results.values()))
            rows = list(mod.history_rows(entries))
            self.assertEqual(rows[1][6:10], (1024, 500, results[files[0].path].duration_ms, "Да"))
            self.assertEqual(rows[1][10], results[files[0].path].failure_reason)
        self.assertEqual(set(Path(self.temp.name).iterdir()),
                         {self.database, Path(str(self.database) + ".lock")})
        saved = self.database.read_bytes()
        for secret in (b"fictional-test-token", b"https://downloader", b"x" * 1000):
            self.assertNotIn(secret, saved)

    def test_cli_passes_custom_limit_to_the_transfer(self):
        response = Response(200, headers={"Content-Length": "10"}, body_size=10)
        with patch.object(mod.requests.Session, "get", side_effect=[
            Response(200, {"href": "https://downloader.disk.yandex.ru/a"}), response,
        ]), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(mod.run_cli("fictional-test-token", 1, "check", ["disk:/a"], self.database,
                                         probe_bytes=7), 0)
        self.assertEqual(response.raw.position, 7)
        with mod.HistoryStore(self.database) as history:
            self.assertEqual(history.entries()[0].result.bytes_requested, 7)
            self.assertEqual(history.entries()[0].result.bytes_received, 7)

    def test_cli_network_timeout_fails_and_changes_access_history(self):
        with mod.HistoryStore(self.database) as history:
            history.record("disk:/a", self.good)
        responses = [response for _ in range(mod.MAX_ATTEMPTS) for response in (
            Response(200, {"href": "https://downloader.disk.yandex.ru/a"}),
            Response(200, headers={"Content-Length": "10"}, body_size=10, fail_after=3,
                     error=ReadTimeoutError(None, "secret-signed-url", "timeout")),
        )]
        with patch.object(mod.requests.Session, "get", side_effect=responses) as get, \
                patch.object(mod.DiskProbe, "_sleep"), contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(mod.run_cli("fictional-test-token", 1, "check", ["disk:/a"], self.database), 1)
        self.assertIn("[FAIL]", output.getvalue())
        self.assertIn("получено: 3 / 10 байт", output.getvalue())
        self.assertNotIn("secret-signed-url", output.getvalue())
        self.assertEqual(get.call_count, mod.MAX_ATTEMPTS * 2)
        with mod.HistoryStore(self.database) as history:
            entries = history.entries()
            self.assertIs(entries[-1].result.ok, False)
            self.assertEqual(entries[-1].result.bytes_received, 3)
            self.assertEqual(entries[-1].result.http_code, 200)
            self.assertTrue(entries[-1].result.failure_reason)
            self.assertEqual([row[3] for row in mod.history_rows(entries)], ["", "Доступ не подтверждён"])
            self.assertEqual(history.select_files("failed").files, self.files[:1])
            self.assertEqual(history.runs()[0].failed_count, 1)

    def test_cli_ctrl_c_stops_worker_and_waits_for_completion(self):
        started = threading.Event()
        stopped = threading.Event()
        original_get = queue.Queue.get
        interrupted = False

        def get(events, *args, **kwargs):
            nonlocal interrupted
            if not interrupted:
                self.assertTrue(started.wait(2))
                interrupted = True
                raise KeyboardInterrupt()
            return original_get(events, *args, **kwargs)

        def scan(_token, _files, events, stop, *_args):
            started.set()
            if stop.wait(2):
                stopped.set()
            events.put(("finished", None))

        with patch.object(queue.Queue, "get", get), patch.object(mod, "scan", side_effect=scan), \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(mod.run_cli("fictional-test-token", 1, "check", history_path=self.database), 0)
        self.assertTrue(stopped.is_set())

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

    def test_refresh_keeps_catalog_and_history_for_files_missing_from_listing(self):
        with mod.HistoryStore(self.database) as history:
            history.record(self.files[0].path, self.good)
            history.merge_files(self.files[1:])
            self.assertEqual(len(history.entries()), 1)
            self.assertEqual(history.files(), self.files)

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
        self.database = Path(self.temp.name) / "empty.sqlite3"
        with contextlib.redirect_stderr(io.StringIO()), patch.object(mod.DiskProbe, "check_file") as check:
            self.assertEqual(mod.run_cli("fictional-test-token", 1, "check", history_path=self.database), 2)
            self.assertEqual(mod.run_cli("fictional-test-token", 1, "check", ["disk:/missing"], self.database), 2)
        check.assert_not_called()


class CheckModeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database = Path(self.temp.name) / "history.sqlite3"
        self.available = [mod.DiskFile(f"good{i}", f"disk:/good{i:02}", 10) for i in range(11)]
        self.failed = [mod.DiskFile(f"bad{i}", f"disk:/bad{i}", 10) for i in range(2)]
        self.unchecked = [mod.DiskFile(f"new{i}", f"disk:/new{i}", 10) for i in range(2)]
        self.files = sorted(self.available + self.failed + self.unchecked, key=lambda file: file.path)
        self.good = mod.CheckResult(True, "Данные получены", 200)
        self.bad = mod.CheckResult(False, "HTTP 403", 403)
        with mod.HistoryStore(self.database) as history:
            history.merge_files(self.files)
            for file in self.available:
                history.record(file.path, self.good)
            for file in self.failed:
                history.record(file.path, self.bad)
            history.record("disk:/removed", self.bad)

    def select(self, *args, **kwargs):
        with mod.HistoryStore(self.database) as history:
            return history.select_files(*args, **kwargs)

    def test_failed_excludes_available_unchecked_and_removed_files(self):
        selection = self.select("failed")
        self.assertEqual(selection.files, self.failed)
        self.assertEqual(selection.total_candidates, 2)
        with mod.HistoryStore(self.database) as history:
            self.assertEqual(history.catalog_counts(), (15, 11, 2, 2))

    def test_random_defaults_to_ten_percent_rounded_up_of_available_only(self):
        selection = self.select("random")
        self.assertEqual(selection.total_candidates, 11)
        self.assertEqual(len(selection.files), 2)
        self.assertTrue(set(selection.files).issubset(self.available))

    def test_random_fixed_count_and_percent_are_capped_by_candidates(self):
        self.assertEqual(len(self.select("random", sample_count=7).files), 7)
        self.assertEqual(len(self.select("random", sample_percent=25).files), 3)
        self.assertCountEqual(self.select("random", sample_count=100).files, self.available)
        self.assertCountEqual(self.select("random", sample_percent=100).files, self.available)

    def test_random_has_no_duplicate_paths(self):
        files = self.select("random", sample_count=10).files
        self.assertEqual(len({file.path for file in files}), 10)

    def test_seed_repeats_after_reopening_and_ordinary_checks_use_fresh_generators(self):
        first = self.select("random", sample_count=7, seed=42)
        self.assertEqual(first, self.select("random", sample_count=7, seed=42))
        self.assertNotEqual(first.files, self.select("random", sample_count=7, seed=43).files)
        with patch.object(mod.random, "Random", wraps=mod.random.Random) as generator:
            self.select("random")
            self.select("random")
        self.assertEqual(generator.call_count, 2)
        self.assertTrue(all(call.args == (None,) for call in generator.call_args_list))

    def test_full_includes_all_catalog_files_once(self):
        for mode in (None, "full"):
            selection = self.select(mode)
            self.assertEqual(selection.files, self.files)
            self.assertEqual(selection.total_candidates, 15)

    def test_last_confirmed_result_follows_id_even_with_equal_or_reversed_times(self):
        path = self.failed[0].path
        with mod.HistoryStore(self.database) as history:
            for result, expected in ((self.good, False), (self.bad, True), (self.good, False)):
                history.record(path, result)
                with history.connection:
                    history.connection.execute("UPDATE checks SET checked_at = '2099-01-01T00:00:00+00:00'")
                self.assertEqual(self.failed[0] in history.select_files("failed").files, expected)
            history.record(path, self.bad)
            self.assertIn(self.failed[0], history.select_files("failed").files)

    def test_unknown_results_preserve_both_previous_confirmed_statuses(self):
        with mod.HistoryStore(self.database) as history:
            for file in (self.available[0], self.failed[0]):
                for result in (mod.CheckResult(None, "HTTP 429", 429),
                               mod.CheckResult(None, "HTTP 503", 503)):
                    history.record(file.path, result)
            self.assertEqual(history.select_files("failed").files, self.failed)
            self.assertCountEqual(history.select_files("random", sample_percent=100).files, self.available)
            self.assertEqual(history.catalog_counts(), (15, 11, 2, 2))
            self.assertTrue(all(entry.result.ok is None for entry in history.entries(latest=True)
                                if entry.path in (self.available[0].path, self.failed[0].path)))

    def test_legacy_network_failures_are_errors_and_other_unknowns_stay_unchanged(self):
        legacy = Path(self.temp.name) / "legacy.sqlite3"
        rows = [(17, "disk:/good", "same", 1, 206, "HTTP 206"),
                (18, "disk:/bad", "same", 0, 404, "HTTP 404"),
                (19, "disk:/good", "same", 0, 429, "HTTP 429"),
                (20, "disk:/bad", "same", 0, None, "Тайм-аут или ошибка сети"),
                (21, "disk:/bad", "same", 0, 503, "HTTP 503"),
                (22, "disk:/unknown", "same", 0, None, "Ошибка HTTP-клиента"),
                (23, "disk:/unknown", "same", 0, 408, "HTTP 408"),
                (24, "disk:/unknown", "same", 0, 416, "HTTP 416"),
                (25, "disk:/unknown", "same", 0, None, "API не выдал корректную HTTPS-ссылку")]
        with contextlib.closing(sqlite3.connect(legacy)) as connection:
            connection.executescript("""CREATE TABLE files (path TEXT PRIMARY KEY, name TEXT NOT NULL, size INTEGER);
                CREATE TABLE checks (id INTEGER PRIMARY KEY, path TEXT NOT NULL, checked_at TEXT NOT NULL,
                    ok INTEGER, http_code INTEGER, note TEXT NOT NULL);
                INSERT INTO files VALUES ('disk:/good', 'good', 10), ('disk:/bad', 'bad', 10),
                    ('disk:/unknown', 'unknown', 10);""")
            connection.executemany("INSERT INTO checks VALUES (?, ?, ?, ?, ?, ?)", rows)
            connection.commit()
        for _ in range(2):
            with mod.HistoryStore(legacy) as history:
                self.assertEqual([file.path for file in history.select_files("failed").files], ["disk:/bad"])
                self.assertEqual([file.path for file in history.select_files("random", sample_count=10).files], ["disk:/good"])
                self.assertEqual(history.catalog_counts(), (3, 1, 1, 1))
                self.assertEqual(list(history.connection.execute(
                    "SELECT id, path, checked_at, ok, http_code, note FROM checks ORDER BY id")), rows)
                self.assertEqual(history.connection.execute("SELECT COUNT(run_id) FROM checks").fetchone()[0], 0)
                self.assertEqual(history.runs(), [])
                query_plan = str(history.connection.execute(
                    f"EXPLAIN QUERY PLAN SELECT f.path, c.ok {mod.CATALOG_JOIN_SQL}").fetchall())
                self.assertIn("checks_confirmed", query_plan)
                entries = history.entries()
                self.assertIs(entries[3].result.ok, False)
                self.assertTrue(all(entry.result.ok is None for entry in entries[2:] if entry != entries[3]))

    def test_cli_modes_use_mock_http_and_save_separate_run_statistics_without_reindexing(self):
        def get(url, **kwargs):
            if url.endswith("/resources/download"):
                return Response(200, {"href": "https://downloader.disk.yandex.ru/" + kwargs["params"]["path"][6:]})
            if "/bad" in url:
                return Response(403)
            return Response(200, headers={"Content-Length": "10"}, body_size=10)

        with patch.object(mod.requests.Session, "get", side_effect=get) as requests, \
                patch.object(mod.DiskProbe, "iter_files") as listing:
            for mode, options in (("failed", {}), ("random", {"sample_count": 3, "seed": 17}), ("full", {})):
                plan = self.select(mode, **options)
                with contextlib.redirect_stdout(io.StringIO()) as output:
                    code = mod.run_cli("fictional-test-token", 6, "check", history_path=self.database, mode=mode, **options)
                self.assertEqual(code, 1 if mode != "random" else 0)
                self.assertIn("длительность:", output.getvalue())
                self.assertIn("без достоверного результата:", output.getvalue())
                with mod.HistoryStore(self.database) as history:
                    run = history.runs()[-1]
                    entries = history.entries(run_id=run.id)
                    self.assertEqual(run.mode, mode)
                    self.assertEqual(run.status, "completed")
                    self.assertEqual(run.total_candidates, plan.total_candidates)
                    self.assertEqual(run.selected_count, len(plan.files))
                    self.assertEqual(run.checked_count, len(plan.files))
                    self.assertEqual(run.failed_count, 0 if mode == "random" else 2)
                    self.assertEqual(run.success_count + run.failed_count, run.checked_count)
                    self.assertEqual(run.unknown_count, 0)
                    self.assertGreaterEqual(run.duration, 0)
                    self.assertTrue(run.finished_at)
                    self.assertCountEqual([entry.path for entry in entries], [file.path for file in plan.files])
                    self.assertEqual(len({entry.path for entry in entries}), len(entries))
            listing.assert_not_called()
            self.assertFalse(any("/resources/files" in call.args[0] for call in requests.call_args_list))
        with mod.HistoryStore(self.database) as reopened:
            self.assertEqual([run.mode for run in reopened.runs()], ["failed", "random", "full"])
            self.assertEqual(len(reopened.entries(run_id=3)), 15)

    def test_empty_subset_records_completed_run_without_probe_or_http(self):
        with mod.HistoryStore(self.database) as history:
            for file in self.failed:
                history.record(file.path, self.good)
        for mode in ("failed", "random"):
            if mode == "random":
                self.database = Path(self.temp.name) / "unchecked.sqlite3"
                with mod.HistoryStore(self.database) as history:
                    history.merge_files(self.unchecked)
            with patch.object(mod, "DiskProbe") as probe, patch.object(mod.requests.Session, "get") as get, \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(mod.run_cli("", 6, "check", history_path=self.database, mode=mode), 0)
            probe.assert_not_called()
            get.assert_not_called()
            with mod.HistoryStore(self.database) as history:
                run = history.runs()[-1]
                self.assertEqual((run.total_candidates, run.selected_count, run.checked_count), (0, 0, 0))
                self.assertEqual((run.success_count, run.failed_count, run.unknown_count), (0, 0, 0))
                self.assertEqual(run.status, "completed")
                self.assertEqual(history.entries(run_id=run.id), [])

    def test_stopped_transfer_saves_partial_run_and_does_not_start_remaining_files(self):
        events, stop = queue.Queue(), threading.Event()
        response = Response(200, headers={"Content-Length": "10"}, body_size=10,
                            max_chunk_size=3, on_read=stop.set)
        with patch.object(mod.requests.Session, "get", side_effect=[
            Response(200, {"href": "https://downloader.disk.yandex.ru/a"}), response,
        ]) as get:
            mod.scan("fictional-test-token", self.files, events, stop, 1, self.database)
        self.assertEqual(get.call_count, 2)
        with mod.HistoryStore(self.database) as history:
            run = history.runs()[0]
            self.assertEqual(run.status, "stopped")
            self.assertEqual((run.total_candidates, run.selected_count, run.checked_count), (15, 15, 1))
            self.assertEqual((run.success_count, run.failed_count, run.unknown_count), (0, 0, 1))
            entry = history.entries(run_id=run.id)[0]
            self.assertEqual(entry.result.bytes_received, 3)
            self.assertEqual(entry.result.failure_reason, "Остановлено")
            self.assertEqual(history.select_files("failed").files, self.failed)
        stop = threading.Event()
        stop.set()
        with patch.object(mod.requests.Session, "get") as get:
            mod.scan("", self.files, events, stop, history_path=self.database)
        get.assert_not_called()
        with mod.HistoryStore(self.database) as history:
            self.assertEqual(history.runs()[-1].status, "stopped")
            self.assertEqual(history.runs()[-1].checked_count, 0)

    def test_run_statistics_separate_access_refusal_rate_limit_and_timeout(self):
        self.database = Path(self.temp.name) / "outcomes.sqlite3"
        files = [mod.DiskFile(name, "disk:/" + name, 10) for name in ("good", "bad", "limited", "timeout")]
        with mod.HistoryStore(self.database) as history:
            history.merge_files(files)

        def get(url, **kwargs):
            if url.endswith("/resources/download"):
                return Response(200, {"href": "https://downloader.disk.yandex.ru/" + kwargs["params"]["path"][6:]})
            if url.endswith("/bad"):
                return Response(404)
            if url.endswith("/limited"):
                return Response(429, headers={"Retry-After": "0"})
            if url.endswith("/timeout"):
                raise mod.requests.Timeout("secret-url")
            return Response(200, headers={"Content-Length": "10"}, body_size=10)

        with patch.object(mod.requests.Session, "get", side_effect=get), patch.object(mod.DiskProbe, "_sleep"), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(mod.run_cli("fictional-test-token", 1, "check", history_path=self.database), 1)
        with mod.HistoryStore(self.database) as history:
            run = history.runs()[0]
            self.assertEqual((run.checked_count, run.success_count, run.failed_count, run.unknown_count), (4, 1, 2, 1))
            self.assertEqual(run.mode, "full")
            self.assertEqual(history.catalog_counts(), (4, 1, 2, 1))
            self.assertEqual(len(history.entries(run_id=run.id)), 4)
        self.assertNotIn("secret-url", output.getvalue())

    def test_duplicate_manual_paths_are_checked_once_and_legacy_cli_is_full(self):
        with patch.object(mod.DiskProbe, "check_file", return_value=self.good) as check, \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(mod.run_cli("token", 1, "check", [self.files[0].path] * 2, self.database), 0)
            check.assert_called_once_with(self.files[0])
            with mod.HistoryStore(self.database) as history:
                self.assertEqual(history.runs()[-1].mode, "manual")
            check.reset_mock()
            self.assertEqual(mod.run_cli("token", 1, "check", history_path=self.database), 0)
            self.assertCountEqual([call.args[0] for call in check.call_args_list], self.files)

    def test_cli_parses_modes_sampling_seed_and_rejects_conflicting_options(self):
        with patch.dict(mod.os.environ, {"YANDEX_DISK_TOKEN": "token"}), patch.object(mod, "run_cli", return_value=0) as cli:
            with patch.object(mod.sys, "argv", ["yaDiskHealth.py", "--cli", "check", "--mode", "random",
                                               "--sample-count", "7", "--seed", "42"]):
                self.assertEqual(mod.main(), 0)
            self.assertEqual(cli.call_args.kwargs, {"probe_bytes": 1024, "mode": "random",
                                                   "sample_percent": None, "sample_count": 7, "seed": 42})
        invalid = (("--mode", "bad"), ("--mode", "full", "--seed", "1"),
                   ("--mode", "failed", "--sample-count", "1"),
                   ("--mode", "full", "--path", "disk:/a"),
                   ("--mode", "random", "--sample-count", "3", "--sample-percent", "10"),
                   ("--mode", "random", "--sample-count", "0"),
                   ("--mode", "random", "--sample-count", "-1"),
                   *(("--mode", "random", "--sample-percent", value) for value in ("0", "101", "nan", "inf")))
        for arguments in invalid:
            with self.subTest(arguments=arguments), \
                    patch.object(mod.sys, "argv", ["yaDiskHealth.py", "--cli", "check", *arguments]), \
                    contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
                mod.main()
            self.assertEqual(raised.exception.code, 2)

    def test_two_scan_operations_cannot_overlap(self):
        started, release = threading.Event(), threading.Event()
        first, second = queue.Queue(), queue.Queue()

        def get(url, **kwargs):
            if url.endswith("/resources/download"):
                started.set()
                self.assertTrue(release.wait(3))
                return Response(200, {"href": "https://downloader.disk.yandex.ru/a"})
            return Response(200, headers={"Content-Length": "10"}, body_size=10)

        with patch.object(mod.requests.Session, "get", side_effect=get) as requests:
            thread = threading.Thread(target=mod.scan, args=("token", self.files[:1], first, threading.Event()),
                                      kwargs={"history_path": self.database})
            thread.start()
            try:
                self.assertTrue(started.wait(3))
                mod.scan("token", self.files[:1], second, threading.Event(), history_path=self.database)
                self.assertEqual(second.get_nowait(), ("finished", "Другая проверка уже выполняется"))
            finally:
                release.set()
                thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(requests.call_count, 2)
        with mod.HistoryStore(self.database) as history:
            self.assertEqual(len(history.runs()), 1)
            self.assertEqual(history.runs()[0].checked_count, 1)

    def test_process_lock_blocks_another_cli_process_and_releases_after_exit(self):
        script = ("import queue, sys, threading, yaDiskHealth as m\n"
                  "q = queue.Queue()\n"
                  "m.scan('', [], q, threading.Event(), history_path=sys.argv[1])\n"
                  "print(list(q.queue))\n")
        arguments = [mod.sys.executable, "-B", "-c", script, str(self.database)]
        with mod.check_operation(self.database):
            blocked = subprocess.run(arguments, capture_output=True, text=True, timeout=5,
                                     cwd=Path(mod.__file__).parent)
        self.assertEqual(blocked.returncode, 0, blocked.stderr)
        self.assertIn("Другая проверка уже выполняется", blocked.stdout)
        completed = subprocess.run(arguments, capture_output=True, text=True, timeout=5,
                                   cwd=Path(mod.__file__).parent)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        with mod.HistoryStore(self.database) as history:
            self.assertEqual(len(history.runs()), 1)
            self.assertEqual(history.runs()[0].status, "completed")

    def test_unexpected_worker_error_is_unknown_and_run_lock_is_released(self):
        events = queue.Queue()
        with patch.object(mod.DiskProbe, "check_file", side_effect=RuntimeError("secret-url")):
            mod.scan("token", self.files[:1], events, threading.Event(), history_path=self.database)
        with mod.HistoryStore(self.database) as history:
            run = history.runs()[0]
            self.assertEqual(run.status, "error")
            self.assertEqual((run.checked_count, run.failed_count, run.unknown_count), (1, 0, 1))
        with patch.object(mod.DiskProbe, "check_file", return_value=self.good):
            mod.scan("token", self.files[:1] * 2, events, threading.Event(), history_path=self.database)
        with mod.HistoryStore(self.database) as history:
            self.assertEqual(history.runs()[-1].status, "completed")
            self.assertEqual(history.runs()[-1].checked_count, 1)
        self.assertNotIn(b"secret-url", self.database.read_bytes())


class GuiWorkflowTests(unittest.TestCase):
    def test_modes_preview_sampling_and_full_ignores_row_selection(self):
        try:
            import tkinter as tk
            from tkinter import ttk
            root = tk.Tk()
        except Exception as error:
            self.skipTest(f"Tkinter/display unavailable: {error}")
        root.withdraw()
        self.addCleanup(root.destroy)
        files = [mod.DiskFile(str(i), f"disk:/{i:02}", 10) for i in range(24)]
        good = mod.CheckResult(True, "Данные получены", 200, 10, 10)
        bad = mod.CheckResult(False, "HTTP 403", 403, 10, 0)

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

        with tempfile.TemporaryDirectory() as folder:
            database = Path(folder) / "history.sqlite3"
            with mod.HistoryStore(database) as history:
                history.merge_files(files)
                for file in files[:21]:
                    history.record(file.path, good)
                for file in files[21:23]:
                    history.record(file.path, bad)

            def drive():
                controls = list(widgets(root))
                buttons = {widget.cget("text"): widget for widget in controls if isinstance(widget, ttk.Button)}
                mode, unit = [widget for widget in controls if isinstance(widget, ttk.Combobox)]
                entries = [widget for widget in controls if isinstance(widget, ttk.Entry)
                           and not isinstance(widget, ttk.Combobox)]
                token, sample = entries
                token.insert(0, "token")
                tree = next(widget for widget in controls if isinstance(widget, ttk.Treeview))
                labels = [widget for widget in controls if isinstance(widget, ttk.Label)]
                self.assertTrue(any("Ранее доступных: 21" in widget.cget("text") and
                                    "Выбрано для проверки: 24" in widget.cget("text") for widget in labels))
                tree.selection_set(tree.get_children()[0])
                root.update()

                def run(expected_files):
                    check.reset_mock()
                    buttons["Проверить"].invoke()
                    self.assertIn("disabled", buttons["Проверить выбранные"].state())
                    wait_until(lambda: "disabled" not in buttons["Проверить"].state())
                    actual = [call.args[0] for call in check.call_args_list]
                    self.assertEqual(len(actual), len(expected_files))
                    self.assertTrue(set(actual).issubset(expected_files))
                    self.assertEqual(len(actual), len(set(actual)))

                mode.set("Только недоступные")
                self.assertTrue(any("Выбрано для проверки: 2" in widget.cget("text") for widget in labels))
                run(files[21:23])
                mode.set("Случайная проверка доступных")
                self.assertNotIn("disabled", sample.state())
                check.reset_mock()
                buttons["Проверить"].invoke()
                wait_until(lambda: "disabled" not in buttons["Проверить"].state())
                actual = [call.args[0] for call in check.call_args_list]
                self.assertEqual(len(actual), 3)
                self.assertTrue(set(actual).issubset(files[:21]))
                unit.set("Количество")
                sample.delete(0, "end")
                sample.insert(0, "2")
                check.reset_mock()
                buttons["Проверить"].invoke()
                wait_until(lambda: "disabled" not in buttons["Проверить"].state())
                self.assertEqual(check.call_count, 2)
                self.assertTrue(all(call.args[0] in files[:21] for call in check.call_args_list))
                sample.delete(0, "end")
                sample.insert(0, "0")
                self.assertIn("disabled", buttons["Проверить"].state())
                mode.set("Полная проверка")
                self.assertIn("disabled", sample.state())
                self.assertEqual(len(tree.selection()), 1)
                run(files)
                with mod.HistoryStore(database) as history:
                    self.assertEqual([run.mode for run in history.runs()], ["failed", "random", "random", "full"])
                    self.assertEqual([run.selected_count for run in history.runs()], [2, 3, 2, 24])
                buttons["Запуски"].invoke()
                runs_tree = [widget for widget in widgets(root) if isinstance(widget, ttk.Treeview)][1]
                self.assertEqual(len(runs_tree.get_children()), 4)
                self.assertEqual(int(runs_tree.set(runs_tree.get_children()[0], "checked")), 24)
                self.assertTrue(any("длительность:" in widget.cget("text") for widget in labels))

            root.mainloop = drive
            with patch.object(tk, "Tk", return_value=root), \
                    patch.object(mod.DiskProbe, "iter_files") as listing, \
                    patch.object(mod.requests.Session, "get") as get, \
                    patch.object(mod.DiskProbe, "check_file", side_effect=lambda file:
                                 bad if file in files[21:23] else good) as check:
                self.assertEqual(mod.run_gui(6, database), 0)
            listing.assert_not_called()
            get.assert_not_called()

    def test_close_waits_for_stopped_run_to_be_saved(self):
        try:
            import tkinter as tk
            from tkinter import ttk
            root = tk.Tk()
        except Exception as error:
            self.skipTest(f"Tkinter/display unavailable: {error}")
        root.withdraw()
        started = threading.Event()
        files = [mod.DiskFile(str(i), f"disk:/{i}", 10) for i in range(10)]

        def widgets(parent):
            for child in parent.winfo_children():
                yield child
                yield from widgets(child)

        def check(client, file):
            started.set()
            self.assertTrue(client.stop.wait(3))
            return mod.CheckResult(None, "Остановлено")

        with tempfile.TemporaryDirectory() as folder:
            database = Path(folder) / "history.sqlite3"
            with mod.HistoryStore(database) as history:
                history.merge_files(files)

            def drive():
                controls = list(widgets(root))
                token = next(widget for widget in controls if isinstance(widget, ttk.Entry))
                token.insert(0, "token")
                button = next(widget for widget in controls if isinstance(widget, ttk.Button)
                              and widget.cget("text") == "Проверить")
                button.invoke()
                self.assertTrue(started.wait(3))
                root.tk.call(root.protocol("WM_DELETE_WINDOW"))
                deadline = time.monotonic() + 5
                while not destroy.called and time.monotonic() < deadline:
                    root.update()
                    time.sleep(0.005)
                self.assertTrue(destroy.called)

            root.mainloop = drive
            with patch.object(tk, "Tk", return_value=root), patch.object(mod.DiskProbe, "check_file", check), \
                    patch.object(root, "destroy", wraps=root.destroy) as destroy:
                try:
                    self.assertEqual(mod.run_gui(1, database), 0)
                finally:
                    if not destroy.called:
                        root.destroy()
            with mod.HistoryStore(database) as history:
                run = history.runs()[0]
                self.assertEqual(run.status, "stopped")
                self.assertEqual((run.selected_count, run.checked_count, run.unknown_count), (10, 1, 1))

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
                self.assertEqual(tuple(tree.cget("displaycolumns")),
                                 ("name", "folder", "size", "checked", "duration"))
                self.assertEqual(tree.heading("name", "text"), "Файл")
                self.assertIn("disabled", buttons["Проверить"].state())
                buttons["Загрузить список"].invoke()
                self.assertIn("disabled", buttons["Проверить"].state())
                wait_until(lambda: "disabled" not in buttons["Проверить"].state())
                check.assert_not_called()
                self.assertEqual(len(tree.get_children()), 2)
                pending_image = tree.item(tree.get_children()[0], "image")

                tree.selection_set(tree.get_children()[0])
                root.update()
                buttons["Проверить выбранные"].invoke()
                wait_until(lambda: "disabled" not in buttons["Проверить"].state())
                check.assert_called_once_with(files[0])
                self.assertTrue(tree.set(tree.get_children()[0], "checked"))
                self.assertEqual(tree.set(tree.get_children()[0], "received"), "10 / 10")
                self.assertEqual(int(tree.set(tree.get_children()[0], "duration")), 125)
                self.assertEqual(int(tree.set(tree.get_children()[0], "http")), 206)
                image = tree.item(tree.get_children()[0], "image")[0]
                self.assertEqual(root.tk.call(image, "get", 8, 8), (33, 177, 90))

                buttons["Снять выбор"].invoke()
                buttons["Проверить"].invoke()
                wait_until(lambda: "disabled" not in buttons["Проверить"].state())
                self.assertEqual(check.call_count, 3)

                buttons["История"].invoke()
                history_tree = [widget for widget in widgets(root) if isinstance(widget, ttk.Treeview)][1]
                self.assertEqual(tuple(history_tree.cget("displaycolumns")),
                                 ("date", "path", "status", "change", "duration", "run"))
                self.assertEqual(len(history_tree.get_children()), 3)
                self.assertEqual(int(history_tree.set(history_tree.get_children()[0], "received")), 10)
                self.assertEqual(int(history_tree.set(history_tree.get_children()[0], "duration")), 125)
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

                reason = "HTTP 429 — проверка временно не подтверждена"
                check.return_value = mod.CheckResult(None, reason, 429, 10, 0, 250, None, reason)
                tree.selection_set(tree.get_children()[0])
                root.update()
                buttons["Проверить выбранные"].invoke()
                wait_until(lambda: "disabled" not in buttons["Проверить"].state())
                first = tree.get_children()[0]
                self.assertEqual(tree.item(first, "image"), pending_image)
                self.assertIn("HTTP 429", tree.set(first, "note"))
                self.assertEqual(tree.set(first, "received"), "0 / 10")
                self.assertEqual(int(tree.set(first, "http")), 429)
                self.assertTrue(tree.set(first, "checked"))
                target = Path(folder) / "checks.csv"
                with patch("tkinter.filedialog.asksaveasfilename", return_value=str(target)):
                    buttons["CSV…"].invoke()
                with target.open(encoding="utf-8-sig", newline="") as output:
                    rows = list(csv.DictReader(output, delimiter=";"))
                self.assertEqual(rows[0]["Доступно"], "Не подтверждён")
                self.assertEqual(rows[0]["HTTP"], "429")
                self.assertEqual(rows[0]["Запрошено, байт"], "10")
                self.assertEqual(rows[0]["Получено, байт"], "0")
                self.assertEqual(rows[0]["Время, мс"], "250")
                self.assertEqual(rows[0]["Причина ошибки"], reason)
                self.assertEqual(rows[1]["Получено, байт"], "10")
                self.assertEqual(rows[1]["Время, мс"], "125")
                self.assertEqual(rows[1]["HTTP Range"], "Да")
                self.assertEqual(rows[1]["Причина ошибки"], "")
                self.assertTrue(rows[0]["Последняя проверка"])

                reason = "HTTP 403 — выдача файла не подтверждена"
                check.return_value = mod.CheckResult(False, reason, 403, 20, 0, 50, None, reason)
                second = tree.get_children()[1]
                tree.selection_set(second)
                root.update()
                buttons["Проверить выбранные"].invoke()
                wait_until(lambda: "disabled" not in buttons["Проверить"].state())
                self.assertEqual(tree.set(second, "received"), "0 / 20")
                self.assertEqual(int(tree.set(second, "http")), 403)
                image = tree.item(second, "image")[0]
                self.assertEqual(root.tk.call(image, "get", 8, 8), (235, 71, 71))
                with patch("tkinter.filedialog.asksaveasfilename", return_value=str(target)):
                    buttons["CSV…"].invoke()
                with target.open(encoding="utf-8-sig", newline="") as output:
                    rows = list(csv.DictReader(output, delimiter=";"))
                self.assertEqual(rows[1]["Доступно"], "Нет")
                self.assertEqual(rows[1]["HTTP"], "403")
                self.assertEqual(rows[1]["Причина ошибки"], reason)
                with mod.HistoryStore(database) as history:
                    self.assertEqual(len(history.entries()), 5)

            root.mainloop = drive
            with patch.object(tk, "Tk", return_value=root), \
                    patch.object(mod.DiskProbe, "iter_files", return_value=iter(files)), \
                    patch.object(mod.DiskProbe, "check_file",
                                 return_value=mod.CheckResult(True, "HTTP 206", 206, 10, 10, 125, True)) as check:
                self.assertEqual(mod.run_gui(1, database), 0)

            # Reopen the saved catalog in another window; no network is needed.
            second = tk.Toplevel(root)
            second.withdraw()
            self.addCleanup(second.destroy)

            def reopened():
                tree = next(widget for widget in widgets(second) if isinstance(widget, ttk.Treeview))
                self.assertEqual(len(tree.get_children()), 2)
                self.assertTrue(all(tree.set(iid, "checked") != "—" for iid in tree.get_children()))
                first = tree.get_children()[0]
                self.assertIn("HTTP 429", tree.set(first, "note"))
                self.assertEqual(tree.set(first, "received"), "0 / 10")
                self.assertEqual(int(tree.set(first, "duration")), 250)
                image = tree.item(first, "image")[0]
                self.assertEqual(second.tk.call(image, "get", 8, 8), (156, 163, 175))

            second.mainloop = reopened
            with patch.object(tk, "Tk", return_value=second), patch.object(mod, "DiskProbe") as probe:
                self.assertEqual(mod.run_gui(1, database), 0)
            probe.assert_not_called()


class GuiDownloadTests(unittest.TestCase):
    def setUp(self):
        try:
            import tkinter as tk
            from tkinter import ttk
            self.root = tk.Tk()
        except Exception as error:
            self.skipTest(f"Tkinter/display unavailable: {error}")
        self.tk = tk
        self.ttk = ttk
        self.root.withdraw()
        self.addCleanup(self.root.destroy)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database = Path(self.temp.name) / "history.sqlite3"
        self.files = [mod.DiskFile("a", "disk:/a", 10), mod.DiskFile("б", "disk:/папка/б", 20)]
        self.failure = mod.CheckResult(False, "Тайм-аут или ошибка сети при чтении", 206,
                                       20, 3, 125, True, "Тайм-аут или ошибка сети при чтении")
        with mod.HistoryStore(self.database) as history:
            history.merge_files(self.files)
            history.record(self.files[1].path, self.failure)
        self.root.mainloop = lambda: None
        with patch.object(tk, "Tk", return_value=self.root), \
                patch.dict(mod.os.environ, {"YANDEX_DISK_TOKEN": "fictional-test-token"}):
            self.assertEqual(mod.run_gui(1, self.database), 0)
        controls = list(self.widgets(self.root))
        self.tree = next(widget for widget in controls if isinstance(widget, ttk.Treeview))
        self.menu = next(widget for widget in controls if isinstance(widget, tk.Menu))
        self.buttons = {widget.cget("text"): widget for widget in controls if isinstance(widget, ttk.Button)}

    def widgets(self, parent):
        for child in parent.winfo_children():
            yield child
            yield from self.widgets(child)

    def wait_until(self, predicate):
        deadline = time.monotonic() + 3
        while not predicate() and time.monotonic() < deadline:
            self.root.update()
            time.sleep(0.005)
        self.assertTrue(predicate())

    def popup(self, iid, event="<Button-3>"):
        with patch.object(self.tree, "identify_row", return_value=iid), \
                patch.object(self.menu, "tk_popup") as popup:
            self.tree.event_generate(event, x=5, y=20, rootx=55, rooty=65)
        return popup

    def wait_for_download(self):
        self.wait_until(lambda: "disabled" not in self.buttons["Проверить"].state())

    def test_oauth_link_opens_polygon_and_footer_is_absent(self):
        labels = [widget for widget in self.widgets(self.root) if isinstance(widget, self.ttk.Label)]
        link = next(widget for widget in labels if widget.cget("text") == "OAuth-токен:")
        self.assertEqual(str(link.cget("foreground")), "#2563EB")
        self.assertEqual(str(link.cget("cursor")), "hand2")
        self.assertEqual(self.root.tk.call("font", "actual", link.cget("font"), "-underline"), 1)
        with patch.object(mod.webbrowser, "open_new_tab") as browser:
            link.event_generate("<Button-1>")
        browser.assert_called_once_with("https://yandex.ru/dev/disk/poligon#access_token")
        self.assertFalse(any("Ctrl/Shift" in widget.cget("text") or
                             "Читается до" in widget.cget("text") or
                             "Зелёный —" in widget.cget("text") for widget in labels))

    def test_reloading_list_restores_saved_results_and_keeps_missing_files(self):
        previous = self.tree.item(self.tree.get_children()[1])
        with mod.HistoryStore(self.database) as history:
            entries, runs = history.entries(), history.runs()
        updated = mod.DiskFile("updated", self.files[0].path, 100)
        added = mod.DiskFile("new", "disk:/new", 30)
        with patch.object(mod.DiskProbe, "iter_files", return_value=iter([updated, added])), \
                patch.object(mod.DiskProbe, "check_file") as check:
            self.buttons["Загрузить список"].invoke()
            self.wait_for_download()
        check.assert_not_called()
        rows = {self.tree.set(iid, "name"): iid for iid in self.tree.get_children()}
        self.assertEqual(set(rows), {"updated", "б", "new"})
        self.assertEqual(self.tree.item(rows["б"], "values"), tuple(previous["values"]))
        self.assertEqual(self.tree.item(rows["б"], "image"), tuple(previous["image"]))
        with mod.HistoryStore(self.database) as history:
            self.assertEqual(history.files(), [updated, added, self.files[1]])
            self.assertEqual(history.entries(), entries)
            self.assertEqual(history.runs(), runs)

    def test_menu_downloads_clicked_file_with_fresh_links_and_preserves_selection(self):
        first, second = self.tree.get_children()
        self.tree.selection_set(first)
        self.root.update()
        self.popup(second).assert_called_once_with(55, 65)
        self.assertEqual(self.menu.entrycget(0, "label"), "Скачать")
        self.assertEqual(self.tree.selection(), (first,))
        self.popup("").assert_not_called()
        if mod.sys.platform == "darwin":
            self.popup(second, "<Control-Button-1>").assert_called_once_with(55, 65)
            self.popup(second, "<Button-2>").assert_called_once_with(55, 65)

        links = ["https://downloader.disk.yandex.ru/x?secret=1",
                 "https://downloader.disk.yandex.ru/x?secret=2"]
        request_threads, browser_threads = [], []

        def get(*args, **kwargs):
            request_threads.append(threading.get_ident())
            return Response(200, {"href": links[len(request_threads) - 1]})

        def open_tab(href):
            browser_threads.append(threading.get_ident())
            return True

        with patch.object(mod.requests.Session, "get", side_effect=get) as request, \
                patch.object(mod.webbrowser, "open_new_tab", side_effect=open_tab) as browser, \
                patch("tkinter.messagebox.showerror") as error:
            for _ in links:
                self.menu.invoke(0)
                self.assertIn("disabled", self.buttons["Проверить"].state())
                self.popup(first).assert_not_called()
                self.menu.invoke(0)
                self.wait_for_download()
            self.assertEqual([call.args[0] for call in browser.call_args_list], links)
            self.assertEqual([call.kwargs["params"] for call in request.call_args_list],
                             [{"path": self.files[1].path}] * 2)
            self.assertTrue(all(value != threading.get_ident() for value in request_threads))
            self.assertEqual(browser_threads, [threading.get_ident()] * 2)
            error.assert_not_called()
        self.assertEqual(self.tree.selection(), (first,))
        with mod.HistoryStore(self.database) as history:
            self.assertEqual(len(history.entries()), 1)
            self.assertEqual(history.entries()[0].result, self.failure)
            self.assertEqual(history.runs(), [])
        self.assertNotIn(b"secret=", self.database.read_bytes())

    def test_download_errors_do_not_open_browser_or_expose_response_details(self):
        self.popup(self.tree.get_children()[0])
        for failure in (mod.DiskAPIError(403, "fictional-test-token secret-url"),
                        mod.requests.Timeout("fictional-test-token secret-url"),
                        ValueError("fictional-test-token secret-url")):
            with self.subTest(error=type(failure).__name__), \
                    patch.object(mod.DiskProbe, "download_url", side_effect=failure), \
                    patch.object(mod.webbrowser, "open_new_tab") as browser, \
                    patch("tkinter.messagebox.showerror") as error:
                self.menu.invoke(0)
                self.wait_for_download()
                browser.assert_not_called()
                error.assert_called_once()
                self.assertNotIn("secret-url", str(error.call_args))
                self.assertNotIn("fictional-test-token", str(error.call_args))

    def test_browser_launch_failures_are_reported(self):
        self.popup(self.tree.get_children()[0])
        href = "https://downloader.disk.yandex.ru/x?secret=1"
        for failure in (False, mod.webbrowser.Error("secret=1"), OSError("secret=1")):
            options = {"return_value": False} if failure is False else {"side_effect": failure}
            with self.subTest(error=failure), patch.object(mod.DiskProbe, "download_url", return_value=href), \
                    patch.object(mod.webbrowser, "open_new_tab", **options) as browser, \
                    patch("tkinter.messagebox.showerror") as error:
                self.menu.invoke(0)
                self.wait_for_download()
                browser.assert_called_once_with(href)
                error.assert_called_once()
                self.assertNotIn("secret=1", str(error.call_args))

    def test_cancel_discards_a_link_already_queued_for_the_browser(self):
        self.popup(self.tree.get_children()[0])
        queued = threading.Event()

        def ready(token, file, events, stop):
            events.put(("download", ("https://downloader.disk.yandex.ru/x", None)))
            queued.set()

        with patch.object(mod, "prepare_download", side_effect=ready), \
                patch.object(mod.webbrowser, "open_new_tab") as browser:
            self.menu.invoke(0)
            self.assertTrue(queued.wait(3))
            self.buttons["Остановить"].invoke()
            self.wait_for_download()
            browser.assert_not_called()

    def test_close_cancels_pending_download_before_opening_the_browser(self):
        self.popup(self.tree.get_children()[0])
        started = threading.Event()

        def download(client, file):
            started.set()
            self.assertTrue(client.stop.wait(3))
            return "https://downloader.disk.yandex.ru/x"

        with patch.object(mod.DiskProbe, "download_url", download), \
                patch.object(mod.webbrowser, "open_new_tab") as browser, \
                patch.object(self.root, "destroy") as destroy:
            self.menu.invoke(0)
            self.assertTrue(started.wait(3))
            self.root.tk.call(self.root.protocol("WM_DELETE_WINDOW"))
            self.wait_until(lambda: destroy.called)
            browser.assert_not_called()

    def test_network_error_is_red_in_table_and_history_and_details_remain_in_csv(self):
        second = self.tree.get_children()[1]
        image = self.tree.item(second, "image")[0]
        self.assertEqual(self.root.tk.call(image, "get", 8, 8), (235, 71, 71))
        self.buttons["История"].invoke()
        history_tree = [widget for widget in self.widgets(self.root)
                        if isinstance(widget, self.ttk.Treeview)][1]
        row = history_tree.get_children()[0]
        self.assertEqual(history_tree.set(row, "status"), "Ошибка")
        self.assertIn("bad", history_tree.item(row, "tags"))
        self.assertEqual(str(history_tree.tag_configure("bad", "foreground")), "#B91C1C")
        labels = [widget.cget("text") for widget in self.widgets(self.root)
                  if isinstance(widget, self.ttk.Label)]
        self.assertTrue(any("Ошибки: 1" in value for value in labels))
        target = Path(self.temp.name) / "checks.csv"
        with patch("tkinter.filedialog.asksaveasfilename", return_value=str(target)):
            self.buttons["CSV…"].invoke()
        with target.open(encoding="utf-8-sig", newline="") as output:
            row = list(csv.DictReader(output, delimiter=";"))[1]
        self.assertEqual(row["Доступно"], "Нет")
        self.assertEqual(row["HTTP"], "206")
        self.assertEqual(row["Запрошено, байт"], "20")
        self.assertEqual(row["Получено, байт"], "3")
        self.assertEqual(row["Причина ошибки"], self.failure.note)


if __name__ == "__main__":
    unittest.main()
