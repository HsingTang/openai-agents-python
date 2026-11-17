"""Tests for the Databricks stdio launcher.

Cover the startup-banner filter, the credential guard in ``main``, and the
``_run`` bridge: forwarding protocol lines, closing the exec-server stdin, and
surfacing the remote command's exit code. These import only the launcher module
(standard library; ``databricks-sdk`` is lazy), so they run without the optional
dependency.
"""

from __future__ import annotations

import os
import sys

from _agents_codex._databricks_launcher import _forward_stdout, _run, main


def _run_forward(lines: list[bytes]) -> list[bytes]:
    pending = list(lines)

    def read_line() -> bytes:
        return pending.pop(0) if pending else b""

    written: list[bytes] = []
    _forward_stdout(read_line, written.append)
    return written


def test_drops_banner_then_forwards_protocol() -> None:
    written = _run_forward(
        [
            b"\xe2\x9c\x94 Databricks auth already available\n",
            b"Launching Codex with Unity Gateway\n",
            b"\n",
            b'{"jsonrpc":"2.0","id":1}\n',
            b'{"jsonrpc":"2.0","method":"x"}\n',
        ]
    )
    assert written == [
        b'{"jsonrpc":"2.0","id":1}\n',
        b'{"jsonrpc":"2.0","method":"x"}\n',
    ]


def test_forwards_everything_after_protocol_starts() -> None:
    written = _run_forward([b'{"a":1}\n', b"not-json-but-mid-stream\n", b'{"b":2}\n'])
    assert written == [b'{"a":1}\n', b"not-json-but-mid-stream\n", b'{"b":2}\n']


def test_leading_whitespace_before_brace_is_protocol() -> None:
    assert _run_forward([b"  banner\n", b'  {"a":1}\n']) == [b'  {"a":1}\n']


def test_main_requires_credentials(monkeypatch) -> None:
    monkeypatch.setattr(sys, "argv", ["launcher", "sandboxes/s"])
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    assert main() == 2


class _FakeOutput:
    def __init__(self, lines: list[bytes]) -> None:
        self._lines = list(lines)

    def readline(self) -> bytes:
        return self._lines.pop(0) if self._lines else b""

    def read1(self) -> bytes:
        return self._lines.pop(0) if self._lines else b""


class _FakeStdin:
    def __init__(self) -> None:
        self.written = bytearray()
        self.closed = False

    def write(self, data: bytes) -> None:
        self.written.extend(data)

    def close(self) -> None:
        self.closed = True


class _FakeProcess:
    def __init__(self, stdout_lines: list[bytes], exit_code: int = 0) -> None:
        self.stdout = _FakeOutput(stdout_lines)
        self.stderr = _FakeOutput([])
        self.stdin = _FakeStdin()
        self._exit_code = exit_code

    def close(self) -> None:
        pass

    def wait(self, timeout: float | None = None) -> int:
        return self._exit_code


def test_run_forwards_protocol_closes_stdin_and_returns_exit(monkeypatch) -> None:
    captured = bytearray()
    monkeypatch.setattr(os, "read", lambda fd, n: b"")  # stdin EOF immediately
    monkeypatch.setattr(os, "write", lambda fd, data: captured.extend(data) if fd == 1 else None)
    process = _FakeProcess([b"banner\n", b'{"a":1}\n'], exit_code=0)
    assert _run(process, install_signals=False) == 0
    assert bytes(captured) == b'{"a":1}\n'
    assert process.stdin.closed


def test_run_surfaces_remote_exit_code(monkeypatch) -> None:
    monkeypatch.setattr(os, "read", lambda fd, n: b"")
    monkeypatch.setattr(os, "write", lambda fd, data: None)
    process = _FakeProcess([b'{"x":1}\n'], exit_code=3)
    assert _run(process, install_signals=False) == 3
