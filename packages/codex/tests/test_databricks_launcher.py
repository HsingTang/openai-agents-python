"""Tests for the Databricks stdio launcher.

Cover the startup-banner filter, the credential guard, the stdin relay, the
teardown/signal path, the remote exit-code handling, and ``main``'s
``execute_command`` wiring. These import only the launcher module (standard
library; ``databricks-sdk`` is lazy), so they run without the optional
dependency; ``main``'s lazy imports are satisfied with injected fakes.
"""

from __future__ import annotations

import os
import signal
import sys
import threading
import types
from types import SimpleNamespace

from _agents_codex._databricks_launcher import (
    _forward_stdout,
    _pump_stdin,
    _run,
    _terminate,
    main,
)


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
    assert written == [b'{"jsonrpc":"2.0","id":1}\n', b'{"jsonrpc":"2.0","method":"x"}\n']


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


class _FakeStdin:
    def __init__(self) -> None:
        self.written = bytearray()
        self.closed = False

    def write(self, data: bytes) -> None:
        self.written.extend(data)

    def close(self) -> None:
        self.closed = True


class _FakeOutput:
    def __init__(self, lines: list[bytes]) -> None:
        self._lines = list(lines)

    def readline(self) -> bytes:
        return self._lines.pop(0) if self._lines else b""

    def read1(self) -> bytes:
        return self._lines.pop(0) if self._lines else b""


class _FakeProcess:
    def __init__(self, stdout_lines: list[bytes], exit_code: int = 0) -> None:
        self.stdout = _FakeOutput(stdout_lines)
        self.stderr = _FakeOutput([])
        self.stdin = _FakeStdin()
        self.closed = False
        self._exit_code = exit_code

    def close(self) -> None:
        self.closed = True

    def wait(self, timeout: float | None = None) -> int:
        return self._exit_code


def test_pump_stdin_relays_chunks_and_closes(monkeypatch) -> None:
    chunks = [b'{"id":1}\n', b"more\n", b""]
    monkeypatch.setattr(os, "read", lambda fd, n: chunks.pop(0) if chunks else b"")
    process = _FakeProcess([])
    _pump_stdin(process)
    assert bytes(process.stdin.written) == b'{"id":1}\nmore\n'
    assert process.stdin.closed


def test_pump_stdin_closes_on_write_error(monkeypatch) -> None:
    monkeypatch.setattr(os, "read", lambda fd, n: b"x")

    class _BadStdin:
        def __init__(self) -> None:
            self.closed = False

        def write(self, data: bytes) -> None:
            raise OSError("boom")

        def close(self) -> None:
            self.closed = True

    process = SimpleNamespace(stdin=_BadStdin())
    _pump_stdin(process)
    assert process.stdin.closed


def test_terminate_closes_stdin_and_sets_done() -> None:
    done = threading.Event()
    process = _FakeProcess([])
    _terminate(process, done)
    assert process.stdin.closed
    assert process.closed
    assert done.is_set()


def test_run_forwards_protocol_and_returns_exit(monkeypatch) -> None:
    captured = bytearray()
    monkeypatch.setattr(os, "read", lambda fd, n: b"")
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


def test_run_installs_signal_handlers(monkeypatch) -> None:
    registered: dict[int, object] = {}
    monkeypatch.setattr(signal, "signal", lambda sig, handler: registered.__setitem__(sig, handler))
    monkeypatch.setattr(os, "read", lambda fd, n: b"")
    monkeypatch.setattr(os, "write", lambda fd, data: None)
    process = _FakeProcess([b'{"a":1}\n'], exit_code=0)
    _run(process, install_signals=True)
    assert signal.SIGTERM in registered
    assert signal.SIGINT in registered
    registered[signal.SIGTERM]()  # type: ignore[operator]
    assert process.stdin.closed
    assert process.closed


def test_main_launches_exec_server(monkeypatch) -> None:
    recorded: dict[str, object] = {}

    class _FakeStreamMode:
        PIPE = "PIPE"
        DEVNULL = "DEVNULL"

    class _FakeSandboxService:
        def execute_command(self, name, cmd, *, args, text, stderr) -> _FakeProcess:
            recorded.update(name=name, cmd=cmd, args=args, text=text, stderr=stderr)
            return _FakeProcess([b'{"a":1}\n'])

    class _FakeWorkspaceClient:
        def __init__(self, *, host, token) -> None:
            recorded.update(host=host, token=token)
            self.sandbox = _FakeSandboxService()

    sdk = types.ModuleType("databricks.sdk")
    sdk.WorkspaceClient = _FakeWorkspaceClient  # type: ignore[attr-defined]
    mixins_sandbox = types.ModuleType("databricks.sdk.mixins.sandbox")
    mixins_sandbox.StreamMode = _FakeStreamMode  # type: ignore[attr-defined]
    modules = {
        "databricks": types.ModuleType("databricks"),
        "databricks.sdk": sdk,
        "databricks.sdk.mixins": types.ModuleType("databricks.sdk.mixins"),
        "databricks.sdk.mixins.sandbox": mixins_sandbox,
    }
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.setenv("DATABRICKS_HOST", "https://h")
    monkeypatch.setenv("DATABRICKS_TOKEN", "t")
    monkeypatch.setattr(sys, "argv", ["launcher", "sandboxes/s"])
    monkeypatch.setattr("_agents_codex._databricks_launcher._run", lambda process, **kwargs: 0)

    assert main() == 0
    assert recorded["host"] == "https://h"
    assert recorded["token"] == "t"
    assert recorded["name"] == "sandboxes/s"
    assert recorded["cmd"] == "codex"
    assert recorded["args"] == ["exec-server", "--listen", "stdio"]
    assert recorded["text"] is False
    assert recorded["stderr"] == "PIPE"
