"""Relay and auth tests for the Databricks sandbox gateway example.

These exercise the certain core - byte relay and bearer auth - with a fake
``CommandProcess`` and a real TLS WebSocket client. Run from the repo root with
the preview wheel and ``examples/codex/databricks/requirements.txt`` installed.
"""

from __future__ import annotations

import queue
import ssl
from typing import TYPE_CHECKING, Any, cast

import pytest

pytest.importorskip("websockets")
pytest.importorskip("cryptography")

from websockets.asyncio.client import connect  # noqa: E402
from websockets.exceptions import ConnectionClosed, InvalidStatus  # noqa: E402

from examples.codex.databricks.gateway import (  # noqa: E402
    SandboxExecServerGateway,
    self_signed_server_context,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


class _FakeStdin:
    def __init__(self) -> None:
        self.written = bytearray()
        self.closed = False

    def write(self, data: bytes) -> None:
        self.written.extend(data)

    def flush(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True


class _FakeStdout:
    """Blocking stdout fed from the test; ``readline`` returns b'' after EOF."""

    def __init__(self) -> None:
        self._lines: queue.Queue[bytes | None] = queue.Queue()

    def feed(self, data: bytes) -> None:
        self._lines.put(data)

    def eof(self) -> None:
        self._lines.put(None)

    def readline(self) -> bytes:
        item = self._lines.get()
        return b"" if item is None else item


class _FakeProcess:
    def __init__(self) -> None:
        self.stdin = _FakeStdin()
        self.stdout = _FakeStdout()
        self.closed = False

    def close(self) -> None:
        self.closed = True
        self.stdout.eof()


class _FakeSandbox:
    def __init__(self, proc: _FakeProcess) -> None:
        self._proc = proc
        self.calls: list[tuple[str, str, list[str]]] = []
        self.envs: list[dict[str, str] | None] = []

    def execute_command(
        self,
        name: str,
        program: str,
        args: list[str],
        text: bool,
        envs: dict[str, str] | None = None,
    ) -> _FakeProcess:
        assert text is False
        self.calls.append((name, program, args))
        self.envs.append(envs)
        return self._proc


class _FakeWorkspaceClient:
    def __init__(self, proc: _FakeProcess) -> None:
        self.sandbox = _FakeSandbox(proc)


def _client_ssl(ca_path: str) -> ssl.SSLContext:
    context = ssl.create_default_context(cafile=ca_path)
    return context


async def _start(proc: _FakeProcess, token: str = "s3cret") -> tuple[SandboxExecServerGateway, Any]:
    server_ctx, ca_path = self_signed_server_context("127.0.0.1")
    gateway = SandboxExecServerGateway(
        workspace_client=cast("WorkspaceClient", _FakeWorkspaceClient(proc)),
        sandbox_name="sbx",
        token=token,
        ssl_context=server_ctx,
        ca_cert_path=ca_path,
    )
    handle = await gateway.start()
    return gateway, handle


async def test_relay_both_directions() -> None:
    proc = _FakeProcess()
    client = _FakeWorkspaceClient(proc)
    server_ctx, ca_path = self_signed_server_context("127.0.0.1")
    gateway = SandboxExecServerGateway(
        workspace_client=cast("WorkspaceClient", client),
        sandbox_name="sbx",
        token="s3cret",
        ssl_context=server_ctx,
        ca_cert_path=ca_path,
    )
    handle = await gateway.start()
    try:
        async with connect(
            handle.url,
            ssl=_client_ssl(ca_path),
            additional_headers={"Authorization": "Bearer s3cret"},
        ) as ws:
            await ws.send(b"hello-stdin")
            proc.stdout.feed(b'{"jsonrpc":"2.0","id":1,"result":{}}\n')
            received = await ws.recv()
            # One WebSocket message per exec-server line; JSON-RPC protocol frame.
            assert received == b'{"jsonrpc":"2.0","id":1,"result":{}}\n'
    finally:
        await gateway.aclose()

    # The gateway appends the newline framing when the client omits it.
    assert proc.stdin.written == b"hello-stdin\n"
    assert proc.closed is True
    assert proc.stdin.closed is True
    assert client.sandbox.calls[0] == ("sbx", "codex", ["exec-server", "--listen", "stdio"])
    assert client.sandbox.envs[0] is None


async def test_drops_startup_banner_preamble() -> None:
    proc = _FakeProcess()
    gateway, handle = await _start(proc)
    try:
        async with connect(
            handle.url,
            ssl=_client_ssl(handle.ca_cert_path or ""),
            additional_headers={"Authorization": "Bearer s3cret"},
        ) as ws:
            # The sandbox's codex prints a startup banner to stdout before the real
            # exec-server protocol. Those non-JSON lines must not reach the client.
            proc.stdout.feed("✔ Databricks auth available\n".encode())
            proc.stdout.feed("╭- Launching Codex with Unity Gateway -╮\n".encode())
            proc.stdout.feed(b"\n")
            proc.stdout.feed(b'{"jsonrpc":"2.0","id":1,"result":{}}\n')
            proc.stdout.feed(b'{"method":"process/output","params":{"seq":1}}\n')
            first = await ws.recv()
            second = await ws.recv()
            assert first == b'{"jsonrpc":"2.0","id":1,"result":{}}\n'
            assert second == b'{"method":"process/output","params":{"seq":1}}\n'
    finally:
        await gateway.aclose()


async def test_relays_large_inbound_frame() -> None:
    # A payload above the websockets 1 MiB default (e.g. a big apply_patch) must
    # still reach stdin rather than closing the connection with 1009.
    proc = _FakeProcess()
    gateway, handle = await _start(proc)
    payload = b"x" * (2 * 1024 * 1024)
    try:
        async with connect(
            handle.url,
            ssl=_client_ssl(handle.ca_cert_path or ""),
            additional_headers={"Authorization": "Bearer s3cret"},
            max_size=4 * 1024 * 1024,
        ) as ws:
            await ws.send(payload)
            proc.stdout.feed(b'{"jsonrpc":"2.0","id":1,"result":{}}\n')
            assert await ws.recv() == b'{"jsonrpc":"2.0","id":1,"result":{}}\n'
    finally:
        await gateway.aclose()

    assert proc.stdin.written == payload + b"\n"


async def test_rejects_wrong_token() -> None:
    proc = _FakeProcess()
    gateway, handle = await _start(proc)
    try:
        with pytest.raises(InvalidStatus):
            async with connect(
                handle.url,
                ssl=_client_ssl(handle.ca_cert_path or ""),
                additional_headers={"Authorization": "Bearer wrong"},
            ):
                pass
    finally:
        await gateway.aclose()

    assert proc.stdin.written == b""


class _RaisingSandbox:
    def execute_command(self, name: str, program: str, args: list[str], text: bool) -> Any:
        raise RuntimeError("exec-server unavailable")


class _RaisingWorkspaceClient:
    def __init__(self) -> None:
        self.sandbox = _RaisingSandbox()


async def test_closes_websocket_when_exec_server_exits() -> None:
    proc = _FakeProcess()
    gateway, handle = await _start(proc)
    try:
        async with connect(
            handle.url,
            ssl=_client_ssl(handle.ca_cert_path or ""),
            additional_headers={"Authorization": "Bearer s3cret"},
        ) as ws:
            proc.stdout.eof()  # exec-server exits first
            with pytest.raises(ConnectionClosed):
                await ws.recv()
    finally:
        await gateway.aclose()

    assert proc.closed is True


async def test_connection_fails_when_exec_server_cannot_start() -> None:
    server_ctx, ca_path = self_signed_server_context("127.0.0.1")
    gateway = SandboxExecServerGateway(
        workspace_client=cast("WorkspaceClient", _RaisingWorkspaceClient()),
        sandbox_name="sbx",
        token="s3cret",
        ssl_context=server_ctx,
        ca_cert_path=ca_path,
    )
    handle = await gateway.start()
    try:
        for _ in range(2):  # the gateway keeps serving after a failed connection
            async with connect(
                handle.url,
                ssl=_client_ssl(ca_path),
                additional_headers={"Authorization": "Bearer s3cret"},
            ) as ws:
                with pytest.raises(ConnectionClosed):
                    await ws.recv()
    finally:
        await gateway.aclose()
