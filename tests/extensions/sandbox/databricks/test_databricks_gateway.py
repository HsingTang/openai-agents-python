"""Relay and auth tests for the Databricks sandbox gateway.

These exercise the certain core - byte relay and bearer auth - with a fake
``CommandProcess`` and a real TLS WebSocket client. They require the
``databricks`` extra's ``websockets``, ``cryptography`` and ``certifi``.
"""

from __future__ import annotations

import queue
import ssl
from typing import Any

import pytest

pytest.importorskip("websockets")
pytest.importorskip("cryptography")

from websockets.asyncio.client import connect  # noqa: E402
from websockets.exceptions import ConnectionClosed, InvalidStatus  # noqa: E402

from agents.extensions.sandbox.databricks.gateway import (  # noqa: E402
    SandboxExecServerGateway,
    self_signed_server_context,
)


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
    """Blocking stdout fed from the test; ``read1`` returns b'' after EOF."""

    def __init__(self) -> None:
        self._chunks: queue.Queue[bytes | None] = queue.Queue()

    def feed(self, data: bytes) -> None:
        self._chunks.put(data)

    def eof(self) -> None:
        self._chunks.put(None)

    def read1(self, _size: int) -> bytes:
        item = self._chunks.get()
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

    def execute_command(self, name: str, program: str, args: list[str], text: bool) -> _FakeProcess:
        assert text is False
        self.calls.append((name, program, args))
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
        workspace_client=_FakeWorkspaceClient(proc),
        sandbox_name="sbx",
        token=token,
        ssl_context=server_ctx,
        exec_server_command=("codex", "exec-server", "stdio"),
        ca_cert_path=ca_path,
    )
    handle = await gateway.start()
    return gateway, handle


async def test_relay_both_directions() -> None:
    proc = _FakeProcess()
    client = _FakeWorkspaceClient(proc)
    server_ctx, ca_path = self_signed_server_context("127.0.0.1")
    gateway = SandboxExecServerGateway(
        workspace_client=client,
        sandbox_name="sbx",
        token="s3cret",
        ssl_context=server_ctx,
        exec_server_command=("codex", "exec-server", "stdio"),
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
            proc.stdout.feed(b"hello-stdout")
            received = await ws.recv()
            assert received == b"hello-stdout"
    finally:
        await gateway.aclose()

    assert proc.stdin.written == b"hello-stdin"
    assert proc.closed is True
    assert proc.stdin.closed is True
    assert client.sandbox.calls[0] == ("sbx", "codex", ["exec-server", "stdio"])


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
        workspace_client=_RaisingWorkspaceClient(),
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
