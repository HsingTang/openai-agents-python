"""Tests for the Databricks sandbox environment example context manager.

Cover the public entry point (Environment wiring, teardown, trust handling) and
the ``SSL_CERT_FILE`` bundle helper. Run from the repo root with the preview
wheel and ``examples/codex/databricks/requirements.txt`` installed.
"""

from __future__ import annotations

import os
import queue
import ssl
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest

pytest.importorskip("websockets")
pytest.importorskip("cryptography")
pytest.importorskip("certifi")

from websockets.asyncio.client import connect  # noqa: E402

from examples.codex.databricks.environment import (  # noqa: E402
    _install_trust_bundle,
    databricks_sandbox_environment,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

_FAKE_CA = b"-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----\n"


class _FakeStdin:
    def __init__(self) -> None:
        self.written = bytearray()

    def write(self, data: bytes) -> None:
        self.written.extend(data)

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


class _FakeStdout:
    def __init__(self) -> None:
        self._lines: queue.Queue[bytes | None] = queue.Queue()

    def eof(self) -> None:
        self._lines.put(None)

    def readline(self) -> bytes:
        item = self._lines.get()
        return b"" if item is None else item


class _FakeProcess:
    def __init__(self) -> None:
        self.stdin = _FakeStdin()
        self.stdout = _FakeStdout()

    def close(self) -> None:
        self.stdout.eof()


class _FakeSandbox:
    def __init__(self, proc: _FakeProcess) -> None:
        self._proc = proc

    def execute_command(
        self,
        name: str,
        program: str,
        args: list[str],
        text: bool,
        envs: dict[str, str] | None = None,
    ) -> _FakeProcess:
        return self._proc


class _FakeWorkspaceClient:
    def __init__(self, proc: _FakeProcess) -> None:
        self.sandbox = _FakeSandbox(proc)


async def test_environment_wiring(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    client = _FakeWorkspaceClient(_FakeProcess())
    async with databricks_sandbox_environment(
        workspace_client=cast("WorkspaceClient", client),
        sandbox_name="sbx",
        cwd="/home/sandbox-agent/work",
        token="tok-123",
    ) as environment:
        assert environment.url.startswith("wss://127.0.0.1:")
        assert environment.cwd == "/home/sandbox-agent/work"
        # Environment lowercases header names.
        assert environment.headers["authorization"] == "Bearer tok-123"
        assert tuple(environment.tools) == ("exec_command", "write_stdin", "apply_patch")
        assert "SSL_CERT_FILE" in os.environ
    assert "SSL_CERT_FILE" not in os.environ


async def test_trust_gateway_ca_false_leaves_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    client = _FakeWorkspaceClient(_FakeProcess())
    async with databricks_sandbox_environment(
        workspace_client=cast("WorkspaceClient", client),
        sandbox_name="sbx",
        cwd="/w",
        token="t",
        trust_gateway_ca=False,
    ):
        assert "SSL_CERT_FILE" not in os.environ
    assert "SSL_CERT_FILE" not in os.environ


async def test_restores_and_closes_on_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SSL_CERT_FILE", "/orig")
    client = _FakeWorkspaceClient(_FakeProcess())
    captured: dict[str, str] = {}
    with pytest.raises(RuntimeError, match="boom"):
        async with databricks_sandbox_environment(
            workspace_client=cast("WorkspaceClient", client),
            sandbox_name="sbx",
            cwd="/w",
            token="t",
        ) as environment:
            captured["url"] = environment.url
            raise RuntimeError("boom")

    assert os.environ["SSL_CERT_FILE"] == "/orig"

    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    with pytest.raises(OSError):
        async with connect(captured["url"], ssl=context):
            pass


def test_install_trust_bundle_sets_and_restores(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import certifi

    ca = tmp_path / "ca.pem"
    ca.write_bytes(_FAKE_CA)
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)

    restore = _install_trust_bundle(str(ca))
    bundle = Path(os.environ["SSL_CERT_FILE"]).read_bytes()
    assert Path(certifi.where()).read_bytes() in bundle
    assert _FAKE_CA in bundle

    restore()
    assert "SSL_CERT_FILE" not in os.environ


def test_install_trust_bundle_restores_previous(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    ca = tmp_path / "ca.pem"
    ca.write_bytes(_FAKE_CA)
    monkeypatch.setenv("SSL_CERT_FILE", "/x")

    restore = _install_trust_bundle(str(ca))
    assert os.environ["SSL_CERT_FILE"] != "/x"

    restore()
    assert os.environ["SSL_CERT_FILE"] == "/x"


def test_install_trust_bundle_rejects_concurrent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import examples.codex.databricks.environment as env_mod

    ca = tmp_path / "ca.pem"
    ca.write_bytes(_FAKE_CA)
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)

    first = _install_trust_bundle(str(ca))
    bundle_path = os.environ["SSL_CERT_FILE"]
    try:
        with pytest.raises(RuntimeError, match="one environment at a time"):
            _install_trust_bundle(str(ca))
    finally:
        first()

    # The guard is released, the bundle is gone, and a fresh install works.
    assert env_mod._trust_active is False
    assert not os.path.exists(bundle_path)
    second = _install_trust_bundle(str(ca))
    second()


def test_install_trust_bundle_rolls_back_on_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import examples.codex.databricks.environment as env_mod

    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    with pytest.raises(OSError):
        _install_trust_bundle(str(tmp_path / "does-not-exist.pem"))

    # The guard is rolled back and no env var leaked, so later calls still work.
    assert env_mod._trust_active is False
    assert "SSL_CERT_FILE" not in os.environ
    ca = tmp_path / "ca.pem"
    ca.write_bytes(_FAKE_CA)
    restore = _install_trust_bundle(str(ca))
    restore()
