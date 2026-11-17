"""Tests for the Databricks sandbox stdio environment.

Cover ``DatabricksStdioEnvironment`` (native ``stdio_command`` emission and
url/header validation) and its ``for_sandbox`` constructor (running precondition,
name normalization, credential derivation, launcher argv/env wiring). The class
is imported from its owning module so these run without the optional
``databricks-sdk`` / ``[classic]`` extras; ``for_sandbox``'s lazy
``databricks.sdk`` import is satisfied with an injected fake.
"""

from __future__ import annotations

import sys
import types
from types import SimpleNamespace
from typing import Any

import pytest

from _agents_codex._databricks import _LAUNCHER_MODULE, DatabricksStdioEnvironment


def test_to_native_emits_stdio_command() -> None:
    environment = DatabricksStdioEnvironment(
        url="stdio://databricks/sbx",
        cwd="/home/sandbox-agent/work",
        tools=("exec_command", "write_stdin", "apply_patch"),
        headers={},
        _argv=("/usr/bin/python", "-m", _LAUNCHER_MODULE, "sandboxes/sbx"),
        _launch_env={"DATABRICKS_HOST": "https://h", "DATABRICKS_TOKEN": "t"},
    )
    native = environment._to_native()
    assert native["stdio_command"] == {
        "program": "/usr/bin/python",
        "args": ["-m", _LAUNCHER_MODULE, "sandboxes/sbx"],
        "env": {"DATABRICKS_HOST": "https://h", "DATABRICKS_TOKEN": "t"},
        "cwd": None,
    }
    assert native["url"] == "stdio://databricks/sbx"


def test_rejects_non_stdio_url() -> None:
    with pytest.raises(ValueError, match="stdio://"):
        DatabricksStdioEnvironment(
            url="wss://127.0.0.1:9000",
            cwd="/w",
            headers={},
            _argv=("python", "-m", _LAUNCHER_MODULE, "sandboxes/sbx"),
        )


def test_rejects_headers() -> None:
    with pytest.raises(ValueError, match="no headers"):
        DatabricksStdioEnvironment(
            url="stdio://databricks/sbx",
            cwd="/w",
            headers={"Authorization": "Bearer x"},
            _argv=("python", "-m", _LAUNCHER_MODULE, "sandboxes/sbx"),
        )


class _FakeSandboxState:
    SANDBOX_STATE_RUNNING = "RUNNING"
    SANDBOX_STATE_STOPPED = "STOPPED"


@pytest.fixture
def fake_sandbox_state(monkeypatch: pytest.MonkeyPatch) -> type[_FakeSandboxState]:
    for name in ("databricks", "databricks.sdk", "databricks.sdk.service"):
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    module = types.ModuleType("databricks.sdk.service.sandbox")
    module.SandboxState = _FakeSandboxState  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "databricks.sdk.service.sandbox", module)
    return _FakeSandboxState


class _FakeSandbox:
    def __init__(self, state: Any) -> None:
        self._state = state

    def get_sandbox(self, name: str) -> SimpleNamespace:
        return SimpleNamespace(status=SimpleNamespace(state=self._state))


class _FakeConfig:
    def __init__(self, auth: dict[str, str]) -> None:
        self.host = "https://my-host.example.com/"
        self._auth = auth

    def authenticate(self) -> dict[str, str]:
        return self._auth


class _FakeWorkspaceClient:
    def __init__(self, state: Any, auth: dict[str, str] | None = None) -> None:
        self.sandbox = _FakeSandbox(state)
        self.config = _FakeConfig({"Authorization": "Bearer tok-abc"} if auth is None else auth)


def test_for_sandbox_rejects_non_running(fake_sandbox_state: type[_FakeSandboxState]) -> None:
    with pytest.raises(RuntimeError, match="not running"):
        DatabricksStdioEnvironment.for_sandbox(
            workspace_client=_FakeWorkspaceClient(fake_sandbox_state.SANDBOX_STATE_STOPPED),
            sandbox_name="sbx",
            cwd="/w",
        )


def test_for_sandbox_rejects_none_status(fake_sandbox_state: type[_FakeSandboxState]) -> None:
    client = _FakeWorkspaceClient(fake_sandbox_state.SANDBOX_STATE_RUNNING)
    client.sandbox.get_sandbox = lambda name: SimpleNamespace(status=None)  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="not running"):
        DatabricksStdioEnvironment.for_sandbox(
            workspace_client=client, sandbox_name="sbx", cwd="/w"
        )


def test_for_sandbox_rejects_missing_token(fake_sandbox_state: type[_FakeSandboxState]) -> None:
    client = _FakeWorkspaceClient(fake_sandbox_state.SANDBOX_STATE_RUNNING, auth={})
    with pytest.raises(RuntimeError, match="Could not derive"):
        DatabricksStdioEnvironment.for_sandbox(
            workspace_client=client, sandbox_name="sbx", cwd="/w"
        )


def test_for_sandbox_wires_launcher_and_credentials(
    fake_sandbox_state: type[_FakeSandboxState],
) -> None:
    environment = DatabricksStdioEnvironment.for_sandbox(
        workspace_client=_FakeWorkspaceClient(fake_sandbox_state.SANDBOX_STATE_RUNNING),
        sandbox_name="sbx",
        cwd="/home/sandbox-agent/work",
    )
    assert environment.url == "stdio://databricks/sbx"
    assert environment.cwd == "/home/sandbox-agent/work"
    assert dict(environment.headers) == {}
    assert tuple(environment.tools) == ("exec_command", "write_stdin", "apply_patch")
    assert environment._argv == (sys.executable, "-m", _LAUNCHER_MODULE, "sandboxes/sbx")
    assert environment._launch_env == {
        "DATABRICKS_HOST": "https://my-host.example.com",
        "DATABRICKS_TOKEN": "tok-abc",
    }


def test_for_sandbox_accepts_resource_name(fake_sandbox_state: type[_FakeSandboxState]) -> None:
    environment = DatabricksStdioEnvironment.for_sandbox(
        workspace_client=_FakeWorkspaceClient(fake_sandbox_state.SANDBOX_STATE_RUNNING),
        sandbox_name="sandboxes/sbx",
        cwd="/w",
    )
    assert environment.url == "stdio://databricks/sbx"
    assert environment._argv[-1] == "sandboxes/sbx"


def test_exported_from_agents_sandbox() -> None:
    sandbox = pytest.importorskip("agents.sandbox")
    assert "DatabricksStdioEnvironment" in sandbox.__all__
    assert sandbox.DatabricksStdioEnvironment is DatabricksStdioEnvironment
