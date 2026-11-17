"""Tests for the Databricks sandbox stdio environment (``agents.sandbox``).

Cover ``DatabricksStdioEnvironment`` (native ``stdio_command`` emission and
url/header validation) and its ``for_sandbox`` constructor (running precondition,
name normalization, credential derivation, and launcher argv/env wiring).
"""

from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("databricks.sdk")
pytest.importorskip("agents.sandbox")

from databricks.sdk.service.sandbox import SandboxState

from agents.sandbox import DatabricksStdioEnvironment

_LAUNCHER_MODULE = "_agents_codex._databricks_launcher"


class _FakeSandbox:
    def __init__(self, state: Any) -> None:
        self._state = state

    def get_sandbox(self, name: str) -> SimpleNamespace:
        return SimpleNamespace(status=SimpleNamespace(state=self._state))


class _FakeConfig:
    host = "https://my-host.example.com/"

    def authenticate(self) -> dict[str, str]:
        return {"Authorization": "Bearer tok-abc"}


class _FakeWorkspaceClient:
    def __init__(self, state: Any = SandboxState.SANDBOX_STATE_RUNNING) -> None:
        self.sandbox = _FakeSandbox(state)
        self.config = _FakeConfig()


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


def test_for_sandbox_rejects_non_running() -> None:
    with pytest.raises(RuntimeError, match="not running"):
        DatabricksStdioEnvironment.for_sandbox(
            workspace_client=_FakeWorkspaceClient(SandboxState.SANDBOX_STATE_STOPPED),
            sandbox_name="sbx",
            cwd="/w",
        )


def test_for_sandbox_wires_launcher_and_credentials() -> None:
    environment = DatabricksStdioEnvironment.for_sandbox(
        workspace_client=_FakeWorkspaceClient(),
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


def test_for_sandbox_accepts_resource_name() -> None:
    environment = DatabricksStdioEnvironment.for_sandbox(
        workspace_client=_FakeWorkspaceClient(),
        sandbox_name="sandboxes/sbx",
        cwd="/w",
    )
    assert environment.url == "stdio://databricks/sbx"
    assert environment._argv[-1] == "sandboxes/sbx"
