"""Tests for the Databricks sandbox stdio environment example.

Cover the ``DatabricksStdioEnvironment`` subclass (native ``stdio_command``
emission and url/header validation) and the ``databricks_sandbox_environment``
factory (running precondition, name normalization, credential derivation, and
launcher argv/env wiring). Run from the repo root with the preview wheel and
``examples/codex/databricks/requirements.txt`` installed.
"""

from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

pytest.importorskip("databricks.sdk")

from databricks.sdk.service.sandbox import SandboxState

from examples.codex.databricks.environment import (
    _LAUNCHER,
    DatabricksStdioEnvironment,
    databricks_sandbox_environment,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


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


def _client(state: Any = SandboxState.SANDBOX_STATE_RUNNING) -> WorkspaceClient:
    return cast("WorkspaceClient", _FakeWorkspaceClient(state))


def test_to_native_emits_stdio_command() -> None:
    environment = DatabricksStdioEnvironment(
        url="stdio://databricks/sbx",
        cwd="/home/sandbox-agent/work",
        tools=("exec_command", "write_stdin", "apply_patch"),
        headers={},
        _argv=("/usr/bin/python", "/pkg/launch.py", "sandboxes/sbx"),
        _launch_env={"DATABRICKS_HOST": "https://h", "DATABRICKS_TOKEN": "t"},
    )
    native = environment._to_native()
    assert native["stdio_command"] == {
        "program": "/usr/bin/python",
        "args": ["/pkg/launch.py", "sandboxes/sbx"],
        "env": {"DATABRICKS_HOST": "https://h", "DATABRICKS_TOKEN": "t"},
        "cwd": None,
    }
    # The stdio url is carried through; the runtime admits it as a host-owned source.
    assert native["url"] == "stdio://databricks/sbx"


def test_rejects_non_stdio_url() -> None:
    with pytest.raises(ValueError, match="stdio://"):
        DatabricksStdioEnvironment(
            url="wss://127.0.0.1:9000",
            cwd="/w",
            headers={},
            _argv=("python", "launch.py", "sandboxes/sbx"),
        )


def test_rejects_headers() -> None:
    with pytest.raises(ValueError, match="no headers"):
        DatabricksStdioEnvironment(
            url="stdio://databricks/sbx",
            cwd="/w",
            headers={"Authorization": "Bearer x"},
            _argv=("python", "launch.py", "sandboxes/sbx"),
        )


def test_factory_rejects_non_running_sandbox() -> None:
    with pytest.raises(RuntimeError, match="not running"):
        databricks_sandbox_environment(
            workspace_client=_client(SandboxState.SANDBOX_STATE_STOPPED),
            sandbox_name="sbx",
            cwd="/w",
        )


def test_factory_wires_launcher_and_credentials() -> None:
    environment = databricks_sandbox_environment(
        workspace_client=_client(),
        sandbox_name="sbx",
        cwd="/home/sandbox-agent/work",
    )
    assert environment.url == "stdio://databricks/sbx"
    assert environment.cwd == "/home/sandbox-agent/work"
    assert dict(environment.headers) == {}
    assert tuple(environment.tools) == ("exec_command", "write_stdin", "apply_patch")
    # Name normalized to the resource form and passed to the launcher.
    assert environment._argv == (sys.executable, str(_LAUNCHER), "sandboxes/sbx")
    # Host trailing slash stripped; token derived from the profile (no OpenAI key).
    assert environment._launch_env == {
        "DATABRICKS_HOST": "https://my-host.example.com",
        "DATABRICKS_TOKEN": "tok-abc",
    }


def test_factory_accepts_resource_name() -> None:
    environment = databricks_sandbox_environment(
        workspace_client=_client(),
        sandbox_name="sandboxes/sbx",
        cwd="/w",
    )
    assert environment.url == "stdio://databricks/sbx"
    assert environment._argv[-1] == "sandboxes/sbx"
