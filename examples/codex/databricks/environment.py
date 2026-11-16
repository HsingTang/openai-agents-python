"""Provision a native ``Environment`` backed by a Databricks sandbox over stdio.

This mirrors the SDK's Docker stdio executor (the private
``_DockerStdioEnvironment`` that :class:`agents.sandbox.DockerGateway` builds): an
``Environment`` subclass declares a host-owned ``stdio_command`` that the native
runtime spawns, then the runtime speaks the exec-server wire protocol over that
process's stdin/stdout. Here the spawned process is ``launch.py``, which relays
the protocol to ``codex exec-server --listen stdio`` inside a Databricks sandbox
over the ``databricks-sdk`` streaming command API. No inbound ingress, no
gateway server, and no TLS termination are needed: the native runtime owns the
launcher process and talks to it over ordinary pipes.

The subclass overrides ``_to_native`` and ``_validate_remote_url`` - preview
internals that can change across SDK builds. The ``stdio://`` executor source
requires an SDK build with host-owned stdio executor support; see README.md.

Provisioning the sandbox, confirming it is running, and ensuring the ``codex``
exec-server image is present are the caller's responsibility. This module
confirms the sandbox is running before returning the environment. See
``runner.py`` and the README for the create / wait-until-running pattern.
"""

from __future__ import annotations

import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from agents import Environment
from agents.executor import ExecutorToolName

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

_DEFAULT_TOOLS: tuple[ExecutorToolName, ...] = ("exec_command", "write_stdin", "apply_patch")

# The launcher the native runtime spawns to bridge executor stdio to the sandbox.
_LAUNCHER = Path(__file__).resolve().parent / "launch.py"


@dataclass(frozen=True, kw_only=True)
class DatabricksStdioEnvironment(Environment[object]):
    """An ``Environment`` that runs the executor through a local launcher process.

    The native runtime spawns ``_argv`` (with ``_launch_env`` added to its
    environment) and speaks the exec-server protocol over the child's stdio. The
    launcher bridges that to ``codex exec-server`` in the sandbox. A ``stdio://``
    url marks a host-owned stdio executor source; ``headers`` must be empty
    because there is no HTTP/WebSocket connection to authenticate.
    """

    _argv: tuple[str, ...] = field(default=(), repr=False)
    _launch_env: Mapping[str, str] = field(default_factory=dict, repr=False)

    def _validate_remote_url(self) -> None:
        if not self.url.startswith("stdio://") or self.headers:
            raise ValueError("DatabricksStdioEnvironment requires a stdio:// url and no headers")

    def _to_native(self) -> dict[str, Any]:
        result = dict(super()._to_native())
        result["stdio_command"] = {
            "program": self._argv[0],
            "args": list(self._argv[1:]),
            "env": dict(self._launch_env),
            "cwd": None,
        }
        return result


def _resource_name(name: str) -> str:
    """Return the ``sandboxes/<id>`` resource name the sandbox SDK APIs expect."""
    return name if name.startswith("sandboxes/") else f"sandboxes/{name}"


def _confirm_running(workspace_client: WorkspaceClient, sandbox_name: str) -> None:
    """Confirm the sandbox is running before the launcher reaches the exec-server.

    The launcher reaches the exec-server with ``execute_command``, which needs a
    running sandbox. Lifecycle (create / start / poll until running) is the
    caller's responsibility; see ``runner.py``. This is a fast precondition
    check, not a wait.
    """
    from databricks.sdk.service.sandbox import SandboxState

    sandbox = workspace_client.sandbox.get_sandbox(sandbox_name)
    state = sandbox.status.state if sandbox.status is not None else None
    if state != SandboxState.SANDBOX_STATE_RUNNING:
        raise RuntimeError(
            f"Sandbox {sandbox_name!r} is not running (state={state}). Create or start "
            "it and poll until running before opening the environment (see runner.py)."
        )


def databricks_sandbox_environment(
    *,
    workspace_client: WorkspaceClient,
    sandbox_name: str,
    cwd: str,
    tools: Sequence[ExecutorToolName] = _DEFAULT_TOOLS,
    launcher: str | Path = _LAUNCHER,
) -> DatabricksStdioEnvironment:
    """Return an ``Environment`` that reaches ``sandbox_name`` through a launcher.

    Confirms the sandbox is running, derives the workspace host and token from
    ``workspace_client``, and builds a :class:`DatabricksStdioEnvironment` whose
    launcher the native runtime spawns when the session opens. The launcher
    authenticates with those credentials and bridges to ``codex exec-server`` in
    the sandbox.

    Args:
        workspace_client: An authenticated ``databricks.sdk.WorkspaceClient``.
        sandbox_name: An existing, running sandbox with the exec-server available,
            as either the bare id or the ``sandboxes/<id>`` resource name.
        cwd: Absolute remote workspace path the executor operates in.
        tools: Native executor tools to expose. ``write_stdin`` requires
            ``exec_command``.
        launcher: Path to the stdio launcher script. Defaults to the bundled
            ``launch.py``.
    """
    sandbox_name = _resource_name(sandbox_name)
    _confirm_running(workspace_client, sandbox_name)
    host = (workspace_client.config.host or "").rstrip("/")
    authorization = workspace_client.config.authenticate().get("Authorization", "")
    token = authorization.removeprefix("Bearer ").strip()
    if not host or not token:
        raise RuntimeError("Could not derive the Databricks host and token from workspace_client")
    sandbox_id = sandbox_name.removeprefix("sandboxes/")
    return DatabricksStdioEnvironment(
        url=f"stdio://databricks/{sandbox_id}",
        cwd=cwd,
        tools=tuple(tools),
        headers={},
        _argv=(sys.executable, str(launcher), sandbox_name),
        _launch_env={"DATABRICKS_HOST": host, "DATABRICKS_TOKEN": token},
    )
