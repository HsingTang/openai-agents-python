"""Databricks sandbox executor as a host-owned stdio environment.

``DatabricksStdioEnvironment`` is the Databricks counterpart of the Docker stdio
executor: it subclasses the native :class:`Environment` and declares a
host-owned ``stdio_command`` the runtime spawns, then speaks the exec-server
protocol over that process's stdio. The launcher relays the protocol to
``codex exec-server --listen stdio`` inside a Databricks sandbox over the
``databricks-sdk`` streaming command API, so there is no gateway server, no TLS,
and no inbound ingress. ``databricks-sdk`` is imported lazily, so importing this
module does not require it.
"""

from __future__ import annotations

import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from ._options import RuntimeEnvironment
from .executor import Environment, ExecutorToolName

_DEFAULT_TOOLS: tuple[ExecutorToolName, ...] = ("exec_command", "write_stdin", "apply_patch")

# The launcher module the native runtime spawns (as ``python -m``) to bridge
# executor stdio to the sandbox. A light module with no native imports, so the
# spawned interpreter does not load the runtime.
_LAUNCHER_MODULE = "_agents_codex._databricks_launcher"


@dataclass(frozen=True, kw_only=True)
class DatabricksStdioEnvironment(Environment[object]):
    """Run the executor in a Databricks sandbox over a host-owned stdio launcher.

    Build one with :meth:`for_sandbox`, then pass it to
    ``agent.session(environment=...)``. The native runtime spawns ``_argv`` (with
    ``_launch_env`` added to its environment) and speaks the exec-server protocol
    over the child's stdio; the launcher relays it to ``codex exec-server`` in the
    sandbox. A ``stdio://`` url marks a host-owned stdio executor source;
    ``headers`` must be empty because there is no HTTP/WebSocket connection to
    authenticate.
    """

    _argv: tuple[str, ...] = field(default=(), repr=False)
    _launch_env: Mapping[str, str] = field(default_factory=dict, repr=False)

    def _validate_remote_url(self) -> None:
        if not self.url.startswith("stdio://") or self.headers:
            raise ValueError("DatabricksStdioEnvironment requires a stdio:// url and no headers")

    def _to_native(self) -> RuntimeEnvironment:
        result = super()._to_native()
        result["stdio_command"] = {
            "program": self._argv[0],
            "args": list(self._argv[1:]),
            "env": dict(self._launch_env),
            "cwd": None,
        }
        return result

    @classmethod
    def for_sandbox(
        cls,
        *,
        workspace_client: Any,
        sandbox_name: str,
        cwd: str,
        tools: Sequence[ExecutorToolName] = _DEFAULT_TOOLS,
    ) -> DatabricksStdioEnvironment:
        """Return an environment bound to a running Databricks sandbox.

        Confirms the sandbox is running and derives the workspace host and token
        from ``workspace_client`` (a ``databricks.sdk.WorkspaceClient``) for the
        launcher to authenticate with. Lifecycle (create / start / poll until
        running) is the caller's responsibility; this only checks the state.

        The derived token is captured into the returned environment. With
        short-lived (OAuth / U2M) credentials it expires (about an hour), so
        build a fresh environment per session rather than reusing one across
        sessions or long-lived turns.

        Args:
            workspace_client: An authenticated ``databricks.sdk.WorkspaceClient``.
            sandbox_name: An existing, running sandbox, as the bare id or the
                ``sandboxes/<id>`` resource name.
            cwd: Absolute remote workspace path the executor operates in.
            tools: Native executor tools to expose. ``write_stdin`` requires
                ``exec_command``.
        """
        from databricks.sdk.service.sandbox import SandboxState

        name = sandbox_name
        if not name.startswith("sandboxes/"):
            name = f"sandboxes/{name}"
        sandbox = workspace_client.sandbox.get_sandbox(name)
        state = sandbox.status.state if sandbox.status is not None else None
        if state != SandboxState.SANDBOX_STATE_RUNNING:
            raise RuntimeError(
                f"Sandbox {name!r} is not running (state={state}). Create or start it and poll "
                "until running before opening the environment."
            )
        host = (workspace_client.config.host or "").rstrip("/")
        authorization = workspace_client.config.authenticate().get("Authorization", "")
        token = authorization.removeprefix("Bearer ").strip()
        if not host or not token:
            raise RuntimeError(
                "Could not derive the Databricks host and token from workspace_client"
            )
        sandbox_id = name.removeprefix("sandboxes/")
        return cls(
            url=f"stdio://databricks/{sandbox_id}",
            cwd=cwd,
            tools=tuple(tools),
            headers={},
            _argv=(sys.executable, "-m", _LAUNCHER_MODULE, name),
            _launch_env={"DATABRICKS_HOST": host, "DATABRICKS_TOKEN": token},
        )
