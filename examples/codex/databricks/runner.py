"""Run an agent against a Databricks sandbox through the native executor.

Prerequisites (see README.md):
  - the SDK preview wheel plus this example's requirements.txt
  - a Databricks sandbox whose image carries the ``codex`` exec-server
  - Databricks auth (a CLI profile)

Environment variables:
  DATABRICKS_CONFIG_PROFILE     Databricks CLI profile (default: DEFAULT)
  SANDBOX_NAME                  sandbox id or ``sandboxes/<id>`` resource name (required)
  SANDBOX_CWD                   remote workspace path (default: /home/sandbox-agent/work)
  SANDBOX_CREATE                "1" to provision the sandbox if absent (default: off)
  SANDBOX_IMAGE                 image with the codex exec-server (used when creating)
  SANDBOX_INACTIVITY_TIMEOUT_S  auto-stop timeout in seconds (default: 900)
  OPENAI_MODEL                  model name (default: gpt-5.5)
  OPENAI_BASE_URL               model-provider base URL (default: the workspace's
                                Databricks AI Gateway codex endpoint)
  OPENAI_API_KEY                model-provider key (default: a token minted from the
                                Databricks profile, so no OpenAI key is needed)

With SANDBOX_CREATE=1 the runner provisions the sandbox. In all cases it then
confirms the sandbox is running (starting it if stopped and polling until ready)
and ensures the workspace directory exists before opening
``databricks_sandbox_environment`` - the gateway reaches the exec-server through
``execute_command``, which needs a running sandbox. The gateway launches
``exec-server stdio`` on each connection, so no separate exec-server start is
needed here; a sandbox that auto-stops is started again before the next session.

By default the model runs through the sandbox workspace's Databricks AI Gateway
codex endpoint, authenticated with the profile's own token (PAT or OAuth), so no
OpenAI key is required. Set OPENAI_BASE_URL / OPENAI_API_KEY to use a different
provider (for example OpenAI directly).

Run:
    SANDBOX_NAME=openai-agent-sandbox DATABRICKS_CONFIG_PROFILE=my-workspace \
      PYTHONPATH=. python -m examples.codex.databricks.runner "Inspect the project"
"""

import asyncio
import os
import sys
import time

from databricks.sdk import WorkspaceClient

from agents import Agent, NativeProvider
from examples.codex.databricks.environment import databricks_sandbox_environment


def _resource_name(name: str) -> str:
    """Return the ``sandboxes/<id>`` resource name the sandbox SDK APIs expect."""
    return name if name.startswith("sandboxes/") else f"sandboxes/{name}"


def _ensure_sandbox(workspace_client: WorkspaceClient, sandbox_id: str) -> str:
    """Provision the sandbox and return its ``sandboxes/<id>`` resource name.

    Mirrors the Databricks sandbox cookbook. Confirm the spec types and their
    import path against your installed ``databricks-sdk`` - the sandbox service
    module layout may differ by SDK version.
    """
    # databricks-sdk re-exports Duration without __all__, so strict mypy flags it.
    from databricks.sdk.service.sandbox import (  # type: ignore[attr-defined]
        ComputeSpec,
        Duration,
        EnvironmentSpec,
        Sandbox,
        SandboxSpec,
    )

    timeout_s = int(os.getenv("SANDBOX_INACTIVITY_TIMEOUT_S", "900"))
    spec = SandboxSpec(compute=ComputeSpec(inactivity_timeout=Duration(seconds=timeout_s)))
    image = os.getenv("SANDBOX_IMAGE")
    if image is not None:
        spec.environment = EnvironmentSpec(image_uri=image)
    created = workspace_client.sandbox.create_sandbox(Sandbox(spec=spec), sandbox_id=sandbox_id)
    return created.name or _resource_name(sandbox_id)


def _wait_until_running(
    workspace_client: WorkspaceClient, sandbox_name: str, timeout_s: int = 300
) -> None:
    """Confirm the sandbox is running before the exec-server is launched.

    Starts a stopped sandbox, then polls until it reports running. Mirrors the
    managed-API pattern of polling status before launching the exec-server.
    """
    from databricks.sdk.service.sandbox import SandboxState

    deadline = time.monotonic() + timeout_s
    started = False
    while True:
        sandbox = workspace_client.sandbox.get_sandbox(sandbox_name)
        state = sandbox.status.state if sandbox.status is not None else None
        if state == SandboxState.SANDBOX_STATE_RUNNING:
            return
        if state == SandboxState.SANDBOX_STATE_STOPPED and not started:
            workspace_client.sandbox.start_sandbox(sandbox_name)
            started = True
        if time.monotonic() > deadline:
            raise TimeoutError(f"sandbox {sandbox_name!r} not running (state={state})")
        time.sleep(2)


def _model_credentials(workspace_client: WorkspaceClient) -> tuple[str, str]:
    """Return ``(base_url, api_key)`` for ``NativeProvider``.

    Defaults to the workspace's Databricks AI Gateway codex endpoint, with a
    token minted from the Databricks profile (PAT or OAuth) - no OpenAI key
    needed. Override with OPENAI_BASE_URL / OPENAI_API_KEY for another provider.
    """
    host = (workspace_client.config.host or "").rstrip("/")
    base_url = os.getenv("OPENAI_BASE_URL") or f"{host}/ai-gateway/codex/v1"
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        authorization = workspace_client.config.authenticate().get("Authorization", "")
        api_key = authorization.removeprefix("Bearer ").strip()
    return base_url, api_key


async def main(task: str) -> None:
    workspace_client = WorkspaceClient(profile=os.getenv("DATABRICKS_CONFIG_PROFILE", "DEFAULT"))
    requested = os.environ["SANDBOX_NAME"]
    if os.getenv("SANDBOX_CREATE") == "1":
        sandbox_name = _ensure_sandbox(workspace_client, requested.removeprefix("sandboxes/"))
    else:
        sandbox_name = _resource_name(requested)
    _wait_until_running(workspace_client, sandbox_name)

    cwd = os.getenv("SANDBOX_CWD", "/home/sandbox-agent/work")
    workspace_client.sandbox.execute_command_sync(
        sandbox_name, "/bin/bash", args=["-lc", f"mkdir -p {cwd}"]
    )

    base_url, api_key = _model_credentials(workspace_client)
    async with databricks_sandbox_environment(
        workspace_client=workspace_client,
        sandbox_name=sandbox_name,
        cwd=cwd,
    ) as environment:
        async with NativeProvider(
            base_url=base_url,
            api_key=api_key,
            supports_websockets=False,
        ) as provider:
            agent = Agent(
                instructions="Inspect the assigned workspace and report findings.",
                model=os.getenv("OPENAI_MODEL", "gpt-5.5"),
                provider=provider,
            )
            async with agent.session(environment=environment) as session:
                result = await (await session.submit(task)).get_result()
                print(result.final_output)


if __name__ == "__main__":
    asyncio.run(main(" ".join(sys.argv[1:]) or "Inspect the assigned workspace"))
