"""Run an agent against a Databricks sandbox through the native executor.

Prerequisites (see README.md):
  - the SDK preview wheel plus this example's requirements.txt
  - a Databricks sandbox whose image carries the ``codex`` exec-server
  - Databricks auth (a CLI profile) and model-provider auth

Environment variables:
  DATABRICKS_CONFIG_PROFILE     Databricks CLI profile (default: DEFAULT)
  SANDBOX_NAME                  target sandbox name/id (required)
  SANDBOX_CWD                   remote workspace path (default: /home/sandbox-agent/work)
  SANDBOX_CREATE                "1" to provision the sandbox if absent (default: off)
  SANDBOX_IMAGE                 image with the codex exec-server (used when creating)
  SANDBOX_INACTIVITY_TIMEOUT_S  auto-stop timeout in seconds (default: 900)
  OPENAI_API_KEY                model-provider key for NativeProvider (required)
  OPENAI_BASE_URL               optional model-provider base URL
  OPENAI_MODEL                  model name (default: gpt-5.5)

With SANDBOX_CREATE=1 the runner provisions the sandbox, then hands its name to
``databricks_sandbox_environment``. The gateway launches ``exec-server stdio`` on
each connection, so no separate exec-server start is needed here; a sandbox that
auto-stops is relaunched on the next connection.

Run:
    PYTHONPATH=. python -m examples.codex.databricks.runner "Inspect the project and list next steps"
"""

import asyncio
import os
import sys

from databricks.sdk import WorkspaceClient

from agents import Agent, NativeProvider
from examples.codex.databricks.environment import databricks_sandbox_environment


def _ensure_sandbox(workspace_client: WorkspaceClient, sandbox_name: str) -> None:
    """Provision ``sandbox_name`` if it does not already exist.

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
    workspace_client.sandbox.create_sandbox(Sandbox(spec=spec), sandbox_id=sandbox_name)


async def main(task: str) -> None:
    workspace_client = WorkspaceClient(profile=os.getenv("DATABRICKS_CONFIG_PROFILE", "DEFAULT"))
    sandbox_name = os.environ["SANDBOX_NAME"]
    if os.getenv("SANDBOX_CREATE") == "1":
        _ensure_sandbox(workspace_client, sandbox_name)

    async with databricks_sandbox_environment(
        workspace_client=workspace_client,
        sandbox_name=sandbox_name,
        cwd=os.getenv("SANDBOX_CWD", "/home/sandbox-agent/work"),
    ) as environment:
        async with NativeProvider(
            base_url=os.getenv("OPENAI_BASE_URL"),
            api_key=os.environ["OPENAI_API_KEY"],
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
