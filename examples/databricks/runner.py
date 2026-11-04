"""Run an agent against a Databricks sandbox through the native executor.

Prerequisites (see src/agents/extensions/sandbox/databricks/README.md):
  - the SDK preview wheel plus the ``databricks`` extra
  - a running Databricks sandbox whose image carries the ``codex`` exec-server
  - Databricks auth (a CLI profile) and model-provider auth

Environment variables:
  DATABRICKS_CONFIG_PROFILE  Databricks CLI profile (default: DEFAULT)
  SANDBOX_NAME               the target sandbox name/id (required)
  SANDBOX_CWD                remote workspace path (default: /home/sandbox-agent/work)
  OPENAI_API_KEY             model-provider key for NativeProvider (required)
  OPENAI_BASE_URL            optional model-provider base URL
  OPENAI_MODEL               model name (default: gpt-5.5)

Run:
    python -m examples.databricks.runner "Inspect the project and list next steps"
"""

import asyncio
import os
import sys

from databricks.sdk import WorkspaceClient

from agents import Agent, NativeProvider
from agents.extensions.sandbox.databricks import databricks_sandbox_environment


async def main(task: str) -> None:
    workspace_client = WorkspaceClient(profile=os.getenv("DATABRICKS_CONFIG_PROFILE", "DEFAULT"))
    async with databricks_sandbox_environment(
        workspace_client=workspace_client,
        sandbox_name=os.environ["SANDBOX_NAME"],
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
