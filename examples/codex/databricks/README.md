# Databricks sandbox executor

Run an agent driven by this SDK with its `exec_command` / `write_stdin` /
`apply_patch` tools executing inside a Databricks sandbox.
`agents.sandbox.DatabricksStdioEnvironment` is the first-class `Environment` for
this - the Databricks counterpart of `agents.sandbox.DockerGateway`, following
the SDK's "remote execution" pattern (`docs/sdk/execution.md`).

## Usage example

```python
from databricks.sdk import WorkspaceClient
from agents import Agent, NativeProvider
from agents.sandbox import DatabricksStdioEnvironment

w = WorkspaceClient(profile="my-workspace")
# Provision "my-sandbox" (create_sandbox + poll until running); see runner.py.

environment = DatabricksStdioEnvironment.for_sandbox(
    workspace_client=w,
    sandbox_name="my-sandbox",
    cwd="/home/sandbox-agent/work",
)
async with NativeProvider(
    base_url="https://<workspace-host>/ai-gateway/codex/v1",
    api_key=w.config.authenticate()["Authorization"].removeprefix("Bearer "),
    supports_websockets=False,
) as provider:
    agent = Agent(model="gpt-5.5", provider=provider, instructions="...")
    async with agent.session(environment=environment) as session:
        result = await (await session.submit("Inspect the workspace")).get_result()
        print(result.final_output)
```

## Install and run the example

The SDK is distributed as a GitHub preview wheel (off PyPI). It ships a compiled
native runtime, so it needs a supported host: **Linux x86_64 with glibc 2.28 or newer**,
macOS arm64, or Windows x64.

```sh
pip install -r examples/codex/databricks/requirements.txt
```

Provide Databricks auth (a `.databrickscfg` profile), then run the example script
`runner.py` from the repo root. The model runs through the workspace's Databricks
AI Gateway by default, using the profile's own token, so no OpenAI key is needed:

```sh
SANDBOX_NAME=my-sandbox DATABRICKS_CONFIG_PROFILE=my-workspace \
  PYTHONPATH=. python -m examples.codex.databricks.runner "Inspect the workspace"
```

`SANDBOX_NAME` accepts the bare id or the `sandboxes/<id>` resource name. Set
`SANDBOX_CREATE=1` (with `SANDBOX_IMAGE=...`) to provision the sandbox first. The
launcher starts `exec-server` on each session; it is killed when the sandbox
stops and relaunched on the next session.

To run the model through OpenAI directly instead of the Databricks AI Gateway,
set `OPENAI_BASE_URL`, `OPENAI_API_KEY`, and an OpenAI model id your account can
use:

```sh
SANDBOX_NAME=my-sandbox DATABRICKS_CONFIG_PROFILE=my-workspace \
  OPENAI_BASE_URL=https://api.openai.com/v1 OPENAI_API_KEY=sk-... \
  OPENAI_MODEL=gpt-5.3-codex \
  PYTHONPATH=. python -m examples.codex.databricks.runner "Inspect the workspace"
```

## Prerequisites

1. **A running sandbox.** The application creates the sandbox (or reuses one),
   starts it if stopped, and polls its status until running before opening the
   environment - the exec-server is reached through `execute_command`, which
   needs a running sandbox.
2. **A `codex` binary in the sandbox image.** The launcher runs
   `codex exec-server --listen stdio` in the sandbox. The default sandbox image
   already contains a compatible `codex` binary; for a
   [user-supplied custom image](https://docs.databricks.com/aws/en/compute/serverless/sandbox-custom-image),
   ensure `codex` is installed.

## Credentials and session lifetime

`DatabricksStdioEnvironment.for_sandbox` captures the workspace token into the
environment. With short-lived (OAuth / U2M) credentials that token expires (about
an hour), so build a fresh environment per session rather than reusing one. The
launcher forwards the exec-server's stderr for diagnostics and returns its exit
code, and closes the exec-server's stdin on teardown so it shuts down.

## References

- [Databricks Sandbox documentation](https://docs.databricks.com/aws/en/compute/serverless/sandbox)
- [Sandbox SDK on databricks-sdk for Python](https://databricks-sdk-py.readthedocs.io/en/latest/workspace/sandbox/sandbox.html)
