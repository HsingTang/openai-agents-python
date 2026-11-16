# Databricks sandbox executor

Back a native `Environment` with a Databricks sandbox, so an agent driven by
this SDK runs `exec_command` / `write_stdin` / `apply_patch` inside the sandbox.
This is the "remote execution" pattern from `docs/sdk/execution.md`, built the
same way the SDK's own Docker support is: `agents.sandbox.DatabricksStdioEnvironment`
is an `Environment` subclass that declares a host-owned `stdio_command` the
native runtime spawns, and the runtime speaks the exec-server protocol over that
process's stdin/stdout. It is the Databricks counterpart of
`agents.sandbox.DockerGateway`.

> **Status.** The stdio transport and the full turn are validated end-to-end
> against a live Databricks sandbox on the `0.24.0-alpha.8` preview wheel: a real
> `codex exec-server` ran the agent's tool call in the sandbox with no gateway
> server and no TLS, driven over the `databricks-sdk` streaming command API, for
> both model providers (Databricks AI Gateway and OpenAI direct). The `stdio://`
> executor source requires the host-owned stdio admission change in this PR's
> native runtime (`packages/codex/rust/environment.rs`); it is not yet in a
> released preview wheel. Until it ships, build the SDK from this branch (see
> `packages/codex/MAINTAINING.md`). The Python pieces are covered by
> `tests/codex/` against the released wheel.

## How it works

`DatabricksStdioEnvironment.for_sandbox(...)` confirms the sandbox is running,
derives the workspace host and token from the `WorkspaceClient`, and returns an
environment to hand to `agent.session(environment=...)`. The environment emits a
`stdio_command`: the argv of a local launcher process. When the session opens,
the native runtime spawns that launcher and speaks the exec-server wire protocol
over its stdin/stdout. The launcher relays that protocol to `codex exec-server
--listen stdio` inside the sandbox over the `databricks-sdk` streaming command
API (`WorkspaceClient.sandbox.execute_command`, whose `CommandProcess` exposes the
command's `stdin`, `stdout` and exit). It does not parse the protocol; it only
drops a startup banner the sandbox's `codex` prints on stdout before the protocol
begins (non-JSON lines) so it never reaches the runtime as a bogus message.

Because the runtime owns the launcher process and talks to it over ordinary
pipes, there is no WebSocket gateway, no TLS termination, and no shared-secret
handshake. `execute_command` reaches the sandbox outbound through the Databricks
control plane, so the sandbox needs no inbound ingress. The agent loop runs in
your process (via `NativeProvider`), which is what makes the `Environment`
approval / transform callbacks and `NetworkPolicy` available.

## Files

- `runner.py` - a runnable end-to-end example. The environment itself is the
  first-class `agents.sandbox.DatabricksStdioEnvironment`.

## Install and run

The v1.0 SDK is a GitHub preview wheel (off PyPI). It ships a compiled native
runtime, so it needs a supported host: **Linux x86_64 with glibc 2.28 or newer**,
macOS arm64, or Windows x64. This example additionally needs the host-owned
`stdio://` executor source (see **Status** above), so build the SDK from this
branch per `packages/codex/MAINTAINING.md` until that change is in a released
wheel. Then install this example's requirements:

```sh
pip install -r examples/codex/databricks/requirements.txt
```

Provide Databricks auth (a CLI profile) and run from the repo root. The model
runs through the workspace's Databricks AI Gateway by default, using the
profile's own token, so no OpenAI key is needed:

```sh
SANDBOX_NAME=openai-agent-sandbox DATABRICKS_CONFIG_PROFILE=my-workspace \
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
SANDBOX_NAME=openai-agent-sandbox DATABRICKS_CONFIG_PROFILE=my-workspace \
  OPENAI_BASE_URL=https://api.openai.com/v1 OPENAI_API_KEY=sk-... \
  OPENAI_MODEL=gpt-5.3-codex \
  PYTHONPATH=. python -m examples.codex.databricks.runner "Inspect the workspace"
```

Only the model provider changes; the sandbox, launcher, and tools are identical.

## Prerequisites

1. **A running sandbox.** The application creates the sandbox (or reuses one),
   starts it if stopped, and polls its status until running before opening the
   environment - the exec-server is reached through `execute_command`, which
   needs a running sandbox. `runner.py` shows this, and
   `DatabricksStdioEnvironment.for_sandbox` confirms it.
2. **exec-server stdio mode in the image.** The sandbox's `codex` runs the
   standalone exec-server over stdio with `codex exec-server --listen stdio`,
   which the launcher relays.
3. **An SDK build with the host-owned `stdio://` executor source** (see
   **Status**).

## Tests

`tests/codex/test_databricks_environment.py` and `test_databricks_launcher.py`
cover the `stdio_command` emission, the `stdio://` / header validation, the
factory's running-sandbox precondition, name normalization, and credential and
launcher-argv wiring, plus the launcher's startup-banner filter. They exercise
only the Python layer (not the native admission or a live sandbox), but they
import the packaged class, so run them against an SDK built from this branch.
Install that wheel, this example's requirements, and `pytest`, then run them
explicitly from the repo root (the default `testpaths`
does not include `tests/codex`):

```sh
pytest tests/codex
```

The native admission change (`stdio://`) is covered by Rust unit tests in
`packages/codex/rust/environment.rs` and qualified by the native build.
