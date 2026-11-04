# Databricks sandbox executor

Back a native `Environment` with a Databricks sandbox, so an agent driven by
this SDK runs `exec_command` / `write_stdin` / `apply_patch` inside the sandbox.
This is the "remote execution" pattern from `docs/sdk/execution.md` - an
application-owned gateway plus a lifetime-managing `Environment` context manager -
the Databricks counterpart of `examples/codex/offline_docker.py`.

> **Validated end-to-end against a live Databricks sandbox (2026-11-11).**
> The native `Environment` connected to the gateway's `wss://` endpoint and drove
> a real `codex exec-server --listen stdio`. `NativeProvider`
> ran against the sandbox's Databricks AI Gateway codex endpoint (PAT from
> `~/.databrickscfg`; no OpenAI key). `openai-agents` 0.24.0-alpha.7 (the preview
> wheel) on Ubuntu 24.04 / glibc 2.39.
>
> Validated two ways: with the controller co-located in the sandbox (a local
> exec-server subprocess), and with an **external controller** (a glibc-2.39
> container) driving a separate sandbox over the real `databricks-sdk`
> `execute_command` streaming transport - the actual product topology, tool
> side-effects confirmed back in the executor sandbox.
>
> Exercised through this gateway over that transport (tool calls scripted
> deterministically with the SDK's Responses fixture):
>
> - **Multi-turn, resume, and sandbox stop-start.** Two turns in one session
>   (turn 2 read turn 1's file); a fresh session relaunched the exec-server; a
>   real `stop_sandbox` + `start_sandbox` cycle returned to service with the
>   workspace preserved.
> - **All three tools.** `exec_command`, interactive-TTY `write_stdin`, and
>   `apply_patch` (which added a file in the sandbox).
> - **Large output.** A 200k-line stream and a single ~4 MB line both relayed
>   through the newline framing without truncation.
> - **Environment policy hooks.** `approval` returning `False` blocked a
>   command's side-effect; `transform_arguments` rewrote the command;
>   `transform_result` changed what the model saw; a `NetworkPolicy` was
>   **enforced** by the executor's managed proxy (an allowed domain returned 200,
>   a non-allowed domain was blocked).
> - **Model.** `model="gpt-5.5"` is served by `system.ai.gpt-5-5` on the AI
>   Gateway.
> - **Startup-banner handling.** The sandbox's `codex` prints a startup banner to
>   stdout before the protocol; a live turn confirmed the gateway drops that
>   non-JSON preamble and the tool still executes (`proof_pw.txt` created).
> - **Static checks.** `pytest tests/codex` (14 passed) and `mypy --strict` run
>   against the preview wheel on a glibc-2.39 host.

## How it works

The native `Environment` dials an authenticated `wss://` executor and relays the
exec-server protocol over it. A Databricks sandbox runs that exec-server in
stdio mode, reached through the `databricks-sdk` streaming command API
(`WorkspaceClient.sandbox.execute_command`). Nothing in the SDK speaks stdio and
nothing in the sandbox speaks WebSocket, so `SandboxExecServerGateway`
(`gateway.py`) bridges them: it terminates the WebSocket, authenticates the
bearer token at the HTTP handshake, opens the exec-server's stdio, and relays
bytes both ways. It does not parse the protocol; it only drops a startup banner
the sandbox's `codex` prints on stdout before the protocol begins (non-JSON
lines) so it never reaches the SDK as a bogus message.

`databricks_sandbox_environment` (`environment.py`) confirms the sandbox is
running, starts that gateway, and yields a configured `Environment`, so the whole
lifecycle fits one `async with` (the "manage the executor lifetime" pattern).

Because `execute_command` reaches the sandbox outbound through the Databricks
control plane, the gateway runs on loopback next to the application and the
sandbox needs no inbound ingress. The agent loop runs in your process (via
`NativeProvider`), which is what makes the `Environment` approval / transform
callbacks and `NetworkPolicy` available.

## Files

- `gateway.py` - `SandboxExecServerGateway` plus a self-signed loopback TLS helper.
- `environment.py` - `databricks_sandbox_environment`, the lifetime context manager.
- `runner.py` - a runnable end-to-end example.

## Install and run

The v1.0 SDK is a GitHub preview wheel (off PyPI); this example is validated
against `openai-agents` 0.24.0-alpha.7. The wheel ships a compiled native
runtime, so it needs a supported host: **Linux x86_64 with glibc 2.39 or
newer**, macOS arm64, or Windows x64. Get the wheel URL from
`docs/sdk/installation.md`, then install it and this example's requirements:

```sh
pip install "<openai-agents preview wheel url>"   # URL in docs/sdk/installation.md
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
`SANDBOX_CREATE=1` (with `SANDBOX_IMAGE=...`) to provision the sandbox first;
`exec-server` is killed when the sandbox stops, so a new connection after a stop
relaunches it.

To run the model through OpenAI directly instead of the Databricks AI Gateway,
set `OPENAI_BASE_URL`, `OPENAI_API_KEY`, and an OpenAI model id your account can
use:

```sh
SANDBOX_NAME=openai-agent-sandbox DATABRICKS_CONFIG_PROFILE=my-workspace \
  OPENAI_BASE_URL=https://api.openai.com/v1 OPENAI_API_KEY=sk-... \
  OPENAI_MODEL=gpt-5.3-codex \
  PYTHONPATH=. python -m examples.codex.databricks.runner "Inspect the workspace"
```

Only the model provider changes; the sandbox, gateway, and tools are identical.

## TLS

`Environment` requires `wss`, so the gateway needs TLS even on loopback. By
default `databricks_sandbox_environment` generates an ephemeral self-signed
certificate and, for the session, points `SSL_CERT_FILE` at a bundle of the
public CAs (`certifi`) plus that certificate, then restores it on exit - so the
native runtime trusts the gateway without breaking model-provider TLS. This
mirrors the `remote_executor.py` guidance ("Set `SSL_CERT_FILE` to your executor
CA if it uses a private certificate"). Pass your own `ssl_context` and set
`trust_gateway_ca=False` to manage trust yourself.

`SSL_CERT_FILE` is process-global, so auto-managed trust supports only one
environment per process; a concurrent second use raises rather than silently
corrupting trust. Open design question for OAI: a per-connection trust lever on
`Environment` (a client CA / SSL context) would let a self-hosted gateway avoid
mutating a process-global variable.

## Prerequisites (validated 2026-11-11 against a Databricks sandbox)

1. **A running sandbox.** The application creates the sandbox (or reuses one),
   starts it if stopped, and polls its status until running before opening the
   environment - the exec-server is reached through `execute_command`, which
   needs a running sandbox. `runner.py` shows this, and
   `databricks_sandbox_environment` confirms it before starting the gateway.
2. **exec-server stdio mode in the image - SUPPORTED.** The sandbox's `codex`
   runs the standalone exec-server over stdio with `codex exec-server --listen
   stdio`, which the gateway relays.
3. **Streaming bidirectional relay over `execute_command` - CONFIRMED.** An
   external-controller turn drove tool calls through the gateway to the
   exec-server and back over the real `databricks-sdk` `execute_command`
   streaming `CommandProcess` (`stdin`, `stdout.readline`, `text=False`),
   creating a file in the executor sandbox's workspace.
4. **WebSocket newline framing - CONFIRMED by the live turn.** The gateway frames
   on newlines (one message per `stdout.readline`, newline appended inbound). The
   live turn's tool calls parsed and executed, so the framing matches the
   exec-server's stdio protocol.

## Tests

`tests/codex/test_databricks_gateway.py` and `test_databricks_environment.py`
cover the relay, startup-banner filtering, bearer auth, the large-frame cap,
teardown paths, the context manager, the running-sandbox precondition, and the
`SSL_CERT_FILE` trust-bundle helper with a fake `CommandProcess`. Install the
preview wheel, this example's requirements, and `pytest` + `pytest-asyncio`,
then run them explicitly from the repo root (the default `testpaths` does not
include `tests/codex`):

```sh
pytest tests/codex
```

(validated: 14 passed, and `mypy --strict` clean, against 0.24.0-alpha.7 on a
glibc-2.39 host.)

These use a fake `CommandProcess`, so they do not exercise the real exec-server
framing themselves - that framing is instead covered end-to-end against a live
sandbox (see the validation note above). A hermetic variant that needs no
Databricks sandbox would point the gateway at a local `codex exec-server
--listen stdio` and drive one `agent.session(environment=...)` turn with the
repo's native model fixtures (see `packages/codex/tests/native_executor_fixtures.py`
and `packages/codex/tests/test_documented_executor_examples.py`).
