# Databricks sandbox backend

Back a native `Environment` with a Databricks sandbox, so an agent driven by
this SDK executes `exec_command` / `write_stdin` / `apply_patch` inside the
sandbox.

## How it works

The native `Environment` dials an authenticated `wss://` executor and relays the
exec-server protocol over it. A Databricks sandbox runs that exec-server in
stdio mode; the `databricks-sdk` streaming command API
(`WorkspaceClient.sandbox.execute_command`) gives bidirectional stdio to it.
Nothing in the SDK speaks stdio and nothing in the sandbox speaks WebSocket, so
`SandboxExecServerGateway` bridges them: it terminates the WebSocket,
authenticates the bearer token, opens the exec-server's stdio, and relays bytes
both ways. It parses neither protocol.

Because `execute_command` reaches the sandbox outbound through the Databricks
control plane, the gateway runs on `localhost` next to the application and the
sandbox needs no inbound ingress. The agent loop runs in your process (via
`NativeProvider`), which is what makes the `Environment` approval / transform
callbacks and `NetworkPolicy` available - the trade against the managed
Agents API path, where OpenAI runs the loop and the sandbox exec-server dials
out instead.

## Install

The v1.0 SDK is a GitHub preview wheel (off PyPI). Install it, then this extra:

```sh
pip install "<openai-agents preview wheel url>"
pip install "openai-agents[databricks]"   # from this source tree
```

The extra pulls `databricks-sdk[sandbox]`, `websockets`, `cryptography`, and
`certifi`.

## Usage

```python
from databricks.sdk import WorkspaceClient
from agents import Agent, NativeProvider
from agents.extensions.sandbox.databricks import databricks_sandbox_environment

async with databricks_sandbox_environment(
    workspace_client=WorkspaceClient(profile="my-workspace"),
    sandbox_name="openai-agent-sandbox",
    cwd="/home/sandbox-agent/work",
) as environment:
    async with NativeProvider(api_key=..., supports_websockets=False) as provider:
        agent = Agent(model="gpt-5.5", provider=provider, instructions="...")
        async with agent.session(environment=environment) as session:
            print((await (await session.submit("...")).get_result()).final_output)
```

A runnable version is in `examples/databricks/runner.py`.

## Provisioning the sandbox

This package wires an `Environment` to an *existing* sandbox; it does not create
one, to avoid depending on sandbox-spec classes it cannot pin. Provision with
the usual API, then ensure the exec-server is reachable, e.g.:

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(profile="my-workspace")
w.sandbox.create_sandbox(..., sandbox_id="openai-agent-sandbox")
```

`exec-server` is killed when the sandbox stops, so a new connection after a
stop relaunches it.

## TLS

`Environment` requires `wss`, so the gateway needs TLS even on loopback. By
default `databricks_sandbox_environment` generates an ephemeral self-signed
certificate and, for the session, points `SSL_CERT_FILE` at a bundle of the
public CAs (`certifi`) plus that certificate, then restores it on exit - so the
native runtime trusts the gateway without breaking model-provider TLS. Pass your
own `ssl_context` and set `trust_gateway_ca=False` to manage trust yourself.

## Unverified prerequisites (draft)

Two assumptions this draft is built on still need confirmation against a live
sandbox before it can be relied on:

1. **exec-server stdio mode in the image.** The default command is
   `("codex", "exec-server", "stdio")`. The SDK's own `offline_docker.py`
   example proves `exec-server stdio` is valid; the Databricks cookbook only
   exercised the outbound `codex exec-server --remote` mode. Confirm with
   `codex exec-server --help` in a sandbox, and override `exec_server_command`
   if the binary path or subcommand differ.
2. **Streaming `execute_command` suitability.** The relay assumes
   `CommandProcess` gives a usable bidirectional binary stdio stream (`stdin`
   writes, `stdout.read1`) for the exec-server protocol. The v0.x provider used
   only the buffered `execute_command_sync`, so this streaming path has not been
   exercised for this purpose. Validate interactive `write_stdin` under load and
   how `CommandProcess` reconnect interacts with the exec-server session.
3. **WebSocket framing.** The gateway relays an opaque byte stream: it forwards
   each inbound frame to stdin as-is and sends arbitrary `read1` chunks of stdout
   back as frames. This is correct only if the native client reassembles the
   byte stream rather than requiring one protocol message per frame (the
   reference `offline_docker.py` bridges the socket straight to byte pipes, which
   indicates byte-stream semantics). If the executor is message-framed, the relay
   must preserve message boundaries instead.
