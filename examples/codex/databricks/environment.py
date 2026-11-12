"""Provision a native ``Environment`` backed by a Databricks sandbox.

This async context manager starts a :class:`SandboxExecServerGateway` for an
existing sandbox and yields an ``Environment`` wired to it. It is the
"manage the executor lifetime" pattern from the remote-execution guide: the SDK
enters the context when opening the Session and exits after releasing native
resources, so the whole start-gateway / tear-down lifecycle fits one
``async with``:

    async with databricks_sandbox_environment(
        workspace_client=WorkspaceClient(profile="my-workspace"),
        sandbox_name="openai-agent-sandbox",
        cwd="/home/sandbox-agent/work",
    ) as environment:
        async with agent.session(environment=environment) as session:
            ...

Provisioning the sandbox itself (``WorkspaceClient.sandbox.create_sandbox``) and
ensuring the ``codex`` exec-server is present are left to the caller. See
``runner.py`` and the example README for the create/ensure pattern.
"""

from __future__ import annotations

import os
import secrets
import ssl
import tempfile
import threading
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING

from agents import Environment
from agents.executor import ExecutorToolName
from examples.codex.databricks.gateway import (
    SandboxExecServerGateway,
    self_signed_server_context,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

_DEFAULT_TOOLS: tuple[ExecutorToolName, ...] = ("exec_command", "write_stdin", "apply_patch")

# Auto-managed trust rewrites the process-global ``SSL_CERT_FILE``, so only one
# auto-managed environment can be active per process. This guard turns a
# concurrent second use into a loud error instead of silent trust corruption.
_trust_lock = threading.Lock()
_trust_active = False


@asynccontextmanager
async def databricks_sandbox_environment(
    *,
    workspace_client: WorkspaceClient,
    sandbox_name: str,
    cwd: str,
    tools: Sequence[ExecutorToolName] = _DEFAULT_TOOLS,
    exec_server_command: Sequence[str] = ("codex", "exec-server", "--listen", "stdio"),
    exec_server_env: Mapping[str, str] | None = None,
    token: str | None = None,
    ssl_context: ssl.SSLContext | None = None,
    trust_gateway_ca: bool = True,
) -> AsyncIterator[Environment]:
    """Yield an ``Environment`` connected to ``sandbox_name`` through a gateway.

    Args:
        workspace_client: An authenticated ``databricks.sdk.WorkspaceClient``.
        sandbox_name: An existing, running sandbox with the exec-server available.
        cwd: Absolute remote workspace path the executor operates in.
        tools: Native executor tools to expose. ``write_stdin`` requires
            ``exec_command``.
        exec_server_command: Argv that starts the standalone exec-server over
            stdio in the sandbox. Defaults to
            ``("codex", "exec-server", "--listen", "stdio")``.
        exec_server_env: Environment for the exec-server command. Defaults to
            ``{"_UCODE_DISPATCH_ACTIVE": "1"}``, the sandbox image's ucode-wrapper
            recursion sentinel (an internal detail; see ``gateway.py``); pass
            ``{}`` to run unmodified.
        token: Bearer secret shared between the gateway and the SDK. A random
            value is generated when omitted.
        ssl_context: Server TLS context for the gateway. A self-signed loopback
            certificate is generated when omitted.
        trust_gateway_ca: When a self-signed context is generated, add its CA to
            a combined bundle and point ``SSL_CERT_FILE`` at it for the session,
            so the native runtime trusts the gateway while keeping public CAs
            (via ``certifi``) for the model provider. Restored on exit. Because
            ``SSL_CERT_FILE`` is process-global, only one auto-managed
            environment may be active per process; a concurrent second use
            raises. To run several at once, pass your own ``ssl_context`` and set
            ``trust_gateway_ca=False``, managing trust yourself.
    """
    secret = token or secrets.token_urlsafe(32)
    generated_ca: str | None = None
    if ssl_context is None:
        ssl_context, generated_ca = self_signed_server_context()

    gateway = SandboxExecServerGateway(
        workspace_client=workspace_client,
        sandbox_name=sandbox_name,
        token=secret,
        ssl_context=ssl_context,
        exec_server_command=exec_server_command,
        exec_server_env=exec_server_env,
        ca_cert_path=generated_ca,
    )
    handle = await gateway.start()

    restore: Callable[[], None] | None = None
    try:
        if trust_gateway_ca and handle.ca_cert_path is not None:
            restore = _install_trust_bundle(handle.ca_cert_path)
        yield Environment(
            url=handle.url,
            cwd=cwd,
            tools=tuple(tools),
            headers={"Authorization": f"Bearer {secret}"},
        )
    finally:
        if restore is not None:
            restore()
        await gateway.aclose()
        if generated_ca is not None:
            with suppress(OSError):
                os.unlink(generated_ca)


def _install_trust_bundle(ca_cert_path: str) -> Callable[[], None]:
    """Point ``SSL_CERT_FILE`` at ``certifi`` plus the gateway CA; return a restorer.

    The native runtime shares one trust store for the model provider and the
    executor connection, so the gateway CA is appended to the public bundle
    rather than replacing it.

    ``SSL_CERT_FILE`` is process-global, so this refuses to run while another
    auto-managed environment is active rather than silently overwriting its
    trust. The returned restorer rewinds the variable and removes the bundle.
    """
    import certifi

    global _trust_active
    with _trust_lock:
        if _trust_active:
            raise RuntimeError(
                "databricks_sandbox_environment is already auto-managing SSL_CERT_FILE "
                "for another active environment in this process. Auto-managed trust is "
                "process-global and supports one environment at a time. To run several, "
                "pass your own ssl_context and set trust_gateway_ca=False."
            )
        _trust_active = True

    try:
        with open(certifi.where(), "rb") as public_bundle:
            public = public_bundle.read()
        with open(ca_cert_path, "rb") as gateway_ca:
            extra = gateway_ca.read()

        combined = tempfile.NamedTemporaryFile(prefix="dbx-trust-", suffix=".pem", delete=False)
        with combined:
            combined.write(public)
            if not public.endswith(b"\n"):
                combined.write(b"\n")
            combined.write(extra)

        previous = os.environ.get("SSL_CERT_FILE")
        os.environ["SSL_CERT_FILE"] = combined.name
    except BaseException:
        with _trust_lock:
            _trust_active = False
        raise

    def restore() -> None:
        global _trust_active
        if previous is None:
            os.environ.pop("SSL_CERT_FILE", None)
        else:
            os.environ["SSL_CERT_FILE"] = previous
        with suppress(OSError):
            os.unlink(combined.name)
        with _trust_lock:
            _trust_active = False

    return restore
