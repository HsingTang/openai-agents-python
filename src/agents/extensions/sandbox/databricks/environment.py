"""Provision a native ``Environment`` backed by a Databricks sandbox.

This async context manager starts a :class:`SandboxExecServerGateway` for an
existing sandbox and yields an ``Environment`` wired to it. The SDK's
``EnvironmentSource`` accepts an async context manager, so the whole
start-gateway / tear-down lifecycle fits one ``async with``:

    async with databricks_sandbox_environment(
        workspace_client=WorkspaceClient(profile="my-workspace"),
        sandbox_name="openai-agent-sandbox",
        cwd="/home/sandbox-agent/work",
    ) as environment:
        async with agent.session(environment=environment) as session:
            ...

Provisioning the sandbox itself (``WorkspaceClient.sandbox.create_sandbox``) and
ensuring the ``codex`` exec-server is present are left to the caller, so this
code takes no dependency on sandbox-spec classes it cannot pin. See the package
README for the create/ensure pattern.
"""

from __future__ import annotations

import os
import secrets
import ssl
import tempfile
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from ....executor import Environment, ExecutorToolName
from .gateway import SandboxExecServerGateway, self_signed_server_context

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

_DEFAULT_TOOLS: tuple[ExecutorToolName, ...] = ("exec_command", "write_stdin", "apply_patch")


@asynccontextmanager
async def databricks_sandbox_environment(
    *,
    workspace_client: WorkspaceClient,
    sandbox_name: str,
    cwd: str,
    tools: Sequence[ExecutorToolName] = _DEFAULT_TOOLS,
    exec_server_command: Sequence[str] = ("codex", "exec-server", "stdio"),
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
        exec_server_command: Argv that starts the exec-server in stdio mode in
            the sandbox. The default assumes the image's ``codex`` binary serves
            ``exec-server stdio`` - confirm this before relying on it.
        token: Bearer secret shared between the gateway and the SDK. A random
            value is generated when omitted.
        ssl_context: Server TLS context for the gateway. A self-signed loopback
            certificate is generated when omitted.
        trust_gateway_ca: When a self-signed context is generated, add its CA to
            a combined bundle and point ``SSL_CERT_FILE`` at it for the session,
            so the native runtime trusts the gateway while keeping public CAs
            (via ``certifi``) for the model provider. Restored on exit. Disable
            if you manage trust yourself.
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
        ca_cert_path=generated_ca,
    )
    handle = await gateway.start()

    restore: Callable[[], None] | None = None
    if trust_gateway_ca and handle.ca_cert_path is not None:
        restore = _install_trust_bundle(handle.ca_cert_path)
    try:
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


def _install_trust_bundle(ca_cert_path: str) -> Callable[[], None]:
    """Point ``SSL_CERT_FILE`` at ``certifi`` plus the gateway CA; return a restorer.

    The native runtime shares one trust store for the model provider and the
    executor connection, so the gateway CA is appended to the public bundle
    rather than replacing it.
    """
    import certifi

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

    def restore() -> None:
        if previous is None:
            os.environ.pop("SSL_CERT_FILE", None)
        else:
            os.environ["SSL_CERT_FILE"] = previous

    return restore
