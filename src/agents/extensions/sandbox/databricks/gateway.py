"""A localhost WSS gateway that bridges the SDK executor to a sandbox exec-server.

The native ``Environment`` dials an authenticated ``wss://`` endpoint and relays
the exec-server wire protocol over it. A Databricks sandbox runs that
exec-server in stdio mode, reached through the ``databricks-sdk`` streaming
command API (``WorkspaceClient.sandbox.execute_command``). Nothing in the SDK
speaks stdio and nothing in the sandbox speaks WebSocket, so this gateway sits
between them: it terminates the WebSocket, authenticates the bearer token,
opens the exec-server's stdio stream, and relays bytes in both directions.

The gateway parses neither protocol. It moves bytes between the WebSocket frames
and the exec-server's stdin/stdout, so it stays valid as the exec-server
protocol evolves. This assumes the native WebSocket client treats the
connection as an opaque byte stream and reassembles messages itself, rather than
requiring one protocol message per WebSocket frame; confirm this framing against
the executor before relying on it (see the package README). Because
``execute_command`` reaches the sandbox outbound
through the Databricks control plane, the gateway can run on ``localhost`` next
to the application; the sandbox needs no inbound ingress.
"""

from __future__ import annotations

import asyncio
import hmac
import http
import logging
import ssl
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from websockets.asyncio.server import ServerConnection
    from websockets.http11 import Request, Response

logger = logging.getLogger("agents.extensions.sandbox.databricks")

_READ_CHUNK = 64 * 1024
_DEFAULT_EXEC_SERVER_COMMAND: tuple[str, ...] = ("codex", "exec-server", "stdio")


@dataclass(frozen=True, kw_only=True)
class GatewayHandle:
    """A running gateway.

    Attributes:
        url: The ``wss://`` endpoint to pass as ``Environment.url``.
        ca_cert_path: Path to the CA that signed the gateway certificate, or
            None when the caller supplied its own already-trusted context. Add
            it to the trust store the native runtime uses (see
            ``environment.databricks_sandbox_environment``).
    """

    url: str
    ca_cert_path: str | None


class SandboxExecServerGateway:
    """Serve one sandbox's exec-server over an authenticated localhost WebSocket.

    Args:
        workspace_client: An authenticated ``databricks.sdk.WorkspaceClient``.
        sandbox_name: The target sandbox name/id.
        token: Shared secret the SDK must present as ``Authorization: Bearer``.
            The provisioner puts the same value in ``Environment.headers``.
        ssl_context: Server-side TLS context. ``Environment`` requires ``wss``,
            so TLS is mandatory even on loopback. Use
            ``self_signed_server_context`` for local runs or pass your own.
        exec_server_command: Argv that starts the exec-server in stdio mode
            inside the sandbox. PREREQUISITE: the sandbox image's ``codex``
            binary must support this; confirm with ``codex exec-server --help``
            in a live sandbox. Override if the binary path or subcommand differ.
        host: Loopback interface to bind.
        ca_cert_path: Path to the CA for ``ssl_context`` when self-signed, echoed
            back on the handle so callers can trust it.
    """

    def __init__(
        self,
        *,
        workspace_client: WorkspaceClient,
        sandbox_name: str,
        token: str,
        ssl_context: ssl.SSLContext,
        exec_server_command: Sequence[str] = _DEFAULT_EXEC_SERVER_COMMAND,
        host: str = "127.0.0.1",
        ca_cert_path: str | None = None,
    ) -> None:
        if not token:
            raise ValueError("Gateway token must be a non-empty secret")
        command = tuple(exec_server_command)
        if not command:
            raise ValueError("exec_server_command must name the exec-server program")
        self._w = workspace_client
        self._sandbox_name = sandbox_name
        self._token = token
        self._ssl_context = ssl_context
        self._command = command
        self._host = host
        self._ca_cert_path = ca_cert_path
        self._server: Any = None

    async def start(self) -> GatewayHandle:
        """Bind the WebSocket server on an ephemeral port and start serving."""
        from websockets.asyncio.server import serve

        self._server = await serve(
            self._handle,
            self._host,
            0,
            ssl=self._ssl_context,
            process_request=self._process_request,
        )
        port = self._server.sockets[0].getsockname()[1]
        return GatewayHandle(url=f"wss://{self._host}:{port}", ca_cert_path=self._ca_cert_path)

    async def aclose(self) -> None:
        """Stop accepting connections and wait for the server to close."""
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    def _process_request(self, connection: ServerConnection, request: Request) -> Response | None:
        """Reject an unauthenticated client at the HTTP handshake with 401."""
        presented = request.headers.get("Authorization", "")
        if hmac.compare_digest(presented, f"Bearer {self._token}"):
            return None
        return connection.respond(http.HTTPStatus.UNAUTHORIZED, "unauthorized\n")

    async def _handle(self, connection: ServerConnection) -> None:
        proc = await asyncio.to_thread(self._open_exec_server)
        inbound = asyncio.create_task(self._ws_to_stdin(connection, proc))
        outbound = asyncio.create_task(self._stdout_to_ws(proc, connection))
        try:
            await asyncio.wait({inbound, outbound}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            # Closing the process unblocks the stdout read thread so both tasks finish.
            await asyncio.to_thread(self._close_quietly, proc)
            inbound.cancel()
            outbound.cancel()
            await asyncio.gather(inbound, outbound, return_exceptions=True)

    def _open_exec_server(self) -> Any:
        return self._w.sandbox.execute_command(
            self._sandbox_name,
            self._command[0],
            args=list(self._command[1:]),
            text=False,
        )

    @staticmethod
    def _close_quietly(proc: Any) -> None:
        try:
            proc.close()
        except Exception:  # teardown must not mask the real error
            logger.debug("exec-server process close raised during teardown", exc_info=True)

    async def _ws_to_stdin(self, connection: ServerConnection, proc: Any) -> None:
        async for message in connection:
            data = message if isinstance(message, bytes) else message.encode()
            await asyncio.to_thread(proc.stdin.write, data)
            flush = getattr(proc.stdin, "flush", None)
            if flush is not None:
                await asyncio.to_thread(flush)
        await asyncio.to_thread(self._close_stdin_quietly, proc)

    @staticmethod
    def _close_stdin_quietly(proc: Any) -> None:
        try:
            proc.stdin.close()
        except Exception:  # half-closed stream during teardown
            logger.debug("exec-server stdin close raised", exc_info=True)

    async def _stdout_to_ws(self, proc: Any, connection: ServerConnection) -> None:
        while True:
            chunk = await asyncio.to_thread(proc.stdout.read1, _READ_CHUNK)
            if not chunk:
                break
            await connection.send(chunk)


def self_signed_server_context(host: str = "127.0.0.1") -> tuple[ssl.SSLContext, str]:
    """Build a TLS server context with an ephemeral self-signed certificate.

    Returns the server context and the path to the generated CA certificate so
    the client side can trust it. Intended for local runs; supply your own
    context and CA in production. Requires the ``cryptography`` dependency from
    the ``databricks`` extra.

    Args:
        host: The loopback host the gateway binds, embedded as a SAN.
    """
    import datetime
    import ipaddress
    import tempfile

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, host)])
    try:
        san: x509.GeneralName = x509.IPAddress(ipaddress.ip_address(host))
    except ValueError:
        san = x509.DNSName(host)
    now = datetime.datetime.now(datetime.timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([san]), critical=False)
        .sign(key, hashes.SHA256())
    )

    cert_pem = certificate.public_bytes(serialization.Encoding.PEM)
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )
    cert_file = tempfile.NamedTemporaryFile(prefix="dbx-gw-", suffix=".crt", delete=False)
    key_file = tempfile.NamedTemporaryFile(prefix="dbx-gw-", suffix=".key", delete=False)
    with cert_file:
        cert_file.write(cert_pem)
    with key_file:
        key_file.write(key_pem)

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert_file.name, key_file.name)
    return context, cert_file.name
