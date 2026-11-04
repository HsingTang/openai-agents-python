"""Databricks sandbox backend for the native ``Environment``.

Install with the ``databricks`` extra. The application provisions a Databricks
sandbox whose image carries the ``codex`` exec-server; this package runs a
localhost WSS gateway that relays that exec-server's stdio to the SDK, and
builds the ``Environment`` pointing at it.
"""

from .environment import databricks_sandbox_environment
from .gateway import GatewayHandle, SandboxExecServerGateway, self_signed_server_context

__all__ = [
    "databricks_sandbox_environment",
    "GatewayHandle",
    "SandboxExecServerGateway",
    "self_signed_server_context",
]
