"""Launcher the native runtime spawns to bridge executor stdio to a sandbox.

:class:`DatabricksStdioEnvironment` declares this file as a host-owned
``stdio_command``. The native runtime starts it and speaks the exec-server wire
protocol over its stdin/stdout. This launcher relays that protocol to
``codex exec-server --listen stdio`` running in the Databricks sandbox, over the
``databricks-sdk`` streaming command API (``WorkspaceClient.sandbox.execute_command``,
whose ``CommandProcess`` exposes the command's stdin, stdout and exit).

It is run as a standalone script - ``python _databricks_launcher.py <sandboxes/ID>``
with ``DATABRICKS_HOST`` and ``DATABRICKS_TOKEN`` in the environment - so it does
not import the ``_agents_codex`` package or the native runtime. Diagnostics go to
stderr so stdout stays a clean protocol channel.
"""

from __future__ import annotations

import os
import sys
import threading
from collections.abc import Callable


def _forward_stdout(read_line: Callable[[], bytes], write: Callable[[bytes], None]) -> None:
    """Forward exec-server stdout, dropping the non-protocol startup banner.

    ``codex`` prints a human-readable startup banner to stdout before the
    exec-server protocol begins; those non-JSON lines would corrupt the stream
    as bogus protocol frames. Drop the leading non-JSON preamble, then forward
    every line (a no-op once the protocol, newline-delimited JSON-RPC, starts).
    """
    protocol_started = False
    while True:
        line = read_line()
        if not line:
            break
        if not protocol_started:
            if not line.lstrip().startswith(b"{"):
                continue
            protocol_started = True
        write(line)


def main() -> int:
    sandbox_name = sys.argv[1]
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not host or not token:
        print("launcher requires DATABRICKS_HOST and DATABRICKS_TOKEN", file=sys.stderr)
        return 2

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.mixins.sandbox import StreamMode

    workspace_client = WorkspaceClient(host=host, token=token)
    process = workspace_client.sandbox.execute_command(
        sandbox_name,
        "codex",
        args=["exec-server", "--listen", "stdio"],
        text=False,
        stderr=StreamMode.DEVNULL,
    )

    def pump_stdin() -> None:
        try:
            while True:
                chunk = os.read(0, 65536)
                if not chunk:
                    break
                process.stdin.write(chunk)
        except (ValueError, OSError):
            pass  # stdin closed when the command exits or the stream dropped
        finally:
            try:
                process.stdin.close()
            except (ValueError, OSError):
                pass

    threading.Thread(target=pump_stdin, daemon=True).start()
    _forward_stdout(process.stdout.readline, lambda line: os.write(1, line))
    return 0


if __name__ == "__main__":
    sys.exit(main())
