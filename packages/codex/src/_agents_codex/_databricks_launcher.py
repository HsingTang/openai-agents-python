"""Launcher the native runtime spawns to bridge executor stdio to a sandbox.

:class:`DatabricksStdioEnvironment` declares this module as a host-owned
``stdio_command``. The native runtime starts it and speaks the exec-server wire
protocol over its stdin/stdout. This launcher relays that protocol to
``codex exec-server --listen stdio`` running in the Databricks sandbox, over the
``databricks-sdk`` streaming command API (``WorkspaceClient.sandbox.execute_command``,
whose ``CommandProcess`` exposes the command's stdin, stdout, stderr and exit).

It is run as a module - ``python -m _agents_codex._databricks_launcher <sandboxes/ID>``
with ``DATABRICKS_HOST`` and ``DATABRICKS_TOKEN`` in the environment. It imports
only the standard library and (lazily) ``databricks-sdk``, so the spawned
interpreter does not load the native runtime. Diagnostics go to stderr so stdout
stays a clean protocol channel.

On session close or cancellation the launcher closes the exec-server's stdin so
it sees EOF and shuts down; the ``databricks-sdk`` command API has no call to kill
the remote process, so a lingering exec-server is reaped by the sandbox's
inactivity timeout. The launcher returns the remote command's exit code.
"""

from __future__ import annotations

import os
import signal
import sys
import threading
from collections.abc import Callable
from contextlib import suppress
from typing import Any


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


def _close_stdin(process: Any) -> None:
    """Close the exec-server's stdin so it sees EOF and shuts down. Idempotent."""
    with suppress(ValueError, OSError):
        process.stdin.close()


def _pump_stdin(process: Any) -> None:
    try:
        while True:
            chunk = os.read(0, 65536)
            if not chunk:
                break
            process.stdin.write(chunk)
    except (ValueError, OSError):
        pass  # stdin closed when the command exits or the stream dropped
    finally:
        _close_stdin(process)


def _pump_stderr(process: Any) -> None:
    # stderr is a PIPE; forward it to the launcher's stderr so a failing or
    # missing exec-server leaves diagnostics instead of vanishing.
    try:
        while True:
            chunk = process.stderr.read1()
            if not chunk:
                break
            os.write(2, chunk)
    except (ValueError, OSError):
        pass


def _run(process: Any, *, install_signals: bool = True) -> int:
    """Relay the exec-server protocol over this process's stdio; return its exit code."""
    done = threading.Event()

    def forward() -> None:
        try:
            _forward_stdout(process.stdout.readline, lambda line: os.write(1, line))
        finally:
            done.set()

    def terminate(*_args: object) -> None:
        # Session close / cancellation: shut the exec-server down and unblock the relay.
        _close_stdin(process)
        with suppress(Exception):
            process.close()

    if install_signals:
        for sig in (signal.SIGTERM, signal.SIGINT):
            with suppress(ValueError, OSError):
                signal.signal(sig, terminate)

    threading.Thread(target=_pump_stdin, args=(process,), daemon=True).start()
    threading.Thread(target=_pump_stderr, args=(process,), daemon=True).start()
    threading.Thread(target=forward, daemon=True).start()

    # Loop on a timeout so signal handlers run in the main thread while we wait.
    while not done.wait(timeout=1.0):
        pass
    _close_stdin(process)
    try:
        return process.wait(timeout=10)
    except Exception:  # best-effort exit code; cancellation closes the handle
        return 0


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
        stderr=StreamMode.PIPE,
    )
    return _run(process)


if __name__ == "__main__":
    sys.exit(main())
