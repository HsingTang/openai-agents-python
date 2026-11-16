"""Tests for the Databricks stdio launcher's protocol forwarding.

Cover the startup-banner filter in ``_forward_stdout``: the launcher must drop
``codex``'s non-JSON startup preamble, then forward every exec-server protocol
line verbatim. Run from the repo root with the example requirements installed.
"""

from __future__ import annotations

from examples.codex.databricks.launch import _forward_stdout


def _run(lines: list[bytes]) -> list[bytes]:
    pending = list(lines)

    def read_line() -> bytes:
        return pending.pop(0) if pending else b""

    written: list[bytes] = []
    _forward_stdout(read_line, written.append)
    return written


def test_drops_banner_then_forwards_protocol() -> None:
    written = _run(
        [
            b"\xe2\x9c\x94 Databricks auth already available\n",
            b"Launching Codex with Unity Gateway\n",
            b"\n",
            b'{"jsonrpc":"2.0","id":1}\n',
            b'{"jsonrpc":"2.0","method":"x"}\n',
        ]
    )
    assert written == [
        b'{"jsonrpc":"2.0","id":1}\n',
        b'{"jsonrpc":"2.0","method":"x"}\n',
    ]


def test_forwards_everything_after_protocol_starts() -> None:
    # A non-JSON line after the protocol has started is still forwarded.
    written = _run([b'{"a":1}\n', b"not-json-but-mid-stream\n", b'{"b":2}\n'])
    assert written == [b'{"a":1}\n', b"not-json-but-mid-stream\n", b'{"b":2}\n']


def test_leading_whitespace_before_brace_is_protocol() -> None:
    written = _run([b"  banner\n", b'  {"a":1}\n'])
    assert written == [b'  {"a":1}\n']


def test_stops_at_eof_with_no_protocol() -> None:
    assert _run([b"banner one\n", b"banner two\n"]) == []
