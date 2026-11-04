"""Sandbox integrations that provision executors for the native ``Environment``.

Each provider lives in its own subpackage and depends on an extra, so importing
``agents`` never requires a provider SDK. Import the provider explicitly, e.g.
``from agents.extensions.sandbox.databricks import databricks_sandbox_environment``.
"""
