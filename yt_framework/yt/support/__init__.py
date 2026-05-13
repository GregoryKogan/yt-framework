"""Support utilities for YT clients (no dependency on ``yt_framework.yt.clients``).

Runtime helpers, max-row-weight parsing, dev simulation, and secure-env splitting
live here so ``yt_framework.yt.clients`` and ``yt_framework.yt._client_split`` can
depend on this package without circular imports.
"""
