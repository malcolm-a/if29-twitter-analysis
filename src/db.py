"""
Centralised PostgreSQL connection utility.

Supports two modes, controlled by the ``USE_SSH_TUNNEL`` env var:

- **Direct** (default):  connects straight to ``DB_HOST:DB_PORT``.
- **SSH tunnel**:       opens an SSH tunnel via ``SSH_HOST``, then connects
                        through the tunnel to the remote PostgreSQL instance.

Usage::

    from src.db import get_db_connection

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager

import psycopg2
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


@contextmanager
def get_db_connection():
    """Yield a psycopg2 connection, optionally tunnelled through SSH.

    Reads every parameter from environment variables.  The tunnel is
    automatically started before the connection and torn down afterwards,
    even if an exception occurs.
    """
    use_tunnel = os.getenv("USE_SSH_TUNNEL", "false").lower() == "true"
    tunnel = None
    conn = None

    try:
        if use_tunnel:
            conn, tunnel = _connect_via_ssh()
        else:
            conn = _connect_direct()

        yield conn

    finally:
        if conn is not None:
            conn.close()
            logger.debug("Database connection closed.")
        if tunnel is not None:
            tunnel.stop()
            logger.debug("SSH tunnel stopped.")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _connect_direct() -> psycopg2.extensions.connection:
    """Open a direct connection using DB_HOST / DB_PORT."""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
    )
    logger.debug("Direct DB connection established.")
    return conn


def _connect_via_ssh():
    import paramiko

    # sshtunnel 0.4.0 references paramiko.DSSKey which was removed in
    # paramiko >= 3.0.  Restore a dummy so the import succeeds.
    if not hasattr(paramiko, "DSSKey"):
        paramiko.DSSKey = paramiko.RSAKey  # safe fallback, never actually used

    from sshtunnel import SSHTunnelForwarder

    ssh_key = os.path.expanduser(os.getenv("SSH_KEY_PATH", "~/.ssh/id_rsa"))

    tunnel = SSHTunnelForwarder(
        (os.getenv("SSH_HOST"), 22),
        ssh_username=os.getenv("SSH_USER"),
        ssh_pkey=ssh_key,
        remote_bind_address=("127.0.0.1", 5432),
    )
    tunnel.start()

    conn = psycopg2.connect(
        host="127.0.0.1",
        port=tunnel.local_bind_port,
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
    )

    return conn, tunnel
