"""
Mission Control — Lakebase Autoscaling Client

Shared module for connecting to Lakebase Autoscaling from notebooks and Python modules.
Provides a module-level connection that is initialized once per notebook session.

Usage in notebooks:
    import lakebase_client
    lakebase_client.init("mission-control")

    # Single-row read
    row = lakebase_client.fetch_one("SELECT * FROM mission_state WHERE state_id = 1")

    # Multi-row read
    rows = lakebase_client.fetch_all("SELECT * FROM active_hazards ORDER BY risk_score DESC")

    # Write with params
    lakebase_client.execute(
        "UPDATE mission_state SET fuel_remaining_kg = %(fuel)s WHERE state_id = 1",
        {"fuel": 450.0}
    )

    # Upsert (INSERT ... ON CONFLICT)
    lakebase_client.upsert("mission_state", {"state_id": 1}, {"fuel_remaining_kg": 450.0, ...})
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

import psycopg
from databricks.sdk import WorkspaceClient

# Module-level connection state
_conn: Optional[psycopg.Connection] = None
_project_id: Optional[str] = None
_branch_id: Optional[str] = None


def init(
    project_id: str,
    branch_id: str = "production",
    database_name: str = "databricks_postgres",
) -> None:
    """Initialize the module-level Lakebase Autoscaling connection."""
    global _conn, _project_id, _branch_id

    if _conn is not None and not _conn.closed:
        if _project_id == project_id and _branch_id == branch_id:
            return  # Already connected
        _conn.close()

    _project_id = project_id
    _branch_id = branch_id

    w = WorkspaceClient()

    # Find primary endpoint
    endpoints = list(w.postgres.list_endpoints(
        parent=f"projects/{project_id}/branches/{branch_id}"
    ))
    if not endpoints:
        raise RuntimeError(
            f"No endpoints found for projects/{project_id}/branches/{branch_id}"
        )
    ep_name = endpoints[0].name

    # Get host and token
    endpoint = w.postgres.get_endpoint(name=ep_name)
    host = endpoint.status.hosts.host
    username = w.current_user.me().user_name
    cred = w.postgres.generate_database_credential(endpoint=ep_name)

    _conn = psycopg.connect(
        host=host,
        dbname=database_name,
        user=username,
        password=cred.token,
        sslmode="require",
        autocommit=True,
    )
    print(f"Lakebase connected → {host} (project: {project_id}/{branch_id})")


def get_conn() -> psycopg.Connection:
    """Get the current connection. Raises if not initialized."""
    if _conn is None or _conn.closed:
        raise RuntimeError("Lakebase not initialized. Call lakebase_client.init(project_id) first.")
    return _conn


def _serialize_value(v: Any) -> Any:
    """Convert non-serializable types for JSON output."""
    if isinstance(v, datetime):
        return v.isoformat()
    return v


def fetch_all(query: str, params: Optional[dict] = None) -> list[dict]:
    """Execute a query and return all rows as dicts."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(query, params)
        if cur.description is None:
            return []
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def fetch_one(query: str, params: Optional[dict] = None) -> Optional[dict]:
    """Execute a query and return the first row as a dict, or None."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(query, params)
        if cur.description is None:
            return None
        columns = [desc[0] for desc in cur.description]
        row = cur.fetchone()
        if row is None:
            return None
        return dict(zip(columns, row))


def execute(query: str, params: Optional[dict] = None) -> int:
    """Execute a write query. Returns the number of affected rows."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.rowcount if cur.rowcount >= 0 else 0


def upsert(
    table: str,
    pk: dict[str, Any],
    data: dict[str, Any],
    constraint: Optional[str] = None,
) -> None:
    """
    Insert or update a row using ON CONFLICT.

    Args:
        table: Table name
        pk: Primary key column(s) and value(s)
        data: Non-PK columns and values to set
        constraint: Optional constraint name (defaults to PK columns)
    """
    all_cols = {**pk, **data}
    col_names = list(all_cols.keys())
    placeholders = [f"%({k})s" for k in col_names]
    conflict_cols = constraint or ", ".join(pk.keys())
    update_set = ", ".join(f"{k} = EXCLUDED.{k}" for k in data.keys())

    sql = (
        f"INSERT INTO {table} ({', '.join(col_names)}) "
        f"VALUES ({', '.join(placeholders)}) "
        f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set}"
    )
    execute(sql, all_cols)


def close() -> None:
    """Close the connection."""
    global _conn
    if _conn is not None and not _conn.closed:
        _conn.close()
    _conn = None
