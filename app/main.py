"""
Mission Control Dashboard — FastAPI Backend

Serves the React frontend and provides API endpoints for:
- Real-time spacecraft state (from Lakebase Autoscaling — direct psycopg)
- Telemetry history (from Delta Lake — SQL warehouse)
- Hazard data
- Command queue management
- Agent decision logs

Architecture:
- Lakebase Autoscaling (psycopg pool) → ops.* tables (sub-10ms latency)
- SQL Statement API (warehouse) → Delta Lake tables (telemetry, hazards, agents, commands history)
"""

import asyncio
import os
from contextlib import asynccontextmanager
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

CATALOG = os.environ.get("CATALOG", "mission_control_dev")
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "")
LAKEBASE_PROJECT_ID = os.environ.get("LAKEBASE_PROJECT_ID", "mission-control")
LAKEBASE_BRANCH_ID = os.environ.get("LAKEBASE_BRANCH_ID", "production")


# ---------------------------------------------------------------------------
# Lakebase Autoscaling Connection Manager
# ---------------------------------------------------------------------------

class LakebaseConnectionManager:
    """Manages Lakebase Autoscaling connections with automatic OAuth token refresh."""

    def __init__(
        self,
        project_id: str,
        branch_id: str = "production",
        database_name: str = "databricks_postgres",
        pool_size: int = 5,
        max_overflow: int = 10,
        token_refresh_seconds: int = 3000,  # 50 minutes (tokens expire at 60)
    ):
        self.project_id = project_id
        self.branch_id = branch_id
        self.database_name = database_name
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.token_refresh_seconds = token_refresh_seconds

        self._current_token: Optional[str] = None
        self._refresh_task: Optional[asyncio.Task] = None
        self._engine = None
        self._session_maker = None

    def _get_workspace_client(self) -> WorkspaceClient:
        return WorkspaceClient()

    def _get_endpoint_name(self) -> str:
        w = self._get_workspace_client()
        endpoints = list(w.postgres.list_endpoints(
            parent=f"projects/{self.project_id}/branches/{self.branch_id}"
        ))
        if not endpoints:
            raise RuntimeError(
                f"No endpoints found for projects/{self.project_id}/branches/{self.branch_id}"
            )
        return endpoints[0].name

    def _get_host(self, endpoint_name: str) -> str:
        w = self._get_workspace_client()
        endpoint = w.postgres.get_endpoint(name=endpoint_name)
        return endpoint.status.hosts.host

    def _generate_token(self, endpoint_name: str) -> str:
        w = self._get_workspace_client()
        cred = w.postgres.generate_database_credential(endpoint=endpoint_name)
        return cred.token

    def initialize(self):
        """Initialize the async engine with token-injecting connection pool."""
        w = self._get_workspace_client()
        endpoint_name = self._get_endpoint_name()
        host = self._get_host(endpoint_name)
        username = w.current_user.me().user_name
        self._endpoint_name = endpoint_name

        # Generate initial token
        self._current_token = self._generate_token(endpoint_name)

        url = (
            f"postgresql+psycopg://{username}@"
            f"{host}:5432/{self.database_name}"
        )

        self._engine = create_async_engine(
            url,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            pool_recycle=3600,
            connect_args={"sslmode": "require"},
        )

        # Inject the latest token on every new connection
        @event.listens_for(self._engine.sync_engine, "do_connect")
        def inject_token(dialect, conn_rec, cargs, cparams):
            cparams["password"] = self._current_token

        self._session_maker = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

        print(f"Lakebase connected → {host} (project: {self.project_id})")

    async def _refresh_loop(self):
        """Background task: refresh OAuth token every 50 minutes."""
        while True:
            await asyncio.sleep(self.token_refresh_seconds)
            try:
                self._current_token = await asyncio.to_thread(
                    self._generate_token, self._endpoint_name
                )
                print("Lakebase token refreshed")
            except Exception as e:
                print(f"Lakebase token refresh failed: {e}")

    def start_refresh(self):
        if not self._refresh_task:
            self._refresh_task = asyncio.create_task(self._refresh_loop())

    async def stop_refresh(self):
        if self._refresh_task:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
            self._refresh_task = None

    @asynccontextmanager
    async def session(self):
        async with self._session_maker() as session:
            yield session

    async def close(self):
        await self.stop_refresh()
        if self._engine:
            await self._engine.dispose()


# ---------------------------------------------------------------------------
# SQL Warehouse helper (Delta Lake queries)
# ---------------------------------------------------------------------------

def get_workspace_client() -> WorkspaceClient:
    return WorkspaceClient()


def execute_sql(query: str) -> list[dict]:
    """Execute SQL query via Databricks SQL Statement API (for Delta Lake tables)."""
    w = get_workspace_client()

    warehouse_id = WAREHOUSE_ID
    if not warehouse_id:
        warehouses = list(w.warehouses.list())
        if warehouses:
            warehouse_id = warehouses[0].id
        else:
            raise HTTPException(status_code=500, detail="No SQL warehouse available")

    response = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=query,
        wait_timeout="30s",
    )

    if response.status.state != StatementState.SUCCEEDED:
        raise HTTPException(
            status_code=500,
            detail=f"Query failed: {response.status.error}",
        )

    if not response.result or not response.result.data_array:
        return []

    columns = [col.name for col in response.manifest.schema.columns]
    return [dict(zip(columns, row)) for row in response.result.data_array]


# ---------------------------------------------------------------------------
# Lakebase helper (ops tables)
# ---------------------------------------------------------------------------

lakebase = LakebaseConnectionManager(
    project_id=LAKEBASE_PROJECT_ID,
    branch_id=LAKEBASE_BRANCH_ID,
    pool_size=5,
    max_overflow=10,
)


async def execute_lakebase(query: str, params: Optional[dict] = None) -> list[dict]:
    """Execute a query against Lakebase Autoscaling via psycopg (for ops tables)."""
    async with lakebase.session() as session:
        result = await session.execute(text(query), params or {})
        if result.returns_rows:
            columns = list(result.keys())
            return [dict(zip(columns, row)) for row in result.fetchall()]
        await session.commit()
        return []


# ---------------------------------------------------------------------------
# App Lifecycle
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"Mission Control starting — catalog: {CATALOG}, lakebase project: {LAKEBASE_PROJECT_ID}")
    lakebase.initialize()
    lakebase.start_refresh()
    yield
    await lakebase.close()
    print("Mission Control shutting down")


app = FastAPI(title="Mission Control", lifespan=lifespan)


# ---------------------------------------------------------------------------
# API Routes — Lakebase Autoscaling (ops tables, sub-10ms)
# ---------------------------------------------------------------------------

@app.get("/api/mission/state")
async def get_mission_state():
    """Get current spacecraft state from Lakebase."""
    rows = await execute_lakebase(
        "SELECT * FROM mission_state WHERE state_id = 1"
    )
    if not rows:
        raise HTTPException(status_code=404, detail="No mission state found")
    return rows[0]


@app.get("/api/mission/clock")
async def get_simulation_clock():
    """Get current simulation clock state."""
    rows = await execute_lakebase(
        "SELECT * FROM simulation_clock WHERE clock_id = 1"
    )
    if not rows:
        raise HTTPException(status_code=404, detail="No clock state found")
    return rows[0]


@app.get("/api/mission/ground-state")
async def get_ground_state():
    """Get what ground control believes the spacecraft state is (delayed)."""
    rows = await execute_lakebase(
        "SELECT * FROM ground_state WHERE state_id = 1"
    )
    if not rows:
        raise HTTPException(status_code=404, detail="No ground state found")
    return rows[0]


@app.get("/api/hazards/active")
async def get_active_hazards():
    """Get currently active hazards from Lakebase."""
    rows = await execute_lakebase(
        "SELECT * FROM active_hazards ORDER BY risk_score DESC"
    )
    return {"hazards": rows, "count": len(rows)}


@app.get("/api/commands/queue")
async def get_command_queue():
    """Get current command queue from Lakebase."""
    rows = await execute_lakebase(
        "SELECT * FROM command_queue ORDER BY created_at DESC LIMIT 20"
    )
    return {"commands": rows, "count": len(rows)}


@app.post("/api/commands/approve")
async def approve_command(command_id: str):
    """Operator approves a pending command for transmission."""
    await execute_lakebase(
        "UPDATE command_queue SET status = 'approved', updated_at = NOW() "
        "WHERE command_id = :command_id AND status = 'pending'",
        {"command_id": command_id},
    )
    return {"status": "approved", "command_id": command_id}


@app.get("/api/agents/messages/latest")
async def get_latest_agent_messages():
    """Get agent messages from the most recent decision tick."""
    rows = await execute_lakebase(
        "SELECT * FROM agent_messages_realtime ORDER BY created_at DESC LIMIT 20"
    )
    return {"messages": rows, "count": len(rows)}


@app.get("/api/spacecraft/autopilot")
async def get_autopilot_state():
    """Get spacecraft autopilot state and mode."""
    rows = await execute_lakebase(
        "SELECT * FROM spacecraft_autopilot_state WHERE autopilot_id = 1"
    )
    if not rows:
        return {"mode": "unknown", "status": "not_initialized"}
    return rows[0]


@app.get("/api/telemetry/realtime")
async def get_realtime_telemetry(limit: int = 60):
    """Get per-second telemetry from Lakebase for live dashboard."""
    rows = await execute_lakebase(
        "SELECT * FROM telemetry_realtime ORDER BY simulation_time_s DESC LIMIT :lim",
        {"lim": min(limit, 600)},
    )
    return {"telemetry": rows, "count": len(rows)}


@app.get("/api/throughput")
async def get_throughput_metrics(limit: int = 60):
    """Get recent Lakebase/Delta throughput metrics for dashboard overlay."""
    rows = await execute_lakebase(
        "SELECT * FROM throughput_metrics ORDER BY timestamp DESC LIMIT :lim",
        {"lim": min(limit, 300)},
    )
    return {"metrics": rows, "count": len(rows)}


# ---------------------------------------------------------------------------
# API Routes — Delta Lake (historical/analytical tables via SQL warehouse)
# ---------------------------------------------------------------------------

@app.get("/api/telemetry/recent")
async def get_recent_telemetry(limit: int = 100):
    """Get most recent telemetry readings from Delta Lake."""
    rows = execute_sql(f"""
        SELECT *
        FROM `{CATALOG}`.telemetry.spacecraft_telemetry
        ORDER BY timestamp DESC
        LIMIT {min(limit, 1000)}
    """)
    return {"telemetry": rows, "count": len(rows)}


@app.get("/api/telemetry/trajectory")
async def get_trajectory(hours: int = 6):
    """Get trajectory data for visualization (sampled every 60s for performance)."""
    rows = execute_sql(f"""
        SELECT
            timestamp,
            position_x, position_y, position_z,
            velocity_x, velocity_y, velocity_z,
            fuel_remaining_kg, communication_delay_s
        FROM `{CATALOG}`.telemetry.spacecraft_telemetry
        WHERE timestamp >= (
            SELECT MAX(timestamp) - INTERVAL {hours} HOURS
            FROM `{CATALOG}`.telemetry.spacecraft_telemetry
        )
        AND SECOND(timestamp) = 0
        ORDER BY timestamp ASC
    """)
    return {"trajectory": rows, "count": len(rows)}


@app.get("/api/hazards/history")
async def get_hazard_history(limit: int = 50):
    """Get hazard history from Delta Lake."""
    rows = execute_sql(f"""
        SELECT *
        FROM `{CATALOG}`.hazards.detected_hazards
        ORDER BY detected_at DESC
        LIMIT {min(limit, 500)}
    """)
    return {"hazards": rows, "count": len(rows)}


@app.get("/api/maneuvers/candidates")
async def get_candidate_maneuvers():
    """Get current candidate maneuvers ranked by score."""
    rows = execute_sql(f"""
        SELECT *
        FROM `{CATALOG}`.navigation.candidate_maneuvers
        WHERE status = 'proposed'
        ORDER BY ranking ASC
        LIMIT 10
    """)
    return {"maneuvers": rows, "count": len(rows)}


@app.get("/api/commands/log")
async def get_command_log(limit: int = 50):
    """Get command execution log from Delta Lake."""
    rows = execute_sql(f"""
        SELECT *
        FROM `{CATALOG}`.commands.command_log
        ORDER BY created_at DESC
        LIMIT {min(limit, 200)}
    """)
    return {"commands": rows, "count": len(rows)}


@app.get("/api/agents/decisions")
async def get_agent_decisions(agent_name: Optional[str] = None, limit: int = 20):
    """Get agent decision log from Delta Lake."""
    where_clause = ""
    if agent_name:
        where_clause = f"WHERE agent_name = '{agent_name}'"

    rows = execute_sql(f"""
        SELECT *
        FROM `{CATALOG}`.agents.decision_log
        {where_clause}
        ORDER BY timestamp DESC
        LIMIT {min(limit, 100)}
    """)
    return {"decisions": rows, "count": len(rows)}


@app.get("/api/agents/messages")
async def get_agent_messages(tick_id: Optional[str] = None, limit: int = 50):
    """Get agent messages from Delta Lake."""
    where_clause = ""
    if tick_id:
        where_clause = f"WHERE tick_id = '{tick_id}'"

    rows = execute_sql(f"""
        SELECT *
        FROM `{CATALOG}`.agents.message_log
        {where_clause}
        ORDER BY timestamp DESC
        LIMIT {min(limit, 200)}
    """)
    return {"messages": rows, "count": len(rows)}


@app.get("/api/models/inference-log")
async def get_inference_log(caller: Optional[str] = None, limit: int = 50):
    """Get model inference call log from Delta Lake."""
    where_clause = ""
    if caller:
        where_clause = f"WHERE caller = '{caller}'"

    rows = execute_sql(f"""
        SELECT *
        FROM `{CATALOG}`.models.inference_log
        {where_clause}
        ORDER BY timestamp DESC
        LIMIT {min(limit, 200)}
    """)
    return {"inferences": rows, "count": len(rows)}


@app.get("/api/stats/summary")
async def get_mission_summary():
    """Get high-level mission statistics (mixed Lakebase + Delta)."""
    stats = {}

    # Delta Lake counts
    rows = execute_sql(f"SELECT COUNT(*) as cnt FROM `{CATALOG}`.telemetry.spacecraft_telemetry")
    stats["telemetry_readings"] = rows[0]["cnt"] if rows else 0

    rows = execute_sql(f"SELECT COUNT(*) as cnt FROM `{CATALOG}`.hazards.detected_hazards")
    stats["total_hazards"] = rows[0]["cnt"] if rows else 0

    rows = execute_sql(f"SELECT COUNT(*) as cnt FROM `{CATALOG}`.commands.command_log")
    stats["commands_executed"] = rows[0]["cnt"] if rows else 0

    rows = execute_sql(f"SELECT COUNT(*) as cnt FROM `{CATALOG}`.agents.decision_log")
    stats["agent_decisions"] = rows[0]["cnt"] if rows else 0

    # Lakebase count
    lb_rows = await execute_lakebase("SELECT COUNT(*) as cnt FROM active_hazards")
    stats["active_hazards"] = lb_rows[0]["cnt"] if lb_rows else 0

    return stats


@app.get("/api/mission/timeline")
async def get_mission_timeline(limit: int = 50):
    """Unified timeline of all mission events (from Delta Lake)."""
    rows = execute_sql(f"""
        SELECT timestamp, 'agent_decision' as event_type, agent_name as source,
               decision_type as detail, confidence_score as score
        FROM `{CATALOG}`.agents.decision_log
        UNION ALL
        SELECT created_at as timestamp, 'command' as event_type, command_type as source,
               status as detail, NULL as score
        FROM `{CATALOG}`.commands.command_log
        UNION ALL
        SELECT detected_at as timestamp, 'hazard' as event_type, hazard_type as source,
               status as detail, risk_score as score
        FROM `{CATALOG}`.hazards.detected_hazards
        ORDER BY timestamp DESC
        LIMIT {min(limit, 200)}
    """)
    return {"timeline": rows, "count": len(rows)}


# ---------------------------------------------------------------------------
# Serve React Frontend
# ---------------------------------------------------------------------------

static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/assets", StaticFiles(directory=os.path.join(static_dir, "assets")), name="assets")

    @app.get("/", response_class=HTMLResponse)
    async def serve_frontend():
        with open(os.path.join(static_dir, "index.html")) as f:
            return f.read()

    @app.get("/{path:path}", response_class=HTMLResponse)
    async def serve_spa(path: str):
        """Catch-all for SPA routing."""
        index_path = os.path.join(static_dir, "index.html")
        if os.path.exists(index_path):
            with open(index_path) as f:
                return f.read()
        raise HTTPException(status_code=404)
else:
    @app.get("/", response_class=HTMLResponse)
    async def serve_placeholder():
        return """
        <html>
        <head><title>Mission Control</title></head>
        <body style="background:#0a0e1a;color:#00ff88;font-family:monospace;padding:40px;text-align:center">
            <h1>MISSION CONTROL</h1>
            <p>Frontend not built yet. API available at /docs</p>
            <p>Run <code>cd app/frontend && npm run build</code> to build the UI.</p>
        </body>
        </html>
        """
