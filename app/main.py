"""
Mission Control Dashboard — FastAPI Backend

Serves the React frontend and provides API endpoints for:
- Real-time spacecraft state (from Lakebase)
- Telemetry history (from Delta Lake)
- Hazard data
- Command queue management
- Agent decision logs
"""

import json
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

CATALOG = os.environ.get("CATALOG", "mission_control_dev")
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "")


def get_workspace_client() -> WorkspaceClient:
    return WorkspaceClient()


def execute_sql(query: str, params: Optional[dict] = None) -> list[dict]:
    """Execute SQL query via Databricks SQL Statement API."""
    w = get_workspace_client()

    # Find a warehouse
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"Mission Control starting — catalog: {CATALOG}")
    yield
    print("Mission Control shutting down")


app = FastAPI(title="Mission Control", lifespan=lifespan)


# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------

@app.get("/api/mission/state")
async def get_mission_state():
    """Get current spacecraft state from Lakebase."""
    rows = execute_sql(f"""
        SELECT * FROM `{CATALOG}`.ops.mission_state
        WHERE state_id = 1
    """)
    if not rows:
        raise HTTPException(status_code=404, detail="No mission state found")
    return rows[0]


@app.get("/api/mission/clock")
async def get_simulation_clock():
    """Get current simulation clock state."""
    rows = execute_sql(f"""
        SELECT * FROM `{CATALOG}`.ops.simulation_clock
        WHERE clock_id = 1
    """)
    if not rows:
        raise HTTPException(status_code=404, detail="No clock state found")
    return rows[0]


@app.get("/api/telemetry/recent")
async def get_recent_telemetry(limit: int = 100):
    """Get most recent telemetry readings."""
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


@app.get("/api/hazards/active")
async def get_active_hazards():
    """Get currently active hazards from Lakebase."""
    rows = execute_sql(f"""
        SELECT * FROM `{CATALOG}`.ops.active_hazards
        ORDER BY risk_score DESC
    """)
    return {"hazards": rows, "count": len(rows)}


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


@app.get("/api/commands/queue")
async def get_command_queue():
    """Get current command queue from Lakebase."""
    rows = execute_sql(f"""
        SELECT *
        FROM `{CATALOG}`.ops.command_queue
        ORDER BY created_at DESC
        LIMIT 20
    """)
    return {"commands": rows, "count": len(rows)}


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
    """Get agent decision log."""
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


@app.get("/api/stats/summary")
async def get_mission_summary():
    """Get high-level mission statistics."""
    stats = {}

    # Telemetry count
    rows = execute_sql(f"SELECT COUNT(*) as cnt FROM `{CATALOG}`.telemetry.spacecraft_telemetry")
    stats["telemetry_readings"] = rows[0]["cnt"] if rows else 0

    # Hazard count
    rows = execute_sql(f"SELECT COUNT(*) as cnt FROM `{CATALOG}`.hazards.detected_hazards")
    stats["total_hazards"] = rows[0]["cnt"] if rows else 0

    # Active hazards
    rows = execute_sql(f"SELECT COUNT(*) as cnt FROM `{CATALOG}`.ops.active_hazards")
    stats["active_hazards"] = rows[0]["cnt"] if rows else 0

    # Commands executed
    rows = execute_sql(f"SELECT COUNT(*) as cnt FROM `{CATALOG}`.commands.command_log")
    stats["commands_executed"] = rows[0]["cnt"] if rows else 0

    # Agent decisions
    rows = execute_sql(f"SELECT COUNT(*) as cnt FROM `{CATALOG}`.agents.decision_log")
    stats["agent_decisions"] = rows[0]["cnt"] if rows else 0

    return stats


# ---------------------------------------------------------------------------
# New Endpoints: Agent Messages, Inference Log, Autopilot, Throughput
# ---------------------------------------------------------------------------

@app.get("/api/agents/messages/latest")
async def get_latest_agent_messages():
    """Get agent messages from the most recent decision tick."""
    rows = execute_sql(f"""
        SELECT * FROM `{CATALOG}`.ops.agent_messages_realtime
        ORDER BY created_at DESC
        LIMIT 20
    """)
    return {"messages": rows, "count": len(rows)}


@app.get("/api/agents/messages")
async def get_agent_messages(tick_id: Optional[str] = None, limit: int = 50):
    """Get agent messages, optionally filtered by tick_id."""
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
    """Get model inference call log with full input/output."""
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


@app.get("/api/spacecraft/autopilot")
async def get_autopilot_state():
    """Get spacecraft autopilot state and mode."""
    rows = execute_sql(f"""
        SELECT * FROM `{CATALOG}`.ops.spacecraft_autopilot_state
        WHERE state_id = 1
    """)
    if not rows:
        return {"mode": "unknown", "status": "not_initialized"}
    return rows[0]


@app.get("/api/mission/ground-state")
async def get_ground_state():
    """Get what ground control believes the spacecraft state is (delayed)."""
    rows = execute_sql(f"""
        SELECT * FROM `{CATALOG}`.ops.ground_state
        WHERE state_id = 1
    """)
    if not rows:
        raise HTTPException(status_code=404, detail="No ground state found")
    return rows[0]


@app.get("/api/throughput")
async def get_throughput_metrics(limit: int = 60):
    """Get recent Lakebase/Delta throughput metrics for dashboard overlay."""
    rows = execute_sql(f"""
        SELECT *
        FROM `{CATALOG}`.ops.throughput_metrics
        ORDER BY timestamp DESC
        LIMIT {min(limit, 300)}
    """)
    return {"metrics": rows, "count": len(rows)}


@app.get("/api/telemetry/realtime")
async def get_realtime_telemetry(limit: int = 60):
    """Get per-second telemetry from Lakebase for live dashboard."""
    rows = execute_sql(f"""
        SELECT *
        FROM `{CATALOG}`.ops.telemetry_realtime
        ORDER BY simulation_time_s DESC
        LIMIT {min(limit, 600)}
    """)
    return {"telemetry": rows, "count": len(rows)}


@app.post("/api/commands/approve")
async def approve_command(command_id: str):
    """Operator approves a pending command for transmission."""
    execute_sql(f"""
        UPDATE `{CATALOG}`.ops.command_queue
        SET status = 'approved', updated_at = CURRENT_TIMESTAMP()
        WHERE command_id = '{command_id}'
          AND status = 'pending'
    """)
    return {"status": "approved", "command_id": command_id}


@app.get("/api/mission/timeline")
async def get_mission_timeline(limit: int = 50):
    """Unified timeline of all mission events (decisions, commands, hazards)."""
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

# Mount static files if the build directory exists
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
