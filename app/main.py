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
import logging
import math
import os
import json
import time
import traceback
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mission-control")

CATALOG = os.environ.get("CATALOG", "mission_control_dev")
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "")
LAKEBASE_PROJECT_ID = os.environ.get("LAKEBASE_PROJECT_ID", "mission-control")
LAKEBASE_BRANCH_ID = os.environ.get("LAKEBASE_BRANCH_ID", "production")
SIMULATION_JOB_ID = os.environ.get("SIMULATION_JOB_ID", "")

# ---------------------------------------------------------------------------
# Inline Physics (lightweight subset — no external dependencies)
# Canonical source: src/python/mission_constants.py
# ---------------------------------------------------------------------------

SPEED_OF_LIGHT_KM_S = 299792.458
EARTH_ORBIT_RADIUS_KM = 1.496e8  # 1 AU
EARTH_ORBIT_PERIOD_S = 365.25 * 86400
EARTH_ANGULAR_VEL = 2.0 * math.pi / EARTH_ORBIT_PERIOD_S
GM_SUN = 1.32712440018e11  # km^3/s^2

# Spacecraft fuel model — must match src/python/mission_constants.py
EXHAUST_VEL = 30.0    # km/s — game-balanced (real ion ~30-50 km/s class)
DRY_MASS = 3000.0     # kg — spacecraft dry mass
FUEL_CAPACITY_KG = 1200.0  # kg — default fuel load (gives ~10.5 km/s delta-v budget)


from functools import lru_cache

# Earth orbital speed (constant for circular orbit)
_EARTH_ORBITAL_SPEED = math.sqrt(GM_SUN / EARTH_ORBIT_RADIUS_KM)  # ~29.78 km/s


@lru_cache(maxsize=256)
def _earth_orbit_cached(bucket: int) -> tuple[float, float, float, float]:
    """Cached Earth position + velocity for a 10-second time bucket.

    Returns (pos_x, pos_y, vel_x, vel_y) for the bucket midpoint.
    """
    t_s = bucket * 10.0
    angle = EARTH_ANGULAR_VEL * t_s
    cos_a = math.cos(angle)
    sin_a = math.sin(angle)
    return (
        EARTH_ORBIT_RADIUS_KM * cos_a,
        EARTH_ORBIT_RADIUS_KM * sin_a,
        -_EARTH_ORBITAL_SPEED * sin_a,
        _EARTH_ORBITAL_SPEED * cos_a,
    )


def earth_position_at(t_s: float) -> tuple[float, float]:
    """Earth (x, y) on circular orbit at simulation time t_s.

    Uses 10-second bucket cache — max position error ~0.003 km at Earth's speed.
    """
    if math.isnan(t_s) or math.isinf(t_s):
        t_s = 0.0
    bucket = int(t_s / 10.0)
    ex, ey, _, _ = _earth_orbit_cached(bucket)
    return ex, ey


def compute_comm_delay(sx: float, sy: float, sz: float, ex: float, ey: float) -> float:
    """One-way light-time delay in seconds between spacecraft and Earth."""
    dist = math.sqrt((sx - ex) ** 2 + (sy - ey) ** 2 + sz ** 2)
    return dist / SPEED_OF_LIGHT_KM_S


def earth_velocity_at(t_s: float) -> tuple[float, float]:
    """Earth (vx, vy) on circular orbit at simulation time t_s."""
    if math.isnan(t_s) or math.isinf(t_s):
        t_s = 0.0
    bucket = int(t_s / 10.0)
    _, _, evx, evy = _earth_orbit_cached(bucket)
    return evx, evy


def estimate_intercept(
    px: float, py: float, vx: float, vy: float, elapsed_s: float,
    max_iterations: int = 10,
) -> tuple[float, float, float, float, float, float]:
    """
    Iteratively estimate where Earth will be when the ship arrives.
    Returns (intercept_x, intercept_y, earth_vx, earth_vy, time_to_arrival_s,
             predicted_miss_km).

    Uses forward physics propagation (gravity + current velocity) to estimate
    when the ship's orbit crosses Earth's orbital radius, then finds Earth's
    position at that time. Falls back to simple speed-based estimate if the
    propagation doesn't converge.

    predicted_miss_km: closest the natural (unpowered) trajectory will pass to
    Earth. The autopilot uses this to decide whether to burn.
    """
    speed = math.sqrt(vx ** 2 + vy ** 2) or 1.0

    # --- Physics-based ETA: propagate the ship forward under solar gravity ---
    # Find when the ship reaches Earth's orbital radius
    prop_x, prop_y = px, py
    prop_vx, prop_vy = vx, vy
    prop_dt = 3600.0  # 1-hour steps
    max_prop_steps = 8760  # up to ~1 year
    physics_eta = 0.0
    best_dist = float("inf")
    best_eta = 0.0

    for step in range(max_prop_steps):
        r = math.sqrt(prop_x * prop_x + prop_y * prop_y) or 1.0
        a_mag = -GM_SUN / (r * r)
        prop_vx += a_mag * prop_x / r * prop_dt
        prop_vy += a_mag * prop_y / r * prop_dt
        prop_x += prop_vx * prop_dt
        prop_y += prop_vy * prop_dt
        physics_eta += prop_dt

        # Check distance to Earth at this future time
        future_t = elapsed_s + physics_eta
        ex, ey = earth_position_at(future_t)
        dist = math.sqrt((prop_x - ex) ** 2 + (prop_y - ey) ** 2)

        if dist < best_dist:
            best_dist = dist
            best_eta = physics_eta

        # Close enough — converged
        if dist < 500_000:  # within 500K km
            break

        # If we've passed perihelion and are heading away, stop
        if step > 100 and dist > best_dist * 1.5:
            break

    predicted_miss_km = best_dist

    # Use the best ETA found
    t_arrival = best_eta if best_eta > 0 else (math.sqrt((px - EARTH_ORBIT_RADIUS_KM) ** 2 + py ** 2) / max(speed, 0.1))

    # Refine with iterative Earth-position convergence
    for _ in range(max_iterations):
        future_t = elapsed_s + t_arrival
        ex, ey = earth_position_at(future_t)
        # Use closing speed toward this Earth position
        dx, dy = ex - px, ey - py
        dist = math.sqrt(dx * dx + dy * dy)
        closing = (vx * dx + vy * dy) / max(dist, 1.0)
        # If closing speed is positive, use it; otherwise use total speed
        effective_speed = max(closing, speed * 0.3)
        t_arrival = dist / max(effective_speed, 0.1)
        # Clamp to reasonable range (1 day to 2 years)
        t_arrival = max(86400.0, min(t_arrival, 2 * EARTH_ORBIT_PERIOD_S))

    evx, evy = earth_velocity_at(elapsed_s + t_arrival)
    return ex, ey, evx, evy, t_arrival, predicted_miss_km


_mini_tick_task: Optional[asyncio.Task] = None
_current_session_id: Optional[str] = None

# ---------------------------------------------------------------------------
# Onboard Trajectory Prediction Configuration
# ---------------------------------------------------------------------------
PREDICTION_CHECK_INTERVAL = 30  # every 30 ticks (~60 real seconds)
PREDICTION_HORIZON_S = 60.0  # predict 60 sim-seconds ahead
PREDICTION_CORRECTION_THRESHOLD_KM = 5000.0  # deviation before micro-correction
PREDICTION_MICRO_DV = 0.002  # km/s^2 max micro-correction authority
MODEL_SERVING_ENDPOINT = os.environ.get(
    "TRAJECTORY_MODEL_ENDPOINT",
    "trajectory_prediction",
)
FUEL_MODEL_ENDPOINT = os.environ.get("FUEL_MODEL_ENDPOINT", "fuel_estimation")
HAZARD_RISK_MODEL_ENDPOINT = os.environ.get("HAZARD_RISK_MODEL_ENDPOINT", "hazard_risk")
MANEUVER_RANK_MODEL_ENDPOINT = os.environ.get("MANEUVER_RANK_MODEL_ENDPOINT", "maneuver_ranking")
DELAY_POLICY_MODEL_ENDPOINT = os.environ.get("DELAY_POLICY_MODEL_ENDPOINT", "delay_aware")

_prediction_counter = 0
_prediction_buffer: list[dict] = []  # buffered for Delta Lake flush
_telemetry_buffer: list[dict] = []   # sampled telemetry for Delta flush at mission end
TELEMETRY_SAMPLE_INTERVAL = 60       # buffer a telemetry snapshot every N ticks
DELTA_STREAM_INTERVAL = 60           # flush telemetry to Delta every N ticks (~2 min)
_delta_stream_counter = 0
_inference_log_buffer: list[dict] = []  # buffered inference log entries for Delta
INFERENCE_LOG_BUFFER_MAX = 500

# ---------------------------------------------------------------------------
# Event Sourcing — capture key mission events for replay and analytics
# ---------------------------------------------------------------------------
_event_buffer: list[dict] = []       # in-memory ring buffer of recent events
EVENT_BUFFER_MAX = 200               # keep last 200 events in memory


def _record_event(
    event_type: str,
    summary: str,
    *,
    simulation_time_s: float = 0.0,
    metadata: dict | None = None,
) -> None:
    """Append a mission event to the in-memory buffer (non-blocking)."""
    evt = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "summary": summary,
        "simulation_time_s": round(simulation_time_s, 2),
        "metadata": metadata or {},
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    _event_buffer.append(evt)
    if len(_event_buffer) > EVENT_BUFFER_MAX:
        _event_buffer[:] = _event_buffer[-EVENT_BUFFER_MAX:]

# Adaptive model selection: track recent errors by source
_ml_recent_errors: list[float] = []   # last N ML prediction errors (km)
_phys_recent_errors: list[float] = [] # last N physics prediction errors (km)
ML_ERROR_WINDOW = 10                  # rolling window size
ML_DRIFT_THRESHOLD = 2.0             # if ML error > 2x physics error, prefer physics
_model_preference: str = "model_serving"  # "model_serving" or "physics_fallback"
_drift_retrain_triggered = False      # only trigger once per session

# ---------------------------------------------------------------------------
# In-App Agent Chain Configuration
# ---------------------------------------------------------------------------
AGENT_CHECK_INTERVAL = 45  # every 45 ticks (~90 real seconds)
_agent_counter = 0
_agent_running = False  # prevents overlapping cycles
_has_outcome_columns = False  # set True after startup migration
_last_outcome: dict | None = None  # in-memory fallback for mission outcome
_captain_tables_ready = False  # set True after captain table migration

# ---------------------------------------------------------------------------
# Ship Captain (onboard autonomous agent)
# ---------------------------------------------------------------------------
# Imported lazily to avoid circular deps; initialized at startup
_ship_captain = None  # ShipCaptain instance
CAPTAIN_CHECK_INTERVAL = 8  # run captain every 8 ticks (~16 real seconds)
_captain_counter = 0
_captain_decisions: list[dict] = []  # in-memory buffer for recent decisions

# ---------------------------------------------------------------------------
# Throughput Tracking
# ---------------------------------------------------------------------------
SUB_TICK_MAX_DT = 300.0  # max 5 minutes of sim time per physics sub-step
MAX_TELEMETRY_WRITES_PER_TICK = 8  # cap sub-tick telemetry writes

_ops_tracker: dict = {
    "reads": 0,
    "writes": 0,
    "latencies_ms": [],  # recent query latencies
    "window_start": 0.0,
    "sub_ticks_last": 1,
    "telemetry_writes_last": 1,
}


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
        pool_size: int = 8,
        max_overflow: int = 12,
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
            pool_pre_ping=True,  # health-check connections before use
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

    async def pre_warm(self, count: int = 3):
        """Pre-warm connection pool by executing lightweight queries."""
        for _ in range(min(count, self.pool_size)):
            try:
                async with self._session_maker() as session:
                    await session.execute(text("SELECT 1"))
            except Exception:
                pass
        logger.info(f"Lakebase pool pre-warmed ({count} connections)")

    def pool_status(self) -> dict:
        """Return current connection pool metrics."""
        if not self._engine:
            return {}
        pool = self._engine.pool
        return {
            "pool_size": pool.size(),
            "checked_in": pool.checkedin(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
        }

    async def close(self):
        await self.stop_refresh()
        if self._engine:
            await self._engine.dispose()


# ---------------------------------------------------------------------------
# SQL Warehouse helper (Delta Lake queries)
# ---------------------------------------------------------------------------

def get_workspace_client() -> WorkspaceClient:
    return WorkspaceClient()


def _sql_escape(value) -> str:
    """Safely escape a value for SQL string interpolation in Delta Lake queries.

    Returns NULL for None, numeric literals for numbers, and single-quote-escaped
    strings for everything else. Prevents SQL injection in Statement API calls
    which don't support full parameterized queries for INSERT VALUES.
    """
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        if math.isnan(value) or math.isinf(value):
            return "NULL"
        return str(value)
    s = str(value)
    # Escape single quotes and backslashes
    s = s.replace("\\", "\\\\").replace("'", "''")
    return f"'{s}'"


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

    try:
        response = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=query,
            wait_timeout="30s",
        )
    except Exception as e:
        logger.error(f"SQL execution error: {e}")
        logger.error(traceback.format_exc())
        return []

    if response.status.state != StatementState.SUCCEEDED:
        error_msg = str(response.status.error) if response.status.error else "Unknown error"
        if any(s in error_msg for s in ["TABLE_OR_VIEW_NOT_FOUND", "SCHEMA_NOT_FOUND",
                                         "does not exist", "not found", "NOT_FOUND",
                                         "INSUFFICIENT_PERMISSIONS"]):
            logger.warning(f"SQL query returned no data (expected): {error_msg[:200]}")
            return []
        logger.error(f"SQL query failed: {error_msg}")
        return []

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
    t0 = time.monotonic()
    try:
        async with lakebase.session() as session:
            result = await session.execute(text(query), params or {})
            if result.returns_rows:
                columns = list(result.keys())
                rows = [dict(zip(columns, row)) for row in result.fetchall()]
                _ops_tracker["reads"] += 1
                latency = (time.monotonic() - t0) * 1000
                _ops_tracker["latencies_ms"].append(latency)
                return rows
            await session.commit()
            _ops_tracker["writes"] += 1
            latency = (time.monotonic() - t0) * 1000
            _ops_tracker["latencies_ms"].append(latency)
            return []
    except Exception as e:
        logger.error(f"Lakebase query error: {e} | query: {query[:100]}")
        logger.error(traceback.format_exc())
        raise


# ---------------------------------------------------------------------------
# Session Management
# ---------------------------------------------------------------------------


async def _create_session(
    scenario_name: str = "Odyssey Return",
    scenario_type: str = "default",
    px: float = None, py: float = None, pz: float = None,
    vx: float = None, vy: float = None, vz: float = None,
    fuel: float = None,
) -> str:
    """Create a new simulation session and return its UUID."""
    global _current_session_id
    import uuid
    session_id = str(uuid.uuid4())
    _current_session_id = session_id

    # Snapshot initial conditions from current state if not provided
    if px is None:
        state_rows = await execute_lakebase(
            "SELECT position_x, position_y, position_z, "
            "velocity_x, velocity_y, velocity_z, fuel_remaining_kg "
            "FROM mission_state WHERE state_id = 1"
        )
        if state_rows:
            s = state_rows[0]
            px = float(s.get("position_x", 0))
            py = float(s.get("position_y", 0))
            pz = float(s.get("position_z", 0))
            vx = float(s.get("velocity_x", 0))
            vy = float(s.get("velocity_y", 0))
            vz = float(s.get("velocity_z", 0))
            fuel = float(s.get("fuel_remaining_kg", 1200))

    await execute_lakebase(
        "INSERT INTO simulation_sessions "
        "(session_id, session_name, scenario_type, "
        " initial_position_x, initial_position_y, initial_position_z, "
        " initial_velocity_x, initial_velocity_y, initial_velocity_z, "
        " initial_fuel_kg, started_at, status, created_at, updated_at) "
        "VALUES (:sid, :name, :stype, :px, :py, :pz, :vx, :vy, :vz, "
        "  :fuel, NOW(), 'active', NOW(), NOW())",
        {"sid": session_id, "name": scenario_name, "stype": scenario_type,
         "px": px, "py": py, "pz": pz, "vx": vx, "vy": vy, "vz": vz,
         "fuel": fuel},
    )

    # Tag current-state tables with session_id
    await execute_lakebase(
        "UPDATE mission_state SET session_id = :sid WHERE state_id = 1",
        {"sid": session_id},
    )
    await execute_lakebase(
        "UPDATE simulation_clock SET session_id = :sid WHERE clock_id = 1",
        {"sid": session_id},
    )
    await execute_lakebase(
        "UPDATE spacecraft_autopilot_state SET session_id = :sid WHERE autopilot_id = 1",
        {"sid": session_id},
    )
    await execute_lakebase(
        "UPDATE ground_state SET session_id = :sid WHERE state_id = 1",
        {"sid": session_id},
    )

    logger.info(f"[SESSION] Created session {session_id[:8]} ({scenario_name}, {scenario_type})")
    return session_id


async def _end_session(session_id: str):
    """Finalize a session with summary stats."""
    if not session_id:
        return

    state = await execute_lakebase("SELECT * FROM mission_state WHERE state_id = 1")
    clock = await execute_lakebase("SELECT * FROM simulation_clock WHERE clock_id = 1")
    autopilot = await execute_lakebase(
        "SELECT * FROM spacecraft_autopilot_state WHERE autopilot_id = 1"
    )
    cmd_count = await execute_lakebase(
        "SELECT COUNT(*) as cnt FROM command_queue "
        "WHERE session_id = :sid AND status = 'executed'",
        {"sid": session_id},
    )
    hazard_count = await execute_lakebase(
        "SELECT COUNT(*) as cnt FROM active_hazards WHERE session_id = :sid",
        {"sid": session_id},
    )

    s = state[0] if state else {}
    c = clock[0] if clock else {}
    a = autopilot[0] if autopilot else {}

    elapsed_s = float(s.get("mission_elapsed_s", 0))
    ex, ey = earth_position_at(elapsed_s)
    dist_to_earth = math.sqrt(
        (float(s.get("position_x", 0)) - ex) ** 2
        + (float(s.get("position_y", 0)) - ey) ** 2
    )

    # Read session start time to compute wall duration
    session_rows = await execute_lakebase(
        "SELECT started_at FROM simulation_sessions WHERE session_id = :sid",
        {"sid": session_id},
    )
    started_at = session_rows[0]["started_at"] if session_rows else None

    await execute_lakebase(
        "UPDATE simulation_sessions SET "
        "  ended_at = NOW(), status = 'completed', "
        "  duration_sim_seconds = :sim_s, "
        "  final_distance_to_earth_km = :dist, "
        "  total_burns_executed = :burns, "
        "  total_corrections = :corrections, "
        "  total_fuel_used_kg = :fuel_used, "
        "  total_hazards_encountered = :hazards, "
        "  max_time_scale_used = GREATEST(COALESCE(max_time_scale_used, 1), :ts), "
        "  updated_at = NOW() "
        "WHERE session_id = :sid",
        {
            "sid": session_id,
            "sim_s": elapsed_s,
            "dist": dist_to_earth,
            "burns": int((cmd_count[0] if cmd_count else {}).get("cnt", 0)),
            "corrections": int(a.get("total_corrections", 0)),
            "fuel_used": float(a.get("fuel_used_by_autopilot_kg", 0)),
            "hazards": int((hazard_count[0] if hazard_count else {}).get("cnt", 0)),
            "ts": float(c.get("time_scale", 1)),
        },
    )

    logger.info(f"[SESSION] Ended session {session_id[:8]} — {elapsed_s:.0f}s sim time, {dist_to_earth:.0f} km from Earth")


async def _flush_session_to_delta(session_id: str):
    """
    Flush all session data from Lakebase to Delta for ML training.
    Runs at mission end to ensure Delta has the complete session history.
    Uses the SQL warehouse (execute_sql) to INSERT into Delta tables.
    """
    if not session_id:
        return

    logger.info(f"[DELTA-SYNC] Flushing session {session_id[:8]} to Delta tables...")
    flushed = {"telemetry": 0, "commands": 0, "predictions": 0, "agents": 0, "captain": 0, "session": 0}

    try:
        # 1. Flush buffered telemetry → Delta telemetry.spacecraft_telemetry
        if _telemetry_buffer:
            batch_size = 50
            for i in range(0, len(_telemetry_buffer), batch_size):
                batch = _telemetry_buffer[i:i + batch_size]
                val_parts = []
                for t in batch:
                    tid = f"{session_id}-{t['tick_seq']}"
                    val_parts.append(
                        f"({_sql_escape(tid)}, CURRENT_TIMESTAMP(), "
                        f"{_sql_escape(t['position_x'])}, {_sql_escape(t['position_y'])}, {_sql_escape(t['position_z'])}, "
                        f"{_sql_escape(t['velocity_x'])}, {_sql_escape(t['velocity_y'])}, {_sql_escape(t['velocity_z'])}, "
                        f"{_sql_escape(t['fuel_remaining_kg'])}, {_sql_escape(t['hull_integrity'])}, "
                        f"{_sql_escape(t['engine_status'])}, {_sql_escape(t['communication_delay_s'])}, "
                        f"CURRENT_TIMESTAMP(), {_sql_escape(session_id)})"
                    )
                values = ", ".join(val_parts)
                execute_sql(
                    f"INSERT INTO `{CATALOG}`.telemetry.spacecraft_telemetry "
                    f"(telemetry_id, timestamp, position_x, position_y, position_z, "
                    f"velocity_x, velocity_y, velocity_z, fuel_remaining_kg, "
                    f"hull_integrity, engine_status, communication_delay_s, "
                    f"ingestion_timestamp, session_id) VALUES {values}"
                )
                flushed["telemetry"] += len(batch)
            logger.info(f"[DELTA-SYNC] Flushed {flushed['telemetry']} telemetry rows")

        # 2. Flush commands → Delta commands.command_log (batched)
        commands = await execute_lakebase(
            "SELECT command_id, command_type, payload, status, approved_by, "
            "  created_at, transmit_time, estimated_receive_time, session_id "
            "FROM command_queue WHERE session_id = :sid",
            {"sid": session_id},
        )
        if commands:
            def _ts_cast(val):
                return "NULL" if not val else f"CAST({_sql_escape(str(val))} AS TIMESTAMP)"

            batch_size = 50
            for i in range(0, len(commands), batch_size):
                batch = commands[i:i + batch_size]
                values = ", ".join(
                    f"({_sql_escape(cmd['command_id'])}, {_sql_escape(cmd.get('command_type', 'burn'))}, "
                    f"{_sql_escape(str(cmd.get('payload', '')))}, "
                    f"{_ts_cast(cmd.get('created_at'))}, "
                    f"{_sql_escape(cmd.get('approved_by', ''))}, "
                    f"{_ts_cast(cmd.get('transmit_time'))}, "
                    f"{_ts_cast(cmd.get('estimated_receive_time'))}, "
                    f"{_sql_escape(cmd.get('status', ''))}, {_sql_escape(session_id)})"
                    for cmd in batch
                )
                execute_sql(
                    f"INSERT INTO `{CATALOG}`.commands.command_log "
                    f"(command_id, command_type, payload, created_at, approved_by, "
                    f"transmit_time, estimated_receive_time, status, session_id) VALUES {values}"
                )
                flushed["commands"] += len(batch)
            logger.info(f"[DELTA-SYNC] Flushed {flushed['commands']} commands")

        # 3. Flush predictions → Delta models.onboard_inference_log (batched)
        predictions = await execute_lakebase(
            "SELECT prediction_id, simulation_time_s, prediction_horizon_s, source, "
            "  input_pos_x, input_pos_y, input_pos_z, "
            "  input_vel_x, input_vel_y, input_vel_z, "
            "  input_fuel, input_comm_delay, "
            "  predicted_pos_x, predicted_pos_y, predicted_pos_z, "
            "  actual_pos_x, actual_pos_y, actual_pos_z, "
            "  prediction_error_km, assessment, action_taken, correction_dv, "
            "  inference_latency_ms, created_at "
            "FROM onboard_predictions WHERE session_id = :sid",
            {"sid": session_id},
        )
        if predictions:
            batch_size = 20  # predictions have many columns, keep batches smaller
            for i in range(0, len(predictions), batch_size):
                batch = predictions[i:i + batch_size]
                values = ", ".join(
                    f"({_sql_escape(p['prediction_id'])}, {_sql_escape(session_id)}, "
                    f"{_sql_escape(p.get('simulation_time_s'))}, {_sql_escape(p.get('prediction_horizon_s'))}, "
                    f"{_sql_escape(p.get('source', 'physics_fallback'))}, "
                    f"{_sql_escape(p.get('input_pos_x'))}, {_sql_escape(p.get('input_pos_y'))}, {_sql_escape(p.get('input_pos_z'))}, "
                    f"{_sql_escape(p.get('input_vel_x'))}, {_sql_escape(p.get('input_vel_y'))}, {_sql_escape(p.get('input_vel_z'))}, "
                    f"{_sql_escape(p.get('input_fuel'))}, {_sql_escape(p.get('input_comm_delay'))}, "
                    f"{_sql_escape(p.get('predicted_pos_x'))}, {_sql_escape(p.get('predicted_pos_y'))}, {_sql_escape(p.get('predicted_pos_z'))}, "
                    f"{_sql_escape(p.get('actual_pos_x'))}, {_sql_escape(p.get('actual_pos_y'))}, {_sql_escape(p.get('actual_pos_z'))}, "
                    f"{_sql_escape(p.get('prediction_error_km'))}, {_sql_escape(p.get('assessment', ''))}, "
                    f"{_sql_escape(p.get('action_taken', ''))}, {_sql_escape(p.get('correction_dv'))}, "
                    f"{_sql_escape(p.get('inference_latency_ms'))}, "
                    f"CAST({_sql_escape(str(p.get('created_at', '')))} AS TIMESTAMP))"
                    for p in batch
                )
                execute_sql(
                    f"INSERT INTO `{CATALOG}`.models.onboard_inference_log "
                    f"(inference_id, session_id, simulation_time_s, prediction_horizon_s, "
                    f"prediction_source, input_pos_x, input_pos_y, input_pos_z, "
                    f"input_vel_x, input_vel_y, input_vel_z, input_fuel, input_comm_delay, "
                    f"predicted_pos_x, predicted_pos_y, predicted_pos_z, "
                    f"actual_pos_x, actual_pos_y, actual_pos_z, "
                    f"prediction_error_km, assessment, action_taken, correction_dv, "
                    f"inference_latency_ms, created_at) VALUES {values}"
                )
                flushed["predictions"] += len(batch)
            logger.info(f"[DELTA-SYNC] Flushed {flushed['predictions']} predictions")

        # 4. Flush agent messages → Delta agents.message_log (batched)
        messages = await execute_lakebase(
            "SELECT message_id, from_agent, to_agent, message_type, content, "
            "  tick_id, created_at "
            "FROM agent_messages_realtime WHERE session_id = :sid",
            {"sid": session_id},
        )
        if messages:
            batch_size = 50
            for i in range(0, len(messages), batch_size):
                batch = messages[i:i + batch_size]
                values = ", ".join(
                    f"({_sql_escape(m['message_id'])}, {_sql_escape(m.get('from_agent', ''))}, "
                    f"{_sql_escape(m.get('to_agent', ''))}, {_sql_escape(m.get('message_type', ''))}, "
                    f"{_sql_escape(str(m.get('content', '')))}, "
                    f"CAST({_sql_escape(str(m.get('created_at', '')))} AS TIMESTAMP), "
                    f"{_sql_escape(m.get('tick_id', ''))}, {_sql_escape(session_id)})"
                    for m in batch
                )
                execute_sql(
                    f"INSERT INTO `{CATALOG}`.agents.message_log "
                    f"(message_id, from_agent, to_agent, message_type, content, "
                    f"timestamp, tick_id, session_id) VALUES {values}"
                )
                flushed["agents"] += len(batch)
            logger.info(f"[DELTA-SYNC] Flushed {flushed['agents']} agent messages")

        # 5. Flush captain decisions → Delta agents.captain_decision_log
        if _captain_tables_ready:
            captain_rows = await execute_lakebase(
                "SELECT decision_id, simulation_time_s, action, priority_level, "
                "  reasoning, override_of_command_id, original_command_summary, "
                "  captain_alternative_summary, delta_v, fuel_cost_kg, "
                "  alert_level, confidence, elapsed_ms, created_at "
                "FROM captain_decisions WHERE session_id = :sid",
                {"sid": session_id},
            ) or []
            if captain_rows:
                batch_size = 50
                for i in range(0, len(captain_rows), batch_size):
                    batch = captain_rows[i:i + batch_size]
                    values = ", ".join(
                        f"({_sql_escape(c['decision_id'])}, {_sql_escape(session_id)}, "
                        f"{_sql_escape(c.get('simulation_time_s'))}, {_sql_escape(c.get('action', ''))}, "
                        f"{_sql_escape(c.get('priority_level'))}, {_sql_escape(c.get('reasoning', ''))}, "
                        f"{_sql_escape(c.get('override_of_command_id'))}, {_sql_escape(c.get('original_command_summary'))}, "
                        f"{_sql_escape(c.get('captain_alternative_summary'))}, {_sql_escape(c.get('delta_v'))}, "
                        f"{_sql_escape(c.get('fuel_cost_kg'))}, {_sql_escape(c.get('alert_level', 'green'))}, "
                        f"{_sql_escape(c.get('confidence'))}, {_sql_escape(c.get('elapsed_ms'))}, "
                        f"CAST({_sql_escape(str(c.get('created_at', '')))} AS TIMESTAMP))"
                        for c in batch
                    )
                    execute_sql(
                        f"INSERT INTO `{CATALOG}`.agents.captain_decision_log "
                        f"(decision_id, session_id, simulation_time_s, action, priority_level, "
                        f"reasoning, override_of_command_id, original_command_summary, "
                        f"captain_alternative_summary, delta_v, fuel_cost_kg, "
                        f"alert_level, confidence, elapsed_ms, created_at) VALUES {values}"
                    )
                    flushed["captain"] = flushed.get("captain", 0) + len(batch)
                logger.info(f"[DELTA-SYNC] Flushed {flushed.get('captain', 0)} captain decisions")

        # 6. Flush mission events → Delta missions.mission_events
        if _event_buffer:
            batch_size = 50
            for i in range(0, len(_event_buffer), batch_size):
                batch = _event_buffer[i:i + batch_size]
                values = ", ".join(
                    f"({_sql_escape(e['event_id'])}, {_sql_escape(session_id)}, "
                    f"{_sql_escape(e['event_type'])}, {_sql_escape(e['summary'])}, "
                    f"{_sql_escape(e['simulation_time_s'])}, "
                    f"{_sql_escape(json.dumps(e.get('metadata', {})))}, "
                    f"CAST({_sql_escape(e['created_at'])} AS TIMESTAMP))"
                    for e in batch
                )
                execute_sql(
                    f"INSERT INTO `{CATALOG}`.missions.mission_events "
                    f"(event_id, session_id, event_type, summary, "
                    f"simulation_time_s, metadata, created_at) VALUES {values}"
                )
                flushed["events"] = flushed.get("events", 0) + len(batch)
            logger.info(f"[DELTA-SYNC] Flushed {flushed.get('events', 0)} mission events")

        # 6b. Flush remaining inference log entries (not yet streamed)
        if _inference_log_buffer:
            to_flush = list(_inference_log_buffer)
            _inference_log_buffer.clear()
            batch_size = 50
            for i in range(0, len(to_flush), batch_size):
                batch = to_flush[i:i + batch_size]
                values = ", ".join(
                    f"({_sql_escape(e['inference_id'])}, {_sql_escape(e['endpoint_name'])}, "
                    f"{_sql_escape(e['caller'])}, {_sql_escape(e['input_features'])}, "
                    f"{_sql_escape(e['output_prediction'])}, {_sql_escape(e.get('latency_ms'))}, "
                    f"CURRENT_TIMESTAMP(), {_sql_escape(e.get('simulation_time_s'))}, "
                    f"{_sql_escape(e.get('tick_id', ''))}, {_sql_escape(json.dumps(e.get('metadata', {})))}, "
                    f"{_sql_escape(session_id)})"
                    for e in batch
                )
                execute_sql(
                    f"INSERT INTO `{CATALOG}`.models.inference_log "
                    f"(inference_id, endpoint_name, caller, input_features, output_prediction, "
                    f"latency_ms, timestamp, simulation_time_s, tick_id, metadata, session_id) "
                    f"VALUES {values}"
                )
                flushed["inference"] = flushed.get("inference", 0) + len(batch)
            logger.info(f"[DELTA-SYNC] Flushed {flushed.get('inference', 0)} inference log entries")

        # 7. Flush session record → Delta missions.simulation_sessions
        session_row = await execute_lakebase(
            "SELECT * FROM simulation_sessions WHERE session_id = :sid",
            {"sid": session_id},
        )
        if session_row:
            sr = session_row[0]
            def _ts(val):
                return "NULL" if val is None else f"CAST({_sql_escape(str(val))} AS TIMESTAMP)"
            execute_sql(
                f"INSERT INTO `{CATALOG}`.missions.simulation_sessions "
                f"(session_id, session_name, scenario_type, "
                f"initial_position_x, initial_position_y, initial_position_z, "
                f"initial_velocity_x, initial_velocity_y, initial_velocity_z, "
                f"initial_fuel_kg, started_at, ended_at, "
                f"duration_sim_seconds, max_time_scale_used, "
                f"final_distance_to_earth_km, total_burns_executed, "
                f"total_fuel_used_kg, total_corrections, "
                f"total_hazards_encountered, total_agent_decisions, "
                f"total_predictions_made, outcome, status) "
                f"VALUES ({_sql_escape(session_id)}, {_sql_escape(sr.get('session_name', ''))}, "
                f"{_sql_escape(sr.get('scenario_type', 'default'))}, "
                f"{_sql_escape(sr.get('initial_position_x'))}, {_sql_escape(sr.get('initial_position_y'))}, "
                f"{_sql_escape(sr.get('initial_position_z'))}, "
                f"{_sql_escape(sr.get('initial_velocity_x'))}, {_sql_escape(sr.get('initial_velocity_y'))}, "
                f"{_sql_escape(sr.get('initial_velocity_z'))}, "
                f"{_sql_escape(sr.get('initial_fuel_kg'))}, "
                f"{_ts(sr.get('started_at'))}, {_ts(sr.get('ended_at'))}, "
                f"{_sql_escape(sr.get('duration_sim_seconds'))}, {_sql_escape(sr.get('max_time_scale_used'))}, "
                f"{_sql_escape(sr.get('final_distance_to_earth_km'))}, {_sql_escape(sr.get('total_burns_executed'))}, "
                f"{_sql_escape(sr.get('total_fuel_used_kg'))}, {_sql_escape(sr.get('total_corrections'))}, "
                f"{_sql_escape(sr.get('total_hazards_encountered'))}, {_sql_escape(sr.get('total_agent_decisions'))}, "
                f"{_sql_escape(sr.get('total_predictions_made'))}, "
                f"{_sql_escape(sr.get('outcome', ''))}, {_sql_escape(sr.get('status', 'completed'))})"
            )
            flushed["session"] = 1
            logger.info(f"[DELTA-SYNC] Flushed session record")

    except Exception as e:
        logger.error(f"[DELTA-SYNC] Error flushing to Delta: {e}")
        logger.error(traceback.format_exc())

    logger.info(
        f"[DELTA-SYNC] Complete — telemetry={flushed['telemetry']}, "
        f"commands={flushed['commands']}, predictions={flushed['predictions']}, "
        f"agents={flushed['agents']}, captain={flushed.get('captain', 0)}, "
        f"events={flushed.get('events', 0)}, session={flushed['session']}"
    )
    return flushed


async def _flush_and_retrain(session_id: str):
    """Flush session data to Delta, then trigger model retrain."""
    try:
        await _flush_session_to_delta(session_id)
        await _auto_retrain()
    except Exception as e:
        logger.error(f"[MISSION-END] Flush/retrain pipeline failed: {e}")


async def _stream_telemetry_to_delta():
    """
    Streaming CDC: flush buffered telemetry + inference logs to Delta during the mission.
    Runs periodically (every DELTA_STREAM_INTERVAL ticks) so Delta stays near-real-time
    rather than only getting data at mission end.
    """
    global _telemetry_buffer, _inference_log_buffer
    session_id = _current_session_id
    if not session_id:
        return

    flushed = {"telemetry": 0, "inference": 0}

    try:
        # 1. Stream telemetry snapshots to Delta
        if _telemetry_buffer:
            to_flush = list(_telemetry_buffer)
            _telemetry_buffer.clear()
            batch_size = 50
            for i in range(0, len(to_flush), batch_size):
                batch = to_flush[i:i + batch_size]
                values = ", ".join(
                    f"({_sql_escape(str(session_id) + '-' + str(t['tick_seq']))}, CURRENT_TIMESTAMP(), "
                    f"{_sql_escape(t['position_x'])}, {_sql_escape(t['position_y'])}, {_sql_escape(t['position_z'])}, "
                    f"{_sql_escape(t['velocity_x'])}, {_sql_escape(t['velocity_y'])}, {_sql_escape(t['velocity_z'])}, "
                    f"{_sql_escape(t['fuel_remaining_kg'])}, {_sql_escape(t['hull_integrity'])}, "
                    f"{_sql_escape(t['engine_status'])}, {_sql_escape(t['communication_delay_s'])}, "
                    f"CURRENT_TIMESTAMP(), {_sql_escape(session_id)})"
                    for t in batch
                )
                execute_sql(
                    f"INSERT INTO `{CATALOG}`.telemetry.spacecraft_telemetry "
                    f"(telemetry_id, timestamp, position_x, position_y, position_z, "
                    f"velocity_x, velocity_y, velocity_z, fuel_remaining_kg, "
                    f"hull_integrity, engine_status, communication_delay_s, "
                    f"ingestion_timestamp, session_id) VALUES {values}"
                )
                flushed["telemetry"] += len(batch)

        # 2. Stream inference log entries to Delta
        if _inference_log_buffer:
            to_flush = list(_inference_log_buffer)
            _inference_log_buffer.clear()
            batch_size = 50
            for i in range(0, len(to_flush), batch_size):
                batch = to_flush[i:i + batch_size]
                values = ", ".join(
                    f"({_sql_escape(e['inference_id'])}, {_sql_escape(e['endpoint_name'])}, "
                    f"{_sql_escape(e['caller'])}, {_sql_escape(e['input_features'])}, "
                    f"{_sql_escape(e['output_prediction'])}, {_sql_escape(e.get('latency_ms'))}, "
                    f"CURRENT_TIMESTAMP(), {_sql_escape(e.get('simulation_time_s'))}, "
                    f"{_sql_escape(e.get('tick_id', ''))}, {_sql_escape(json.dumps(e.get('metadata', {})))}, "
                    f"{_sql_escape(session_id)})"
                    for e in batch
                )
                execute_sql(
                    f"INSERT INTO `{CATALOG}`.models.inference_log "
                    f"(inference_id, endpoint_name, caller, input_features, output_prediction, "
                    f"latency_ms, timestamp, simulation_time_s, tick_id, metadata, session_id) "
                    f"VALUES {values}"
                )
                flushed["inference"] += len(batch)

        if flushed["telemetry"] or flushed["inference"]:
            logger.info(
                f"[DELTA-STREAM] Flushed telemetry={flushed['telemetry']}, "
                f"inference={flushed['inference']} to Delta (mid-mission)"
            )
    except Exception as e:
        logger.debug(f"[DELTA-STREAM] Streaming flush failed (non-fatal): {e}")


def _log_inference(
    endpoint_name: str,
    caller: str,
    input_features: dict,
    output_prediction: dict,
    latency_ms: float = 0.0,
    simulation_time_s: float = 0.0,
) -> None:
    """Buffer an inference log entry for periodic Delta flush."""
    _inference_log_buffer.append({
        "inference_id": str(uuid.uuid4()),
        "endpoint_name": endpoint_name,
        "caller": caller,
        "input_features": json.dumps(input_features),
        "output_prediction": json.dumps(output_prediction),
        "latency_ms": round(latency_ms, 1),
        "simulation_time_s": round(simulation_time_s, 2),
        "tick_id": str(int(simulation_time_s)),
        "metadata": {},
    })
    if len(_inference_log_buffer) > INFERENCE_LOG_BUFFER_MAX:
        _inference_log_buffer[:] = _inference_log_buffer[-INFERENCE_LOG_BUFFER_MAX:]


async def _auto_retrain():
    """Trigger the retrain job automatically after a mission ends."""
    retrain_job_id = os.environ.get("RETRAIN_JOB_ID", "")
    if not retrain_job_id:
        logger.info("[RETRAIN] No RETRAIN_JOB_ID configured — skipping auto-retrain")
        return

    try:
        w = get_workspace_client()
        run = w.jobs.run_now(job_id=int(retrain_job_id))
        logger.info(
            f"[RETRAIN] Auto-triggered model retrain job {retrain_job_id}, "
            f"run_id={run.run_id} — model will learn from this mission's data"
        )
    except Exception as e:
        logger.warning(f"[RETRAIN] Auto-retrain failed (non-fatal): {e}")


async def _recover_or_create_session():
    """On app startup, recover an active session or create a new one."""
    global _current_session_id
    try:
        active = await execute_lakebase(
            "SELECT session_id FROM simulation_sessions WHERE status = 'active' "
            "ORDER BY started_at DESC LIMIT 1"
        )
        if active:
            _current_session_id = active[0]["session_id"]
            logger.info(f"[SESSION] Recovered active session {_current_session_id[:8]}")
        else:
            await _create_session()
    except Exception as e:
        logger.warning(f"[SESSION] Could not recover session (table may not exist yet): {e}")
        # Session management will activate once setup notebooks are run


# ---------------------------------------------------------------------------
# Mini-Tick Simulation Engine (lightweight physics between Databricks job runs)
# ---------------------------------------------------------------------------

_trim_counter = 0
_autopilot_counter = 0

# Autopilot tuning
AUTOPILOT_CHECK_INTERVAL = 30  # check every 30 ticks (~60 real seconds) — give burns time to settle
COURSE_CORRECTION_THRESHOLD = 0.25  # radians (~14 degrees off-course before correcting)
BURN_AUTHORITY_KM_S = 0.01  # max acceleration per correction burn (km/s²) — gentle nudges
MIN_CORRECTION_BURN_S = 5.0
MAX_CORRECTION_BURN_S = 60.0  # short burns — many small corrections beat few large ones

# Cooldown: minimum sim-seconds between burns to prevent oscillation
AUTOPILOT_BURN_COOLDOWN_S = 86400.0  # 1 sim-day minimum between burns

VELOCITY_MATCH_THRESHOLD_KM = 5_000_000.0  # 5M km — start blending toward velocity match
APPROACH_BRAKE_THRESHOLD_KM = 500_000.0    # 500K km — braking for orbit insertion


_prev_dist_to_earth: float = 0.0  # track closing rate across autopilot calls
_prev_eta_s: float = 0.0
_last_burn_sim_time: float = 0.0  # cooldown: sim-time of last burn
_last_known_pos: tuple[float, float, float] | None = None  # teleport guard
_last_written_elapsed: float = 0.0  # monotonic guard: reject stale clock reads


async def _check_autopilot_course_correction(
    px: float, py: float, pz: float,
    vx: float, vy: float, vz: float,
    elapsed_s: float, fuel: float,
    sim_time, comm_delay_s: float,
):
    """
    Conservative intercept-based autopilot for return to Earth.

    Philosophy: trust the transfer orbit. Only correct when the ship is
    genuinely off-course (large angle error to intercept point). Small
    corrections, long cooldowns, preserve fuel for orbit insertion.

    Phases:
      Phase 1 (far):  Gentle nudges toward Earth's future position
      Phase 2 (mid):  Blend toward matching Earth's orbital velocity
      Phase 3 (close): Brake to match Earth's speed for orbit insertion
    """
    global _prev_dist_to_earth, _prev_eta_s, _last_burn_sim_time

    if fuel < 20:
        return  # absolute minimum reserve

    # --- Cooldown check: don't burn too frequently ---
    # At close range (<5M km), allow faster corrections
    cur_ex, cur_ey = earth_position_at(elapsed_s)
    dist_to_earth = math.sqrt((px - cur_ex) ** 2 + (py - cur_ey) ** 2 + pz ** 2)

    if dist_to_earth > VELOCITY_MATCH_THRESHOLD_KM:
        cooldown = AUTOPILOT_BURN_COOLDOWN_S  # 1 sim-day when far
    elif dist_to_earth > APPROACH_BRAKE_THRESHOLD_KM:
        cooldown = AUTOPILOT_BURN_COOLDOWN_S * 0.25  # 6 sim-hours when mid-range
    else:
        cooldown = 3600.0  # 1 sim-hour when close

    if elapsed_s - _last_burn_sim_time < cooldown:
        return

    # Compute intercept point and predicted miss distance
    int_ex, int_ey, earth_vx, earth_vy, eta_s, predicted_miss_km = estimate_intercept(
        px, py, vx, vy, elapsed_s
    )

    v_mag = math.sqrt(vx ** 2 + vy ** 2 + vz ** 2) or 1.0

    _prev_dist_to_earth = dist_to_earth
    _prev_eta_s = eta_s

    # --- Key insight: if the natural (unpowered) orbit already passes close ---
    # --- to Earth, DON'T burn during intercept phase. Trust the orbit.     ---
    # The transfer orbit was designed to naturally intercept Earth. Burning
    # during intercept phase only perturbs the orbit away from the natural
    # trajectory. Only activate corrections when:
    #   - The predicted miss is large (natural orbit won't reach Earth)
    #   - We're in velocity_blend or orbit_insertion (close to Earth)
    INTERCEPT_MISS_THRESHOLD_KM = 5_000_000.0  # 5M km (~0.033 AU)

    # --- Choose navigation phase ---
    if dist_to_earth < APPROACH_BRAKE_THRESHOLD_KM:
        # Phase 3: Match Earth's velocity + closing component for orbit capture
        closing_fraction = min(1.0, dist_to_earth / APPROACH_BRAKE_THRESHOLD_KM)
        closing_speed = closing_fraction * 5.0  # up to 5 km/s closing
        to_earth_x = cur_ex - px
        to_earth_y = cur_ey - py
        to_earth_mag = math.sqrt(to_earth_x**2 + to_earth_y**2) or 1.0
        target_vx = earth_vx + (to_earth_x / to_earth_mag) * closing_speed
        target_vy = earth_vy + (to_earth_y / to_earth_mag) * closing_speed
        target_vz = 0.0
        phase = "orbit_insertion"
        angle_threshold = 0.05  # ~3 degrees
    elif dist_to_earth < VELOCITY_MATCH_THRESHOLD_KM:
        # Phase 2: Blend intercept + velocity matching
        blend = 1.0 - (dist_to_earth - APPROACH_BRAKE_THRESHOLD_KM) / (
            VELOCITY_MATCH_THRESHOLD_KM - APPROACH_BRAKE_THRESHOLD_KM
        )
        blend = max(0.0, min(1.0, blend))

        to_int_x = int_ex - px
        to_int_y = int_ey - py
        to_int_mag = math.sqrt(to_int_x ** 2 + to_int_y ** 2) or 1.0
        intercept_vx = (to_int_x / to_int_mag) * v_mag
        intercept_vy = (to_int_y / to_int_mag) * v_mag

        target_vx = intercept_vx * (1 - blend) + earth_vx * blend
        target_vy = intercept_vy * (1 - blend) + earth_vy * blend
        target_vz = 0.0
        phase = "velocity_blend"
        angle_threshold = 0.12  # ~7 degrees
    else:
        # Phase 1: Intercept targeting — gentle nudges only
        to_int_x = int_ex - px
        to_int_y = int_ey - py
        to_int_z = 0.0 - pz
        to_int_mag = math.sqrt(to_int_x ** 2 + to_int_y ** 2 + to_int_z ** 2) or 1.0

        target_vx = (to_int_x / to_int_mag) * v_mag
        target_vy = (to_int_y / to_int_mag) * v_mag
        target_vz = (to_int_z / to_int_mag) * v_mag
        phase = "intercept"
        angle_threshold = COURSE_CORRECTION_THRESHOLD  # ~14 degrees

    # Delta-v needed
    dvx = target_vx - vx
    dvy = target_vy - vy
    dvz = (target_vz if phase != "intercept" else 0.0) - vz
    dv_mag = math.sqrt(dvx ** 2 + dvy ** 2 + dvz ** 2)

    # Check if correction is needed (angle between current and target velocity)
    target_mag = math.sqrt(target_vx ** 2 + target_vy ** 2 + (target_vz or 0) ** 2) or 1.0
    dot = (vx * target_vx + vy * target_vy + vz * (target_vz or 0)) / (v_mag * target_mag)
    dot = max(-1.0, min(1.0, dot))
    angle = math.acos(dot)

    if angle < angle_threshold and dv_mag < 0.3:
        return  # On course — trust the orbit

    # In intercept phase: skip burn if the natural trajectory already
    # passes close enough to Earth. Burning would only perturb the orbit.
    if phase == "intercept" and predicted_miss_km < INTERCEPT_MISS_THRESHOLD_KM:
        return  # Natural orbit is on target — no correction needed

    # Check for existing pending burns
    pending = await execute_lakebase(
        "SELECT COUNT(*) as cnt FROM command_queue "
        "WHERE status IN ('approved', 'in_flight') AND command_type = 'burn'"
    )
    if pending and int(pending[0].get("cnt", 0)) > 0:
        return

    if dv_mag < 0.005:
        return

    # --- Conservative burn sizing ---
    # Only apply a FRACTION of the needed correction per burn.
    # This prevents oscillation: each burn nudges slightly, then we wait
    # for the cooldown to see the effect before correcting again.

    # Fraction of dv to apply: smaller when far (orbit does the work),
    # larger when close (need precision)
    if phase == "orbit_insertion":
        dv_fraction = 0.4   # apply 40% of needed correction
        phase_authority = 0.02  # km/s²
    elif phase == "velocity_blend":
        dv_fraction = 0.25  # apply 25%
        phase_authority = 0.01
    else:  # intercept
        dv_fraction = 0.10  # apply only 10% — tiny nudges, let gravity do the rest
        phase_authority = BURN_AUTHORITY_KM_S  # 0.01 km/s²

    target_dv = dv_mag * dv_fraction

    # Cap to what authority allows in the max burn window
    max_authority_dv = phase_authority * MAX_CORRECTION_BURN_S
    effective_dv = min(target_dv, max_authority_dv)

    # Cap to what fuel can afford — always reserve fuel for later phases
    # Budget: divide remaining fuel across estimated remaining corrections
    estimated_remaining_burns = max(3, eta_s / max(cooldown, 3600))
    per_burn_fuel_budget = fuel * 0.7 / estimated_remaining_burns  # 70% usable, rest reserved
    max_affordable_dv = EXHAUST_VEL * math.log(1 + per_burn_fuel_budget / DRY_MASS) if per_burn_fuel_budget > 0 else 0
    effective_dv = min(effective_dv, max_affordable_dv)

    if effective_dv < 0.001:
        return

    burn_duration = max(MIN_CORRECTION_BURN_S, effective_dv / phase_authority)
    burn_duration = min(burn_duration, MAX_CORRECTION_BURN_S)
    accel = effective_dv / burn_duration  # actual acceleration for this burn

    bvx = (dvx / dv_mag) * accel
    bvy = (dvy / dv_mag) * accel
    bvz = (dvz / dv_mag) * accel

    import json as _json
    import uuid as _uuid
    cmd_id = str(_uuid.uuid4())

    # Closing rate toward Earth
    to_e_x = cur_ex - px
    to_e_y = cur_ey - py
    to_e_mag = math.sqrt(to_e_x ** 2 + to_e_y ** 2) or 1.0
    closing_rate = (vx * to_e_x + vy * to_e_y) / to_e_mag

    payload = _json.dumps({
        "burn_vector_x": round(bvx, 8),
        "burn_vector_y": round(bvy, 8),
        "burn_vector_z": round(bvz, 8),
        "burn_duration_s": round(burn_duration, 2),
        "phase": phase,
        "eta_days": round(eta_s / 86400, 1),
        "closing_rate_kms": round(closing_rate, 2),
    })

    from datetime import timedelta as _td
    est_receive = sim_time + _td(seconds=comm_delay_s) if sim_time else None

    await execute_lakebase(
        "INSERT INTO command_queue (command_id, command_type, payload, priority, "
        "  created_at, approved_by, approved_at, transmit_time, estimated_receive_time, "
        "  status, session_id, updated_at) "
        "VALUES (:cid, 'burn', :payload, 3, NOW(), 'autopilot', NOW(), NOW(), "
        "  :est_receive, 'in_flight', :sid, NOW())",
        {
            "cid": cmd_id,
            "payload": payload,
            "est_receive": est_receive,
            "sid": _current_session_id,
        },
    )

    # Record cooldown
    _last_burn_sim_time = elapsed_s

    logger.info(
        f"[AUTOPILOT] {phase} correction: angle={math.degrees(angle):.1f}deg, "
        f"eff_dv={effective_dv:.4f}km/s ({dv_fraction*100:.0f}% of {dv_mag:.2f}), burn={burn_duration:.0f}s, "
        f"fuel={fuel:.0f}kg, ETA={eta_s / 86400:.1f}d, dist={dist_to_earth / 1.496e8:.3f}AU, cmd={cmd_id[:8]}"
    )
    _record_event(
        "autopilot_correction",
        f"Autopilot {phase}: Δv={effective_dv:.4f} km/s, ETA={eta_s / 86400:.1f}d",
        simulation_time_s=elapsed_s,
        metadata={"phase": phase, "delta_v": round(effective_dv, 4), "eta_days": round(eta_s / 86400, 1)},
    )


# ---------------------------------------------------------------------------
# Onboard Trajectory Prediction (ML model or physics fallback)
# ---------------------------------------------------------------------------

def _physics_predict(
    px: float, py: float, pz: float,
    vx: float, vy: float, vz: float,
    horizon_s: float,
) -> tuple[float, float, float]:
    """Simple Euler+gravity forward propagation as fallback when Model Serving is unavailable."""
    steps = max(1, int(horizon_s / 2.0))
    step_dt = horizon_s / steps
    x, y, z = px, py, pz
    _vx, _vy, _vz = vx, vy, vz
    for _ in range(steps):
        r = math.sqrt(x * x + y * y + z * z) or 1.0
        a_mag = -GM_SUN / (r * r)
        _vx += a_mag * x / r * step_dt
        _vy += a_mag * y / r * step_dt
        _vz += a_mag * z / r * step_dt
        x += _vx * step_dt
        y += _vy * step_dt
        z += _vz * step_dt
    return x, y, z


async def _call_model_serving(
    px: float, py: float, pz: float,
    vx: float, vy: float, vz: float,
    fuel: float, comm_delay: float,
    sim_time_s: float = 0.0,
) -> tuple[float, float, float, float]:
    """
    Call the Databricks Model Serving endpoint for trajectory prediction.
    Returns (predicted_x, predicted_y, predicted_z, latency_ms).
    Raises on failure so caller can fall back to physics.
    """
    record = {
        "pos_x": px, "pos_y": py, "pos_z": pz,
        "vel_x": vx, "vel_y": vy, "vel_z": vz,
        "fuel": fuel, "comm_delay": comm_delay,
    }

    t0 = time.monotonic()
    result = await _call_ml_endpoint(MODEL_SERVING_ENDPOINT, record, caller="onboard_prediction")
    latency_ms = (time.monotonic() - t0) * 1000

    if result is None:
        raise ValueError("No response from trajectory model")
    if isinstance(result, dict):
        out = (
            float(result.get("future_pos_x", px)),
            float(result.get("future_pos_y", py)),
            float(result.get("future_pos_z", pz)),
            latency_ms,
        )
    elif isinstance(result, (list, tuple)) and len(result) >= 3:
        out = (float(result[0]), float(result[1]), float(result[2]), latency_ms)
    else:
        raise ValueError(f"Unexpected model response format: {result}")

    _log_inference(
        MODEL_SERVING_ENDPOINT, "onboard_prediction", record,
        {"pred_x": out[0], "pred_y": out[1], "pred_z": out[2]},
        latency_ms=latency_ms, simulation_time_s=sim_time_s,
    )
    return out


# Cache for route-optimized endpoint URLs (endpoint_name -> full URL)
_route_url_cache: dict[str, str] = {}


def _get_serving_headers() -> dict[str, str]:
    """Get auth headers for serving endpoint calls using the workspace client."""
    w = get_workspace_client()
    headers = {"Content-Type": "application/json"}
    # Use the SDK's credential provider (handles OAuth service principal tokens)
    try:
        import types
        auth_provider = w.config.authenticate
        if callable(auth_provider):
            # In newer SDK versions, authenticate() returns headers dict
            result = auth_provider()
            if isinstance(result, dict):
                headers.update(result)
                return headers
    except Exception:
        pass
    # Fallback: try token directly
    try:
        token = w.config.token
        if token:
            headers["Authorization"] = f"Bearer {token}"
    except Exception:
        pass
    return headers


async def _call_ml_endpoint(endpoint_name: str, record: dict, *, caller: str = "system") -> dict | list | float | None:
    """Call any ML serving endpoint. Tries SDK first, falls back to REST
    with route-optimized URL discovery for endpoints that require it.
    Logs every call to the inference log buffer for Delta CDC."""
    import re
    import requests as _requests

    _call_t0 = time.monotonic()

    # --- Attempt 1: SDK (handles non-route-optimized endpoints) ---
    try:
        w = get_workspace_client()
        response = await asyncio.to_thread(
            w.serving_endpoints.query,
            name=endpoint_name,
            dataframe_records=[record],
        )
        predictions = response.predictions
        if predictions is None:
            raw = response.as_dict() if hasattr(response, "as_dict") else {}
            predictions = raw.get("predictions", raw.get("outputs"))

        if isinstance(predictions, (int, float)):
            return predictions
        if isinstance(predictions, list) and len(predictions) > 0:
            return predictions[0]
        if isinstance(predictions, dict):
            return predictions
        return predictions
    except Exception as sdk_err:
        err_str = str(sdk_err)
        if "route-optim" not in err_str.lower():
            logger.info(f"[ML] {endpoint_name} SDK call failed: {sdk_err}")
            return None

    # --- Attempt 2: REST to route-optimized URL ---
    try:
        match = re.search(r'(\S+\.serving\.azuredatabricks\.net\S*/invocations)', err_str)
        if not match:
            logger.info(f"[ML] {endpoint_name} route-optimized but no URL found in error")
            return None

        direct_url = f"https://{match.group(1)}"

        if endpoint_name not in _route_url_cache:
            _route_url_cache[endpoint_name] = direct_url
            logger.info(f"[ML] {endpoint_name} using route-optimized URL: {direct_url[:60]}...")

        # Get auth token — try multiple methods
        w = get_workspace_client()
        headers = {"Content-Type": "application/json"}

        # Method 1: SDK authenticate() (returns headers dict for OAuth)
        try:
            auth_headers = w.config.authenticate()
            if isinstance(auth_headers, dict):
                headers.update(auth_headers)
                auth_method = "sdk_authenticate"
            else:
                raise ValueError("authenticate() did not return dict")
        except Exception:
            # Method 2: Direct token
            try:
                token = w.config.token
                if token:
                    headers["Authorization"] = f"Bearer {token}"
                    auth_method = "config_token"
                else:
                    raise ValueError("No token available")
            except Exception:
                # Method 3: Environment variable
                token = os.environ.get("DATABRICKS_TOKEN", "")
                if token:
                    headers["Authorization"] = f"Bearer {token}"
                    auth_method = "env_token"
                else:
                    logger.warning(f"[ML] {endpoint_name} no auth token available")
                    return None

        # Log auth method (once per endpoint)
        if endpoint_name not in _route_url_cache or not _route_url_cache.get(f"_{endpoint_name}_logged"):
            auth_preview = headers.get("Authorization", "")[:30] + "..." if "Authorization" in headers else "none"
            logger.info(f"[ML] {endpoint_name} auth method={auth_method}, token_preview={auth_preview}")
            _route_url_cache[f"_{endpoint_name}_logged"] = True

        payload = {"dataframe_records": [record]}
        resp = await asyncio.to_thread(
            lambda: _requests.post(direct_url, json=payload, headers=headers, timeout=30)
        )

        if resp.status_code == 401:
            logger.warning(
                f"[ML] {endpoint_name} route-optimized 401 (auth={auth_method}). "
                f"Service principal may need CAN_QUERY on this endpoint."
            )
            return None

        resp.raise_for_status()
        result = resp.json()
        predictions = result.get("predictions", result.get("outputs"))

        if isinstance(predictions, (int, float)):
            return predictions
        if isinstance(predictions, list) and len(predictions) > 0:
            return predictions[0]
        if isinstance(predictions, dict):
            return predictions
        return predictions
    except Exception as e:
        logger.info(f"[ML] {endpoint_name} route-optimized call failed: {e}")
        return None


async def _call_fuel_estimation(burn_vx: float, burn_vy: float, burn_vz: float,
                                 burn_duration_s: float, delta_v: float) -> float | None:
    """Predict fuel cost (kg) for a proposed maneuver."""
    burn_mag = math.sqrt(burn_vx**2 + burn_vy**2 + burn_vz**2)
    record = {
        "burn_vector_x": burn_vx, "burn_vector_y": burn_vy, "burn_vector_z": burn_vz,
        "burn_duration_s": burn_duration_s, "delta_v": delta_v, "burn_magnitude": burn_mag,
    }
    t0 = time.monotonic()
    result = await _call_ml_endpoint(FUEL_MODEL_ENDPOINT, record, caller="autopilot")
    latency_ms = (time.monotonic() - t0) * 1000
    out = None
    if isinstance(result, (int, float)):
        out = float(result)
    elif isinstance(result, dict):
        out = float(next(iter(result.values())))
    elif isinstance(result, list) and len(result) > 0:
        out = float(result[0])
    if out is not None:
        _log_inference(FUEL_MODEL_ENDPOINT, "autopilot", record, {"fuel_cost_kg": out}, latency_ms=latency_ms)
    return out


async def _call_hazard_risk(h_px: float, h_py: float, h_pz: float,
                             h_vx: float, h_vy: float, h_vz: float,
                             radius_km: float, closest_approach_km: float) -> float | None:
    """Predict risk score (0-1) for a hazard."""
    record = {
        "position_x": h_px, "position_y": h_py, "position_z": h_pz,
        "velocity_x": h_vx, "velocity_y": h_vy, "velocity_z": h_vz,
        "radius_km": radius_km, "closest_approach_km": closest_approach_km,
    }
    t0 = time.monotonic()
    result = await _call_ml_endpoint(HAZARD_RISK_MODEL_ENDPOINT, record, caller="hazard_assessment")
    latency_ms = (time.monotonic() - t0) * 1000
    out = None
    if isinstance(result, (int, float)):
        out = float(result)
    elif isinstance(result, dict):
        out = float(next(iter(result.values())))
    elif isinstance(result, list) and len(result) > 0:
        out = float(result[0])
    if out is not None:
        _log_inference(HAZARD_RISK_MODEL_ENDPOINT, "hazard_assessment", record, {"risk_score": out}, latency_ms=latency_ms)
    return out


async def _call_maneuver_ranking(burn_vx: float, burn_vy: float, burn_vz: float,
                                  burn_duration_s: float, delta_v: float,
                                  fuel_cost_kg: float, risk_reduction: float) -> float | None:
    """Predict composite quality score for a candidate maneuver."""
    record = {
        "burn_vector_x": burn_vx, "burn_vector_y": burn_vy, "burn_vector_z": burn_vz,
        "burn_duration_s": burn_duration_s, "delta_v": delta_v,
        "fuel_cost_kg": fuel_cost_kg, "risk_reduction_score": risk_reduction,
    }
    t0 = time.monotonic()
    result = await _call_ml_endpoint(MANEUVER_RANK_MODEL_ENDPOINT, record, caller="autopilot")
    latency_ms = (time.monotonic() - t0) * 1000
    out = None
    if isinstance(result, (int, float)):
        out = float(result)
    elif isinstance(result, dict):
        out = float(next(iter(result.values())))
    elif isinstance(result, list) and len(result) > 0:
        out = float(result[0])
    if out is not None:
        _log_inference(MANEUVER_RANK_MODEL_ENDPOINT, "autopilot", record, {"quality_score": out}, latency_ms=latency_ms)
    return out


async def _call_delay_policy(speed: float, fuel_remaining: float,
                              distance_to_earth: float, comm_delay_s: float,
                              hazard_risk: float, maneuver_urgency: float) -> float | None:
    """Predict optimal command timing offset (seconds) given comm delay."""
    record = {
        "speed": speed, "fuel_remaining": fuel_remaining,
        "distance_to_earth": distance_to_earth, "comm_delay_s": comm_delay_s,
        "hazard_risk": hazard_risk, "maneuver_urgency": maneuver_urgency,
    }
    t0 = time.monotonic()
    result = await _call_ml_endpoint(DELAY_POLICY_MODEL_ENDPOINT, record, caller="communications")
    latency_ms = (time.monotonic() - t0) * 1000
    out = None
    if isinstance(result, (int, float)):
        out = float(result)
    elif isinstance(result, dict):
        out = float(next(iter(result.values())))
    elif isinstance(result, list) and len(result) > 0:
        out = float(result[0])
    if out is not None:
        _log_inference(DELAY_POLICY_MODEL_ENDPOINT, "communications", record, {"timing_offset_s": out}, latency_ms=latency_ms)
    return out


async def _warmup_models() -> dict:
    """Ping all ML + LLM endpoints in parallel to wake them from scale-to-zero.
    Returns a dict of endpoint -> status."""
    import time as _time

    endpoints = {
        "trajectory_prediction": (MODEL_SERVING_ENDPOINT, {
            "pos_x": 0, "pos_y": 0, "pos_z": 0,
            "vel_x": 0, "vel_y": 0, "vel_z": 0,
            "fuel": 100, "comm_delay": 300,
        }),
        "fuel_estimation": (FUEL_MODEL_ENDPOINT, {
            "burn_vector_x": 0, "burn_vector_y": 0, "burn_vector_z": 0,
            "burn_duration_s": 30, "delta_v": 0.01, "burn_magnitude": 0.01,
        }),
        "hazard_risk": (HAZARD_RISK_MODEL_ENDPOINT, {
            "position_x": 0, "position_y": 0, "position_z": 0,
            "velocity_x": 0, "velocity_y": 0, "velocity_z": 0,
            "radius_km": 5, "closest_approach_km": 1000,
        }),
        "maneuver_ranking": (MANEUVER_RANK_MODEL_ENDPOINT, {
            "burn_vector_x": 0, "burn_vector_y": 0, "burn_vector_z": 0,
            "burn_duration_s": 30, "delta_v": 0.01,
            "fuel_cost_kg": 1, "risk_reduction_score": 0.5,
        }),
        "delay_aware_policy": (DELAY_POLICY_MODEL_ENDPOINT, {
            "speed": 20, "fuel_remaining": 200,
            "distance_to_earth": 2e8, "comm_delay_s": 600,
            "hazard_risk": 0.1, "maneuver_urgency": 0.3,
        }),
    }

    async def _ping(name: str, endpoint: str, payload: dict) -> tuple[str, str, float]:
        """Ping an ML endpoint — result must be non-None to count as ready."""
        t0 = _time.monotonic()
        try:
            result = await _call_ml_endpoint(endpoint, payload)
            elapsed = _time.monotonic() - t0
            if result is not None:
                return (name, "ready", elapsed)
            return (name, "reachable_no_prediction", elapsed)
        except Exception as e:
            elapsed = _time.monotonic() - t0
            return (name, f"error: {e}", elapsed)

    async def _ping_llm() -> tuple[str, str, float]:
        """Ping the LLM endpoint using the same auth as _call_foundation_model_sync."""
        import requests as _requests
        t0 = _time.monotonic()
        try:
            headers = _get_serving_headers()
            w = get_workspace_client()
            host = w.config.host.rstrip("/")
            endpoint = "databricks-meta-llama-3-3-70b-instruct"
            url = f"{host}/serving-endpoints/{endpoint}/invocations"
            resp = await asyncio.to_thread(
                lambda: _requests.post(
                    url,
                    headers=headers,
                    json={"messages": [{"role": "user", "content": "Reply OK"}], "max_tokens": 5},
                    timeout=30,
                )
            )
            elapsed = _time.monotonic() - t0
            if resp.status_code == 200:
                return ("llm_llama_70b", "ready", elapsed)
            logger.info(f"[WARMUP] LLM status={resp.status_code} body={resp.text[:200]}")
            return ("llm_llama_70b", f"http_{resp.status_code}", elapsed)
        except Exception as e:
            elapsed = _time.monotonic() - t0
            return ("llm_llama_70b", f"error: {e}", elapsed)

    tasks = [_ping(name, ep, payload) for name, (ep, payload) in endpoints.items()]
    tasks.append(_ping_llm())

    results = await asyncio.gather(*tasks)
    status = {}
    for name, state, elapsed in results:
        status[name] = {"status": state, "latency_s": round(elapsed, 2)}
        logger.info(f"[WARMUP] {name}: {state} ({elapsed:.1f}s)")

    return status


async def _onboard_trajectory_prediction(
    px: float, py: float, pz: float,
    vx: float, vy: float, vz: float,
    elapsed_s: float, fuel: float, comm_delay: float,
) -> None:
    """
    Onboard ML prediction: forecast position PREDICTION_HORIZON_S ahead,
    assess deviation from desired trajectory, apply micro-correction if needed,
    and log the prediction for agent consumption and model retraining.
    """
    import json as _json
    import uuid as _uuid

    global _model_preference, _drift_retrain_triggered
    prediction_id = str(_uuid.uuid4())
    source = "physics_fallback"
    latency_ms = 0.0

    # Adaptive model selection: use ML if it's performing well, physics if drifting
    use_ml = _model_preference == "model_serving"

    if use_ml:
        try:
            pred_x, pred_y, pred_z, latency_ms = await _call_model_serving(
                px, py, pz, vx, vy, vz, fuel, comm_delay, sim_time_s=elapsed_s,
            )
            source = "model_serving"
        except Exception as e:
            logger.debug(f"[PREDICTION] Model Serving unavailable ({e}), using physics fallback")
            pred_x, pred_y, pred_z = _physics_predict(px, py, pz, vx, vy, vz, PREDICTION_HORIZON_S)
    else:
        pred_x, pred_y, pred_z = _physics_predict(px, py, pz, vx, vy, vz, PREDICTION_HORIZON_S)
        logger.debug("[PREDICTION] Using physics fallback (ML drift detected)")

    # Assess: where should we be heading? (toward Earth)
    future_elapsed = elapsed_s + PREDICTION_HORIZON_S
    ex, ey = earth_position_at(future_elapsed)

    # Distance from predicted position to Earth at that future time
    pred_to_earth = math.sqrt((pred_x - ex) ** 2 + (pred_y - ey) ** 2 + pred_z ** 2)

    # Distance from current position to Earth now
    ex_now, ey_now = earth_position_at(elapsed_s)
    current_to_earth = math.sqrt((px - ex_now) ** 2 + (py - ey_now) ** 2 + pz ** 2)

    # Are we getting closer or farther?
    closing = pred_to_earth < current_to_earth

    # Desired future position: straight-line toward Earth from predicted pos
    # Check deviation from ideal trajectory
    ideal_x, ideal_y, ideal_z = _physics_predict(px, py, pz, vx, vy, vz, PREDICTION_HORIZON_S)
    deviation_km = math.sqrt(
        (pred_x - ideal_x) ** 2 + (pred_y - ideal_y) ** 2 + (pred_z - ideal_z) ** 2
    )

    # Assessment
    if closing and deviation_km < PREDICTION_CORRECTION_THRESHOLD_KM:
        assessment = "on_course"
        action = "none"
        correction_dv = 0.0
    elif deviation_km < PREDICTION_CORRECTION_THRESHOLD_KM * 3:
        assessment = "minor_deviation"
        action = "none"
        correction_dv = 0.0
    else:
        assessment = "correction_needed"
        action = "micro_correction"
        # Apply a small velocity nudge toward Earth
        to_earth_x = ex - px
        to_earth_y = ey - py
        to_earth_mag = math.sqrt(to_earth_x ** 2 + to_earth_y ** 2) or 1.0
        correction_dv = PREDICTION_MICRO_DV * PREDICTION_HORIZON_S * 0.1

    # Write to Lakebase onboard_predictions table
    await execute_lakebase(
        "INSERT INTO onboard_predictions ("
        "  prediction_id, simulation_time_s, "
        "  current_pos_x, current_pos_y, current_pos_z, "
        "  current_vel_x, current_vel_y, current_vel_z, "
        "  predicted_pos_x, predicted_pos_y, predicted_pos_z, "
        "  prediction_horizon_s, prediction_source, "
        "  assessment, action_taken, correction_dv, "
        "  session_id, created_at"
        ") VALUES ("
        "  :pid, :sim_t, :px, :py, :pz, :vx, :vy, :vz, "
        "  :pred_x, :pred_y, :pred_z, :horizon, :source, "
        "  :assess, :action, :cdv, :sid, NOW()"
        ")",
        {
            "pid": prediction_id, "sim_t": elapsed_s,
            "px": px, "py": py, "pz": pz,
            "vx": vx, "vy": vy, "vz": vz,
            "pred_x": pred_x, "pred_y": pred_y, "pred_z": pred_z,
            "horizon": PREDICTION_HORIZON_S, "source": source,
            "assess": assessment, "action": action, "cdv": correction_dv,
            "sid": _current_session_id,
        },
    )

    # Buffer for Delta Lake flush
    _prediction_buffer.append({
        "inference_id": prediction_id,
        "session_id": _current_session_id,
        "simulation_time_s": elapsed_s,
        "prediction_horizon_s": PREDICTION_HORIZON_S,
        "prediction_source": source,
        "input_pos_x": px, "input_pos_y": py, "input_pos_z": pz,
        "input_vel_x": vx, "input_vel_y": vy, "input_vel_z": vz,
        "input_fuel": fuel, "input_comm_delay": comm_delay,
        "predicted_pos_x": pred_x, "predicted_pos_y": pred_y, "predicted_pos_z": pred_z,
        "assessment": assessment, "action_taken": action,
        "correction_dv": correction_dv,
        "inference_latency_ms": latency_ms,
    })
    if len(_prediction_buffer) > 5000:
        _prediction_buffer.pop(0)

    # Update session prediction count
    if _current_session_id:
        await execute_lakebase(
            "UPDATE simulation_sessions SET "
            "  total_predictions_made = COALESCE(total_predictions_made, 0) + 1, "
            "  updated_at = NOW() "
            "WHERE session_id = :sid",
            {"sid": _current_session_id},
        )

    logger.info(
        f"[PREDICTION] {source}: assess={assessment}, "
        f"pred_to_earth={pred_to_earth/1e6:.2f}Mkm, "
        f"deviation={deviation_km:.0f}km, action={action}"
    )


async def _backfill_predictions(elapsed_s: float, px: float, py: float, pz: float):
    """
    Backfill actual position on predictions whose horizon has elapsed.
    This creates the training signal for model retraining.
    Also updates adaptive model selection based on per-source error tracking.
    """
    global _model_preference, _drift_retrain_triggered

    # Find predictions where simulation_time_s + horizon <= current elapsed
    rows = await execute_lakebase(
        "SELECT prediction_id, predicted_pos_x, predicted_pos_y, predicted_pos_z, "
        "  prediction_source "
        "FROM onboard_predictions "
        "WHERE actual_pos_x IS NULL "
        "  AND simulation_time_s + prediction_horizon_s <= :now "
        "  AND session_id = :sid "
        "LIMIT 10",
        {"now": elapsed_s, "sid": _current_session_id},
    )
    for row in rows:
        pred_x = float(row["predicted_pos_x"])
        pred_y = float(row["predicted_pos_y"])
        pred_z = float(row["predicted_pos_z"])
        error_km = math.sqrt(
            (px - pred_x) ** 2 + (py - pred_y) ** 2 + (pz - pred_z) ** 2
        )
        await execute_lakebase(
            "UPDATE onboard_predictions SET "
            "  actual_pos_x = :ax, actual_pos_y = :ay, actual_pos_z = :az, "
            "  prediction_error_km = :err, backfilled_at = NOW() "
            "WHERE prediction_id = :pid",
            {
                "ax": px, "ay": py, "az": pz,
                "err": error_km, "pid": row["prediction_id"],
            },
        )

        # Track errors by source for adaptive model selection
        src = row.get("prediction_source", "physics_fallback")
        if src == "model_serving":
            _ml_recent_errors.append(error_km)
            if len(_ml_recent_errors) > ML_ERROR_WINDOW:
                _ml_recent_errors.pop(0)
        else:
            _phys_recent_errors.append(error_km)
            if len(_phys_recent_errors) > ML_ERROR_WINDOW:
                _phys_recent_errors.pop(0)

        logger.debug(f"[PREDICTION] Backfilled {row['prediction_id'][:8]}: error={error_km:.0f}km ({src})")

    # Adaptive model selection: compare ML vs physics error rates
    if len(_ml_recent_errors) >= 3 and len(_phys_recent_errors) >= 3:
        ml_avg = sum(_ml_recent_errors) / len(_ml_recent_errors)
        phys_avg = sum(_phys_recent_errors) / len(_phys_recent_errors)

        if ml_avg > phys_avg * ML_DRIFT_THRESHOLD and _model_preference == "model_serving":
            _model_preference = "physics_fallback"
            logger.warning(
                f"[DRIFT] ML model drifting — avg error ML={ml_avg:.0f}km vs physics={phys_avg:.0f}km. "
                f"Switching to physics fallback."
            )
            # Auto-trigger retrain if not already triggered this session
            if not _drift_retrain_triggered:
                _drift_retrain_triggered = True
                try:
                    await _auto_retrain()
                    logger.info("[DRIFT] Auto-retrain triggered due to model drift")
                    _record_event("model_drift_retrain", "Auto-retrain triggered: ML model drifting")
                except Exception:
                    pass
        elif ml_avg <= phys_avg * 1.2 and _model_preference == "physics_fallback":
            # ML recovered (or was retrained) — switch back
            _model_preference = "model_serving"
            logger.info(
                f"[DRIFT] ML model recovered — avg error ML={ml_avg:.0f}km vs physics={phys_avg:.0f}km. "
                f"Switching back to model_serving."
            )


# ---------------------------------------------------------------------------
# Agent Memory — persistent cross-tick state for smarter reasoning
# ---------------------------------------------------------------------------

class AgentMemoryStore:
    """In-memory agent memory with Lakebase persistence.

    Each agent tracks observations across ticks:
    - flight_dynamics: correction effectiveness, fuel burn rate, phase history
    - hazard_assessment: hazard patterns, evasion outcomes
    - communications: command latency trends, delay patterns
    - mission_commander: decision success rate, override patterns
    """

    def __init__(self):
        self._mem: dict[str, dict[str, any]] = {
            "flight_dynamics": {},
            "hazard_assessment": {},
            "communications": {},
            "mission_commander": {},
        }

    def get(self, agent_name: str, key: str, default=None):
        return self._mem.get(agent_name, {}).get(key, default)

    def set(self, agent_name: str, key: str, value):
        if agent_name not in self._mem:
            self._mem[agent_name] = {}
        self._mem[agent_name][key] = value

    def get_all(self, agent_name: str) -> dict:
        return dict(self._mem.get(agent_name, {}))

    def get_summary(self) -> dict:
        """Return a compact summary of all agent memories for API/frontend."""
        summary = {}
        for agent, mem in self._mem.items():
            if mem:
                summary[agent] = {k: v for k, v in mem.items()
                                  if not k.startswith("_")}
        return summary

    def reset(self):
        for agent in self._mem:
            self._mem[agent] = {}

    async def flush_to_lakebase(self, session_id: str):
        """Persist current memory to Lakebase agent_memory table."""
        import json as _json
        for agent_name, mem in self._mem.items():
            for key, value in mem.items():
                if key.startswith("_"):
                    continue
                try:
                    val_str = _json.dumps(value, default=str) if not isinstance(value, str) else value
                    await execute_lakebase(
                        "INSERT INTO agent_memory (agent_name, memory_key, memory_value, created_at, updated_at) "
                        "VALUES (:agent, :key, :val, NOW(), NOW()) "
                        "ON CONFLICT (agent_name, memory_key) DO UPDATE SET "
                        "memory_value = :val, updated_at = NOW()",
                        {"agent": agent_name, "key": key, "val": val_str},
                    )
                except Exception:
                    pass  # Non-critical — memory is also in-memory

    async def load_from_lakebase(self, session_id: str):
        """Load persisted memory from Lakebase (e.g., after restart)."""
        import json as _json
        try:
            rows = await execute_lakebase(
                "SELECT agent_name, memory_key, memory_value FROM agent_memory"
            )
            for row in (rows or []):
                agent = row["agent_name"]
                key = row["memory_key"]
                try:
                    val = _json.loads(row["memory_value"])
                except (ValueError, TypeError):
                    val = row["memory_value"]
                self.set(agent, key, val)
        except Exception:
            pass


_agent_memory = AgentMemoryStore()


def _update_agent_memory(
    peer_context: dict,
    state: dict,
    hazards: list,
    elapsed_s: float,
):
    """Update agent memories based on the current tick's observations.

    Called after each agent cycle to record patterns and outcomes.
    """
    fd = peer_context.get("flight_dynamics", {})
    ha = peer_context.get("hazard_assessment", {})
    comms = peer_context.get("communications", {})
    mc = peer_context.get("mission_commander", {})

    # --- Flight Dynamics memory ---
    # Track correction history
    corrections = _agent_memory.get("flight_dynamics", "correction_history", [])
    closing_rate = fd.get("closing_rate_kms", 0)
    heading_err = fd.get("heading_error_deg", 0)
    corrections.append({
        "t": round(elapsed_s),
        "closing": closing_rate,
        "heading_err": round(heading_err, 1),
        "on_course": fd.get("on_course", False),
        "phase": fd.get("phase", "unknown"),
    })
    if len(corrections) > 20:
        corrections = corrections[-20:]
    _agent_memory.set("flight_dynamics", "correction_history", corrections)

    # Compute correction effectiveness trend
    if len(corrections) >= 3:
        recent_on_course = sum(1 for c in corrections[-5:] if c["on_course"])
        total_recent = min(5, len(corrections))
        effectiveness = round(recent_on_course / total_recent, 2)
        _agent_memory.set("flight_dynamics", "correction_effectiveness", effectiveness)

    # Fuel burn rate trend
    fuel = float(state.get("fuel_remaining_kg") or 0)
    fuel_history = _agent_memory.get("flight_dynamics", "fuel_history", [])
    fuel_history.append({"t": round(elapsed_s), "fuel": round(fuel, 1)})
    if len(fuel_history) > 20:
        fuel_history = fuel_history[-20:]
    _agent_memory.set("flight_dynamics", "fuel_history", fuel_history)
    if len(fuel_history) >= 3:
        dt = fuel_history[-1]["t"] - fuel_history[-3]["t"]
        dfuel = fuel_history[-3]["fuel"] - fuel_history[-1]["fuel"]
        if dt > 0:
            rate = round(dfuel / dt * 86400, 1)  # kg/day
            _agent_memory.set("flight_dynamics", "fuel_burn_rate_kg_per_day", rate)

    # --- Hazard Assessment memory ---
    n_hazards = ha.get("hazard_count", 0)
    hazard_log = _agent_memory.get("hazard_assessment", "hazard_encounter_log", [])
    if n_hazards > 0:
        hazard_log.append({
            "t": round(elapsed_s),
            "count": n_hazards,
            "max_risk": ha.get("max_risk", 0),
        })
        if len(hazard_log) > 20:
            hazard_log = hazard_log[-20:]
    _agent_memory.set("hazard_assessment", "hazard_encounter_log", hazard_log)

    # Detect hazard clustering pattern
    if len(hazard_log) >= 3:
        recent_times = [h["t"] for h in hazard_log[-5:]]
        if len(recent_times) >= 2:
            gaps = [recent_times[i+1] - recent_times[i] for i in range(len(recent_times)-1)]
            avg_gap = sum(gaps) / len(gaps)
            pattern = "cluster" if avg_gap < 300 else ("periodic" if avg_gap < 1000 else "sporadic")
            _agent_memory.set("hazard_assessment", "hazard_pattern", pattern)
            _agent_memory.set("hazard_assessment", "total_encounters", len(hazard_log))

    # --- Communications memory ---
    delay = float(state.get("comm_delay_s") or state.get("communication_delay_s") or 0)
    delay_history = _agent_memory.get("communications", "delay_history", [])
    delay_history.append({"t": round(elapsed_s), "delay_s": round(delay, 1)})
    if len(delay_history) > 20:
        delay_history = delay_history[-20:]
    _agent_memory.set("communications", "delay_history", delay_history)

    # Track delay trend
    if len(delay_history) >= 3:
        recent = [d["delay_s"] for d in delay_history[-5:]]
        older = [d["delay_s"] for d in delay_history[:5]]
        avg_recent = sum(recent) / len(recent)
        avg_older = sum(older) / len(older)
        trend = "increasing" if avg_recent > avg_older * 1.1 else (
            "decreasing" if avg_recent < avg_older * 0.9 else "stable"
        )
        _agent_memory.set("communications", "delay_trend", trend)

    # --- Mission Commander memory ---
    decision = mc.get("decision", "HOLD")
    decision_log = _agent_memory.get("mission_commander", "decision_log", [])
    decision_log.append({
        "t": round(elapsed_s),
        "decision": decision,
        "confidence": mc.get("confidence", 0),
    })
    if len(decision_log) > 20:
        decision_log = decision_log[-20:]
    _agent_memory.set("mission_commander", "decision_log", decision_log)

    # Track MC command success patterns (how often GO decisions lead to on-course next tick)
    if len(decision_log) >= 2 and len(corrections) >= 2:
        go_decisions = [d for d in decision_log[:-1] if d["decision"] == "GO"]
        if go_decisions:
            successes = sum(1 for c in corrections[-len(go_decisions):] if c["on_course"])
            success_rate = round(successes / len(go_decisions), 2) if go_decisions else 0
            _agent_memory.set("mission_commander", "command_success_rate", success_rate)

    # Count overrides if captain is active
    captain_overrides = 0
    if _ship_captain:
        captain_overrides = _ship_captain.state.total_overrides
    _agent_memory.set("mission_commander", "captain_overrides", captain_overrides)


# ---------------------------------------------------------------------------
# In-App Agent Decision Chain
# ---------------------------------------------------------------------------

AGENT_SYSTEM_PROMPTS = {
    "flight_dynamics": (
        "You are the Flight Dynamics Officer for a deep-space mission. "
        "Analyze the spacecraft trajectory, velocity, fuel, and onboard prediction accuracy. "
        "You have access to ML model predictions including fuel estimation and maneuver ranking. "
        "Reference ML scores when available. Recommend maneuvers if the ship is off-course. "
        "You also receive your memory from prior observations — use it to identify trends "
        "(correction effectiveness, fuel burn rate, recurring issues). "
        "Be concise: 2-3 sentences."
    ),
    "hazard_assessment": (
        "You are the Hazard Assessment Officer. Review the current threat environment. "
        "You have access to ML hazard risk model scores for each active hazard. "
        "Use ML risk scores when available and note them in your assessment. "
        "Your memory includes hazard encounter history and patterns (cluster, periodic, sporadic). "
        "Reference patterns when recommending defensive posture. "
        "Recommend evasive action if needed. Be concise: 2-3 sentences."
    ),
    "communications": (
        "You are the Communications Officer. Account for the one-way signal delay. "
        "You have access to an ML delay-aware policy model that recommends optimal command "
        "timing offsets. Reference the ML timing recommendation when available. "
        "Your memory tracks delay trends — note if delay is increasing (moving away) or decreasing. "
        "Flag if delay makes ground control commands stale. Be concise: 2-3 sentences."
    ),
    "mission_commander": (
        "You are the Mission Commander. Synthesize inputs from Flight Dynamics, "
        "Hazard Assessment, and Communications. All agents may include ML model predictions. "
        "Issue a decision: GO (continue current trajectory), "
        "HOLD (maintain position, await data), or EMERGENCY_EVASION (immediate burn to avoid threat). "
        "Your memory includes command success rate and captain override history. "
        "If commands are frequently failing or being overridden, adjust strategy. "
        "Note which ML models informed the decision. "
        "State your decision and confidence (0-1). Be concise: 2-3 sentences."
    ),
}



def _rule_based_agent(
    agent_name: str,
    state: dict,
    hazards: list,
    predictions: dict,
    peer_context: dict,
    memory: dict | None = None,
) -> dict:
    """Rule-based fallback when LLM endpoint is unavailable.

    memory: cross-tick observations from AgentMemoryStore for this agent.
    """
    import random

    memory = memory or {}
    px = float(state.get("position_x") or 0)
    py = float(state.get("position_y") or 0)
    pz = float(state.get("position_z") or 0)
    vx = float(state.get("velocity_x") or 0)
    vy = float(state.get("velocity_y") or 0)
    fuel = float(state.get("fuel_remaining_kg") or 0)
    elapsed_s = float(state.get("elapsed_s") or 0)
    _cur_ex, _cur_ey = earth_position_at(elapsed_s)
    dist_to_earth = math.sqrt((px - _cur_ex) ** 2 + (py - _cur_ey) ** 2 + pz ** 2)
    speed = math.sqrt(vx ** 2 + vy ** 2 + vz ** 2) if (vz := float(state.get("velocity_z") or 0)) or True else 0

    if agent_name == "flight_dynamics":
        # Compute intercept point and check heading
        int_ex, int_ey, earth_vx, earth_vy, eta_s, _miss = estimate_intercept(
            px, py, vx, vy, elapsed_s
        )
        # Vector from ship to intercept point
        to_int_x = int_ex - px
        to_int_y = int_ey - py
        to_int_mag = math.sqrt(to_int_x ** 2 + to_int_y ** 2) or 1.0
        # Angle between velocity and intercept direction
        v_mag = speed or 1.0
        dot = (vx * to_int_x + vy * to_int_y) / (v_mag * to_int_mag)
        dot = max(-1.0, min(1.0, dot))
        angle_deg = math.degrees(math.acos(dot))

        # Current Earth position for distance and closing rate
        cur_ex, cur_ey = earth_position_at(elapsed_s)
        dist_to_earth_actual = math.sqrt((px - cur_ex) ** 2 + (py - cur_ey) ** 2)

        # Radial velocity toward Earth (positive = closing)
        to_earth_x = cur_ex - px
        to_earth_y = cur_ey - py
        to_earth_mag = math.sqrt(to_earth_x ** 2 + to_earth_y ** 2) or 1.0
        radial_vel = (vx * to_earth_x + vy * to_earth_y) / to_earth_mag
        tangential_vel = abs(vx * (-to_earth_y / to_earth_mag) + vy * (to_earth_x / to_earth_mag))

        # On-course requires BOTH good heading AND positive closing rate
        heading_ok = angle_deg < 20
        closing = radial_vel > 2.0  # at least 2 km/s toward Earth
        is_drifting = radial_vel < 2.0 and tangential_vel > radial_vel * 3
        on_course = heading_ok and closing and not is_drifting

        # Determine navigation phase
        if dist_to_earth_actual < 500_000:
            phase = "orbit_insertion"
        elif dist_to_earth_actual < 5_000_000:
            phase = "velocity_matching"
        elif is_drifting:
            phase = "DRIFTING_PARALLEL"
        else:
            phase = "intercept_cruise"

        avg_err = float(predictions.get("avg_error_km") or 0)
        reasoning_parts = [
            f"Spacecraft at {dist_to_earth_actual / 1.496e8:.3f} AU from Earth, speed {speed:.2f} km/s.",
            f"Phase: {phase}. Heading {angle_deg:.1f}deg off intercept.",
            f"Closing rate: {radial_vel:.1f} km/s, tangential: {tangential_vel:.1f} km/s.",
            f"ETA to Earth intercept: {eta_s / 86400:.1f} days.",
        ]
        if is_drifting:
            reasoning_parts.append(
                "WARNING: Ship is drifting parallel to Earth — tangential velocity dominates, "
                "insufficient radial approach. IMMEDIATE CORRECTION BURN REQUIRED toward Earth intercept."
            )
        elif not closing and dist_to_earth_actual > 500_000:
            reasoning_parts.append(
                "CAUTION: Closing rate is low or negative. Ship may not reach Earth without correction."
            )
        elif on_course:
            reasoning_parts.append("On course for intercept.")
        else:
            reasoning_parts.append("CORRECTION NEEDED — heading off intercept point.")
        reasoning_parts.append(
            f"Earth velocity: ({earth_vx:.1f}, {earth_vy:.1f}) km/s. "
            f"Prediction avg error: {avg_err:.0f} km. Fuel: {fuel:.0f} kg remaining."
        )
        # Inject ML fuel estimation and maneuver ranking
        ml_preds = state.get("ml_predictions", {})
        fuel_est = ml_preds.get("est_correction_fuel_kg")
        if fuel_est is not None:
            reasoning_parts.append(f"ML fuel model: correction burn ~{fuel_est:.1f} kg.")
        ranked = ml_preds.get("ranked_maneuvers")
        if ranked:
            top = ranked[0]
            reasoning_parts.append(
                f"ML maneuver ranking: top candidate score {top['ml_score']:.3f} "
                f"({len(ranked)} evaluated)."
            )
        # Memory-informed reasoning
        corr_eff = memory.get("correction_effectiveness")
        if corr_eff is not None:
            if corr_eff < 0.5:
                reasoning_parts.append(
                    f"MEMORY: Recent corrections only {corr_eff:.0%} effective — "
                    "increasing burn authority recommended."
                )
            elif corr_eff >= 0.8:
                reasoning_parts.append(
                    f"MEMORY: Corrections {corr_eff:.0%} effective — current strategy working."
                )
        burn_rate = memory.get("fuel_burn_rate_kg_per_day")
        if burn_rate is not None and burn_rate > 0:
            days_remaining = fuel / burn_rate if burn_rate > 0 else float("inf")
            if days_remaining < 30:
                reasoning_parts.append(
                    f"MEMORY: Fuel burn rate {burn_rate:.1f} kg/day — "
                    f"only ~{days_remaining:.0f} days of fuel remain at current rate."
                )
        return {
            "reasoning": " ".join(reasoning_parts),
            "on_course": on_course,
            "is_drifting": is_drifting,
            "closing_rate_kms": round(radial_vel, 1),
            "phase": phase,
            "intercept_eta_days": round(eta_s / 86400, 1),
            "heading_error_deg": round(angle_deg, 1),
            "est_correction_fuel_kg": fuel_est,
            "top_maneuver_score": ranked[0]["ml_score"] if ranked else None,
            "confidence": 0.90 if on_course else (0.40 if is_drifting else 0.55),
        }
    elif agent_name == "hazard_assessment":
        n_hazards = len(hazards)
        max_risk = max((float(h.get("risk_score") or 0) for h in hazards), default=0)
        # Use ML hazard risk model scores when available
        ml_preds = state.get("ml_predictions", {})
        ml_max_risk = ml_preds.get("ml_max_hazard_risk")
        ml_scores = ml_preds.get("hazard_risk_scores", {})
        if ml_max_risk is not None:
            effective_risk = ml_max_risk
            source_tag = "ML-scored"
        else:
            effective_risk = max_risk
            source_tag = "rule-based"
        reasoning_parts = [f"{n_hazards} active hazard(s)."]
        if n_hazards:
            reasoning_parts.append(f"Highest risk: {effective_risk:.2f} ({source_tag}).")
            if ml_scores:
                reasoning_parts.append(f"ML risk scores: {ml_scores}.")
        else:
            reasoning_parts.append("No immediate threats.")
        reasoning_parts.append(
            "Evasive action recommended." if effective_risk > 0.7 else "Threat level manageable."
        )
        # Memory-informed reasoning
        hazard_pattern = memory.get("hazard_pattern")
        total_enc = memory.get("total_encounters", 0)
        if hazard_pattern == "cluster":
            reasoning_parts.append(
                f"MEMORY: Hazards appearing in clusters ({total_enc} encounters) — "
                "recommend preemptive course adjustment to avoid hazard-dense region."
            )
        elif hazard_pattern == "periodic" and total_enc > 2:
            reasoning_parts.append(
                f"MEMORY: Hazards show periodic pattern ({total_enc} encounters) — "
                "likely traversing debris field. Maintain heightened alert."
            )
        return {
            "reasoning": " ".join(reasoning_parts),
            "hazard_count": n_hazards,
            "max_risk": effective_risk,
            "ml_scored": ml_max_risk is not None,
            "confidence": 0.92 if ml_max_risk is not None else (0.90 if max_risk < 0.3 else 0.65),
        }
    elif agent_name == "communications":
        delay = float(state.get("comm_delay_s") or state.get("communication_delay_s") or 0)
        delay_min = delay / 60
        ml_preds = state.get("ml_predictions", {})
        cmd_offset = ml_preds.get("optimal_cmd_offset_s")
        reasoning_parts = [
            f"One-way signal delay: {delay_min:.1f} minutes ({delay:.0f}s).",
            f"Commands sent now arrive in {delay_min:.1f}m.",
        ]
        if cmd_offset is not None:
            reasoning_parts.append(
                f"ML delay-policy recommends sending commands {cmd_offset:.0f}s early "
                f"({cmd_offset / 60:.1f} min lead time)."
            )
        if delay > 600:
            reasoning_parts.append("Delay is significant — ship must rely on autopilot between commands.")
        else:
            reasoning_parts.append("Delay within acceptable bounds for real-time guidance.")
        # Memory-informed reasoning
        delay_trend = memory.get("delay_trend")
        if delay_trend == "increasing":
            reasoning_parts.append(
                "MEMORY: Signal delay trending upward — spacecraft moving away from Earth. "
                "Consider pre-sending critical commands with larger timing margins."
            )
        elif delay_trend == "decreasing":
            reasoning_parts.append(
                "MEMORY: Signal delay decreasing — approaching Earth. "
                "Ground control responsiveness improving."
            )
        return {
            "reasoning": " ".join(reasoning_parts),
            "delay_seconds": delay,
            "delay_critical": delay > 1200,
            "optimal_cmd_offset_s": cmd_offset,
            "ml_enhanced": cmd_offset is not None,
            "confidence": 0.92 if cmd_offset is not None else 0.88,
        }
    else:  # mission_commander
        fd = peer_context.get("flight_dynamics", {})
        ha = peer_context.get("hazard_assessment", {})
        comms = peer_context.get("communications", {})
        on_course = fd.get("on_course", True)
        is_drifting = fd.get("is_drifting", False)
        max_risk = ha.get("max_risk", 0)
        closing_rate = fd.get("closing_rate_kms", 0)
        phase = fd.get("phase", "unknown")
        ml_preds = state.get("ml_predictions", {})

        if max_risk > 0.7:
            decision = "EMERGENCY_EVASION"
            confidence = 0.75
            reason = f"High hazard risk ({max_risk:.2f}). Evasive action required."
        elif is_drifting:
            decision = "GO"
            confidence = 0.85
            reason = (
                f"CRITICAL: Ship is drifting parallel to Earth (closing rate: {closing_rate} km/s). "
                f"Immediate intercept correction burn authorized. Phase: {phase}."
            )
        elif not on_course and fuel > 100:
            decision = "GO"
            confidence = 0.70
            reason = f"Off course (closing rate: {closing_rate} km/s). Correction burn authorized."
        elif on_course:
            decision = "GO"
            confidence = 0.92
            reason = f"On course. Closing rate: {closing_rate} km/s. Phase: {phase}."
        else:
            decision = "HOLD"
            confidence = 0.60
            reason = f"Low fuel ({fuel:.0f} kg) and off course. Holding for operator input."

        # Enrich with ML insights
        ml_notes = []
        if ha.get("ml_scored"):
            ml_notes.append("hazard risk ML-scored")
        if comms.get("ml_enhanced"):
            cmd_off = comms.get("optimal_cmd_offset_s")
            ml_notes.append(f"cmd timing optimized ({cmd_off:.0f}s lead)" if cmd_off else "cmd timing ML-enhanced")
        fuel_est = fd.get("est_correction_fuel_kg")
        if fuel_est is not None:
            ml_notes.append(f"fuel est {fuel_est:.1f}kg")
        ranked = ml_preds.get("ranked_maneuvers")
        if ranked:
            ml_notes.append(f"top maneuver score {ranked[0]['ml_score']:.3f}")

        ml_suffix = f" [ML: {', '.join(ml_notes)}]" if ml_notes else ""

        # Memory-informed reasoning
        mem_suffix = ""
        cmd_success = memory.get("command_success_rate")
        captain_overrides = memory.get("captain_overrides", 0)
        if cmd_success is not None and cmd_success < 0.5:
            mem_suffix += (
                f" [MEMORY: MC commands only {cmd_success:.0%} effective recently — "
                "consider adjusting timing or burn parameters.]"
            )
        if captain_overrides > 2:
            mem_suffix += (
                f" [MEMORY: Captain has overridden {captain_overrides} commands — "
                "review command timing relative to ship's local conditions.]"
            )

        return {
            "reasoning": f"Decision: {decision}. {reason} Confidence: {confidence:.0%}.{ml_suffix}{mem_suffix}",
            "decision": decision,
            "confidence": confidence,
            "ml_enhanced": len(ml_notes) > 0,
        }


async def _run_agent_cycle(
    px: float, py: float, pz: float,
    vx: float, vy: float, vz: float,
    fuel: float, comm_delay: float, elapsed_s: float,
):
    """
    Run the 4-agent decision chain: read state, call LLMs (with rule-based fallback),
    write messages and decisions to Lakebase.
    """
    global _agent_running
    if _agent_running:
        return  # Don't overlap cycles
    _agent_running = True

    import json as _json
    import uuid as _uuid

    try:
        tick_id = f"tick-{_uuid.uuid4().hex[:12]}"
        logger.info(f"[AGENT] Starting decision cycle {tick_id}")
        start = time.perf_counter()

        # Build state context
        state = {
            "position_x": px, "position_y": py, "position_z": pz,
            "velocity_x": vx, "velocity_y": vy, "velocity_z": vz,
            "fuel_remaining_kg": fuel,
            "comm_delay_s": comm_delay,
            "elapsed_s": elapsed_s,
            "distance_to_earth_km": math.sqrt(
                (px - earth_position_at(elapsed_s)[0]) ** 2 +
                (py - earth_position_at(elapsed_s)[1]) ** 2 + pz ** 2
            ),
            "speed_km_s": math.sqrt(vx ** 2 + vy ** 2 + vz ** 2),
        }

        # Get active hazards
        hazards = await execute_lakebase(
            "SELECT * FROM active_hazards WHERE session_id = :sid",
            {"sid": _current_session_id},
        ) or []

        # Get prediction accuracy
        pred_rows = await execute_lakebase(
            "SELECT AVG(prediction_error_km) as avg_error_km, "
            "  COUNT(*) as total "
            "FROM onboard_predictions "
            "WHERE session_id = :sid AND prediction_error_km IS NOT NULL",
            {"sid": _current_session_id},
        )
        predictions = pred_rows[0] if pred_rows else {}

        # --- Call ML model serving endpoints in parallel ---
        ml_predictions: dict = {}
        speed = state["speed_km_s"]
        dist_earth = state["distance_to_earth_km"]

        # Hazard risk scoring: score each active hazard via the ML model
        ml_hazard_risks = {}
        for h in hazards[:5]:
            h_px = float(h.get("position_x") or 0)
            h_py = float(h.get("position_y") or 0)
            h_vx = float(h.get("velocity_x") or 0)
            h_vy = float(h.get("velocity_y") or 0)
            h_radius = float(h.get("radius_km") or 5.0)
            approach_dist = math.sqrt((h_px - px)**2 + (h_py - py)**2)
            risk = await _call_hazard_risk(
                h_px, h_py, 0.0, h_vx, h_vy, 0.0, h_radius, approach_dist
            )
            if risk is not None:
                hid = h.get("hazard_id", "unknown")
                ml_hazard_risks[hid] = round(max(0.0, min(1.0, risk)), 3)
        if ml_hazard_risks:
            ml_predictions["hazard_risk_scores"] = ml_hazard_risks
            ml_predictions["ml_max_hazard_risk"] = max(ml_hazard_risks.values())
            logger.info(f"[ML] Hazard risk model scored {len(ml_hazard_risks)} hazards")

        # Delay-aware policy: optimal command timing
        max_h_risk = ml_predictions.get("ml_max_hazard_risk", 0.0)
        urgency = 0.8 if max_h_risk > 0.5 else (0.4 if not predictions.get("avg_error_km") else 0.2)
        delay_offset = await _call_delay_policy(
            speed, fuel, dist_earth, comm_delay, max_h_risk, urgency,
        )
        if delay_offset is not None:
            ml_predictions["optimal_cmd_offset_s"] = round(delay_offset, 1)
            logger.info(f"[ML] Delay policy: send commands {delay_offset:.1f}s early")

        # Fuel estimation for a hypothetical correction burn
        if not state.get("on_course", True) or speed > 0:
            hypothetical_dv = 0.005
            fuel_cost = await _call_fuel_estimation(
                0.0, 0.0, 0.0,  # direction TBD, use zeros for estimate
                30.0, hypothetical_dv,
            )
            if fuel_cost is not None:
                ml_predictions["est_correction_fuel_kg"] = round(fuel_cost, 2)
                logger.info(f"[ML] Fuel model: correction burn ~{fuel_cost:.1f} kg")

        # Maneuver ranking for candidate maneuvers from DB
        try:
            maneuver_rows = await execute_lakebase(
                "SELECT * FROM candidate_maneuvers WHERE session_id = :sid AND status = 'pending' LIMIT 5",
                {"sid": _current_session_id},
            ) or []
        except Exception:
            maneuver_rows = []  # Table may not exist yet
        if maneuver_rows:
            ranked = []
            for m in maneuver_rows:
                score = await _call_maneuver_ranking(
                    float(m.get("burn_vector_x") or 0),
                    float(m.get("burn_vector_y") or 0),
                    float(m.get("burn_vector_z") or 0),
                    float(m.get("burn_duration_s") or 30),
                    float(m.get("delta_v") or 0),
                    float(m.get("fuel_cost_kg") or 0),
                    float(m.get("risk_reduction_score") or 0),
                )
                if score is not None:
                    ranked.append({"maneuver_id": m.get("maneuver_id"), "ml_score": round(score, 3)})
            if ranked:
                ranked.sort(key=lambda x: x["ml_score"], reverse=True)
                ml_predictions["ranked_maneuvers"] = ranked
                logger.info(f"[ML] Maneuver ranking: top={ranked[0]['ml_score']:.3f}")

        # Inject ML predictions into state for agents
        state["ml_predictions"] = ml_predictions

        agent_order = ["flight_dynamics", "hazard_assessment", "communications", "mission_commander"]
        peer_context: dict = {}

        for agent_name in agent_order:
            # Build user message for LLM
            user_msg = f"Current state: {_json.dumps(state, default=str)}\n"
            if hazards:
                user_msg += f"Active hazards: {_json.dumps(hazards[:5], default=str)}\n"
            if predictions:
                user_msg += f"Prediction accuracy: {_json.dumps(predictions, default=str)}\n"
            if ml_predictions:
                user_msg += f"ML model predictions: {_json.dumps(ml_predictions, default=str)}\n"
            if peer_context:
                user_msg += f"Inputs from other agents: {_json.dumps(peer_context, default=str)}\n"
            # Include agent memory for cross-tick context
            agent_mem = _agent_memory.get_all(agent_name)
            if agent_mem:
                user_msg += f"Your memory from prior observations: {_json.dumps(agent_mem, default=str)}\n"

            # Try LLM, fall back to rules
            llm_response = await asyncio.to_thread(
                _call_foundation_model_sync,
                AGENT_SYSTEM_PROMPTS[agent_name],
                user_msg,
            )

            if llm_response:
                reasoning = llm_response
                confidence = 0.85
                # Try to extract decision from commander
                if agent_name == "mission_commander":
                    for kw in ["GO", "HOLD", "EMERGENCY_EVASION", "ABORT"]:
                        if kw in llm_response.upper():
                            peer_context[agent_name] = {
                                "reasoning": reasoning,
                                "decision": kw,
                                "confidence": confidence,
                                "source": "llm",
                            }
                            break
                    else:
                        peer_context[agent_name] = {
                            "reasoning": reasoning,
                            "decision": "GO",
                            "confidence": confidence,
                            "source": "llm",
                        }
                else:
                    peer_context[agent_name] = {
                        "reasoning": reasoning,
                        "confidence": confidence,
                        "source": "llm",
                    }
            else:
                # Rule-based fallback — pass agent memory for contextual reasoning
                agent_mem = _agent_memory.get_all(agent_name)
                result = _rule_based_agent(agent_name, state, hazards, predictions, peer_context, agent_mem)
                reasoning = result.get("reasoning", "")
                confidence = result.get("confidence", 0.5)
                peer_context[agent_name] = {**result, "source": "rule_based"}

            # Determine target agent
            to_agent = {
                "flight_dynamics": "hazard_assessment",
                "hazard_assessment": "communications",
                "communications": "mission_commander",
                "mission_commander": "operator",
            }[agent_name]

            msg_type = {
                "flight_dynamics": "analysis",
                "hazard_assessment": "alert" if len(hazards) > 0 else "analysis",
                "communications": "timing_plan",
                "mission_commander": "order",
            }[agent_name]

            # Write message to Lakebase
            msg_id = str(_uuid.uuid4())
            content = _json.dumps(peer_context[agent_name], default=str)
            await execute_lakebase(
                "INSERT INTO agent_messages_realtime "
                "(message_id, from_agent, to_agent, message_type, content, tick_id, created_at, session_id) "
                "VALUES (:mid, :from_a, :to_a, :mtype, :content, :tid, NOW(), :sid)",
                {
                    "mid": msg_id,
                    "from_a": agent_name,
                    "to_a": to_agent,
                    "mtype": msg_type,
                    "content": content,
                    "tid": tick_id,
                    "sid": _current_session_id,
                },
            )

            # Write decision to Lakebase-backed decisions (for Agent Console)
            decision_id = str(_uuid.uuid4())
            action = peer_context[agent_name].get("decision", "analysis_complete")
            await execute_lakebase(
                "INSERT INTO agent_messages_realtime "
                "(message_id, from_agent, to_agent, message_type, content, tick_id, created_at, session_id) "
                "VALUES (:mid, :from_a, 'broadcast', 'status', :content, :tid, NOW(), :sid)",
                {
                    "mid": decision_id,
                    "from_a": agent_name,
                    "content": _json.dumps({"summary": reasoning[:200], "action": action}, default=str),
                    "tid": tick_id,
                    "sid": _current_session_id,
                },
            )

        # --- Commander decision → queue command to ship ---
        commander = peer_context.get("mission_commander", {})
        decision = commander.get("decision", "HOLD")
        fd = peer_context.get("flight_dynamics", {})

        if decision in ("GO", "EMERGENCY_EVASION") and not fd.get("on_course", True):
            # Compute intercept-based correction burn
            int_ex, int_ey, earth_vx_i, earth_vy_i, eta_s_i, _miss_i = estimate_intercept(
                px, py, vx, vy, elapsed_s
            )
            cur_ex, cur_ey = earth_position_at(elapsed_s)
            dist_to_earth = math.sqrt((px - cur_ex) ** 2 + (py - cur_ey) ** 2)
            v_mag_c = math.sqrt(vx ** 2 + vy ** 2) or 1.0

            if dist_to_earth < APPROACH_BRAKE_THRESHOLD_KM:
                # Close: match Earth's velocity
                dvx_c = earth_vx_i - vx
                dvy_c = earth_vy_i - vy
                dvz_c = -vz
            elif dist_to_earth < VELOCITY_MATCH_THRESHOLD_KM:
                # Mid: blend intercept + velocity match
                blend = 1.0 - (dist_to_earth - APPROACH_BRAKE_THRESHOLD_KM) / (
                    VELOCITY_MATCH_THRESHOLD_KM - APPROACH_BRAKE_THRESHOLD_KM)
                blend = max(0.0, min(1.0, blend))
                to_int_x = int_ex - px
                to_int_y = int_ey - py
                to_int_m = math.sqrt(to_int_x ** 2 + to_int_y ** 2) or 1.0
                int_vx = (to_int_x / to_int_m) * v_mag_c
                int_vy = (to_int_y / to_int_m) * v_mag_c
                target_vx = int_vx * (1 - blend) + earth_vx_i * blend
                target_vy = int_vy * (1 - blend) + earth_vy_i * blend
                dvx_c = target_vx - vx
                dvy_c = target_vy - vy
                dvz_c = -vz
            else:
                # Far: aim for intercept
                to_int_x = int_ex - px
                to_int_y = int_ey - py
                to_int_m = math.sqrt(to_int_x ** 2 + to_int_y ** 2) or 1.0
                dvx_c = (to_int_x / to_int_m) * v_mag_c - vx
                dvy_c = (to_int_y / to_int_m) * v_mag_c - vy
                dvz_c = -vz

            dv_mag_c = math.sqrt(dvx_c ** 2 + dvy_c ** 2 + dvz_c ** 2) or 0.001
            burn_dv = 0.005 if decision == "GO" else 0.02  # km/s^2
            burn_dur = 30.0 if decision == "GO" else 60.0
            bvx = (dvx_c / dv_mag_c) * burn_dv
            bvy = (dvy_c / dv_mag_c) * burn_dv
            bvz = (dvz_c / dv_mag_c) * burn_dv

            cmd_id = str(_uuid.uuid4())
            payload = _json.dumps({
                "burn_vector_x": round(bvx, 8),
                "burn_vector_y": round(bvy, 8),
                "burn_vector_z": round(bvz, 8),
                "burn_duration_s": burn_dur,
                "direction": "earth",
                "source": "agent_chain",
                "commander_decision": decision,
            })

            # Read sim clock for receive time
            clock = await execute_lakebase(
                "SELECT simulation_time FROM simulation_clock WHERE clock_id = 1"
            )
            sim_time = clock[0].get("simulation_time") if clock else None
            from datetime import timedelta as _td
            est_receive = sim_time + _td(seconds=comm_delay) if sim_time else None

            await execute_lakebase(
                "INSERT INTO command_queue (command_id, command_type, payload, priority, "
                "  created_at, approved_by, approved_at, transmit_time, estimated_receive_time, "
                "  status, session_id, updated_at) "
                "VALUES (:cid, 'burn', :payload, 3, NOW(), :approver, NOW(), NOW(), "
                "  :est_receive, 'in_flight', :sid, NOW())",
                {
                    "cid": cmd_id,
                    "payload": payload,
                    "est_receive": est_receive,
                    "approver": "mission_commander",
                    "sid": _current_session_id,
                },
            )

            # Write a "command transmitted" message
            tx_msg_id = str(_uuid.uuid4())
            await execute_lakebase(
                "INSERT INTO agent_messages_realtime "
                "(message_id, from_agent, to_agent, message_type, content, tick_id, created_at, session_id) "
                "VALUES (:mid, 'mission_commander', 'spacecraft', 'order', :content, :tid, NOW(), :sid)",
                {
                    "mid": tx_msg_id,
                    "content": _json.dumps({
                        "summary": f"COMMAND TRANSMITTED: {decision} — correction burn queued. "
                                   f"ETA to ship: {comm_delay / 60:.1f} min.",
                        "command_id": cmd_id,
                        "decision": decision,
                    }),
                    "tid": tick_id,
                    "sid": _current_session_id,
                },
            )

            logger.info(
                f"[AGENT] Commander issued {decision} — burn cmd {cmd_id[:8]} "
                f"queued, ETA {comm_delay / 60:.1f}m"
            )

        # Update agent memory with observations from this cycle
        _update_agent_memory(peer_context, state, hazards, elapsed_s)

        # Flush memory to Lakebase periodically (every cycle)
        try:
            await _agent_memory.flush_to_lakebase(_current_session_id)
        except Exception as mem_err:
            logger.debug(f"[AGENT] Memory flush failed (non-critical): {mem_err}")

        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(
            f"[AGENT] Decision cycle complete: {commander.get('decision', '?')} "
            f"(conf={commander.get('confidence', 0):.0%}, "
            f"source={commander.get('source', '?')}, "
            f"{elapsed_ms:.0f}ms)"
        )

    except Exception as e:
        logger.error(f"[AGENT] Decision cycle failed: {e}")
        logger.debug(traceback.format_exc())
    finally:
        _agent_running = False


def _call_foundation_model_sync(system_prompt: str, user_message: str) -> Optional[str]:
    """Synchronous wrapper for Foundation Model API call (runs in thread)."""
    import requests as _requests

    try:
        headers = _get_serving_headers()
        w = get_workspace_client()
        host = w.config.host.rstrip("/")

        endpoint = "databricks-meta-llama-3-3-70b-instruct"
        url = f"{host}/serving-endpoints/{endpoint}/invocations"

        resp = _requests.post(
            url,
            headers=headers,
            json={
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message},
                ],
                "temperature": 0.2,
                "max_tokens": 300,
            },
            timeout=30,
        )
        resp.raise_for_status()
        choices = resp.json().get("choices", [])
        if choices:
            return choices[0].get("message", {}).get("content", "")
    except Exception as e:
        logger.debug(f"[AGENT] Foundation Model call failed: {e}")
    return None


async def _run_mini_tick():
    """Single mini-tick: propagate position, advance clock, write telemetry.

    Adaptive sub-ticking: at high time scales (dt > SUB_TICK_MAX_DT), the tick
    is split into multiple physics sub-steps to maintain telemetry granularity.
    This means Lakebase throughput ramps up proportionally at higher speeds.
    """
    global _trim_counter, _autopilot_counter, _prediction_counter, _captain_counter
    global _last_written_elapsed

    # Initialise ops tracking window if needed
    if _ops_tracker["window_start"] == 0.0:
        _ops_tracker["window_start"] = time.monotonic()

    # 1. Read clock — bail if simulation is paused
    clock_rows = await execute_lakebase(
        "SELECT is_running, time_scale, total_elapsed_s, simulation_time "
        "FROM simulation_clock WHERE clock_id = 1"
    )
    if not clock_rows or not clock_rows[0].get("is_running"):
        return

    cr = clock_rows[0]
    time_scale = float(cr.get("time_scale", 1.0))
    elapsed_s = float(cr.get("total_elapsed_s", 0))
    total_dt = 2.0 * time_scale  # 2 real seconds * time_scale

    # Monotonic guard: if the clock reads LESS than what we last wrote,
    # another process (e.g. the Databricks simulation job) overwrote it with
    # stale data. Skip this tick — the GREATEST guard on the clock UPDATE will
    # restore the correct value on our next successful write.
    if elapsed_s < _last_written_elapsed - total_dt:
        logger.warning(
            f"[CLOCK-GUARD] Clock regressed: read {elapsed_s:.0f}s but last wrote "
            f"{_last_written_elapsed:.0f}s — skipping tick (external writer?)"
        )
        # Force-restore the clock to our last known good value
        await execute_lakebase(
            "UPDATE simulation_clock SET total_elapsed_s = GREATEST(total_elapsed_s, :elapsed) "
            "WHERE clock_id = 1",
            {"elapsed": _last_written_elapsed},
        )
        return

    # Adaptive sub-ticking: split large dt into smaller physics steps
    num_sub_ticks = max(1, int(total_dt / SUB_TICK_MAX_DT))
    sub_dt = total_dt / num_sub_ticks
    # Decide which sub-ticks get a telemetry write (evenly spaced, capped)
    telem_write_count = min(num_sub_ticks, MAX_TELEMETRY_WRITES_PER_TICK)
    telem_interval = max(1, num_sub_ticks // telem_write_count)
    _ops_tracker["sub_ticks_last"] = num_sub_ticks
    _ops_tracker["telemetry_writes_last"] = telem_write_count

    # 2. Read current spacecraft state
    state_rows = await execute_lakebase(
        "SELECT position_x, position_y, position_z, "
        "       velocity_x, velocity_y, velocity_z, "
        "       fuel_remaining_kg, hull_integrity, engine_status, "
        "       mission_elapsed_s "
        "FROM mission_state WHERE state_id = 1"
    )
    if not state_rows:
        return
    s = state_rows[0]

    px, py, pz = float(s["position_x"]), float(s["position_y"]), float(s["position_z"])
    vx, vy, vz = float(s["velocity_x"]), float(s["velocity_y"]), float(s["velocity_z"])
    fuel = float(s["fuel_remaining_kg"])
    hull = float(s["hull_integrity"])
    engine = s["engine_status"]
    sim_time = cr.get("simulation_time")

    # Teleport guard: reject state reads that jump impossibly far between ticks
    # Uses 5x safety margin and high absolute floor to avoid false positives
    # at high time scales or after burns that change velocity significantly.
    global _last_known_pos
    if _last_known_pos is not None:
        lpx, lpy, lpz = _last_known_pos
        jump_km = math.sqrt((px - lpx) ** 2 + (py - lpy) ** 2 + (pz - lpz) ** 2)
        speed = math.sqrt(vx ** 2 + vy ** 2 + vz ** 2) or 30.0
        max_jump = speed * total_dt * 5.0  # 5x safety margin
        if jump_km > max(max_jump, 5e7):  # must exceed 50M km absolute floor
            logger.error(
                f"[TELEPORT-GUARD] Position jumped {jump_km:.0f} km "
                f"(max plausible {max_jump:.0f} km at speed={speed:.1f}, dt={total_dt:.0f}) "
                f"— resetting guard (session change?)"
            )
            # Don't skip — just reset the guard. This handles session transitions gracefully.
            _last_known_pos = (px, py, pz)
            return

    # Elapsed time consistency: use clock's total_elapsed_s as source of truth
    # (mission_state.mission_elapsed_s may lag after deploys)
    state_elapsed = float(s.get("mission_elapsed_s", 0))
    if abs(state_elapsed - elapsed_s) > total_dt * 2:
        logger.warning(
            f"[TIME-GUARD] Clock elapsed={elapsed_s:.0f}s vs state elapsed={state_elapsed:.0f}s "
            f"— using clock value"
        )
    # Always use clock elapsed_s for physics continuity
    _last_known_pos = (px, py, pz)

    # 4a. Ship Captain evaluation — intercept/evaluate MC commands before execution
    _captain_counter += 1
    captain_decision = None
    if _ship_captain and _captain_tables_ready and _captain_counter >= CAPTAIN_CHECK_INTERVAL:
        _captain_counter = 0
        try:
            from physics_engine import Vector3 as _V3, SpacecraftState as _SS, Hazard as _Hz
            import json as _json
            import uuid as _uuid

            ship_state = _SS(
                position=_V3(px, py, pz),
                velocity=_V3(vx, vy, vz),
                fuel_remaining_kg=fuel,
                timestamp_s=elapsed_s,
                hull_integrity=hull,
            )

            # Query active hazards
            hazard_rows = await execute_lakebase(
                "SELECT hazard_id, hazard_type, position_x, position_y, position_z, "
                "  velocity_x, velocity_y, velocity_z, radius_km "
                "FROM active_hazards WHERE session_id = :sid",
                {"sid": _current_session_id},
            ) or []
            hazards = [
                _Hz(
                    hazard_id=h["hazard_id"],
                    hazard_type=h.get("hazard_type", "asteroid"),
                    position=_V3(float(h.get("position_x", 0)), float(h.get("position_y", 0)), float(h.get("position_z", 0))),
                    velocity=_V3(float(h.get("velocity_x", 0)), float(h.get("velocity_y", 0)), float(h.get("velocity_z", 0))),
                    radius_km=float(h.get("radius_km", 5)),
                )
                for h in hazard_rows
            ]

            # Query arrived but unexecuted MC commands
            pending_cmds = []
            if sim_time:
                pending_rows = await execute_lakebase(
                    "SELECT command_id, command_type, payload "
                    "FROM command_queue "
                    "WHERE status = 'in_flight' AND estimated_receive_time <= :sim_time",
                    {"sim_time": sim_time},
                ) or []
                for c in pending_rows:
                    raw_p = c.get("payload", "{}")
                    try:
                        pl = _json.loads(raw_p) if isinstance(raw_p, str) else (raw_p or {})
                    except Exception:
                        pl = {}
                    pending_cmds.append({
                        "command_id": c["command_id"],
                        "command_type": c.get("command_type", ""),
                        "payload": pl,
                    })

            # Query recent predictions for micro-correction
            pred_rows = await execute_lakebase(
                "SELECT predicted_pos_x, predicted_pos_y, predicted_pos_z, "
                "  prediction_error_km, assessment "
                "FROM onboard_predictions "
                "WHERE session_id = :sid AND assessment IS NOT NULL "
                "ORDER BY simulation_time_s DESC LIMIT 3",
                {"sid": _current_session_id},
            ) or []

            captain_decision = _ship_captain.evaluate_tick(
                ship=ship_state,
                hazards=hazards,
                pending_mc_commands=pending_cmds,
                onboard_predictions=pred_rows,
                elapsed_days=elapsed_s / 86400.0,
            )

            # Handle captain decision
            if captain_decision:
                # Apply captain burns (evasion or micro-correction)
                if captain_decision.burn_vector and captain_decision.delta_v > 0:
                    bv = captain_decision.burn_vector
                    burn_dur = max(1.0, captain_decision.delta_v / max(
                        math.sqrt(bv.x**2 + bv.y**2 + bv.z**2), 1e-10
                    ))
                    vx += bv.x * burn_dur
                    vy += bv.y * burn_dur
                    vz += bv.z * burn_dur
                    fuel = max(0, fuel - captain_decision.fuel_cost_kg)
                    logger.info(
                        f"[CAPTAIN] {captain_decision.action}: dv={captain_decision.delta_v:.4f} km/s, "
                        f"fuel={captain_decision.fuel_cost_kg:.1f} kg — {captain_decision.reasoning[:80]}"
                    )

                # Override/veto commands — mark them rejected in command_queue
                if captain_decision.action in ("override_command", "modify_command") and captain_decision.override_of_command_id:
                    if captain_decision.action == "override_command":
                        await execute_lakebase(
                            "UPDATE command_queue SET status = 'rejected', updated_at = NOW() "
                            "WHERE command_id = :cid",
                            {"cid": captain_decision.override_of_command_id},
                        )
                    # For modify_command, we let the command execute but it will be
                    # scaled down by the captain — mark as captain_modified
                    if captain_decision.action == "modify_command":
                        await execute_lakebase(
                            "UPDATE command_queue SET status = 'captain_modified', updated_at = NOW() "
                            "WHERE command_id = :cid",
                            {"cid": captain_decision.override_of_command_id},
                        )

                # Log decision to captain_decisions table
                await execute_lakebase(
                    "INSERT INTO captain_decisions ("
                    "  decision_id, simulation_time_s, session_id, action, priority_level, "
                    "  reasoning, override_of_command_id, original_command_summary, "
                    "  captain_alternative_summary, delta_v, fuel_cost_kg, "
                    "  alert_level, confidence, elapsed_ms, created_at"
                    ") VALUES ("
                    "  :did, :sim_t, :sid, :act, :pri, :reason, :oid, :orig, :alt, "
                    "  :dv, :fc, :alert, :conf, :ems, NOW()"
                    ")",
                    {
                        "did": captain_decision.decision_id,
                        "sim_t": captain_decision.simulation_time_s,
                        "sid": _current_session_id,
                        "act": captain_decision.action,
                        "pri": captain_decision.priority_level,
                        "reason": captain_decision.reasoning,
                        "oid": captain_decision.override_of_command_id,
                        "orig": captain_decision.original_command_summary,
                        "alt": captain_decision.captain_alternative_summary,
                        "dv": captain_decision.delta_v,
                        "fc": captain_decision.fuel_cost_kg,
                        "alert": captain_decision.alert_level,
                        "conf": captain_decision.confidence,
                        "ems": captain_decision.elapsed_ms,
                    },
                )

                # Add captain message to agent feed (visible in AgentCommsFeed)
                if captain_decision.action != "await_orders":
                    await execute_lakebase(
                        "INSERT INTO agent_messages_realtime "
                        "(message_id, from_agent, to_agent, message_type, content, tick_id, created_at, session_id) "
                        "VALUES (:mid, 'ship_captain', 'mission_control', :mtype, :content, :tid, NOW(), :sid)",
                        {
                            "mid": str(_uuid.uuid4()),
                            "mtype": "captain_decision",
                            "content": _json.dumps({
                                "action": captain_decision.action,
                                "priority": captain_decision.priority_level,
                                "reasoning": captain_decision.reasoning,
                                "alert_level": captain_decision.alert_level,
                                "delta_v": captain_decision.delta_v,
                                "fuel_cost_kg": captain_decision.fuel_cost_kg,
                                "override_of": captain_decision.override_of_command_id,
                            }),
                            "tid": int(elapsed_s),
                            "sid": _current_session_id,
                        },
                    )

                # Record event
                if captain_decision.action != "await_orders":
                    _record_event(
                        f"captain_{captain_decision.action}",
                        f"Captain: {captain_decision.action} — {captain_decision.reasoning[:100]}",
                        simulation_time_s=elapsed_s,
                        metadata={
                            "delta_v": captain_decision.delta_v,
                            "fuel_cost_kg": captain_decision.fuel_cost_kg,
                            "alert_level": captain_decision.alert_level,
                            "override_of": captain_decision.override_of_command_id,
                        },
                    )

                # Buffer for in-memory access
                _captain_decisions.append({
                    "decision_id": captain_decision.decision_id,
                    "action": captain_decision.action,
                    "priority_level": captain_decision.priority_level,
                    "reasoning": captain_decision.reasoning,
                    "alert_level": captain_decision.alert_level,
                    "delta_v": captain_decision.delta_v,
                    "fuel_cost_kg": captain_decision.fuel_cost_kg,
                    "simulation_time_s": captain_decision.simulation_time_s,
                })
                if len(_captain_decisions) > 50:
                    _captain_decisions.pop(0)

        except Exception as e:
            logger.debug(f"[CAPTAIN] Tick error: {e}")

    # 4b. Execute received commands (burns) — once per tick, before sub-stepping
    # Skip commands that the captain already vetoed/rejected
    if sim_time:
        arrived_cmds = await execute_lakebase(
            "SELECT command_id, command_type, payload "
            "FROM command_queue "
            "WHERE status = 'in_flight' AND estimated_receive_time <= :sim_time",
            {"sim_time": sim_time},
        )
        for cmd in arrived_cmds:
            cmd_id = cmd["command_id"]
            cmd_type = cmd.get("command_type", "")
            try:
                import json as _json
                raw = cmd.get("payload", "{}")
                payload = _json.loads(raw) if isinstance(raw, str) else (raw or {})
            except Exception:
                payload = {}

            if cmd_type == "burn" and payload:
                bvx = float(payload.get("burn_vector_x", 0))
                bvy = float(payload.get("burn_vector_y", 0))
                bvz = float(payload.get("burn_vector_z", 0))
                dur = float(payload.get("burn_duration_s", 0))
                vx += bvx * dur
                vy += bvy * dur
                vz += bvz * dur
                dv = math.sqrt(bvx**2 + bvy**2 + bvz**2) * dur
                # Tsiolkovsky fuel model (game-balanced)
                fuel_cost = DRY_MASS * (math.exp(dv / EXHAUST_VEL) - 1)
                fuel = max(0, fuel - fuel_cost)
                logger.info(
                    f"[MINI-TICK] Executed burn {cmd_id[:8]}: "
                    f"dv={dv:.3f} km/s, fuel_used={fuel_cost:.1f} kg"
                )
                _record_event(
                    "burn_executed",
                    f"Burn {cmd_id[:8]}: Δv={dv:.3f} km/s, fuel={fuel_cost:.1f} kg",
                    simulation_time_s=elapsed_s,
                    metadata={"command_id": cmd_id, "delta_v": round(dv, 4), "fuel_cost_kg": round(fuel_cost, 1)},
                )
            await execute_lakebase(
                "UPDATE command_queue SET status = 'executed', updated_at = NOW() "
                "WHERE command_id = :cid",
                {"cid": cmd_id},
            )

    # --- Sub-tick physics loop ---
    new_elapsed = elapsed_s
    telem_writes = 0
    telem_batch: list[dict] = []  # collect telemetry for batched write
    for sub_i in range(num_sub_ticks):
        # 3. Solar gravity
        r = math.sqrt(px * px + py * py + pz * pz) or 1.0
        a_mag = -GM_SUN / (r * r)
        ax, ay, az = a_mag * px / r, a_mag * py / r, a_mag * pz / r

        # 4. Euler integration
        vx += ax * sub_dt
        vy += ay * sub_dt
        vz += az * sub_dt
        px += vx * sub_dt
        py += vy * sub_dt
        pz += vz * sub_dt
        new_elapsed += sub_dt

        # Collect telemetry for selected sub-ticks (batched write below)
        is_last = sub_i == num_sub_ticks - 1
        if is_last or (sub_i % telem_interval == 0 and telem_writes < telem_write_count):
            speed = math.sqrt(vx ** 2 + vy ** 2 + vz ** 2)
            ex_t, ey_t = earth_position_at(new_elapsed)
            comm_d = compute_comm_delay(px, py, pz, ex_t, ey_t)
            tick_seq = int(new_elapsed)
            telem_batch.append({
                "seq": tick_seq, "sim_t": new_elapsed,
                "px": px, "py": py, "pz": pz,
                "vx": vx, "vy": vy, "vz": vz,
                "speed": speed, "fuel": fuel, "hull": hull,
                "engine": engine, "comm": comm_d,
            })
            telem_writes += 1

            # Sample telemetry for Delta flush at mission end (capped at 10K rows)
            if len(_telemetry_buffer) == 0 or tick_seq - (_telemetry_buffer[-1].get("tick_seq", 0)) >= TELEMETRY_SAMPLE_INTERVAL:
                _telemetry_buffer.append({
                    "tick_seq": tick_seq, "simulation_time_s": new_elapsed,
                    "position_x": px, "position_y": py, "position_z": pz,
                    "velocity_x": vx, "velocity_y": vy, "velocity_z": vz,
                    "fuel_remaining_kg": fuel, "hull_integrity": hull,
                    "engine_status": engine, "communication_delay_s": comm_d,
                })
                if len(_telemetry_buffer) > 10000:
                    _telemetry_buffer.pop(0)

    # Batched telemetry write — single multi-row INSERT instead of N round trips
    if telem_batch:
        if len(telem_batch) == 1:
            # Single row — use simple INSERT
            t = telem_batch[0]
            await execute_lakebase(
                "INSERT INTO telemetry_realtime ("
                "  tick_seq, simulation_time_s, "
                "  position_x, position_y, position_z, "
                "  velocity_x, velocity_y, velocity_z, "
                "  speed_km_s, fuel_remaining_kg, hull_integrity, "
                "  engine_status, communication_delay_s, session_id, updated_at"
                ") VALUES ("
                "  :seq, :sim_t, :px, :py, :pz, :vx, :vy, :vz, "
                "  :speed, :fuel, :hull, :engine, :comm, :sid, NOW()"
                ") ON CONFLICT (tick_seq) DO UPDATE SET "
                "  simulation_time_s = EXCLUDED.simulation_time_s, "
                "  position_x = EXCLUDED.position_x, position_y = EXCLUDED.position_y, "
                "  position_z = EXCLUDED.position_z, "
                "  velocity_x = EXCLUDED.velocity_x, velocity_y = EXCLUDED.velocity_y, "
                "  velocity_z = EXCLUDED.velocity_z, "
                "  speed_km_s = EXCLUDED.speed_km_s, session_id = EXCLUDED.session_id, "
                "  updated_at = NOW()",
                {**t, "sid": _current_session_id},
            )
        else:
            # Multi-row batch INSERT — 3-5x fewer round trips at high time_scale
            value_clauses = []
            params: dict = {}
            for i, t in enumerate(telem_batch):
                value_clauses.append(
                    f"(:seq_{i}, :sim_t_{i}, :px_{i}, :py_{i}, :pz_{i}, "
                    f":vx_{i}, :vy_{i}, :vz_{i}, :speed_{i}, :fuel_{i}, "
                    f":hull_{i}, :engine_{i}, :comm_{i}, :sid_{i}, NOW())"
                )
                params.update({
                    f"seq_{i}": t["seq"], f"sim_t_{i}": t["sim_t"],
                    f"px_{i}": t["px"], f"py_{i}": t["py"], f"pz_{i}": t["pz"],
                    f"vx_{i}": t["vx"], f"vy_{i}": t["vy"], f"vz_{i}": t["vz"],
                    f"speed_{i}": t["speed"], f"fuel_{i}": t["fuel"],
                    f"hull_{i}": t["hull"], f"engine_{i}": t["engine"],
                    f"comm_{i}": t["comm"], f"sid_{i}": _current_session_id,
                })
            await execute_lakebase(
                "INSERT INTO telemetry_realtime ("
                "  tick_seq, simulation_time_s, "
                "  position_x, position_y, position_z, "
                "  velocity_x, velocity_y, velocity_z, "
                "  speed_km_s, fuel_remaining_kg, hull_integrity, "
                "  engine_status, communication_delay_s, session_id, updated_at"
                ") VALUES " + ", ".join(value_clauses) +
                " ON CONFLICT (tick_seq) DO UPDATE SET "
                "  simulation_time_s = EXCLUDED.simulation_time_s, "
                "  position_x = EXCLUDED.position_x, position_y = EXCLUDED.position_y, "
                "  position_z = EXCLUDED.position_z, "
                "  velocity_x = EXCLUDED.velocity_x, velocity_y = EXCLUDED.velocity_y, "
                "  velocity_z = EXCLUDED.velocity_z, "
                "  speed_km_s = EXCLUDED.speed_km_s, session_id = EXCLUDED.session_id, "
                "  updated_at = NOW()",
                params,
            )

    # --- Post sub-tick: final state ---
    _last_known_pos = (px, py, pz)
    ex, ey = earth_position_at(new_elapsed)
    comm_delay = compute_comm_delay(px, py, pz, ex, ey)
    speed = math.sqrt(vx ** 2 + vy ** 2 + vz ** 2)

    # 5b. Autopilot
    _autopilot_counter += 1
    if _autopilot_counter % AUTOPILOT_CHECK_INTERVAL == 0:
        try:
            await _check_autopilot_course_correction(
                px, py, pz, vx, vy, vz,
                new_elapsed, fuel, sim_time, comm_delay,
            )
        except Exception as e:
            logger.warning(f"Autopilot check failed: {e}")

    # 5c. Onboard trajectory prediction (every 30 ticks ~60 real seconds)
    _prediction_counter += 1
    if _prediction_counter % PREDICTION_CHECK_INTERVAL == 0:
        try:
            await _onboard_trajectory_prediction(
                px, py, pz, vx, vy, vz,
                new_elapsed, fuel, comm_delay,
            )
        except Exception as e:
            logger.warning(f"Onboard prediction failed: {e}")

    # 5d. Backfill prediction accuracy (check every 10 ticks)
    if _prediction_counter % 10 == 0:
        try:
            await _backfill_predictions(new_elapsed, px, py, pz)
        except Exception as e:
            logger.debug(f"Prediction backfill failed: {e}")

    # 5e. Agent decision chain (every AGENT_CHECK_INTERVAL ticks, runs in background)
    global _agent_counter
    _agent_counter += 1
    if _agent_counter % AGENT_CHECK_INTERVAL == 0:
        asyncio.create_task(
            _run_agent_cycle(px, py, pz, vx, vy, vz, fuel, comm_delay, new_elapsed)
        )

    # 6. Update mission_state (monotonic guard: never overwrite with older state)
    await execute_lakebase(
        "UPDATE mission_state SET "
        "  position_x = :px, position_y = :py, position_z = :pz, "
        "  velocity_x = :vx, velocity_y = :vy, velocity_z = :vz, "
        "  fuel_remaining_kg = :fuel, "
        "  communication_delay_s = :comm_delay, "
        "  mission_elapsed_s = :elapsed, "
        "  updated_at = NOW() "
        "WHERE state_id = 1 AND mission_elapsed_s <= :elapsed",
        {"px": px, "py": py, "pz": pz,
         "vx": vx, "vy": vy, "vz": vz,
         "comm_delay": comm_delay, "elapsed": new_elapsed,
         "fuel": fuel},
    )

    # 7. Advance simulation clock (GREATEST guard: clock can never go backward)
    await execute_lakebase(
        "UPDATE simulation_clock SET "
        "  simulation_time = simulation_time + make_interval(secs => :dt), "
        "  total_elapsed_s = GREATEST(total_elapsed_s, :elapsed) "
        "WHERE clock_id = 1",
        {"dt": total_dt, "elapsed": new_elapsed},
    )
    _last_written_elapsed = new_elapsed

    # 8. End-state detection
    dist_to_earth = math.sqrt(
        (px - ex) ** 2 + (py - ey) ** 2 + pz ** 2
    )
    EARTH_ARRIVAL_THRESHOLD_KM = 500_000.0  # 500K km — flyby detection zone
    mission_outcome = None
    outcome_detail = None

    if dist_to_earth < EARTH_ARRIVAL_THRESHOLD_KM:
        mission_outcome = "arrived"
        outcome_detail = (
            f"Odyssey reached Earth at {dist_to_earth:.0f} km after "
            f"{new_elapsed / 86400:.1f} days. Speed: {speed:.2f} km/s, "
            f"fuel remaining: {fuel:.0f} kg."
        )
    elif fuel <= 0:
        mission_outcome = "fuel_exhausted"
        outcome_detail = (
            f"Fuel depleted at {dist_to_earth / 1.496e8:.3f} AU from Earth after "
            f"{new_elapsed / 86400:.1f} days. Speed: {speed:.2f} km/s. "
            f"Odyssey is now in an uncontrolled trajectory."
        )
    elif hull < 10:
        mission_outcome = "hull_failure"
        outcome_detail = (
            f"Hull integrity critical ({hull:.0f}%) at {dist_to_earth / 1.496e8:.3f} AU from Earth. "
            f"Structural collapse imminent."
        )

    if mission_outcome:
        global _last_outcome
        _last_outcome = {"outcome": mission_outcome, "detail": outcome_detail}
        logger.info(f"[MISSION] Outcome: {mission_outcome} — {outcome_detail}")
        _record_event(
            f"mission_{mission_outcome}",
            outcome_detail or f"Mission ended: {mission_outcome}",
            simulation_time_s=new_elapsed,
            metadata={"outcome": mission_outcome, "distance_to_earth_km": round(dist_to_earth, 0)},
        )
        # Stop the simulation
        await execute_lakebase(
            "UPDATE simulation_clock SET is_running = FALSE WHERE clock_id = 1"
        )
        # Record outcome in mission_state (only if columns exist)
        if _has_outcome_columns:
            await execute_lakebase(
                "UPDATE mission_state SET "
                "  mission_outcome = :outcome, "
                "  mission_outcome_detail = :detail "
                "WHERE state_id = 1",
                {"outcome": mission_outcome, "detail": outcome_detail},
            )
        # Update session and flush to Delta for ML training
        if _current_session_id:
            await _end_session(_current_session_id)
            # Flush all session data to Delta Lake (async, non-blocking)
            asyncio.create_task(_flush_and_retrain(_current_session_id))

    # 9. Trim buffer every ~60 seconds (30 ticks * 2s)
    _trim_counter += 1
    if _trim_counter % 30 == 0:
        await execute_lakebase(
            "DELETE FROM telemetry_realtime "
            "WHERE tick_seq NOT IN ("
            "  SELECT tick_seq FROM telemetry_realtime "
            "  ORDER BY simulation_time_s DESC LIMIT 600"
            ")"
        )

    # 10. Write throughput metrics every 10 ticks (~20 real seconds)
    if _trim_counter % 10 == 0:
        try:
            now = time.monotonic()
            window = max(now - _ops_tracker["window_start"], 0.1)
            reads = _ops_tracker["reads"]
            writes = _ops_tracker["writes"]
            total = reads + writes
            ops_s = total / window
            lats = _ops_tracker["latencies_ms"]
            # Compute latency percentiles
            sorted_lats = sorted(lats) if lats else [0.0]
            p50 = sorted_lats[len(sorted_lats) // 2] if sorted_lats else 0.0
            p99_idx = min(int(len(sorted_lats) * 0.99), len(sorted_lats) - 1)
            p99 = sorted_lats[p99_idx] if sorted_lats else 0.0

            import uuid
            await execute_lakebase(
                "INSERT INTO throughput_metrics "
                "(metric_id, component, timestamp, wall_time_s, read_ops, write_ops, "
                " total_ops, ops_per_second, sim_seconds_processed, rows_generated) "
                "VALUES (:mid, 'mini_tick', NOW(), :wall, :reads, :writes, "
                " :total, :ops_s, :sim_s, :sub_ticks)",
                {
                    "mid": str(uuid.uuid4()),
                    "wall": round(window, 2),
                    "reads": reads,
                    "writes": writes,
                    "total": total,
                    "ops_s": round(ops_s, 2),
                    "sim_s": int(total_dt),
                    "sub_ticks": num_sub_ticks,
                },
            )
            # Reset window
            _ops_tracker["reads"] = 0
            _ops_tracker["writes"] = 0
            _ops_tracker["latencies_ms"] = []
            _ops_tracker["window_start"] = now
        except Exception as e:
            logger.debug(f"Throughput metrics write failed: {e}")

    # 11. Streaming CDC: flush telemetry + inference logs to Delta periodically
    global _delta_stream_counter
    _delta_stream_counter += 1
    if _delta_stream_counter >= DELTA_STREAM_INTERVAL:
        _delta_stream_counter = 0
        asyncio.create_task(_stream_telemetry_to_delta())


async def mini_tick_loop():
    """Background loop: run mini-tick every 2 seconds."""
    logger.info("Mini-tick simulation engine started")
    while True:
        await asyncio.sleep(2.0)
        try:
            await _run_mini_tick()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"Mini-tick error (non-fatal): {e}")


# ---------------------------------------------------------------------------
# App Lifecycle
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _mini_tick_task
    print(f"Mission Control starting — catalog: {CATALOG}, lakebase project: {LAKEBASE_PROJECT_ID}")
    lakebase.initialize()
    lakebase.start_refresh()
    await lakebase.pre_warm(3)

    # Migrate: ensure mission_outcome columns exist on mission_state
    global _has_outcome_columns
    _has_outcome_columns = False
    for col, ctype in [("mission_outcome", "TEXT"), ("mission_outcome_detail", "TEXT")]:
        try:
            # Check if column already exists
            check = await execute_lakebase(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'mission_state' AND column_name = :col",
                {"col": col},
            )
            if not check:
                await execute_lakebase(f"ALTER TABLE mission_state ADD COLUMN {col} {ctype}")
                logger.info(f"Added column {col} to mission_state")
            else:
                logger.info(f"Column {col} already exists on mission_state")
            _has_outcome_columns = True
        except Exception as e:
            logger.warning(f"Column {col} migration failed: {e}")
    logger.info(f"Mission outcome columns available: {_has_outcome_columns}")

    # Migrate: create captain_decisions table if it doesn't exist
    global _captain_tables_ready, _ship_captain
    try:
        await execute_lakebase("""
            CREATE TABLE IF NOT EXISTS captain_decisions (
                decision_id TEXT PRIMARY KEY,
                simulation_time_s DOUBLE PRECISION,
                action TEXT NOT NULL,
                priority_level INTEGER,
                reasoning TEXT,
                override_of_command_id TEXT,
                original_command_summary TEXT,
                captain_alternative_summary TEXT,
                delta_v DOUBLE PRECISION DEFAULT 0.0,
                fuel_cost_kg DOUBLE PRECISION DEFAULT 0.0,
                alert_level TEXT DEFAULT 'green',
                confidence DOUBLE PRECISION DEFAULT 0.85,
                elapsed_ms DOUBLE PRECISION DEFAULT 0.0,
                session_id TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await execute_lakebase("""
            CREATE TABLE IF NOT EXISTS captain_mc_dialogue (
                dialogue_id TEXT PRIMARY KEY,
                simulation_time_s DOUBLE PRECISION,
                direction TEXT NOT NULL,
                message_type TEXT,
                content TEXT,
                comm_delay_s DOUBLE PRECISION,
                estimated_receive_time DOUBLE PRECISION,
                acknowledged BOOLEAN DEFAULT FALSE,
                session_id TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        _captain_tables_ready = True
        logger.info("Captain tables ready")
    except Exception as e:
        logger.warning(f"Captain table migration failed: {e}")

    # Migrate: create mission_events table if it doesn't exist
    try:
        await execute_lakebase("""
            CREATE TABLE IF NOT EXISTS mission_events (
                event_id TEXT PRIMARY KEY,
                session_id TEXT,
                event_type TEXT NOT NULL,
                summary TEXT NOT NULL,
                simulation_time_s DOUBLE PRECISION DEFAULT 0.0,
                metadata JSONB DEFAULT '{}',
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        logger.info("mission_events table ready")
    except Exception as e:
        logger.warning(f"mission_events table migration failed: {e}")

    # Initialize Ship Captain
    try:
        import sys as _sys
        _sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "python"))
        from ship_captain import ShipCaptain
        _ship_captain = ShipCaptain()
        logger.info("Ship Captain initialized")
    except Exception as e:
        logger.warning(f"Could not initialize Ship Captain: {e}")

    await _recover_or_create_session()
    _mini_tick_task = asyncio.create_task(mini_tick_loop())
    yield
    _mini_tick_task.cancel()
    try:
        await _mini_tick_task
    except asyncio.CancelledError:
        pass
    # End the active session on graceful shutdown
    if _current_session_id:
        try:
            await _end_session(_current_session_id)
        except Exception as e:
            logger.warning(f"Could not finalize session on shutdown: {e}")
    await lakebase.close()
    print("Mission Control shutting down")


app = FastAPI(title="Mission Control", lifespan=lifespan)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled error on {request.method} {request.url.path}: {exc}")
    logger.error(traceback.format_exc())
    return JSONResponse(status_code=500, content={"detail": str(exc)})


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
    r = rows[0]
    speed = (float(r.get("velocity_x", 0))**2 + float(r.get("velocity_y", 0))**2 + float(r.get("velocity_z", 0))**2) ** 0.5
    fuel_kg = float(r.get("fuel_remaining_kg", 0))
    hull = float(r.get("hull_integrity", 100))
    status = "nominal"
    if fuel_kg < 100 or hull < 50:
        status = "critical"
    elif fuel_kg < 200 or hull < 75:
        status = "warning"
    elif fuel_kg < 300 or hull < 90:
        status = "caution"
    elapsed_s = float(r.get("mission_elapsed_s", 0))
    px = float(r.get("position_x", 0))
    py = float(r.get("position_y", 0))
    pz = float(r.get("position_z", 0))
    # Earth position at current elapsed time
    ex, ey = earth_position_at(elapsed_s)
    dist_to_earth = math.sqrt((px - ex) ** 2 + (py - ey) ** 2 + pz ** 2)

    return {
        "mission_id": r.get("state_id", 1),
        "spacecraft_name": r.get("mission_name", "Odyssey"),
        "status": status,
        "position": {"x": px, "y": py, "z": pz},
        "velocity": {"vx": float(r.get("velocity_x", 0)), "vy": float(r.get("velocity_y", 0)), "vz": float(r.get("velocity_z", 0)), "magnitude": speed},
        "fuel_remaining_pct": round(fuel_kg / FUEL_CAPACITY_KG * 100, 1),
        "hull_integrity_pct": hull,
        "comm_delay_seconds": float(r.get("communication_delay_s", 0)),
        "engine_status": r.get("engine_status", "nominal"),
        "mission_elapsed_days": round(elapsed_s / 86400, 2),
        "target": "Earth",
        "session_id": _current_session_id,
        "mission_outcome": r.get("mission_outcome") or (_last_outcome or {}).get("outcome"),
        "mission_outcome_detail": r.get("mission_outcome_detail") or (_last_outcome or {}).get("detail"),
        "distance_to_earth_km": round(dist_to_earth, 0),
        "captain": _ship_captain.get_state_summary() if _ship_captain else None,
    }


@app.get("/api/mission/clock")
async def get_simulation_clock():
    """Get current simulation clock state."""
    rows = await execute_lakebase(
        "SELECT * FROM simulation_clock WHERE clock_id = 1"
    )
    if not rows:
        raise HTTPException(status_code=404, detail="No clock state found")
    r = rows[0]
    elapsed_s = float(r.get("total_elapsed_s", 0))
    hours = int(elapsed_s // 3600)
    minutes = int((elapsed_s % 3600) // 60)
    seconds = int(elapsed_s % 60)
    sim_time = r.get("simulation_time", "")
    return {
        "mission_elapsed_time": f"{hours:03d}:{minutes:02d}:{seconds:02d}",
        "utc_time": str(sim_time) if sim_time else "N/A",
        "simulation_speed": float(r.get("time_scale", 1.0)),
        "is_running": bool(r.get("is_running", False)),
        "session_id": _current_session_id,
    }


@app.post("/api/simulation/start")
async def start_simulation():
    """Start the simulation: set is_running=true, boost time_scale, and trigger the simulation loop job."""
    # Ensure we have an active session
    if not _current_session_id:
        await _create_session()

    # Warm up all ML + LLM endpoints in the background (don't block start)
    asyncio.create_task(_warmup_models())
    logger.info("[START] Model warmup fired in background")
    _record_event("mission_start", f"Simulation started — session {(_current_session_id or '')[:8]}")

    await execute_lakebase(
        "UPDATE simulation_clock SET is_running = true, time_scale = COALESCE("
        "  NULLIF(time_scale, 1.0), 600.0"
        ") WHERE clock_id = 1"
    )
    # NOTE: Do NOT trigger the external simulation loop job here.
    # The app's internal mini_tick_loop (every 2s) is the sole simulation
    # driver.  Triggering the Databricks job creates a competing writer that
    # overwrites the clock/state with stale values, causing time regressions.
    logger.info("[START] Simulation clock started — mini_tick_loop is the primary driver")
    return {"status": "started", "session_id": _current_session_id}


@app.post("/api/simulation/stop")
async def stop_simulation():
    """Stop the simulation: set is_running=false and update session time_scale high-water mark."""
    # Update max_time_scale_used before stopping
    if _current_session_id:
        clock = await execute_lakebase(
            "SELECT time_scale FROM simulation_clock WHERE clock_id = 1"
        )
        if clock:
            ts = float(clock[0].get("time_scale", 1))
            await execute_lakebase(
                "UPDATE simulation_sessions SET "
                "  max_time_scale_used = GREATEST(COALESCE(max_time_scale_used, 1), :ts), "
                "  updated_at = NOW() "
                "WHERE session_id = :sid",
                {"ts": ts, "sid": _current_session_id},
            )

    await execute_lakebase(
        "UPDATE simulation_clock SET is_running = false WHERE clock_id = 1"
    )
    return {"status": "stopped", "session_id": _current_session_id}


@app.post("/api/simulation/timescale")
async def set_time_scale(request: Request):
    """Set simulation time scale (1 = realtime, 600 = 10 min per second)."""
    body = await request.json()
    scale = float(body.get("scale", 1.0))
    scale = max(1.0, min(scale, 3600.0))
    await execute_lakebase(
        "UPDATE simulation_clock SET time_scale = :scale WHERE clock_id = 1",
        {"scale": scale},
    )
    logger.info(f"Time scale set to {scale}x")
    return {"status": "ok", "time_scale": scale}


@app.post("/api/simulation/reposition")
async def reposition_simulation(request: Request):
    """
    Move the spacecraft to a new position/velocity for custom scenarios.
    Only allowed when simulation is stopped.
    """
    # Check that simulation is not running
    clock_rows = await execute_lakebase(
        "SELECT is_running FROM simulation_clock WHERE clock_id = 1"
    )
    if clock_rows and clock_rows[0].get("is_running"):
        raise HTTPException(
            status_code=409,
            detail="Stop the simulation before repositioning the spacecraft",
        )

    body = await request.json()
    px = float(body.get("position_x", 0))
    py = float(body.get("position_y", 0))
    pz = float(body.get("position_z", 0))
    vx = float(body.get("velocity_x", 0))
    vy = float(body.get("velocity_y", 0))
    vz = float(body.get("velocity_z", 0))
    fuel = float(body.get("fuel_remaining_kg", 1200.0))
    scenario_name = body.get("scenario_name", "Custom Scenario")

    # Validate: position must be between 0.3 AU and 6.0 AU from the Sun
    dist_au = math.sqrt(px**2 + py**2 + pz**2) / EARTH_ORBIT_RADIUS_KM
    if dist_au < 0.3:
        raise HTTPException(status_code=400, detail=f"Too close to the Sun ({dist_au:.2f} AU). Minimum is 0.3 AU.")
    if dist_au > 6.0:
        raise HTTPException(status_code=400, detail=f"Too far from the Sun ({dist_au:.2f} AU). Maximum is 6.0 AU.")

    # Validate speed: cap at 100 km/s
    speed = math.sqrt(vx**2 + vy**2 + vz**2)
    if speed > 100.0:
        raise HTTPException(status_code=400, detail=f"Speed {speed:.1f} km/s exceeds 100 km/s limit.")

    # Read current elapsed time for comm delay calculation
    state_rows = await execute_lakebase(
        "SELECT mission_elapsed_s FROM mission_state WHERE state_id = 1"
    )
    elapsed_s = float(state_rows[0]["mission_elapsed_s"]) if state_rows else 0.0
    ex, ey = earth_position_at(elapsed_s)
    comm_delay = compute_comm_delay(px, py, pz, ex, ey)

    # Update mission_state
    await execute_lakebase(
        "UPDATE mission_state SET "
        "  mission_name = :name, "
        "  position_x = :px, position_y = :py, position_z = :pz, "
        "  velocity_x = :vx, velocity_y = :vy, velocity_z = :vz, "
        "  fuel_remaining_kg = :fuel, "
        "  communication_delay_s = :comm, "
        "  updated_at = NOW()" +
        (",  mission_outcome = NULL, mission_outcome_detail = NULL " if _has_outcome_columns else " ") +
        "WHERE state_id = 1",
        {"name": scenario_name, "px": px, "py": py, "pz": pz,
         "vx": vx, "vy": vy, "vz": vz, "fuel": fuel, "comm": comm_delay},
    )

    # Update ground_state to match
    await execute_lakebase(
        "UPDATE ground_state SET "
        "  position_x = :px, position_y = :py, position_z = :pz, "
        "  velocity_x = :vx, velocity_y = :vy, velocity_z = :vz, "
        "  fuel_remaining_kg = :fuel, communication_delay_s = :comm, "
        "  updated_at = NOW() "
        "WHERE state_id = 1",
        {"px": px, "py": py, "pz": pz, "vx": vx, "vy": vy, "vz": vz,
         "fuel": fuel, "comm": comm_delay},
    )

    # Clear per-run tables (fresh scenario)
    await execute_lakebase("DELETE FROM command_queue")
    await execute_lakebase("DELETE FROM active_hazards")
    await execute_lakebase("DELETE FROM agent_messages_realtime")
    await execute_lakebase("DELETE FROM onboard_predictions")

    # Update session name if we have one
    if _current_session_id:
        await execute_lakebase(
            "UPDATE simulation_sessions SET "
            "  session_name = :name, scenario_type = 'custom', updated_at = NOW() "
            "WHERE session_id = :sid",
            {"name": scenario_name, "sid": _current_session_id},
        )

    logger.info(
        f"[REPOSITION] Ship moved to ({px/1e6:.1f}M, {py/1e6:.1f}M) km, "
        f"v=({vx:.2f}, {vy:.2f}) km/s, {dist_au:.2f} AU from Sun"
    )
    _record_event(
        "scenario_loaded",
        f"Scenario '{scenario_name}' — {dist_au:.2f} AU from Sun, fuel={fuel:.0f} kg",
        metadata={"scenario_name": scenario_name, "distance_au": round(dist_au, 2), "fuel_kg": fuel},
    )
    return {
        "status": "repositioned",
        "position_au": dist_au,
        "speed_km_s": speed,
        "comm_delay_s": comm_delay,
        "scenario_name": scenario_name,
    }


@app.post("/api/simulation/reset")
async def reset_simulation():
    """Reset simulation to fresh state: end current session, clear all ops tables, create new session."""
    # 1. End the current session
    if _current_session_id:
        try:
            await _end_session(_current_session_id)
        except Exception as e:
            logger.warning(f"Could not finalize session on reset: {e}")

    # 2. Initial position & velocity: Mars-return transfer orbit
    #
    # The ship starts at ~1.45 AU (near Mars orbit) at 267° in the orbital plane.
    # Velocity is a blend of Earth-intercept aim and prograde tangential, producing
    # an elliptical orbit with perihelion at ~0.5 AU that naturally passes within
    # ~360K km of Earth at ~day 159. The autopilot handles the final corrections
    # for orbit insertion.
    #
    # These values were computed by simulating forward under solar gravity and
    # searching for the starting angle + velocity blend that minimizes closest
    # approach to Earth while maintaining a physically valid orbit.
    #
    init_px = -11335491.0   # ~-0.076 AU (nearly on y-axis, below)
    init_py = -216294050.0  # ~-1.446 AU
    init_pz = 0.0
    init_vx = 19.0085       # km/s — mostly toward +x (sunward + prograde blend)
    init_vy = 11.8033       # km/s — prograde component
    init_vz = 0.0
    init_fuel = FUEL_CAPACITY_KG  # 1200 kg → ~10.1 km/s delta-v budget

    # 3. Reset mission_state
    await execute_lakebase(
        "UPDATE mission_state SET "
        "  mission_name = 'Odyssey Return', mission_status = 'active', "
        "  timestamp = NOW(), "
        "  position_x = :px, position_y = :py, position_z = :pz, "
        "  velocity_x = :vx, velocity_y = :vy, velocity_z = :vz, "
        "  fuel_remaining_kg = :fuel, hull_integrity = 100.0, "
        "  engine_status = 'nominal', "
        "  communication_delay_s = :comm, "
        "  mission_elapsed_s = 0, updated_at = NOW()" +
        (",  mission_outcome = NULL, mission_outcome_detail = NULL " if _has_outcome_columns else " ") +
        "WHERE state_id = 1",
        {"px": init_px, "py": init_py, "pz": init_pz,
         "vx": init_vx, "vy": init_vy, "vz": init_vz,
         "fuel": init_fuel,
         "comm": compute_comm_delay(init_px, init_py, init_pz,
                                     EARTH_ORBIT_RADIUS_KM, 0.0)},
    )

    # 4. Reset simulation_clock
    await execute_lakebase(
        "UPDATE simulation_clock SET "
        "  is_running = false, time_scale = 1.0, total_elapsed_s = 0, "
        "  simulation_time = NOW(), started_at = NULL, paused_at = NULL "
        "WHERE clock_id = 1"
    )

    # 5. Reset autopilot state
    await execute_lakebase(
        "UPDATE spacecraft_autopilot_state SET "
        "  mode = 'nominal', total_corrections = 0, total_evasions = 0, "
        "  total_commands_executed = 0, total_commands_rejected = 0, "
        "  fuel_used_by_autopilot_kg = 0, last_decision_time_s = 0, "
        "  ticks_processed = 0, updated_at = NOW() "
        "WHERE autopilot_id = 1"
    )

    # 6. Reset ground_state
    await execute_lakebase(
        "UPDATE ground_state SET "
        "  timestamp = NOW(), "
        "  position_x = :px, position_y = :py, position_z = :pz, "
        "  velocity_x = :vx, velocity_y = :vy, velocity_z = :vz, "
        "  fuel_remaining_kg = :fuel, hull_integrity = 100.0, "
        "  engine_status = 'nominal', communication_delay_s = 0, "
        "  mission_elapsed_s = 0, telemetry_age_s = 0, updated_at = NOW() "
        "WHERE state_id = 1",
        {"px": init_px, "py": init_py, "pz": init_pz,
         "vx": init_vx, "vy": init_vy, "vz": init_vz,
         "fuel": init_fuel},
    )

    # 7. Clear per-run operational tables
    await execute_lakebase("DELETE FROM command_queue")
    await execute_lakebase("DELETE FROM active_hazards")
    await execute_lakebase("DELETE FROM agent_messages_realtime")
    await execute_lakebase("DELETE FROM telemetry_realtime")
    await execute_lakebase("DELETE FROM agent_memory")
    await execute_lakebase("DELETE FROM onboard_predictions")
    if _captain_tables_ready:
        await execute_lakebase("DELETE FROM captain_decisions")
        await execute_lakebase("DELETE FROM captain_mc_dialogue")
    try:
        await execute_lakebase("DELETE FROM mission_events")
    except Exception:
        pass  # table may not exist yet

    # 8. Reset tick counters
    global _trim_counter, _autopilot_counter, _prediction_counter, _agent_counter, _last_outcome, _delta_stream_counter, _last_burn_sim_time
    _trim_counter = 0
    _autopilot_counter = 0
    _last_burn_sim_time = 0.0
    _prediction_counter = 0
    _agent_counter = 0
    _delta_stream_counter = 0
    _last_outcome = None
    _agent_memory.reset()
    _event_buffer.clear()
    _inference_log_buffer.clear()
    global _captain_counter
    _captain_counter = 0
    _captain_decisions.clear()
    if _ship_captain:
        from ship_captain import CaptainState
        _ship_captain.state = CaptainState()
    global _prev_dist_to_earth, _prev_eta_s
    _prev_dist_to_earth = 0.0
    _prev_eta_s = 0.0
    global _last_known_pos, _last_written_elapsed
    _last_known_pos = None
    _last_written_elapsed = 0.0
    _prediction_buffer.clear()
    _telemetry_buffer.clear()
    global _model_preference, _drift_retrain_triggered
    _ml_recent_errors.clear()
    _phys_recent_errors.clear()
    _model_preference = "model_serving"
    _drift_retrain_triggered = False
    _ops_tracker["reads"] = 0
    _ops_tracker["writes"] = 0
    _ops_tracker["latencies_ms"] = []
    _ops_tracker["window_start"] = time.monotonic()

    # 9. Create a new session
    new_session_id = await _create_session(
        scenario_name="Odyssey Return",
        scenario_type="default",
        px=init_px, py=init_py, pz=init_pz,
        vx=init_vx, vy=init_vy, vz=init_vz,
        fuel=init_fuel,
    )

    logger.info(f"[RESET] Simulation reset complete — new session {new_session_id[:8]}")
    return {
        "status": "reset",
        "session_id": new_session_id,
        "message": "Simulation reset to initial state near Earth",
    }


@app.get("/api/sessions")
async def list_sessions(limit: int = 20):
    """List simulation sessions (most recent first)."""
    rows = await execute_lakebase(
        "SELECT session_id, session_name, scenario_type, started_at, ended_at, "
        "  duration_sim_seconds, max_time_scale_used, "
        "  total_burns_executed, total_corrections, total_fuel_used_kg, "
        "  total_hazards_encountered, final_distance_to_earth_km, "
        "  outcome, status "
        "FROM simulation_sessions ORDER BY started_at DESC LIMIT :lim",
        {"lim": min(limit, 100)},
    )
    return {"sessions": rows, "count": len(rows), "active_session_id": _current_session_id}


@app.get("/api/sessions/{session_id}")
async def get_session(session_id: str):
    """Get details for a specific simulation session."""
    rows = await execute_lakebase(
        "SELECT * FROM simulation_sessions WHERE session_id = :sid",
        {"sid": session_id},
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Session not found")
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
    hazards = []
    for r in rows:
        hazards.append({
            "id": r.get("hazard_id", ""),
            "type": r.get("hazard_type", "asteroid"),
            "name": f'{r.get("hazard_type", "Unknown")} {str(r.get("hazard_id", ""))[:6]}',
            "risk_score": float(r.get("risk_score", 0)),
            "closest_approach_time": str(r.get("closest_approach_time", "")),
            "closest_approach_km": float(r.get("closest_approach_km", 0)),
            "position_x": float(r.get("position_x") or 0),
            "position_y": float(r.get("position_y") or 0),
            "status": "tracking",
            "description": f'Radius: {float(r.get("radius_km", 0)):.0f} km',
        })
    return {"hazards": hazards, "count": len(hazards)}


@app.post("/api/hazards/inject")
async def inject_hazard(request: Request):
    """Inject a hazard (asteroid/meteor) at a specific position on the map."""
    import uuid as _uuid

    body = await request.json()
    hazard_id = str(_uuid.uuid4())
    hazard_type = body.get("hazard_type", "asteroid")
    position_x = float(body.get("position_x", 0))  # km
    position_y = float(body.get("position_y", 0))  # km
    velocity_x = float(body.get("velocity_x", 0))  # km/s
    velocity_y = float(body.get("velocity_y", 0))  # km/s
    radius_km = float(body.get("radius_km", 5.0))

    # Calculate risk score based on proximity to spacecraft
    ship_x = 0.0
    ship_y = 0.0
    if _current_session_id:
        state_rows = await execute_lakebase(
            "SELECT position_x, position_y FROM mission_state WHERE session_id = :sid LIMIT 1",
            {"sid": _current_session_id},
        )
        if state_rows:
            ship_x = float(state_rows[0].get("position_x", 0))
            ship_y = float(state_rows[0].get("position_y", 0))

    dist_to_ship = math.sqrt((position_x - ship_x) ** 2 + (position_y - ship_y) ** 2)
    # Risk: closer = higher risk (0.0 to 1.0 scale)
    risk_score = max(0.1, min(0.95, 0.95 - (dist_to_ship / 1e8) * 0.5))

    # Closest approach: approximate based on velocity toward ship
    dx = ship_x - position_x
    dy = ship_y - position_y
    dist = math.sqrt(dx * dx + dy * dy) or 1.0
    # Component of hazard velocity toward ship
    closing_speed = (velocity_x * dx + velocity_y * dy) / dist
    if closing_speed > 0:
        closest_approach_km = max(0, dist - radius_km * 10)
    else:
        closest_approach_km = dist

    await execute_lakebase(
        "INSERT INTO active_hazards "
        "(hazard_id, hazard_type, position_x, position_y, position_z, "
        " velocity_x, velocity_y, velocity_z, radius_km, risk_score, "
        " closest_approach_km, detected_at, updated_at, session_id) "
        "VALUES (:hid, :htype, :px, :py, 0, :vx, :vy, 0, :radius, :risk, "
        "  :approach, NOW(), NOW(), :sid)",
        {
            "hid": hazard_id,
            "htype": hazard_type,
            "px": position_x,
            "py": position_y,
            "vx": velocity_x,
            "vy": velocity_y,
            "radius": radius_km,
            "risk": risk_score,
            "approach": closest_approach_km,
            "sid": _current_session_id or "",
        },
    )

    logger.info(
        f"[HAZARD] Injected {hazard_type} {hazard_id[:8]} at "
        f"({position_x/1e6:.1f}M, {position_y/1e6:.1f}M) km, "
        f"risk={risk_score:.0f}, dist_to_ship={dist_to_ship/1e6:.1f}M km"
    )
    _record_event(
        "hazard_injected",
        f"{hazard_type.title()} detected at {dist_to_ship/1e6:.1f}M km — risk {risk_score:.0%}",
        metadata={"hazard_id": hazard_id, "hazard_type": hazard_type, "risk_score": round(risk_score, 2)},
    )

    return {
        "status": "injected",
        "hazard_id": hazard_id,
        "hazard_type": hazard_type,
        "risk_score": risk_score,
        "closest_approach_km": closest_approach_km,
    }


@app.get("/api/commands/queue")
async def get_command_queue():
    """Get current command queue from Lakebase."""
    rows = await execute_lakebase(
        "SELECT * FROM command_queue ORDER BY created_at DESC LIMIT 20"
    )
    commands = []
    for r in rows:
        commands.append({
            "id": r.get("command_id", ""),
            "type": r.get("command_type", "unknown"),
            "transmit_time": str(r.get("transmit_time", "pending")),
            "estimated_receive_time": str(r.get("estimated_receive_time", "pending")),
            "status": r.get("status", "pending"),
            "payload_summary": str(r.get("payload", ""))[:80] or "No payload",
        })
    return {"commands": commands, "count": len(commands)}


@app.post("/api/commands/approve")
async def approve_command(command_id: str):
    """Operator approves a pending command for transmission."""
    await execute_lakebase(
        "UPDATE command_queue SET status = 'approved', updated_at = NOW() "
        "WHERE command_id = :command_id AND status = 'pending'",
        {"command_id": command_id},
    )
    return {"status": "approved", "command_id": command_id}


@app.post("/api/commands/manual-burn")
async def manual_burn(request: Request):
    """Queue a manual burn command from the operator."""
    import json as _json
    import uuid as _uuid

    body = await request.json()
    direction = body.get("direction", "prograde")
    delta_v = float(body.get("delta_v", 0.5))
    burn_duration_s = float(body.get("burn_duration_s", 30))

    # Validate burn parameters
    MAX_DELTA_V = 5.0  # km/s
    MAX_BURN_DURATION = 300.0  # seconds
    if delta_v > MAX_DELTA_V:
        raise HTTPException(status_code=400, detail=f"Delta-V exceeds maximum of {MAX_DELTA_V} km/s")
    if delta_v <= 0:
        raise HTTPException(status_code=400, detail="Delta-V must be positive")
    if burn_duration_s > MAX_BURN_DURATION:
        raise HTTPException(status_code=400, detail=f"Burn duration exceeds maximum of {MAX_BURN_DURATION}s")
    if burn_duration_s <= 0:
        raise HTTPException(status_code=400, detail="Burn duration must be positive")

    # Read current state to compute burn vector
    state = await execute_lakebase(
        "SELECT velocity_x, velocity_y, velocity_z, "
        "  position_x, position_y, position_z, communication_delay_s "
        "FROM mission_state WHERE state_id = 1"
    )
    if not state:
        raise HTTPException(status_code=400, detail="No mission state available")

    s = state[0]
    vx = float(s.get("velocity_x") or 0)
    vy = float(s.get("velocity_y") or 0)
    vz = float(s.get("velocity_z") or 0)
    px = float(s.get("position_x") or 0)
    py = float(s.get("position_y") or 0)
    pz = float(s.get("position_z") or 0)
    comm_delay = float(s.get("communication_delay_s") or 0)

    speed = math.sqrt(vx * vx + vy * vy + vz * vz) or 1.0
    dist = math.sqrt(px * px + py * py + pz * pz) or 1.0

    # Compute unit burn vector based on direction
    if direction == "prograde":
        bvx, bvy, bvz = vx / speed, vy / speed, vz / speed
    elif direction == "retrograde":
        bvx, bvy, bvz = -vx / speed, -vy / speed, -vz / speed
    elif direction == "earth":
        bvx, bvy, bvz = -px / dist, -py / dist, -pz / dist
    elif direction == "brake_earth":
        # Retrograde burn to slow down for Earth orbit insertion
        bvx, bvy, bvz = -vx / speed, -vy / speed, -vz / speed
    elif direction == "radial_in":
        bvx, bvy, bvz = -px / dist, -py / dist, -pz / dist
    elif direction == "radial_out":
        bvx, bvy, bvz = px / dist, py / dist, pz / dist
    elif direction == "normal":
        bvx, bvy, bvz = 0.0, 0.0, 1.0
    else:
        bvx, bvy, bvz = vx / speed, vy / speed, vz / speed

    # Scale by delta_v / burn_duration to get acceleration
    accel = delta_v / burn_duration_s if burn_duration_s > 0 else delta_v
    bvx *= accel
    bvy *= accel
    bvz *= accel

    cmd_id = str(_uuid.uuid4())
    payload = _json.dumps({
        "burn_vector_x": round(bvx, 8),
        "burn_vector_y": round(bvy, 8),
        "burn_vector_z": round(bvz, 8),
        "burn_duration_s": round(burn_duration_s, 2),
        "direction": direction,
        "delta_v_requested": round(delta_v, 4),
    })

    # Read sim clock for estimated receive time
    clock = await execute_lakebase(
        "SELECT simulation_time FROM simulation_clock WHERE clock_id = 1"
    )
    sim_time = clock[0].get("simulation_time") if clock else None
    from datetime import timedelta as _td
    est_receive = sim_time + _td(seconds=comm_delay) if sim_time else None

    await execute_lakebase(
        "INSERT INTO command_queue (command_id, command_type, payload, priority, "
        "  created_at, approved_by, approved_at, transmit_time, estimated_receive_time, "
        "  status, session_id, updated_at) "
        "VALUES (:cid, 'burn', :payload, 5, NOW(), 'operator', NOW(), NOW(), "
        "  :est_receive, 'in_flight', :sid, NOW())",
        {
            "cid": cmd_id,
            "payload": payload,
            "est_receive": est_receive,
            "sid": _current_session_id,
        },
    )

    logger.info(
        f"[MANUAL BURN] Queued {direction} burn: dv={delta_v} km/s, "
        f"dur={burn_duration_s}s, cmd={cmd_id[:8]}"
    )
    return {
        "status": "queued",
        "command_id": cmd_id,
        "direction": direction,
        "delta_v": delta_v,
        "burn_duration_s": burn_duration_s,
        "estimated_receive_delay_s": comm_delay,
    }


@app.post("/api/maneuvers/{maneuver_id}/approve")
async def approve_maneuver(maneuver_id: str):
    """Approve an agent-generated maneuver candidate — creates a burn command."""
    import json as _json
    import uuid as _uuid

    # Look up the maneuver in Delta Lake
    try:
        rows = execute_sql(f"""
            SELECT burn_vector_x, burn_vector_y, burn_vector_z, burn_duration_s,
                   fuel_cost_kg, delta_v, status
            FROM `{CATALOG}`.navigation.candidate_maneuvers
            WHERE maneuver_id = '{maneuver_id}'
            LIMIT 1
        """)
    except Exception:
        raise HTTPException(status_code=404, detail="Candidate maneuvers table not available")
    if not rows:
        raise HTTPException(status_code=404, detail="Maneuver not found")

    m = rows[0]
    if m.get("status") not in ("proposed", "pending"):
        raise HTTPException(status_code=400, detail=f"Maneuver already {m.get('status')}")

    # Update maneuver status
    execute_sql(f"""
        UPDATE `{CATALOG}`.navigation.candidate_maneuvers
        SET status = 'approved'
        WHERE maneuver_id = '{maneuver_id}'
    """)

    # Create a burn command
    cmd_id = str(_uuid.uuid4())
    payload = _json.dumps({
        "burn_vector_x": float(m.get("burn_vector_x") or 0),
        "burn_vector_y": float(m.get("burn_vector_y") or 0),
        "burn_vector_z": float(m.get("burn_vector_z") or 0),
        "burn_duration_s": float(m.get("burn_duration_s") or 30),
        "maneuver_id": maneuver_id,
    })

    clock = await execute_lakebase(
        "SELECT simulation_time FROM simulation_clock WHERE clock_id = 1"
    )
    sim_time = clock[0].get("simulation_time") if clock else None
    state = await execute_lakebase(
        "SELECT communication_delay_s FROM mission_state WHERE state_id = 1"
    )
    comm_delay = float(state[0].get("communication_delay_s") or 0) if state else 0
    from datetime import timedelta as _td
    est_receive = sim_time + _td(seconds=comm_delay) if sim_time else None

    await execute_lakebase(
        "INSERT INTO command_queue (command_id, command_type, payload, priority, "
        "  created_at, approved_by, approved_at, transmit_time, estimated_receive_time, "
        "  status, session_id, updated_at) "
        "VALUES (:cid, 'burn', :payload, 4, NOW(), 'operator', NOW(), NOW(), "
        "  :est_receive, 'in_flight', :sid, NOW())",
        {
            "cid": cmd_id,
            "payload": payload,
            "est_receive": est_receive,
            "sid": _current_session_id,
        },
    )

    logger.info(f"[MANEUVER] Approved maneuver {maneuver_id[:8]}, queued burn cmd {cmd_id[:8]}")
    return {"status": "approved", "maneuver_id": maneuver_id, "command_id": cmd_id}


@app.post("/api/maneuvers/{maneuver_id}/reject")
async def reject_maneuver(maneuver_id: str):
    """Reject an agent-generated maneuver candidate."""
    try:
        execute_sql(f"""
            UPDATE `{CATALOG}`.navigation.candidate_maneuvers
            SET status = 'rejected'
            WHERE maneuver_id = '{maneuver_id}'
        """)
    except Exception:
        raise HTTPException(status_code=404, detail="Candidate maneuvers table not available")
    logger.info(f"[MANEUVER] Rejected maneuver {maneuver_id[:8]}")
    return {"status": "rejected", "maneuver_id": maneuver_id}


@app.get("/api/agents/messages/latest")
async def get_latest_agent_messages():
    """Get agent messages from the most recent decision tick."""
    rows = await execute_lakebase(
        "SELECT * FROM agent_messages_realtime "
        "WHERE session_id = :sid "
        "ORDER BY created_at DESC LIMIT 100",
        {"sid": _current_session_id},
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


@app.get("/api/predictions/onboard")
async def get_onboard_predictions(limit: int = 10):
    """Get recent onboard trajectory predictions from Lakebase."""
    rows = await execute_lakebase(
        "SELECT prediction_id, simulation_time_s, "
        "  current_pos_x, current_pos_y, current_pos_z, "
        "  predicted_pos_x, predicted_pos_y, predicted_pos_z, "
        "  prediction_horizon_s, prediction_source, "
        "  assessment, action_taken, correction_dv, "
        "  actual_pos_x, actual_pos_y, actual_pos_z, "
        "  prediction_error_km, session_id, created_at "
        "FROM onboard_predictions "
        "WHERE session_id = :sid "
        "ORDER BY simulation_time_s DESC LIMIT :lim",
        {"sid": _current_session_id, "lim": min(limit, 50)},
    )
    predictions = []
    for r in rows:
        predictions.append({
            "id": r.get("prediction_id", ""),
            "simulation_time_s": float(r.get("simulation_time_s", 0)),
            "current_position": {
                "x": float(r.get("current_pos_x", 0)),
                "y": float(r.get("current_pos_y", 0)),
                "z": float(r.get("current_pos_z", 0)),
            },
            "predicted_position": {
                "x": float(r.get("predicted_pos_x", 0)),
                "y": float(r.get("predicted_pos_y", 0)),
                "z": float(r.get("predicted_pos_z", 0)),
            },
            "prediction_horizon_s": float(r.get("prediction_horizon_s", 60)),
            "source": r.get("prediction_source", "unknown"),
            "assessment": r.get("assessment", "unknown"),
            "action_taken": r.get("action_taken", "none"),
            "correction_dv": float(r.get("correction_dv", 0)),
            "actual_position": {
                "x": float(r.get("actual_pos_x", 0)),
                "y": float(r.get("actual_pos_y", 0)),
                "z": float(r.get("actual_pos_z", 0)),
            } if r.get("actual_pos_x") is not None else None,
            "prediction_error_km": float(r.get("prediction_error_km", 0))
                if r.get("prediction_error_km") is not None else None,
            "created_at": str(r.get("created_at", "")),
        })
    return {"predictions": predictions, "count": len(predictions)}


@app.get("/api/predictions/accuracy")
async def get_prediction_accuracy():
    """Get prediction accuracy metrics for the current session."""
    if not _current_session_id:
        return {"error": "No active session", "metrics": None}

    rows = await execute_lakebase(
        "SELECT "
        "  COUNT(*) as total_predictions, "
        "  COUNT(prediction_error_km) as backfilled_count, "
        "  AVG(prediction_error_km) as avg_error_km, "
        "  MIN(prediction_error_km) as min_error_km, "
        "  MAX(prediction_error_km) as max_error_km, "
        "  SUM(CASE WHEN prediction_source = 'model_serving' THEN 1 ELSE 0 END) as ml_count, "
        "  SUM(CASE WHEN prediction_source = 'physics_fallback' THEN 1 ELSE 0 END) as physics_count, "
        "  SUM(CASE WHEN assessment = 'on_course' THEN 1 ELSE 0 END) as on_course_count, "
        "  SUM(CASE WHEN assessment = 'minor_deviation' THEN 1 ELSE 0 END) as minor_deviation_count, "
        "  SUM(CASE WHEN assessment = 'correction_needed' THEN 1 ELSE 0 END) as correction_count, "
        "  SUM(CASE WHEN action_taken != 'none' THEN 1 ELSE 0 END) as corrections_applied "
        "FROM onboard_predictions "
        "WHERE session_id = :sid",
        {"sid": _current_session_id},
    )
    if not rows:
        return {"metrics": None}

    r = rows[0]
    total = int(r.get("total_predictions", 0))
    backfilled = int(r.get("backfilled_count", 0))

    # Recent trend: last 5 backfilled predictions
    trend_rows = await execute_lakebase(
        "SELECT prediction_error_km, prediction_source, assessment "
        "FROM onboard_predictions "
        "WHERE session_id = :sid AND prediction_error_km IS NOT NULL "
        "ORDER BY simulation_time_s DESC LIMIT 5",
        {"sid": _current_session_id},
    )
    trend = [float(r.get("prediction_error_km") or 0) for r in trend_rows]
    # Drift detection: if last 3 errors are all above 2x the session average
    avg_err = float(r.get("avg_error_km", 0)) if r.get("avg_error_km") else 0
    recent_avg = sum(trend[:3]) / len(trend[:3]) if len(trend) >= 3 else 0
    model_drifting = recent_avg > avg_err * 2 and avg_err > 0 and len(trend) >= 3

    # Per-source accuracy breakdown
    source_rows = await execute_lakebase(
        "SELECT prediction_source, "
        "  AVG(prediction_error_km) as avg_error, "
        "  COUNT(*) as count "
        "FROM onboard_predictions "
        "WHERE session_id = :sid AND prediction_error_km IS NOT NULL "
        "GROUP BY prediction_source",
        {"sid": _current_session_id},
    ) or []
    source_accuracy = {}
    for sr in source_rows:
        src = sr.get("prediction_source", "unknown")
        source_accuracy[src] = {
            "avg_error_km": round(float(sr.get("avg_error", 0) or 0), 1),
            "backfilled_count": int(sr.get("count", 0)),
        }

    return {
        "metrics": {
            "total_predictions": total,
            "backfilled_count": backfilled,
            "avg_error_km": float(r.get("avg_error_km", 0)) if r.get("avg_error_km") else None,
            "min_error_km": float(r.get("min_error_km", 0)) if r.get("min_error_km") else None,
            "max_error_km": float(r.get("max_error_km", 0)) if r.get("max_error_km") else None,
            "ml_predictions": int(r.get("ml_count") or 0),
            "physics_predictions": int(r.get("physics_count") or 0),
            "assessments": {
                "on_course": int(r.get("on_course_count") or 0),
                "minor_deviation": int(r.get("minor_deviation_count") or 0),
                "correction_needed": int(r.get("correction_count") or 0),
            },
            "corrections_applied": int(r.get("corrections_applied") or 0),
            "recent_error_trend": trend,
            "model_drifting": model_drifting,
            "active_model": _model_preference,
            "source_accuracy": source_accuracy,
            "drift_retrain_triggered": _drift_retrain_triggered,
        },
        "session_id": _current_session_id,
    }


@app.post("/api/models/warmup")
async def warmup_models():
    """Ping all ML + LLM endpoints to wake them from scale-to-zero."""
    status = await _warmup_models()
    ready = sum(1 for v in status.values() if v["status"] == "ready")
    reachable = sum(1 for v in status.values() if "reachable" in v["status"])
    total = len(status)
    return {"status": "complete", "ready": ready, "reachable": reachable, "total": total, "endpoints": status}


@app.post("/api/models/retrain")
async def trigger_model_retrain(request: Request):
    """Trigger retraining of the trajectory prediction model via Databricks Jobs API."""
    RETRAIN_JOB_ID = os.environ.get("RETRAIN_JOB_ID", "")
    if not RETRAIN_JOB_ID:
        logger.warning("[RETRAIN] RETRAIN_JOB_ID not configured — returning mock success for demo")
        return {
            "status": "simulated",
            "job_id": None,
            "run_id": None,
            "message": "Retrain triggered (demo mode). Set RETRAIN_JOB_ID env var to connect to a real Databricks Job.",
        }

    try:
        w = get_workspace_client()
        run = w.jobs.run_now(job_id=int(RETRAIN_JOB_ID))
        logger.info(f"[RETRAIN] Triggered model retrain job {RETRAIN_JOB_ID}, run_id={run.run_id}")
        return {
            "status": "triggered",
            "job_id": RETRAIN_JOB_ID,
            "run_id": run.run_id,
            "message": "Model retraining started. New model version will be deployed automatically.",
        }
    except Exception as e:
        logger.error(f"Failed to trigger retrain job: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to trigger retrain: {e}")


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


@app.get("/api/throughput/current")
async def get_throughput_current():
    """Live throughput snapshot: in-memory counters + recent DB rows for sparkline."""
    now = time.monotonic()
    window = max(now - _ops_tracker["window_start"], 0.1)
    reads = _ops_tracker["reads"]
    writes = _ops_tracker["writes"]
    total = reads + writes
    lats = _ops_tracker["latencies_ms"]
    sorted_lats = sorted(lats) if lats else [0.0]
    p50 = sorted_lats[len(sorted_lats) // 2] if sorted_lats else 0.0
    p99_idx = min(int(len(sorted_lats) * 0.99), len(sorted_lats) - 1)
    p99 = sorted_lats[p99_idx] if sorted_lats else 0.0

    # Recent history for sparkline (last 30 throughput_metrics rows)
    history_rows = await execute_lakebase(
        "SELECT ops_per_second, read_ops, write_ops, sim_seconds_processed, rows_generated "
        "FROM throughput_metrics WHERE component = 'mini_tick' "
        "ORDER BY timestamp DESC LIMIT 30"
    )
    sparkline = [float(r.get("ops_per_second", 0)) for r in reversed(history_rows)]

    return {
        "ops_per_second": round(total / window, 1),
        "reads": reads,
        "writes": writes,
        "total_ops": total,
        "window_seconds": round(window, 1),
        "latency_p50_ms": round(p50, 2),
        "latency_p99_ms": round(p99, 2),
        "sub_ticks": _ops_tracker["sub_ticks_last"],
        "telemetry_writes_per_tick": _ops_tracker["telemetry_writes_last"],
        "sparkline": sparkline,
        "pool": lakebase.pool_status(),
    }


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
    # Transform to frontend TrajectoryData format
    spacecraft_path = [
        {"x": float(r.get("position_x", 0)), "y": float(r.get("position_y", 0)), "timestamp": str(r.get("timestamp", ""))}
        for r in rows
    ]
    # Static celestial body positions for the navigation map
    celestial_bodies = [
        {"name": "Sun", "x": 0, "y": 0, "color": "#ffcc00", "radius": 20},
        {"name": "Earth", "x": 149600000, "y": 0, "color": "#4488ff", "radius": 8},
        {"name": "Mars", "x": 227900000, "y": 0, "color": "#ff4422", "radius": 6},
    ]
    return {
        "spacecraft_path": spacecraft_path,
        "celestial_bodies": celestial_bodies,
        "hazard_positions": [],
    }


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
    try:
        rows = execute_sql(f"""
            SELECT *
            FROM `{CATALOG}`.navigation.candidate_maneuvers
            WHERE status = 'proposed'
            ORDER BY ranking ASC
            LIMIT 10
        """)
    except Exception:
        return {"maneuvers": [], "count": 0}
    maneuvers = []
    for r in rows:
        dv = float(r.get("delta_v", 0))
        fuel_kg = float(r.get("fuel_cost_kg", 0))
        feasibility_score = float(r.get("feasibility_score", 0))
        feasibility = "high" if feasibility_score >= 0.7 else ("medium" if feasibility_score >= 0.4 else "low")
        maneuvers.append({
            "id": r.get("maneuver_id", ""),
            "rank": int(r.get("ranking", 0)),
            "name": f"Burn {str(r.get('maneuver_id', ''))[:6]}",
            "delta_v_ms": round(dv * 1000, 1),
            "fuel_cost_pct": round(fuel_kg / FUEL_CAPACITY_KG * 100, 1),
            "risk_reduction": float(r.get("risk_reduction_score", 0)),
            "feasibility": feasibility,
            "status": r.get("status", "proposed"),
            "description": f"Δv={dv:.3f} km/s, burn {float(r.get('burn_duration_s', 0)):.0f}s",
        })
    return {"maneuvers": maneuvers, "count": len(maneuvers)}


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
    """Get agent decisions — tries Lakebase realtime first, falls back to Delta."""
    import json as _json

    # First try Lakebase realtime (in-app agent chain writes here)
    where_parts = ["to_agent != 'broadcast'"]
    params: dict = {}
    if agent_name:
        where_parts.append("from_agent = :agent")
        params["agent"] = agent_name
    if _current_session_id:
        where_parts.append("session_id = :sid")
        params["sid"] = _current_session_id

    where_sql = " AND ".join(where_parts)
    lb_rows = await execute_lakebase(
        f"SELECT * FROM agent_messages_realtime "
        f"WHERE {where_sql} "
        f"ORDER BY created_at DESC LIMIT :lim",
        {**params, "lim": min(limit, 100)},
    ) or []

    if lb_rows:
        decisions = []
        for r in lb_rows:
            # Parse content JSON for reasoning
            content_raw = r.get("content", "{}")
            try:
                content = _json.loads(content_raw) if isinstance(content_raw, str) else (content_raw or {})
            except Exception:
                content = {}
            reasoning = content.get("reasoning", content.get("summary", str(content_raw)[:200]))
            confidence = float(content.get("confidence", 0.75))
            action = content.get("decision", content.get("action", r.get("message_type", "analysis")))

            decisions.append({
                "id": r.get("message_id", ""),
                "agent_name": r.get("from_agent", "unknown"),
                "timestamp": str(r.get("created_at", "")),
                "reasoning": reasoning,
                "confidence": confidence,
                "action_taken": action,
            })
        return {"decisions": decisions, "count": len(decisions)}

    # Fallback: Delta Lake
    delta_where = ""
    if agent_name:
        delta_where = f"WHERE agent_name = '{agent_name}'"

    rows = execute_sql(f"""
        SELECT *
        FROM `{CATALOG}`.agents.decision_log
        {delta_where}
        ORDER BY timestamp DESC
        LIMIT {min(limit, 100)}
    """)
    decisions = []
    for r in rows:
        decisions.append({
            "id": r.get("decision_id", r.get("id", "")),
            "agent_name": r.get("agent_name", "unknown"),
            "timestamp": str(r.get("timestamp", "")),
            "reasoning": r.get("reasoning", r.get("explanation", "No reasoning provided")),
            "confidence": float(r.get("confidence_score", r.get("confidence", 0))),
            "action_taken": r.get("action_taken", r.get("decision_type", "observe")),
        })
    return {"decisions": decisions, "count": len(decisions)}


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


@app.get("/api/agents/memory")
async def get_agent_memory():
    """Get cross-tick agent memory — observations, patterns, and trends."""
    return _agent_memory.get_summary()


@app.get("/api/events")
async def get_mission_events():
    """Get recent mission events (event sourcing feed)."""
    return _event_buffer[-50:]  # last 50 events, newest last


@app.get("/api/inference/stats")
async def get_inference_stats():
    """Get inference log stats — model call counts, latencies, and buffer size."""
    from collections import Counter, defaultdict
    counts: Counter = Counter()
    latencies: defaultdict[str, list[float]] = defaultdict(list)
    for entry in _inference_log_buffer:
        ep = entry.get("endpoint_name", "unknown")
        counts[ep] += 1
        lat = entry.get("latency_ms", 0)
        if lat > 0:
            latencies[ep].append(lat)
    stats = {}
    for ep, count in counts.items():
        lats = sorted(latencies.get(ep, []))
        stats[ep] = {
            "calls": count,
            "avg_latency_ms": round(sum(lats) / len(lats), 1) if lats else 0,
            "p50_latency_ms": round(lats[len(lats) // 2], 1) if lats else 0,
            "p99_latency_ms": round(lats[min(int(len(lats) * 0.99), len(lats) - 1)], 1) if lats else 0,
        }
    return {
        "total_calls": sum(counts.values()),
        "buffer_size": len(_inference_log_buffer),
        "endpoints": stats,
    }


@app.get("/api/captain/state")
async def get_captain_state():
    """Get the ship captain's current state and statistics."""
    if not _ship_captain:
        return {"captain_available": False}
    summary = _ship_captain.get_state_summary()
    summary["captain_available"] = True
    summary["recent_decisions"] = _captain_decisions[-10:]
    return summary


@app.get("/api/captain/decisions")
async def get_captain_decisions(limit: int = 30):
    """Get recent captain decisions from Lakebase."""
    if not _captain_tables_ready:
        return {"decisions": [], "count": 0}
    rows = await execute_lakebase(
        "SELECT decision_id, simulation_time_s, action, priority_level, "
        "  reasoning, override_of_command_id, original_command_summary, "
        "  captain_alternative_summary, delta_v, fuel_cost_kg, "
        "  alert_level, confidence, elapsed_ms, created_at "
        "FROM captain_decisions "
        "WHERE session_id = :sid "
        "ORDER BY created_at DESC LIMIT :lim",
        {"sid": _current_session_id, "lim": min(limit, 100)},
    ) or []
    return {"decisions": rows, "count": len(rows)}


@app.get("/api/captain/dialogue")
async def get_captain_dialogue(limit: int = 20):
    """Get captain<->MC dialogue log."""
    if not _captain_tables_ready:
        return {"messages": [], "count": 0}
    rows = await execute_lakebase(
        "SELECT dialogue_id, simulation_time_s, direction, speaker, "
        "  message_type, content, related_decision_id, created_at "
        "FROM captain_mc_dialogue "
        "WHERE session_id = :sid "
        "ORDER BY created_at DESC LIMIT :lim",
        {"sid": _current_session_id, "lim": min(limit, 100)},
    ) or []
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
            content = f.read()
        return HTMLResponse(
            content=content,
            headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache"},
        )

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
