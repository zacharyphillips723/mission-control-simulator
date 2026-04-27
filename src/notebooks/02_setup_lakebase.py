# Databricks notebook source
# MAGIC %md
# MAGIC # Mission Control — Lakebase Autoscaling Setup
# MAGIC Creates a **Lakebase Autoscaling** project with a production branch and operational Postgres tables
# MAGIC for low-latency mission state, command queue, and agent memory.
# MAGIC
# MAGIC **Lakebase Autoscaling** provides:
# MAGIC - Sub-10ms reads/writes via direct Postgres connections
# MAGIC - Autoscaling compute (2–8 CU, ~4–16 GB RAM)
# MAGIC - Scale-to-zero on non-production branches
# MAGIC - Git-like branching for dev/test isolation

# COMMAND ----------

# MAGIC %pip install -U "databricks-sdk>=0.81.0" "psycopg[binary]>=3.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("lakebase_project_id", "mission-control", "Lakebase Project ID")
dbutils.widgets.text("min_cu", "2.0", "Min Compute Units")
dbutils.widgets.text("max_cu", "8.0", "Max Compute Units")

project_id = dbutils.widgets.get("lakebase_project_id")
min_cu = float(dbutils.widgets.get("min_cu"))
max_cu = float(dbutils.widgets.get("max_cu"))

print(f"Project: {project_id}  |  Autoscale range: {min_cu}–{max_cu} CU ({min_cu * 2:.0f}–{max_cu * 2:.0f} GB RAM)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Create the Lakebase Autoscaling Project
# MAGIC A project auto-creates a `production` branch, default compute endpoint, and `databricks_postgres` database.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import (
    Endpoint,
    EndpointSpec,
    EndpointType,
    FieldMask,
    Project,
    ProjectSpec,
)

w = WorkspaceClient()

# Check if project already exists
existing_projects = {p.name.split("/")[-1]: p for p in w.postgres.list_projects()}

if project_id in existing_projects:
    print(f"Project '{project_id}' already exists — skipping creation")
    project = w.postgres.get_project(name=f"projects/{project_id}")
else:
    print(f"Creating Lakebase Autoscaling project '{project_id}'...")
    operation = w.postgres.create_project(
        project=Project(
            spec=ProjectSpec(
                display_name="Mission Control Simulator",
                pg_version="17",
            )
        ),
        project_id=project_id,
    )
    project = operation.wait()
    print(f"Project created: {project.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Configure Autoscaling on Production Compute
# MAGIC Set the primary endpoint to autoscale between the configured CU range.

# COMMAND ----------

# Find the production branch's primary endpoint
endpoints = list(w.postgres.list_endpoints(
    parent=f"projects/{project_id}/branches/production"
))
primary_ep = endpoints[0]
ep_name = primary_ep.name

print(f"Primary endpoint: {ep_name}")
print(f"Current CU range: {primary_ep.status.autoscaling_limit_min_cu}–{primary_ep.status.autoscaling_limit_max_cu}")

# Update autoscaling if needed
current_min = primary_ep.status.autoscaling_limit_min_cu
current_max = primary_ep.status.autoscaling_limit_max_cu

if current_min != min_cu or current_max != max_cu:
    print(f"Resizing to {min_cu}–{max_cu} CU...")
    w.postgres.update_endpoint(
        name=ep_name,
        endpoint=Endpoint(
            name=ep_name,
            spec=EndpointSpec(
                endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                autoscaling_limit_min_cu=min_cu,
                autoscaling_limit_max_cu=max_cu,
            ),
        ),
        update_mask=FieldMask(field_mask=[
            "spec.autoscaling_limit_min_cu",
            "spec.autoscaling_limit_max_cu",
        ]),
    ).wait()
    print(f"Compute resized to {min_cu}–{max_cu} CU")
else:
    print("Autoscaling already configured — no change needed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Connect via psycopg and Create Tables
# MAGIC Direct Postgres DDL for all operational tables.

# COMMAND ----------

import psycopg

# Resolve host and generate OAuth token
endpoint = w.postgres.get_endpoint(name=ep_name)
host = endpoint.status.hosts.host
username = w.current_user.me().user_name
cred = w.postgres.generate_database_credential(endpoint=ep_name)

print(f"Connecting to {host} as {username}...")

conn = psycopg.connect(
    host=host,
    dbname="databricks_postgres",
    user=username,
    password=cred.token,
    sslmode="require",
    autocommit=True,
)
print("Connected to Lakebase Autoscaling")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create operational tables
# MAGIC These tables power the real-time mission control loop — all accessed via direct psycopg from the app.

# COMMAND ----------

cur = conn.cursor()

# --- Mission State (single-row) ---
cur.execute("""
CREATE TABLE IF NOT EXISTS mission_state (
    state_id INT NOT NULL,
    mission_name TEXT NOT NULL DEFAULT 'Odyssey Return',
    mission_status TEXT NOT NULL DEFAULT 'active',
    timestamp TIMESTAMPTZ NOT NULL,
    position_x DOUBLE PRECISION NOT NULL,
    position_y DOUBLE PRECISION NOT NULL,
    position_z DOUBLE PRECISION NOT NULL,
    velocity_x DOUBLE PRECISION NOT NULL,
    velocity_y DOUBLE PRECISION NOT NULL,
    velocity_z DOUBLE PRECISION NOT NULL,
    fuel_remaining_kg DOUBLE PRECISION NOT NULL,
    hull_integrity DOUBLE PRECISION DEFAULT 100.0,
    engine_status TEXT DEFAULT 'nominal',
    communication_delay_s DOUBLE PRECISION NOT NULL,
    active_plan_id TEXT,
    mission_elapsed_s DOUBLE PRECISION DEFAULT 0.0,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (state_id)
)
""")
print("mission_state created")

# --- Command Queue ---
cur.execute("""
CREATE TABLE IF NOT EXISTS command_queue (
    command_id TEXT NOT NULL,
    command_type TEXT NOT NULL,
    payload TEXT,
    priority INT DEFAULT 5,
    created_at TIMESTAMPTZ NOT NULL,
    approved_by TEXT,
    approved_at TIMESTAMPTZ,
    transmit_time TIMESTAMPTZ,
    estimated_receive_time TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'pending',
    response_reason TEXT,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (command_id)
)
""")
print("command_queue created")

# --- Agent Memory ---
cur.execute("""
CREATE TABLE IF NOT EXISTS agent_memory (
    agent_name TEXT NOT NULL,
    memory_key TEXT NOT NULL,
    memory_value TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    ttl_seconds INT,
    PRIMARY KEY (agent_name, memory_key)
)
""")
print("agent_memory created")

# --- Active Hazards ---
cur.execute("""
CREATE TABLE IF NOT EXISTS active_hazards (
    hazard_id TEXT NOT NULL,
    hazard_type TEXT NOT NULL,
    position_x DOUBLE PRECISION,
    position_y DOUBLE PRECISION,
    position_z DOUBLE PRECISION,
    velocity_x DOUBLE PRECISION,
    velocity_y DOUBLE PRECISION,
    velocity_z DOUBLE PRECISION,
    radius_km DOUBLE PRECISION,
    risk_score DOUBLE PRECISION,
    closest_approach_time TIMESTAMPTZ,
    closest_approach_km DOUBLE PRECISION,
    detected_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (hazard_id)
)
""")
print("active_hazards created")

# --- Simulation Clock (single-row) ---
cur.execute("""
CREATE TABLE IF NOT EXISTS simulation_clock (
    clock_id INT NOT NULL DEFAULT 1,
    simulation_time TIMESTAMPTZ NOT NULL,
    time_scale DOUBLE PRECISION DEFAULT 1.0,
    is_running BOOLEAN DEFAULT false,
    started_at TIMESTAMPTZ,
    paused_at TIMESTAMPTZ,
    total_elapsed_s DOUBLE PRECISION DEFAULT 0.0,
    PRIMARY KEY (clock_id)
)
""")
print("simulation_clock created")

# --- Telemetry Realtime (rolling buffer) ---
cur.execute("""
CREATE TABLE IF NOT EXISTS telemetry_realtime (
    tick_seq INT NOT NULL,
    simulation_time_s DOUBLE PRECISION NOT NULL,
    position_x DOUBLE PRECISION NOT NULL,
    position_y DOUBLE PRECISION NOT NULL,
    position_z DOUBLE PRECISION NOT NULL,
    velocity_x DOUBLE PRECISION NOT NULL,
    velocity_y DOUBLE PRECISION NOT NULL,
    velocity_z DOUBLE PRECISION NOT NULL,
    speed_km_s DOUBLE PRECISION NOT NULL,
    fuel_remaining_kg DOUBLE PRECISION NOT NULL,
    hull_integrity DOUBLE PRECISION NOT NULL,
    engine_status TEXT,
    communication_delay_s DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tick_seq)
)
""")
print("telemetry_realtime created")

# --- Spacecraft Autopilot State (single-row) ---
cur.execute("""
CREATE TABLE IF NOT EXISTS spacecraft_autopilot_state (
    autopilot_id INT NOT NULL DEFAULT 1,
    mode TEXT DEFAULT 'nominal',
    total_corrections INT DEFAULT 0,
    total_evasions INT DEFAULT 0,
    total_commands_executed INT DEFAULT 0,
    total_commands_rejected INT DEFAULT 0,
    fuel_used_by_autopilot_kg DOUBLE PRECISION DEFAULT 0.0,
    last_decision_time_s DOUBLE PRECISION DEFAULT 0.0,
    ticks_processed INT DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (autopilot_id)
)
""")
print("spacecraft_autopilot_state created")

# --- Ground State (single-row) ---
cur.execute("""
CREATE TABLE IF NOT EXISTS ground_state (
    state_id INT NOT NULL DEFAULT 1,
    timestamp TIMESTAMPTZ NOT NULL,
    position_x DOUBLE PRECISION NOT NULL,
    position_y DOUBLE PRECISION NOT NULL,
    position_z DOUBLE PRECISION NOT NULL,
    velocity_x DOUBLE PRECISION NOT NULL,
    velocity_y DOUBLE PRECISION NOT NULL,
    velocity_z DOUBLE PRECISION NOT NULL,
    fuel_remaining_kg DOUBLE PRECISION,
    hull_integrity DOUBLE PRECISION,
    engine_status TEXT,
    communication_delay_s DOUBLE PRECISION,
    mission_elapsed_s DOUBLE PRECISION DEFAULT 0.0,
    telemetry_age_s DOUBLE PRECISION DEFAULT 0.0,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (state_id)
)
""")
print("ground_state created")

# --- Agent Messages Realtime ---
cur.execute("""
CREATE TABLE IF NOT EXISTS agent_messages_realtime (
    message_id TEXT NOT NULL,
    from_agent TEXT NOT NULL,
    to_agent TEXT NOT NULL,
    message_type TEXT NOT NULL,
    content TEXT NOT NULL,
    tick_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (message_id)
)
""")
print("agent_messages_realtime created")

# --- Throughput Metrics ---
cur.execute("""
CREATE TABLE IF NOT EXISTS throughput_metrics (
    metric_id TEXT NOT NULL,
    component TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    wall_time_s DOUBLE PRECISION DEFAULT 0.0,
    read_ops INT DEFAULT 0,
    write_ops INT DEFAULT 0,
    total_ops INT DEFAULT 0,
    ops_per_second DOUBLE PRECISION DEFAULT 0.0,
    sim_seconds_processed INT DEFAULT 0,
    rows_generated INT DEFAULT 0,
    hazards_detected INT DEFAULT 0,
    PRIMARY KEY (metric_id)
)
""")
print("throughput_metrics created")

# --- Simulation Sessions ---
cur.execute("""
CREATE TABLE IF NOT EXISTS simulation_sessions (
    session_id TEXT NOT NULL,
    session_name TEXT NOT NULL DEFAULT 'Odyssey Return',
    scenario_type TEXT DEFAULT 'default',
    initial_position_x DOUBLE PRECISION,
    initial_position_y DOUBLE PRECISION,
    initial_position_z DOUBLE PRECISION,
    initial_velocity_x DOUBLE PRECISION,
    initial_velocity_y DOUBLE PRECISION,
    initial_velocity_z DOUBLE PRECISION,
    initial_fuel_kg DOUBLE PRECISION,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at TIMESTAMPTZ,
    duration_wall_seconds DOUBLE PRECISION,
    duration_sim_seconds DOUBLE PRECISION,
    max_time_scale_used DOUBLE PRECISION DEFAULT 1.0,
    final_distance_to_earth_km DOUBLE PRECISION,
    total_burns_executed INT DEFAULT 0,
    total_fuel_used_kg DOUBLE PRECISION DEFAULT 0.0,
    total_corrections INT DEFAULT 0,
    total_hazards_encountered INT DEFAULT 0,
    total_agent_decisions INT DEFAULT 0,
    total_predictions_made INT DEFAULT 0,
    prediction_accuracy_avg DOUBLE PRECISION,
    outcome TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (session_id)
)
""")
print("simulation_sessions created")

# --- Onboard Predictions (ML-based trajectory forecasts from the spacecraft) ---
cur.execute("""
CREATE TABLE IF NOT EXISTS onboard_predictions (
    prediction_id TEXT NOT NULL,
    simulation_time_s DOUBLE PRECISION NOT NULL,
    current_pos_x DOUBLE PRECISION NOT NULL,
    current_pos_y DOUBLE PRECISION NOT NULL,
    current_pos_z DOUBLE PRECISION NOT NULL,
    current_vel_x DOUBLE PRECISION NOT NULL,
    current_vel_y DOUBLE PRECISION NOT NULL,
    current_vel_z DOUBLE PRECISION NOT NULL,
    predicted_pos_x DOUBLE PRECISION NOT NULL,
    predicted_pos_y DOUBLE PRECISION NOT NULL,
    predicted_pos_z DOUBLE PRECISION NOT NULL,
    prediction_horizon_s DOUBLE PRECISION DEFAULT 60.0,
    prediction_source TEXT DEFAULT 'physics',
    assessment TEXT,
    action_taken TEXT,
    correction_dv DOUBLE PRECISION DEFAULT 0.0,
    actual_pos_x DOUBLE PRECISION,
    actual_pos_y DOUBLE PRECISION,
    actual_pos_z DOUBLE PRECISION,
    prediction_error_km DOUBLE PRECISION,
    backfilled_at TIMESTAMPTZ,
    session_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (prediction_id)
)
""")
print("onboard_predictions created")

# --- Captain Decisions (ship captain autonomous decisions) ---
cur.execute("""
CREATE TABLE IF NOT EXISTS captain_decisions (
    decision_id TEXT NOT NULL,
    simulation_time_s DOUBLE PRECISION NOT NULL,
    session_id TEXT,
    action TEXT NOT NULL,
    priority_level INT,
    reasoning TEXT,
    override_of_command_id TEXT,
    original_command_summary TEXT,
    captain_alternative_summary TEXT,
    delta_v DOUBLE PRECISION DEFAULT 0.0,
    fuel_cost_kg DOUBLE PRECISION DEFAULT 0.0,
    alert_level TEXT DEFAULT 'green',
    confidence DOUBLE PRECISION DEFAULT 0.85,
    elapsed_ms DOUBLE PRECISION DEFAULT 0.0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (decision_id)
)
""")
print("captain_decisions created")

# --- Captain-MC Dialogue ---
cur.execute("""
CREATE TABLE IF NOT EXISTS captain_mc_dialogue (
    dialogue_id TEXT NOT NULL,
    simulation_time_s DOUBLE PRECISION NOT NULL,
    session_id TEXT,
    direction TEXT NOT NULL,
    speaker TEXT NOT NULL,
    message_type TEXT NOT NULL,
    content TEXT NOT NULL,
    related_decision_id TEXT,
    priority INT DEFAULT 5,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dialogue_id)
)
""")
print("captain_mc_dialogue created")

# --- Mission Events (event sourcing) ---
cur.execute("""
CREATE TABLE IF NOT EXISTS mission_events (
    event_id TEXT NOT NULL,
    session_id TEXT,
    event_type TEXT NOT NULL,
    summary TEXT NOT NULL,
    simulation_time_s DOUBLE PRECISION DEFAULT 0.0,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id)
)
""")
print("mission_events created")

# --- Candidate Maneuvers (agent-proposed burns for approval/rejection) ---
cur.execute("""
CREATE TABLE IF NOT EXISTS candidate_maneuvers (
    maneuver_id TEXT NOT NULL,
    session_id TEXT,
    ranking INT DEFAULT 0,
    name TEXT,
    description TEXT,
    burn_vector_x DOUBLE PRECISION DEFAULT 0.0,
    burn_vector_y DOUBLE PRECISION DEFAULT 0.0,
    burn_vector_z DOUBLE PRECISION DEFAULT 0.0,
    burn_duration_s DOUBLE PRECISION DEFAULT 0.0,
    delta_v DOUBLE PRECISION DEFAULT 0.0,
    fuel_cost_kg DOUBLE PRECISION DEFAULT 0.0,
    risk_reduction DOUBLE PRECISION DEFAULT 0.0,
    feasibility TEXT DEFAULT 'medium',
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (maneuver_id)
)
""")
print("candidate_maneuvers created")

# --- Add session_id to all per-run and state tables ---
session_id_tables = [
    "mission_state",
    "simulation_clock",
    "command_queue",
    "active_hazards",
    "telemetry_realtime",
    "spacecraft_autopilot_state",
    "ground_state",
    "agent_messages_realtime",
    "throughput_metrics",
]
for tbl in session_id_tables:
    try:
        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS session_id TEXT")
        print(f"  + session_id added to {tbl}")
    except Exception as e:
        print(f"  ~ skipped {tbl}: {e}")

print("session_id column migration complete")

# --- Add mission outcome columns to mission_state ---
try:
    cur.execute("ALTER TABLE mission_state ADD COLUMN IF NOT EXISTS mission_outcome TEXT")
    cur.execute("ALTER TABLE mission_state ADD COLUMN IF NOT EXISTS mission_outcome_detail TEXT")
    print("  + mission_outcome columns added to mission_state")
except Exception as e:
    print(f"  ~ skipped mission_outcome columns: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Create Indexes for Query Performance

# COMMAND ----------

indexes = [
    "CREATE INDEX IF NOT EXISTS idx_command_queue_status ON command_queue (status)",
    "CREATE INDEX IF NOT EXISTS idx_command_queue_created ON command_queue (created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_command_queue_session ON command_queue (session_id)",
    "CREATE INDEX IF NOT EXISTS idx_active_hazards_risk ON active_hazards (risk_score DESC)",
    "CREATE INDEX IF NOT EXISTS idx_agent_memory_agent ON agent_memory (agent_name)",
    "CREATE INDEX IF NOT EXISTS idx_telemetry_rt_time ON telemetry_realtime (simulation_time_s DESC)",
    "CREATE INDEX IF NOT EXISTS idx_telemetry_rt_session ON telemetry_realtime (session_id)",
    "CREATE INDEX IF NOT EXISTS idx_agent_msgs_created ON agent_messages_realtime (created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_throughput_ts ON throughput_metrics (timestamp DESC)",
    "CREATE INDEX IF NOT EXISTS idx_sessions_status ON simulation_sessions (status)",
    "CREATE INDEX IF NOT EXISTS idx_sessions_started ON simulation_sessions (started_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_predictions_session ON onboard_predictions (session_id)",
    "CREATE INDEX IF NOT EXISTS idx_predictions_time ON onboard_predictions (simulation_time_s DESC)",
    "CREATE INDEX IF NOT EXISTS idx_captain_session ON captain_decisions (session_id)",
    "CREATE INDEX IF NOT EXISTS idx_captain_created ON captain_decisions (created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_captain_dialogue_session ON captain_mc_dialogue (session_id)",
    "CREATE INDEX IF NOT EXISTS idx_captain_dialogue_created ON captain_mc_dialogue (created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_mission_events_session ON mission_events (session_id)",
    "CREATE INDEX IF NOT EXISTS idx_mission_events_created ON mission_events (created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_mission_events_type ON mission_events (event_type)",
]

for idx_sql in indexes:
    try:
        cur.execute(idx_sql)
        idx_name = idx_sql.split("IF NOT EXISTS ")[1].split(" ON")[0]
        print(f"Index {idx_name} created")
    except Exception as e:
        idx_name = idx_sql.split("IF NOT EXISTS ")[1].split(" ON")[0]
        print(f"  ~ skipped index {idx_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Verify Setup

# COMMAND ----------

cur.execute("""
    SELECT tablename FROM pg_tables
    WHERE schemaname = 'public'
    ORDER BY tablename
""")
tables = [row[0] for row in cur.fetchall()]
print(f"\n=== Lakebase Autoscaling tables in project '{project_id}' ===")
for t in tables:
    print(f"  - {t}")
print(f"\nTotal: {len(tables)} tables")

# COMMAND ----------

cur.close()
conn.close()
print("Connection closed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Component | Details |
# MAGIC |-----------|---------|
# MAGIC | **Project** | `projects/{project_id}` |
# MAGIC | **Branch** | `production` (always-on, no scale-to-zero) |
# MAGIC | **Compute** | Autoscaling {min_cu}–{max_cu} CU ({min_cu*2:.0f}–{max_cu*2:.0f} GB RAM) |
# MAGIC | **Database** | `databricks_postgres` |
# MAGIC | **Tables** | 12 operational tables in `public` schema |
# MAGIC | **Access** | Direct psycopg from the Databricks App (OAuth token refresh) |
# MAGIC
# MAGIC The app connects directly to this Postgres instance for sub-10ms ops queries,
# MAGIC while Delta Lake tables are queried via the SQL warehouse for historical/analytical data.
