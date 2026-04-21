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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Create Indexes for Query Performance

# COMMAND ----------

indexes = [
    "CREATE INDEX IF NOT EXISTS idx_command_queue_status ON command_queue (status)",
    "CREATE INDEX IF NOT EXISTS idx_command_queue_created ON command_queue (created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_active_hazards_risk ON active_hazards (risk_score DESC)",
    "CREATE INDEX IF NOT EXISTS idx_agent_memory_agent ON agent_memory (agent_name)",
    "CREATE INDEX IF NOT EXISTS idx_telemetry_rt_time ON telemetry_realtime (simulation_time_s DESC)",
    "CREATE INDEX IF NOT EXISTS idx_agent_msgs_created ON agent_messages_realtime (created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_throughput_ts ON throughput_metrics (timestamp DESC)",
]

for idx_sql in indexes:
    cur.execute(idx_sql)
    idx_name = idx_sql.split("IF NOT EXISTS ")[1].split(" ON")[0]
    print(f"Index {idx_name} created")

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
# MAGIC | **Tables** | 10 operational tables in `public` schema |
# MAGIC | **Access** | Direct psycopg from the Databricks App (OAuth token refresh) |
# MAGIC
# MAGIC The app connects directly to this Postgres instance for sub-10ms ops queries,
# MAGIC while Delta Lake tables are queried via the SQL warehouse for historical/analytical data.
