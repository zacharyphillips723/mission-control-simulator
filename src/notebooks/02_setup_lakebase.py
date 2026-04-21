# Databricks notebook source
# MAGIC %md
# MAGIC # Mission Control — Lakebase (Postgres) Setup
# MAGIC Creates the operational Lakebase tables for low-latency mission state, command queue, and agent memory.
# MAGIC
# MAGIC **Lakebase** provides sub-10ms reads/writes for the real-time mission control loop.

# COMMAND ----------

dbutils.widgets.text("catalog", "mission_control_dev", "Catalog Name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Lakebase Schema
# MAGIC Lakebase tables live in a dedicated schema within the same catalog.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.ops COMMENT 'Lakebase operational tables for real-time mission state'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mission State Table
# MAGIC Single-row table representing the current spacecraft state. Updated every telemetry tick.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.ops.mission_state (
    state_id INT NOT NULL COMMENT 'Always 1 — single-row state table',
    mission_name STRING NOT NULL DEFAULT 'Odyssey Return',
    mission_status STRING NOT NULL DEFAULT 'active' COMMENT 'active, paused, completed, aborted',
    timestamp TIMESTAMP NOT NULL,
    position_x DOUBLE NOT NULL,
    position_y DOUBLE NOT NULL,
    position_z DOUBLE NOT NULL,
    velocity_x DOUBLE NOT NULL,
    velocity_y DOUBLE NOT NULL,
    velocity_z DOUBLE NOT NULL,
    fuel_remaining_kg DOUBLE NOT NULL,
    hull_integrity DOUBLE DEFAULT 100.0,
    engine_status STRING DEFAULT 'nominal',
    communication_delay_s DOUBLE NOT NULL,
    active_plan_id STRING COMMENT 'Current active maneuver plan',
    mission_elapsed_s DOUBLE DEFAULT 0.0 COMMENT 'Seconds since mission start',
    updated_at TIMESTAMP NOT NULL
)
USING lakebase
COMMENT 'Current spacecraft state — single row, updated every tick'
""")

print("✓ mission_state table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Command Queue
# MAGIC Commands pending transmission, in-flight, and recently executed.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.ops.command_queue (
    command_id STRING NOT NULL,
    command_type STRING NOT NULL COMMENT 'burn, attitude_adjust, system_check, abort',
    payload STRING COMMENT 'JSON command payload',
    priority INT DEFAULT 5 COMMENT '1=critical, 10=routine',
    created_at TIMESTAMP NOT NULL,
    approved_by STRING,
    approved_at TIMESTAMP,
    transmit_time TIMESTAMP,
    estimated_receive_time TIMESTAMP,
    status STRING NOT NULL DEFAULT 'pending' COMMENT 'pending, approved, transmitting, in_flight, received, executed, failed',
    response_reason STRING COMMENT 'Reason for execution result or rejection by spacecraft',
    updated_at TIMESTAMP NOT NULL
)
USING lakebase
COMMENT 'Real-time command queue with transmission tracking'
""")

print("✓ command_queue table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Memory
# MAGIC Persists agent state between decision cycles.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.ops.agent_memory (
    agent_name STRING NOT NULL COMMENT 'flight_dynamics, hazard_assessment, communications, mission_commander',
    memory_key STRING NOT NULL COMMENT 'Key for this memory entry',
    memory_value STRING NOT NULL COMMENT 'JSON-serialized memory value',
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    ttl_seconds INT COMMENT 'Optional time-to-live'
)
USING lakebase
COMMENT 'Agent working memory for stateful decision-making'
""")

print("✓ agent_memory table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Active Hazards
# MAGIC Low-latency view of currently active hazards for real-time decision loop.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.ops.active_hazards (
    hazard_id STRING NOT NULL,
    hazard_type STRING NOT NULL,
    position_x DOUBLE,
    position_y DOUBLE,
    position_z DOUBLE,
    velocity_x DOUBLE,
    velocity_y DOUBLE,
    velocity_z DOUBLE,
    radius_km DOUBLE,
    risk_score DOUBLE,
    closest_approach_time TIMESTAMP,
    closest_approach_km DOUBLE,
    detected_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
)
USING lakebase
COMMENT 'Currently active hazards for real-time decision loop'
""")

print("✓ active_hazards table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulation Clock
# MAGIC Tracks simulation time independently of wall clock time.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.ops.simulation_clock (
    clock_id INT NOT NULL DEFAULT 1 COMMENT 'Single-row table',
    simulation_time TIMESTAMP NOT NULL COMMENT 'Current simulation time',
    time_scale DOUBLE DEFAULT 1.0 COMMENT 'Simulation speed multiplier (1.0 = realtime)',
    is_running BOOLEAN DEFAULT false,
    started_at TIMESTAMP,
    paused_at TIMESTAMP,
    total_elapsed_s DOUBLE DEFAULT 0.0
)
USING lakebase
COMMENT 'Simulation clock state for time management'
""")

print("✓ simulation_clock table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Telemetry Realtime
# MAGIC Per-second spacecraft telemetry for real-time dashboard. Rolling buffer of latest readings.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.ops.telemetry_realtime (
    tick_seq INT NOT NULL COMMENT 'Sequence number within current session',
    simulation_time_s DOUBLE NOT NULL,
    position_x DOUBLE NOT NULL,
    position_y DOUBLE NOT NULL,
    position_z DOUBLE NOT NULL,
    velocity_x DOUBLE NOT NULL,
    velocity_y DOUBLE NOT NULL,
    velocity_z DOUBLE NOT NULL,
    speed_km_s DOUBLE NOT NULL,
    fuel_remaining_kg DOUBLE NOT NULL,
    hull_integrity DOUBLE NOT NULL,
    engine_status STRING,
    communication_delay_s DOUBLE,
    updated_at TIMESTAMP NOT NULL
)
USING lakebase
COMMENT 'Per-second spacecraft telemetry — rolling buffer for real-time dashboard'
""")

print("✓ telemetry_realtime table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spacecraft Autopilot State
# MAGIC Tracks the ship autonomous autopilot mode and decisions.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.ops.spacecraft_autopilot_state (
    autopilot_id INT NOT NULL DEFAULT 1 COMMENT 'Single-row table',
    mode STRING DEFAULT 'nominal' COMMENT 'nominal, evasion, station_keeping, safe_mode, emergency_only',
    total_corrections INT DEFAULT 0,
    total_evasions INT DEFAULT 0,
    total_commands_executed INT DEFAULT 0,
    total_commands_rejected INT DEFAULT 0,
    fuel_used_by_autopilot_kg DOUBLE DEFAULT 0.0,
    last_decision_time_s DOUBLE DEFAULT 0.0,
    ticks_processed INT DEFAULT 0,
    updated_at TIMESTAMP NOT NULL
)
USING lakebase
COMMENT 'Spacecraft autopilot working state'
""")

print("✓ spacecraft_autopilot_state table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ground State
# MAGIC What mission control believes the spacecraft state is — always delayed by comm latency.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.ops.ground_state (
    state_id INT NOT NULL DEFAULT 1 COMMENT 'Single-row table',
    timestamp TIMESTAMP NOT NULL COMMENT 'Simulation time of this observation',
    position_x DOUBLE NOT NULL,
    position_y DOUBLE NOT NULL,
    position_z DOUBLE NOT NULL,
    velocity_x DOUBLE NOT NULL,
    velocity_y DOUBLE NOT NULL,
    velocity_z DOUBLE NOT NULL,
    fuel_remaining_kg DOUBLE,
    hull_integrity DOUBLE,
    engine_status STRING,
    communication_delay_s DOUBLE,
    mission_elapsed_s DOUBLE DEFAULT 0.0 COMMENT 'Simulation seconds elapsed at observation',
    telemetry_age_s DOUBLE DEFAULT 0.0 COMMENT 'How stale this data is (communication delay)',
    updated_at TIMESTAMP NOT NULL
)
USING lakebase
COMMENT 'Ground perceived spacecraft state — delayed by communication latency'
""")

print("✓ ground_state table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Messages Realtime
# MAGIC Low-latency inter-agent message store for the current decision cycle.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.ops.agent_messages_realtime (
    message_id STRING NOT NULL,
    from_agent STRING NOT NULL,
    to_agent STRING NOT NULL,
    message_type STRING NOT NULL,
    content STRING NOT NULL COMMENT 'JSON message body',
    tick_id STRING NOT NULL,
    created_at TIMESTAMP NOT NULL
)
USING lakebase
COMMENT 'Real-time inter-agent messages for current decision cycle'
""")

print("✓ agent_messages_realtime table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Throughput Metrics
# MAGIC Tracks Lakebase and Delta read/write operations per second for demo showcase.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.ops.throughput_metrics (
    metric_id STRING NOT NULL COMMENT 'Unique metric ID',
    component STRING NOT NULL COMMENT 'Which component: spacecraft_tick, ground_tick, agent_loop, command_transmission',
    timestamp TIMESTAMP NOT NULL,
    wall_time_s DOUBLE DEFAULT 0.0 COMMENT 'Wall-clock seconds for this tick',
    read_ops INT DEFAULT 0,
    write_ops INT DEFAULT 0,
    total_ops INT DEFAULT 0,
    ops_per_second DOUBLE DEFAULT 0.0,
    sim_seconds_processed INT DEFAULT 0 COMMENT 'Simulation seconds covered in this tick',
    rows_generated INT DEFAULT 0 COMMENT 'Telemetry or data rows produced',
    hazards_detected INT DEFAULT 0
)
USING lakebase
COMMENT 'Per-second throughput metrics — showcases Databricks read/write performance'
""")

print("✓ throughput_metrics table created")

# COMMAND ----------

print(f"\n=== All Lakebase tables created in {catalog}.ops ===")
display(spark.sql(f"SHOW TABLES IN `{catalog}`.ops"))
