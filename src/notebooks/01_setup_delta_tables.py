# Databricks notebook source
# MAGIC %md
# MAGIC # Mission Control — Delta Lake Table Setup
# MAGIC Creates the analytical Delta tables for telemetry history, hazard data, simulation logs, and model training data.

# COMMAND ----------

dbutils.widgets.text("catalog", "mission_control_dev", "Catalog Name")
catalog = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG `{catalog}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Telemetry Schema

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.telemetry.spacecraft_telemetry (
    telemetry_id STRING NOT NULL COMMENT 'Unique telemetry reading ID',
    timestamp TIMESTAMP NOT NULL COMMENT 'UTC timestamp of reading',
    position_x DOUBLE NOT NULL COMMENT 'X position in km (heliocentric)',
    position_y DOUBLE NOT NULL COMMENT 'Y position in km',
    position_z DOUBLE NOT NULL COMMENT 'Z position in km',
    velocity_x DOUBLE NOT NULL COMMENT 'X velocity in km/s',
    velocity_y DOUBLE NOT NULL COMMENT 'Y velocity in km/s',
    velocity_z DOUBLE NOT NULL COMMENT 'Z velocity in km/s',
    fuel_remaining_kg DOUBLE NOT NULL COMMENT 'Remaining fuel in kg',
    hull_integrity DOUBLE COMMENT 'Hull integrity percentage (0-100)',
    engine_status STRING COMMENT 'Engine status: nominal, degraded, offline',
    communication_delay_s DOUBLE COMMENT 'One-way signal delay in seconds',
    ingestion_timestamp TIMESTAMP COMMENT 'When data arrived at mission control'
)
COMMENT 'Historical spacecraft telemetry readings at 1-second intervals'
TBLPROPERTIES ('quality' = 'silver')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.telemetry.trajectory_predictions (
    prediction_id STRING NOT NULL,
    generated_at TIMESTAMP NOT NULL,
    prediction_horizon_s INT COMMENT 'How far ahead this predicts in seconds',
    predicted_positions ARRAY<STRUCT<
        t_offset_s: INT,
        position_x: DOUBLE,
        position_y: DOUBLE,
        position_z: DOUBLE
    >> COMMENT 'Array of predicted positions over time',
    model_version STRING,
    confidence DOUBLE
)
COMMENT 'Model-generated trajectory predictions'
TBLPROPERTIES ('quality' = 'gold')
""")

print("✓ Telemetry tables created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Navigation Schema

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.navigation.celestial_bodies (
    body_id STRING NOT NULL,
    name STRING NOT NULL COMMENT 'e.g., Earth, Mars, Jupiter',
    body_type STRING COMMENT 'planet, moon, asteroid, sun',
    mass_kg DOUBLE,
    radius_km DOUBLE,
    orbit_semi_major_axis_km DOUBLE,
    orbit_eccentricity DOUBLE,
    orbit_period_days DOUBLE,
    current_position_x DOUBLE,
    current_position_y DOUBLE,
    current_position_z DOUBLE,
    updated_at TIMESTAMP
)
COMMENT 'Celestial body positions and orbital parameters'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.navigation.gravity_assists (
    assist_id STRING NOT NULL,
    body_id STRING NOT NULL COMMENT 'Which body provides the assist',
    closest_approach_km DOUBLE,
    delta_v_gained DOUBLE COMMENT 'Velocity change from assist in km/s',
    entry_velocity DOUBLE,
    exit_velocity DOUBLE,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    feasibility_score DOUBLE COMMENT '0-1 feasibility rating'
)
COMMENT 'Planned and evaluated gravity assist maneuvers'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.navigation.candidate_maneuvers (
    maneuver_id STRING NOT NULL,
    generated_at TIMESTAMP NOT NULL,
    burn_vector_x DOUBLE COMMENT 'Thrust direction X component',
    burn_vector_y DOUBLE COMMENT 'Thrust direction Y component',
    burn_vector_z DOUBLE COMMENT 'Thrust direction Z component',
    burn_duration_s DOUBLE COMMENT 'Duration of engine burn in seconds',
    delta_v DOUBLE COMMENT 'Total velocity change in km/s',
    fuel_cost_kg DOUBLE COMMENT 'Estimated fuel consumption',
    risk_reduction_score DOUBLE COMMENT 'How much risk this maneuver reduces (0-1)',
    feasibility_score DOUBLE COMMENT 'Overall feasibility (0-1)',
    ranking INT COMMENT 'Model-assigned rank among candidates',
    status STRING COMMENT 'proposed, approved, executed, rejected'
)
COMMENT 'AI-generated candidate maneuvers ranked by model scoring'
""")

print("✓ Navigation tables created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hazards Schema

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.hazards.detected_hazards (
    hazard_id STRING NOT NULL,
    detected_at TIMESTAMP NOT NULL,
    hazard_type STRING NOT NULL COMMENT 'asteroid, meteor_shower, debris_field, solar_flare',
    position_x DOUBLE,
    position_y DOUBLE,
    position_z DOUBLE,
    velocity_x DOUBLE COMMENT 'Hazard velocity if moving',
    velocity_y DOUBLE,
    velocity_z DOUBLE,
    radius_km DOUBLE COMMENT 'Estimated hazard radius or field size',
    risk_score DOUBLE COMMENT 'Collision probability (0-1)',
    closest_approach_time TIMESTAMP COMMENT 'Predicted time of closest approach',
    closest_approach_km DOUBLE COMMENT 'Predicted minimum distance',
    time_window_start TIMESTAMP,
    time_window_end TIMESTAMP,
    status STRING COMMENT 'active, passed, mitigated'
)
COMMENT 'Detected hazards with risk assessments'
""")

print("✓ Hazard tables created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Commands Schema

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.commands.command_log (
    command_id STRING NOT NULL,
    maneuver_id STRING COMMENT 'Associated maneuver if applicable',
    command_type STRING NOT NULL COMMENT 'burn, attitude_adjust, system_check, abort',
    payload STRING COMMENT 'JSON command payload',
    created_at TIMESTAMP NOT NULL,
    approved_by STRING COMMENT 'Operator who approved',
    approved_at TIMESTAMP,
    transmit_time TIMESTAMP COMMENT 'When signal was sent',
    estimated_receive_time TIMESTAMP COMMENT 'When spacecraft should receive',
    actual_receive_time TIMESTAMP,
    execution_time TIMESTAMP,
    status STRING NOT NULL COMMENT 'queued, transmitted, received, executing, completed, failed',
    result STRING COMMENT 'Execution result or error'
)
COMMENT 'Command transmission log with full lifecycle tracking'
""")

print("✓ Command tables created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agents Schema

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.agents.decision_log (
    decision_id STRING NOT NULL,
    agent_name STRING NOT NULL COMMENT 'flight_dynamics, hazard_assessment, communications, mission_commander',
    decision_type STRING COMMENT 'maneuver_recommendation, hazard_alert, delay_adjustment, final_recommendation',
    timestamp TIMESTAMP NOT NULL,
    input_summary STRING COMMENT 'Summary of inputs the agent considered',
    reasoning STRING COMMENT 'Agent reasoning and explanation',
    recommendation STRING COMMENT 'Structured recommendation',
    confidence_score DOUBLE COMMENT 'Agent confidence (0-1)',
    accepted BOOLEAN COMMENT 'Whether operator accepted the recommendation',
    metadata STRING COMMENT 'JSON metadata'
)
COMMENT 'Agent decision log with full reasoning traces'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.agents.message_log (
    message_id STRING NOT NULL,
    from_agent STRING NOT NULL COMMENT 'Sending agent name',
    to_agent STRING NOT NULL COMMENT 'Target agent or broadcast',
    message_type STRING NOT NULL COMMENT 'analysis, alert, timing_plan, order, acknowledgment',
    content STRING NOT NULL COMMENT 'JSON structured message body',
    timestamp TIMESTAMP NOT NULL,
    tick_id STRING COMMENT 'Correlates all messages within one simulation tick',
    metadata STRING COMMENT 'JSON metadata'
)
COMMENT 'Inter-agent communication log — every message between agents'
TBLPROPERTIES ('quality' = 'gold')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.agents.captain_decision_log (
    decision_id STRING NOT NULL COMMENT 'Unique captain decision ID',
    session_id STRING COMMENT 'Simulation session this decision belongs to',
    simulation_time_s DOUBLE NOT NULL COMMENT 'Simulation clock at decision time',
    action STRING NOT NULL COMMENT 'emergency_evasion, micro_correction, override_command, modify_command, enter_safe_mode, await_orders',
    priority_level INT COMMENT 'Decision priority: 1=evasion, 2=override, 3=correction, 4=safe_mode, 5=nominal',
    reasoning STRING COMMENT 'Captain reasoning for the decision',
    override_of_command_id STRING COMMENT 'Command ID if this was an override/veto',
    original_command_summary STRING COMMENT 'Summary of the original MC command',
    captain_alternative_summary STRING COMMENT 'What the captain did instead',
    delta_v DOUBLE COMMENT 'Velocity change applied (km/s)',
    fuel_cost_kg DOUBLE COMMENT 'Fuel consumed by this decision',
    alert_level STRING COMMENT 'Alert level at decision time: green, yellow, red',
    confidence DOUBLE COMMENT 'Captain confidence in the decision (0-1)',
    elapsed_ms DOUBLE COMMENT 'Decision computation time in milliseconds',
    created_at TIMESTAMP NOT NULL COMMENT 'When the decision was made'
)
COMMENT 'Ship Captain autonomous decisions — overrides, evasions, corrections, and approvals'
TBLPROPERTIES ('quality' = 'gold')
""")

print("✓ Agent tables created (including captain_decision_log)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Models Schema

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.models.inference_log (
    inference_id STRING NOT NULL,
    endpoint_name STRING NOT NULL COMMENT 'Model serving endpoint called',
    caller STRING NOT NULL COMMENT 'Who called: agent name, autopilot, or ground',
    input_features STRING NOT NULL COMMENT 'JSON input payload',
    output_prediction STRING NOT NULL COMMENT 'JSON output from model',
    latency_ms DOUBLE COMMENT 'Round-trip inference latency',
    timestamp TIMESTAMP NOT NULL,
    simulation_time_s DOUBLE COMMENT 'Simulation clock at time of call',
    tick_id STRING COMMENT 'Correlates with agent tick',
    metadata STRING COMMENT 'JSON metadata'
)
COMMENT 'Every model serving call logged with full input/output for audit and retraining'
TBLPROPERTIES ('quality' = 'gold')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.models.evaluation_metrics (
    eval_id STRING NOT NULL,
    model_name STRING NOT NULL,
    model_version STRING,
    evaluated_at TIMESTAMP NOT NULL,
    metric_name STRING NOT NULL,
    metric_value DOUBLE NOT NULL,
    dataset_name STRING,
    notes STRING
)
COMMENT 'Model evaluation metrics over time'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.models.onboard_inference_log (
    inference_id STRING NOT NULL COMMENT 'Unique prediction ID (matches Lakebase prediction_id)',
    session_id STRING COMMENT 'Simulation session this prediction belongs to',
    simulation_time_s DOUBLE NOT NULL COMMENT 'Simulation clock at prediction time',
    prediction_horizon_s DOUBLE COMMENT 'How far ahead the prediction looks (seconds)',
    prediction_source STRING COMMENT 'model_serving or physics_fallback',
    input_pos_x DOUBLE COMMENT 'Current position X at prediction time',
    input_pos_y DOUBLE COMMENT 'Current position Y at prediction time',
    input_pos_z DOUBLE COMMENT 'Current position Z at prediction time',
    input_vel_x DOUBLE COMMENT 'Current velocity X at prediction time',
    input_vel_y DOUBLE COMMENT 'Current velocity Y at prediction time',
    input_vel_z DOUBLE COMMENT 'Current velocity Z at prediction time',
    input_fuel DOUBLE COMMENT 'Fuel remaining at prediction time',
    input_comm_delay DOUBLE COMMENT 'Communication delay at prediction time',
    predicted_pos_x DOUBLE COMMENT 'Predicted X position',
    predicted_pos_y DOUBLE COMMENT 'Predicted Y position',
    predicted_pos_z DOUBLE COMMENT 'Predicted Z position',
    actual_pos_x DOUBLE COMMENT 'Actual X position (backfilled after horizon)',
    actual_pos_y DOUBLE COMMENT 'Actual Y position (backfilled)',
    actual_pos_z DOUBLE COMMENT 'Actual Z position (backfilled)',
    prediction_error_km DOUBLE COMMENT 'Euclidean distance between predicted and actual (backfilled)',
    assessment STRING COMMENT 'on_course, minor_deviation, correction_needed',
    action_taken STRING COMMENT 'none, micro_correction, correction_burn',
    correction_dv DOUBLE COMMENT 'Delta-v applied if correction was made',
    inference_latency_ms DOUBLE COMMENT 'Model serving round-trip latency',
    created_at TIMESTAMP NOT NULL COMMENT 'When prediction was generated'
)
COMMENT 'Onboard ML trajectory predictions with actuals for retraining — full input/output/outcome loop'
TBLPROPERTIES ('quality' = 'gold')
""")

print("✓ Model tables created (including onboard_inference_log)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Missions Schema (Session Identity)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.missions")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.missions.simulation_sessions (
    session_id STRING NOT NULL COMMENT 'Unique simulation run identifier',
    session_name STRING NOT NULL COMMENT 'Human-readable session name',
    scenario_type STRING COMMENT 'default, earth_departure, mars_orbit, deep_space, custom',
    initial_position_x DOUBLE COMMENT 'Starting X position in km',
    initial_position_y DOUBLE COMMENT 'Starting Y position in km',
    initial_position_z DOUBLE COMMENT 'Starting Z position in km',
    initial_velocity_x DOUBLE COMMENT 'Starting X velocity in km/s',
    initial_velocity_y DOUBLE COMMENT 'Starting Y velocity in km/s',
    initial_velocity_z DOUBLE COMMENT 'Starting Z velocity in km/s',
    initial_fuel_kg DOUBLE COMMENT 'Starting fuel in kg',
    started_at TIMESTAMP NOT NULL COMMENT 'Wall-clock start time',
    ended_at TIMESTAMP COMMENT 'Wall-clock end time',
    duration_wall_seconds DOUBLE COMMENT 'Real-time duration of session',
    duration_sim_seconds DOUBLE COMMENT 'Simulated time elapsed',
    max_time_scale_used DOUBLE COMMENT 'Highest time scale used during session',
    final_distance_to_earth_km DOUBLE COMMENT 'Distance to Earth at session end',
    total_burns_executed INT COMMENT 'Number of burn commands executed',
    total_fuel_used_kg DOUBLE COMMENT 'Total fuel consumed',
    total_corrections INT COMMENT 'Autopilot course corrections',
    total_hazards_encountered INT COMMENT 'Hazards detected during session',
    total_agent_decisions INT COMMENT 'Agent decisions made during session',
    total_predictions_made INT COMMENT 'Onboard ML predictions made',
    prediction_accuracy_avg DOUBLE COMMENT 'Average prediction accuracy',
    outcome STRING COMMENT 'Session outcome summary',
    status STRING NOT NULL COMMENT 'active, completed, aborted'
)
COMMENT 'Simulation session registry — every run gets a unique session_id'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.missions.mission_events (
    event_id STRING NOT NULL COMMENT 'Unique event ID',
    session_id STRING COMMENT 'Simulation session this event belongs to',
    event_type STRING NOT NULL COMMENT 'Event category: mission_start, burn_executed, captain_evasion, hazard_injected, etc.',
    summary STRING NOT NULL COMMENT 'Human-readable event description',
    simulation_time_s DOUBLE COMMENT 'Simulation clock at event time',
    metadata STRING COMMENT 'JSON metadata with event-specific details',
    created_at TIMESTAMP NOT NULL COMMENT 'When event was recorded'
)
COMMENT 'Event-sourced mission timeline — every significant action during a mission'
TBLPROPERTIES ('quality' = 'gold')
""")

print("✓ Missions schema, simulation_sessions, and mission_events tables created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add session_id to All Per-Run Tables
# MAGIC Idempotent ALTER TABLE — safe to re-run.

# COMMAND ----------

session_id_tables = [
    "telemetry.spacecraft_telemetry",
    "telemetry.trajectory_predictions",
    "navigation.candidate_maneuvers",
    "hazards.detected_hazards",
    "commands.command_log",
    "agents.decision_log",
    "agents.message_log",
    "models.inference_log",
    "models.onboard_inference_log",
    "agents.captain_decision_log",
]

for table in session_id_tables:
    try:
        spark.sql(f"ALTER TABLE `{catalog}`.{table} ADD COLUMN session_id STRING COMMENT 'Simulation session this row belongs to'")
        print(f"  + session_id added to {table}")
    except Exception as e:
        if "already exists" in str(e).lower() or "FIELDS_ALREADY_EXISTS" in str(e):
            print(f"  ✓ session_id already exists on {table}")
        else:
            print(f"  ⚠ {table}: {e}")

print("✓ session_id column added to all per-run Delta tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant App Service Principal Access
# MAGIC The Databricks App service principal needs MODIFY on all Delta tables the app flushes to.

# COMMAND ----------

APP_SERVICE_PRINCIPAL = "1dc884b2-1f8a-4f6c-a4ec-52b8dc17a94a"

delta_flush_tables = [
    "telemetry.spacecraft_telemetry",
    "commands.command_log",
    "agents.decision_log",
    "agents.message_log",
    "agents.captain_decision_log",
    "models.inference_log",
    "models.onboard_inference_log",
    "missions.simulation_sessions",
    "missions.mission_events",
]

for table in delta_flush_tables:
    try:
        spark.sql(f"GRANT MODIFY ON TABLE `{catalog}`.{table} TO `{APP_SERVICE_PRINCIPAL}`")
        print(f"  ✓ MODIFY granted on {table}")
    except Exception as e:
        print(f"  ⚠ {table}: {e}")

print("✓ App service principal grants applied")
print(f"\n=== All Delta tables created in catalog: {catalog} ===")
