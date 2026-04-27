# Databricks notebook source
# MAGIC %md
# MAGIC # Mission Control — Generate Synthetic Telemetry
# MAGIC Runs the physics engine + telemetry generator to populate Delta and Lakebase tables
# MAGIC with realistic spacecraft telemetry data.

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]>=3.0" "databricks-sdk>=0.81.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "mission_control_dev", "Catalog Name")
dbutils.widgets.text("duration_hours", "24", "Simulation Duration (hours)")
dbutils.widgets.text("batch_size_s", "3600", "Batch size in seconds")
dbutils.widgets.dropdown("mode", "single", ["single", "multi_profile"], "Generation Mode")
dbutils.widgets.text("lakebase_project_id", "", "Lakebase Project ID")

catalog = dbutils.widgets.get("catalog")
duration_hours = int(dbutils.widgets.get("duration_hours"))
batch_size_s = int(dbutils.widgets.get("batch_size_s"))
mode = dbutils.widgets.get("mode")
lakebase_project_id = dbutils.widgets.get("lakebase_project_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add source code to path

# COMMAND ----------

import sys, os
# Add the python source directory to path
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_dir = os.path.dirname(notebook_path)
repo_root = "/".join(notebook_dir.split("/")[:-2])
python_src = os.path.join("/Workspace", repo_root, "src", "python")
# Fallback: try relative to CWD if workspace path doesn't exist
if not os.path.exists(python_src):
    cwd = os.getcwd()
    for candidate in [
        os.path.join(cwd, "src", "python"),
        os.path.join(cwd, "..", "src", "python"),
        os.path.join(cwd, "..", "..", "src", "python"),
    ]:
        if os.path.exists(candidate):
            python_src = os.path.abspath(candidate)
            break
print(f"Python source path: {python_src}")
sys.path.insert(0, python_src)

# COMMAND ----------

from physics_engine import create_initial_state, BODIES, communication_delay
from telemetry_generator import generate_telemetry_batch, generate_candidate_maneuvers
from mission_profiles import PROFILES, generate_profile_telemetry, generate_all_profiles
import lakebase_client
from pyspark.sql.types import *
from pyspark.sql import functions as F
from datetime import datetime, timezone

lakebase_client.init(lakebase_project_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Mission State

# COMMAND ----------

if mode == "single":
    initial_state = create_initial_state(departure_body="mars", fuel_kg=500.0)
    earth_pos = BODIES["earth"].position_at(0)
    delay = communication_delay(initial_state.position, earth_pos)

    print(f"Mode: single profile")
    print(f"Initial position: ({initial_state.position.x:.0f}, {initial_state.position.y:.0f}, {initial_state.position.z:.0f}) km")
    print(f"Initial velocity: {initial_state.speed:.2f} km/s")
    print(f"Fuel: {initial_state.fuel_remaining_kg:.1f} kg")
    print(f"Communication delay: {delay:.1f} seconds ({delay/60:.1f} minutes)")
else:
    print(f"Mode: multi_profile — {len(PROFILES)} mission profiles queued")
    for p in PROFILES:
        print(f"  - {p.name}: {p.description}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate & Write Telemetry Data

# COMMAND ----------

if mode == "single":
    # ---- Single-profile generation (original behaviour) ----
    mission_start = datetime(2087, 3, 15, 0, 0, 0, tzinfo=timezone.utc)
    total_batches = (duration_hours * 3600) // batch_size_s

    state = initial_state
    all_hazards = []

    for batch_num in range(total_batches):
        telemetry, hazards, state = generate_telemetry_batch(
            initial_state=state,
            duration_s=batch_size_s,
            dt=1.0,
            hazard_probability=0.0005,
            mission_start_time=mission_start,
        )

        from datetime import timedelta
        mission_start = mission_start + timedelta(seconds=batch_size_s)

        telem_df = spark.createDataFrame(telemetry)
        telem_df = telem_df.withColumn("timestamp", F.to_timestamp("timestamp"))
        telem_df = telem_df.withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))

        telem_df.write.mode("append").saveAsTable(f"`{catalog}`.telemetry.spacecraft_telemetry")

        if hazards:
            all_hazards.extend(hazards)
            haz_df = spark.createDataFrame(hazards)
            for col_name in ["detected_at", "closest_approach_time", "time_window_start", "time_window_end"]:
                haz_df = haz_df.withColumn(col_name, F.to_timestamp(col_name))
            haz_df.write.mode("append").saveAsTable(f"`{catalog}`.hazards.detected_hazards")

        print(f"Batch {batch_num + 1}/{total_batches}: {len(telemetry)} telemetry rows, {len(hazards)} hazards")

else:
    # ---- Multi-profile generation ----
    print(f"\n{'='*60}")
    print("MULTI-PROFILE TELEMETRY GENERATION")
    print(f"{'='*60}\n")

    all_hazards = []
    profile_summary = []

    for profile in PROFILES:
        print(f"\n--- Profile: {profile.name} ---")
        print(f"    {profile.description}")
        telemetry, hazards, final_state = generate_profile_telemetry(profile)

        # Write telemetry with mission_profile column
        telem_df = spark.createDataFrame(telemetry)
        telem_df = telem_df.withColumn("timestamp", F.to_timestamp("timestamp"))
        telem_df = telem_df.withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
        telem_df.write.mode("append").saveAsTable(f"`{catalog}`.telemetry.spacecraft_telemetry")

        # Write hazards with mission_profile column
        if hazards:
            all_hazards.extend(hazards)
            haz_df = spark.createDataFrame(hazards)
            for col_name in ["detected_at", "closest_approach_time", "time_window_start", "time_window_end"]:
                haz_df = haz_df.withColumn(col_name, F.to_timestamp(col_name))
            haz_df.write.mode("append").saveAsTable(f"`{catalog}`.hazards.detected_hazards")

        # Generate maneuvers for this profile's final state
        maneuvers = generate_candidate_maneuvers(final_state, num_candidates=10)
        man_df = spark.createDataFrame(maneuvers)
        man_df = man_df.withColumn("generated_at", F.to_timestamp("generated_at"))
        man_df = man_df.withColumn("mission_profile", F.lit(profile.name))
        man_df = man_df.withColumn("ranking", F.col("ranking").cast("int"))
        man_df.write.mode("append").option("mergeSchema", "true").saveAsTable(f"`{catalog}`.navigation.candidate_maneuvers")

        profile_summary.append({
            "profile": profile.name,
            "telemetry_rows": len(telemetry),
            "hazards": len(hazards),
            "maneuvers": len(maneuvers),
            "final_fuel_kg": round(final_state.fuel_remaining_kg, 1),
            "final_speed_km_s": round(final_state.speed, 2),
        })

        print(f"    {len(telemetry):,} telemetry | {len(hazards):,} hazards | {len(maneuvers)} maneuvers")

    # Use last profile's final state as the reference for downstream cells
    state = final_state

    # Print summary table
    print(f"\n{'='*60}")
    print("PROFILE SUMMARY")
    print(f"{'='*60}")
    summary_df = spark.createDataFrame(profile_summary)
    display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Candidate Maneuvers (single-profile mode)

# COMMAND ----------

if mode == "single":
    maneuvers = generate_candidate_maneuvers(state, num_candidates=10)
    man_df = spark.createDataFrame(maneuvers)
    man_df = man_df.withColumn("generated_at", F.to_timestamp("generated_at"))
    man_df = man_df.withColumn("ranking", F.col("ranking").cast("int"))
    man_df.write.mode("append").option("mergeSchema", "true").saveAsTable(f"`{catalog}`.navigation.candidate_maneuvers")

    print(f"Generated {len(maneuvers)} candidate maneuvers")
    display(man_df.select("ranking", "delta_v", "fuel_cost_kg", "risk_reduction_score", "feasibility_score"))
else:
    print("Candidate maneuvers already generated per-profile above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Lakebase Mission State

# COMMAND ----------

from datetime import datetime, timezone

init_time = datetime(2087, 3, 15, 0, 0, 0, tzinfo=timezone.utc)

# In multi-profile mode, use the first profile's initial state for Lakebase init
if mode == "multi_profile":
    from mission_profiles import create_initial_state_for_profile
    _init_state = create_initial_state_for_profile(PROFILES[0])
    _earth_pos = BODIES["earth"].position_at(0)
    _delay = communication_delay(_init_state.position, _earth_pos)
else:
    _init_state = initial_state
    _delay = delay

lakebase_client.execute(
    """
    INSERT INTO mission_state (state_id, mission_name, mission_status, timestamp,
        position_x, position_y, position_z, velocity_x, velocity_y, velocity_z,
        fuel_remaining_kg, hull_integrity, engine_status, communication_delay_s,
        active_plan_id, mission_elapsed_s, updated_at)
    VALUES (
        %(mission_id)s, %(mission_name)s, %(status)s, %(mission_start_time)s,
        %(pos_x)s, %(pos_y)s, %(pos_z)s, %(vel_x)s, %(vel_y)s, %(vel_z)s,
        %(fuel_remaining_kg)s, %(hull_integrity_pct)s, %(system_status)s,
        %(comm_delay_s)s, NULL, 0.0, NOW()
    )
    ON CONFLICT (state_id) DO UPDATE SET
        mission_name = EXCLUDED.mission_name,
        mission_status = EXCLUDED.mission_status,
        timestamp = EXCLUDED.timestamp,
        position_x = EXCLUDED.position_x, position_y = EXCLUDED.position_y, position_z = EXCLUDED.position_z,
        velocity_x = EXCLUDED.velocity_x, velocity_y = EXCLUDED.velocity_y, velocity_z = EXCLUDED.velocity_z,
        fuel_remaining_kg = EXCLUDED.fuel_remaining_kg, hull_integrity = EXCLUDED.hull_integrity,
        engine_status = EXCLUDED.engine_status, communication_delay_s = EXCLUDED.communication_delay_s,
        mission_elapsed_s = EXCLUDED.mission_elapsed_s, updated_at = NOW()
    """,
    {
        "mission_id": 1,
        "mission_name": "Odyssey Return",
        "status": "active",
        "mission_start_time": init_time.strftime("%Y-%m-%d %H:%M:%S"),
        "pos_x": _init_state.position.x,
        "pos_y": _init_state.position.y,
        "pos_z": _init_state.position.z,
        "vel_x": _init_state.velocity.x,
        "vel_y": _init_state.velocity.y,
        "vel_z": _init_state.velocity.z,
        "fuel_remaining_kg": _init_state.fuel_remaining_kg,
        "hull_integrity_pct": 100.0,
        "system_status": "nominal",
        "comm_delay_s": _delay,
    },
)

print("Mission state initialized in Lakebase")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Simulation Clock

# COMMAND ----------

lakebase_client.execute(
    """
    INSERT INTO simulation_clock (clock_id, simulation_time, time_scale, is_running,
        started_at, paused_at, total_elapsed_s)
    VALUES (%(clock_id)s, %(current_time)s, %(time_scale)s, %(is_paused)s, NULL, NULL, %(elapsed_s)s)
    ON CONFLICT (clock_id) DO UPDATE SET
        simulation_time = EXCLUDED.simulation_time,
        time_scale = EXCLUDED.time_scale,
        is_running = EXCLUDED.is_running,
        total_elapsed_s = EXCLUDED.total_elapsed_s
    """,
    {
        "clock_id": 1,
        "current_time": init_time.strftime("%Y-%m-%d %H:%M:%S"),
        "time_scale": 1.0,
        "is_paused": False,
        "elapsed_s": 0.0,
    },
)

print("Simulation clock initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"\n{'='*60}")
print(f"MISSION CONTROL TELEMETRY GENERATION COMPLETE")
print(f"{'='*60}")
telem_count = spark.sql(f"SELECT COUNT(*) as cnt FROM `{catalog}`.telemetry.spacecraft_telemetry").collect()[0]["cnt"]
hazard_count = spark.sql(f"SELECT COUNT(*) as cnt FROM `{catalog}`.hazards.detected_hazards").collect()[0]["cnt"]
print(f"Telemetry readings: {telem_count:,}")
print(f"Hazards detected:   {hazard_count:,}")
print(f"Final fuel:         {state.fuel_remaining_kg:.1f} kg")
print(f"Final speed:        {state.speed:.2f} km/s")
print(f"Distance from Sun:  {state.distance_from_sun:.0f} km")
