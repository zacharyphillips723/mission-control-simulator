# Databricks notebook source
# MAGIC %md
# MAGIC # Mission Control — Ground-Side Simulation Tick
# MAGIC
# MAGIC This notebook runs **at ground control**: it reads telemetry that is `comm_delay`
# MAGIC seconds old (the ground always sees stale data), detects new hazards via ground
# MAGIC sensors, generates candidate maneuvers, and maintains the ground's estimate of
# MAGIC the spacecraft state.
# MAGIC
# MAGIC **Separation of concerns:**
# MAGIC - `04_simulation_tick.py` — Ground side (this notebook): delayed view, hazard detection, maneuver planning
# MAGIC - `04b_spacecraft_tick.py` — Ship side: true state, autopilot, command execution
# MAGIC
# MAGIC The ground **never** directly modifies spacecraft state. Commands are placed into
# MAGIC the command queue and travel to the ship with realistic communication delay.

# COMMAND ----------

dbutils.widgets.text("catalog", "mission_control_dev", "Catalog Name")
dbutils.widgets.text("lakebase_project_id", "mission-control", "Lakebase Project ID")

catalog = dbutils.widgets.get("catalog")
lakebase_project_id = dbutils.widgets.get("lakebase_project_id")

# COMMAND ----------

import sys, os, time, uuid, math, random
notebook_path = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
repo_root = "/".join(notebook_path.split("/")[:-2])
sys.path.insert(0, os.path.join("/Workspace", repo_root, "src", "python"))

from physics_engine import (
    SpacecraftState, Vector3, BODIES,
    propagate_state, communication_delay, closest_approach, collision_risk,
)
from telemetry_generator import (
    generate_candidate_maneuvers, generate_hazard, add_sensor_noise,
)
import lakebase_client
from pyspark.sql import functions as F
from datetime import datetime, timezone, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Initialize Lakebase Connection

# COMMAND ----------

lakebase_client.init(lakebase_project_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Simulation Clock and Communication Delay

# COMMAND ----------

clock_row = lakebase_client.fetch_one(
    "SELECT * FROM simulation_clock WHERE clock_id = %(clock_id)s",
    {"clock_id": 1},
)

sim_time = clock_row["simulation_time"]
time_scale = clock_row["time_scale"]
total_elapsed_s = clock_row["total_elapsed_s"]

# Read the TRUE spacecraft state just to get comm delay (ground knows this from physics)
true_state_row = lakebase_client.fetch_one(
    "SELECT communication_delay_s FROM mission_state WHERE state_id = %(state_id)s",
    {"state_id": 1},
)

comm_delay_s = true_state_row["communication_delay_s"]

print(f"[GROUND] Simulation time: {sim_time}")
print(f"[GROUND] Communication delay: {comm_delay_s:.1f}s ({comm_delay_s/60:.1f} min)")

read_count = 1
write_count = 0
tick_wall_start = time.time()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Delayed Telemetry — Ground Sees Stale Data
# MAGIC
# MAGIC The ground receives telemetry that was sent `comm_delay_s` seconds ago.
# MAGIC This is the core realism constraint: every decision ground makes is based
# MAGIC on where the spacecraft **was**, not where it **is**.

# COMMAND ----------

# The latest telemetry ground can see is from comm_delay_s ago
delay_cutoff = sim_time - timedelta(seconds=comm_delay_s)

delayed_telemetry = spark.sql(f"""
    SELECT * FROM `{catalog}`.telemetry.spacecraft_telemetry
    WHERE timestamp <= TIMESTAMP '{delay_cutoff.strftime("%Y-%m-%d %H:%M:%S")}'
    ORDER BY timestamp DESC
    LIMIT 1
""").collect()

if delayed_telemetry:
    t = delayed_telemetry[0]
    ground_view_state = SpacecraftState(
        position=Vector3(t.position_x, t.position_y, t.position_z),
        velocity=Vector3(t.velocity_x, t.velocity_y, t.velocity_z),
        fuel_remaining_kg=t.fuel_remaining_kg,
        hull_integrity=t.hull_integrity,
        engine_status=t.engine_status,
        timestamp_s=total_elapsed_s - comm_delay_s,
    )
    telemetry_age_s = comm_delay_s
    print(f"[GROUND] Latest telemetry from: {t.timestamp} (delayed by {comm_delay_s:.0f}s)")
    print(f"[GROUND] Ground view position: ({ground_view_state.position.x:.0f}, {ground_view_state.position.y:.0f}, {ground_view_state.position.z:.0f})")
    print(f"[GROUND] Ground view speed: {ground_view_state.speed:.2f} km/s | Fuel: {ground_view_state.fuel_remaining_kg:.1f} kg")
else:
    # Fallback: read from ground_state if no telemetry available yet
    gs_row = lakebase_client.fetch_one(
        "SELECT * FROM ground_state WHERE state_id = %(state_id)s",
        {"state_id": 1},
    )
    if gs_row:
        ground_view_state = SpacecraftState(
            position=Vector3(gs_row["position_x"], gs_row["position_y"], gs_row["position_z"]),
            velocity=Vector3(gs_row["velocity_x"], gs_row["velocity_y"], gs_row["velocity_z"]),
            fuel_remaining_kg=gs_row["fuel_remaining_kg"],
            hull_integrity=gs_row["hull_integrity"],
            engine_status=gs_row["engine_status"],
            timestamp_s=gs_row["mission_elapsed_s"],
        )
    else:
        # No data at all — use mission_state as bootstrap
        ms = lakebase_client.fetch_one(
            "SELECT * FROM mission_state WHERE state_id = %(state_id)s",
            {"state_id": 1},
        )
        ground_view_state = SpacecraftState(
            position=Vector3(ms["position_x"], ms["position_y"], ms["position_z"]),
            velocity=Vector3(ms["velocity_x"], ms["velocity_y"], ms["velocity_z"]),
            fuel_remaining_kg=ms["fuel_remaining_kg"],
            hull_integrity=ms["hull_integrity"],
            engine_status=ms["engine_status"],
            timestamp_s=ms["mission_elapsed_s"],
        )
    telemetry_age_s = 0
    print("[GROUND] No delayed telemetry available — using last known ground state")

read_count += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Update Ground State in Lakebase
# MAGIC
# MAGIC Ground state represents what ground **thinks** the spacecraft state is.
# MAGIC It is always behind the true state by the communication delay.

# COMMAND ----------

earth_pos = BODIES["earth"].position_at(ground_view_state.timestamp_s)
ground_delay = communication_delay(ground_view_state.position, earth_pos)

ground_ts = sim_time - timedelta(seconds=comm_delay_s)

lakebase_client.upsert(
    "ground_state",
    {"state_id": 1},
    {
        "timestamp": ground_ts,
        "position_x": ground_view_state.position.x,
        "position_y": ground_view_state.position.y,
        "position_z": ground_view_state.position.z,
        "velocity_x": ground_view_state.velocity.x,
        "velocity_y": ground_view_state.velocity.y,
        "velocity_z": ground_view_state.velocity.z,
        "fuel_remaining_kg": ground_view_state.fuel_remaining_kg,
        "hull_integrity": ground_view_state.hull_integrity,
        "engine_status": ground_view_state.engine_status,
        "communication_delay_s": ground_delay,
        "mission_elapsed_s": ground_view_state.timestamp_s,
        "telemetry_age_s": telemetry_age_s,
        "updated_at": datetime.now(timezone.utc),
    },
)
write_count += 1
print(f"[GROUND] Ground state updated — sees spacecraft {comm_delay_s:.0f}s in the past")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Detect New Hazards via Ground Sensors
# MAGIC
# MAGIC Ground-based sensors (telescopes, radar, deep-space network) detect hazards
# MAGIC that the spacecraft cannot see. These are added to the shared active_hazards table.

# COMMAND ----------

# Random hazard generation based on ground sensor sweep
# Ground sensors can detect farther out than spacecraft sensors
new_hazards = []
num_sensor_sweeps = random.randint(1, 3)

for _ in range(num_sensor_sweeps):
    if random.random() < 0.15:  # 15% chance per sweep of detecting something
        hazard = generate_hazard(
            ground_view_state,
            ground_view_state.timestamp_s,
            difficulty="medium",
        )
        new_hazards.append(hazard)

if new_hazards:
    for hazard in new_hazards:
        # Calculate risk based on ground's (delayed) view
        future_states = [ground_view_state]
        temp = ground_view_state
        for _ in range(60):
            temp = propagate_state(temp, 60.0)
            future_states.append(temp)

        min_dist, approach_time = closest_approach(future_states, hazard)
        risk = collision_risk(min_dist, hazard.radius_km)

        ts_detected = sim_time
        closest_approach_ts = sim_time + timedelta(seconds=approach_time - ground_view_state.timestamp_s)
        window_end_ts = ts_detected + timedelta(hours=2)

        # Write to Delta hazard history
        hazard_row = {
            "hazard_id": hazard.hazard_id,
            "detected_at": ts_detected.isoformat(),
            "hazard_type": hazard.hazard_type,
            "position_x": hazard.position.x,
            "position_y": hazard.position.y,
            "position_z": hazard.position.z,
            "velocity_x": hazard.velocity.x,
            "velocity_y": hazard.velocity.y,
            "velocity_z": hazard.velocity.z,
            "radius_km": hazard.radius_km,
            "risk_score": risk,
            "closest_approach_time": closest_approach_ts.isoformat(),
            "closest_approach_km": min_dist,
            "time_window_start": ts_detected.isoformat(),
            "time_window_end": window_end_ts.isoformat(),
            "status": "active",
        }

        haz_df = spark.createDataFrame([hazard_row])
        for col_name in ["detected_at", "closest_approach_time", "time_window_start", "time_window_end"]:
            haz_df = haz_df.withColumn(col_name, F.to_timestamp(col_name))
        haz_df.write.mode("append").saveAsTable(f"`{catalog}`.hazards.detected_hazards")
        write_count += 1

        # Insert into Lakebase active_hazards
        lakebase_client.execute(
            """INSERT INTO active_hazards (
                hazard_id, hazard_type,
                position_x, position_y, position_z,
                velocity_x, velocity_y, velocity_z,
                radius_km, risk_score,
                closest_approach_time, closest_approach_km,
                detected_at, updated_at
            ) VALUES (
                %(hazard_id)s, %(hazard_type)s,
                %(position_x)s, %(position_y)s, %(position_z)s,
                %(velocity_x)s, %(velocity_y)s, %(velocity_z)s,
                %(radius_km)s, %(risk_score)s,
                %(closest_approach_time)s, %(closest_approach_km)s,
                %(detected_at)s, NOW()
            )""",
            {
                "hazard_id": hazard.hazard_id,
                "hazard_type": hazard.hazard_type,
                "position_x": hazard.position.x,
                "position_y": hazard.position.y,
                "position_z": hazard.position.z,
                "velocity_x": hazard.velocity.x,
                "velocity_y": hazard.velocity.y,
                "velocity_z": hazard.velocity.z,
                "radius_km": hazard.radius_km,
                "risk_score": risk,
                "closest_approach_time": closest_approach_ts,
                "closest_approach_km": min_dist,
                "detected_at": ts_detected,
            },
        )
        write_count += 1

    print(f"[GROUND] Detected {len(new_hazards)} new hazards via ground sensors")
else:
    print("[GROUND] No new hazards detected this tick")

# Expire old hazards from Lakebase
expire_cutoff = sim_time - timedelta(hours=1)
lakebase_client.execute(
    "DELETE FROM active_hazards WHERE closest_approach_time < %(cutoff)s",
    {"cutoff": expire_cutoff},
)
read_count += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Generate Candidate Maneuvers Based on Delayed State
# MAGIC
# MAGIC Ground generates maneuver candidates based on where the spacecraft **was**
# MAGIC (not where it is now). The autopilot on the ship will validate these when
# MAGIC they arrive after another comm_delay trip.

# COMMAND ----------

# Read current active hazards for maneuver planning
active_hazard_rows = lakebase_client.fetch_all("SELECT * FROM active_hazards")
read_count += 1

# Generate maneuver candidates using the DELAYED state
maneuvers = generate_candidate_maneuvers(ground_view_state, num_candidates=5)

if maneuvers:
    man_df = spark.createDataFrame(maneuvers)
    man_df = man_df.withColumn("generated_at", F.to_timestamp("generated_at"))
    man_df.write.mode("append").saveAsTable(f"`{catalog}`.navigation.candidate_maneuvers")
    write_count += 1
    print(f"[GROUND] Generated {len(maneuvers)} candidate maneuvers (based on delayed state)")

    # Log the top candidate details
    top = maneuvers[0]
    print(f"[GROUND] Top maneuver: delta-v={top['delta_v']:.4f} km/s, "
          f"fuel={top['fuel_cost_kg']:.1f}kg, "
          f"risk_reduction={top['risk_reduction_score']:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Check Autopilot Status and Command Acknowledgments
# MAGIC
# MAGIC Ground reads the autopilot state (delayed by comm_delay) and any command
# MAGIC responses to maintain situational awareness.

# COMMAND ----------

# Read autopilot state (what ground knows, which is delayed)
try:
    ap_row = lakebase_client.fetch_one(
        "SELECT * FROM spacecraft_autopilot_state WHERE autopilot_id = %(autopilot_id)s",
        {"autopilot_id": 1},
    )
    read_count += 1
    if ap_row:
        print(f"[GROUND] Autopilot status (delayed): mode={ap_row['mode']}, "
              f"evasions={ap_row['total_evasions']}, corrections={ap_row['total_corrections']}, "
              f"cmds_executed={ap_row['total_commands_executed']}, cmds_rejected={ap_row['total_commands_rejected']}")
except Exception:
    print("[GROUND] Autopilot state not yet available")

# Check for recently executed/rejected commands
try:
    cmd_cutoff = sim_time - timedelta(minutes=30)
    cmd_responses = lakebase_client.fetch_all(
        """SELECT command_id, command_type, status, response_reason
           FROM command_queue
           WHERE status IN ('executed', 'rejected')
             AND updated_at > %(cutoff)s
           ORDER BY updated_at DESC
           LIMIT 10""",
        {"cutoff": cmd_cutoff},
    )
    read_count += 1

    if cmd_responses:
        print(f"[GROUND] Recent command responses: {len(cmd_responses)}")
        for cr in cmd_responses:
            print(f"  - {cr['command_id'][:8]}: {cr['status']} ({cr['command_type']})")
except Exception:
    pass  # command_queue may not have response_reason column yet

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write Throughput Metrics

# COMMAND ----------

tick_wall_elapsed = time.time() - tick_wall_start
ops_per_second = (read_count + write_count) / max(tick_wall_elapsed, 0.001)

try:
    lakebase_client.execute(
        """INSERT INTO throughput_metrics (
            metric_id, tick_type, timestamp,
            wall_time_s, read_ops, write_ops,
            total_ops, ops_per_second,
            lakebase_read_ops, lakebase_write_ops,
            hazards_detected
        ) VALUES (
            %(metric_id)s, %(tick_type)s, NOW(),
            %(wall_time_s)s, %(read_ops)s, %(write_ops)s,
            %(total_ops)s, %(ops_per_second)s,
            %(lakebase_read_ops)s, %(lakebase_write_ops)s,
            %(hazards_detected)s
        )""",
        {
            "metric_id": str(uuid.uuid4()),
            "tick_type": "ground_tick",
            "wall_time_s": tick_wall_elapsed,
            "read_ops": read_count,
            "write_ops": write_count,
            "total_ops": read_count + write_count,
            "ops_per_second": ops_per_second,
            "lakebase_read_ops": 0,
            "lakebase_write_ops": 0,
            "hazards_detected": len(new_hazards),
        },
    )
    write_count += 1
except Exception as e:
    print(f"[GROUND] Throughput metrics write skipped: {e}")

print(f"[GROUND] Throughput: {ops_per_second:.1f} ops/s | Wall time: {tick_wall_elapsed:.2f}s | Reads: {read_count} | Writes: {write_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Tick Summary

# COMMAND ----------

print(f"\n{'='*60}")
print(f"  GROUND TICK COMPLETE")
print(f"{'='*60}")
print(f"  Simulation time:      {sim_time}")
print(f"  Comm delay:           {comm_delay_s:.1f}s ({comm_delay_s/60:.1f} min)")
print(f"  Ground sees data from: {(sim_time - timedelta(seconds=comm_delay_s)).strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  Ground view position: ({ground_view_state.position.x:.0f}, {ground_view_state.position.y:.0f}, {ground_view_state.position.z:.0f}) km")
print(f"  Ground view speed:    {ground_view_state.speed:.2f} km/s")
print(f"  Ground view fuel:     {ground_view_state.fuel_remaining_kg:.1f} kg")
print(f"  Ground view hull:     {ground_view_state.hull_integrity:.1f}%")
print(f"  New hazards detected: {len(new_hazards)}")
print(f"  Active hazards:       {len(active_hazard_rows)}")
print(f"  Maneuvers proposed:   {len(maneuvers)}")
print(f"  Throughput:           {ops_per_second:.1f} ops/s")
print(f"{'='*60}")
