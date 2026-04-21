# Databricks notebook source
# MAGIC %md
# MAGIC # Mission Control — Spacecraft-Side Simulation Tick
# MAGIC
# MAGIC This notebook runs **on the spacecraft**: it advances the true physical state,
# MAGIC runs the onboard autopilot, processes received commands, and writes telemetry.
# MAGIC
# MAGIC **Separation of concerns:**
# MAGIC - `04_simulation_tick.py` — Ground side (delayed view, hazard detection, maneuver planning)
# MAGIC - `04b_spacecraft_tick.py` — Ship side (true state, autopilot, command execution)
# MAGIC
# MAGIC The ground never directly modifies spacecraft state. Commands travel through the
# MAGIC command queue with realistic communication delay.

# COMMAND ----------

dbutils.widgets.text("catalog", "mission_control_dev", "Catalog Name")
dbutils.widgets.text("tick_duration_s", "300", "Simulation seconds per tick")
dbutils.widgets.text("lakebase_project_id", "mission-control", "Lakebase Project ID")

catalog = dbutils.widgets.get("catalog")
tick_duration_s = int(dbutils.widgets.get("tick_duration_s"))
lakebase_project_id = dbutils.widgets.get("lakebase_project_id")

# COMMAND ----------

import sys, os, time, uuid, json, math
notebook_path = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
repo_root = "/".join(notebook_path.split("/")[:-2])
sys.path.insert(0, os.path.join("/Workspace", repo_root, "src", "python"))

from physics_engine import (
    SpacecraftState, Vector3, BODIES,
    propagate_state, communication_delay,
)
from telemetry_generator import add_sensor_noise
from spacecraft_autopilot import (
    SpacecraftAutopilot, AutopilotState, AutopilotDecision,
    GroundCommand,
)
import lakebase_client

lakebase_client.init(lakebase_project_id)

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, IntegerType,
)
from datetime import datetime, timezone, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Current Spacecraft State

# COMMAND ----------

state_row = lakebase_client.fetch_one(
    "SELECT * FROM mission_state WHERE state_id = %(state_id)s",
    {"state_id": 1},
)

clock_row = lakebase_client.fetch_one(
    "SELECT * FROM simulation_clock WHERE clock_id = %(clock_id)s",
    {"clock_id": 1},
)

current_state = SpacecraftState(
    position=Vector3(state_row["position_x"], state_row["position_y"], state_row["position_z"]),
    velocity=Vector3(state_row["velocity_x"], state_row["velocity_y"], state_row["velocity_z"]),
    fuel_remaining_kg=state_row["fuel_remaining_kg"],
    hull_integrity=state_row["hull_integrity"],
    engine_status=state_row["engine_status"],
    timestamp_s=state_row["mission_elapsed_s"],
)

sim_time = clock_row["simulation_time"]
time_scale = clock_row["time_scale"]

print(f"[SHIP] Current position: ({current_state.position.x:.0f}, {current_state.position.y:.0f}, {current_state.position.z:.0f})")
print(f"[SHIP] Speed: {current_state.speed:.2f} km/s | Fuel: {current_state.fuel_remaining_kg:.1f} kg | Hull: {current_state.hull_integrity:.1f}%")
print(f"[SHIP] Simulation time: {sim_time} | Time scale: {time_scale}x")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Check Command Queue for Received Commands

# COMMAND ----------

# Commands whose estimated_receive_time has passed are now "received" by the ship
lakebase_client.execute(
    "UPDATE command_queue SET status = 'received' WHERE status = 'in_flight' AND estimated_receive_time <= %(sim_time)s",
    {"sim_time": sim_time},
)

# Fetch all commands that are now in 'received' status and ready for autopilot
pending_cmd_rows = lakebase_client.fetch_all(
    "SELECT * FROM command_queue WHERE status = 'received' ORDER BY priority DESC, estimated_receive_time ASC"
)

pending_commands = []
for row in pending_cmd_rows:
    pending_commands.append(GroundCommand(
        command_id=row["command_id"],
        command_type=row["command_type"],
        burn_vector_x=float(row["burn_vector_x"]) if row.get("burn_vector_x") else 0.0,
        burn_vector_y=float(row["burn_vector_y"]) if row.get("burn_vector_y") else 0.0,
        burn_vector_z=float(row["burn_vector_z"]) if row.get("burn_vector_z") else 0.0,
        burn_duration_s=float(row["burn_duration_s"]) if row.get("burn_duration_s") else 0.0,
        delta_v=float(row["delta_v"]) if row.get("delta_v") else 0.0,
        fuel_cost_kg=float(row["fuel_cost_kg"]) if row.get("fuel_cost_kg") else 0.0,
        priority=row["priority"] if row.get("priority") else "normal",
        status="received",
    ))

print(f"[SHIP] Pending commands: {len(pending_commands)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read Active Hazards for Autopilot Awareness

# COMMAND ----------

from physics_engine import Hazard

hazard_rows = lakebase_client.fetch_all("SELECT * FROM active_hazards")

active_hazards = []
for h in hazard_rows:
    active_hazards.append(Hazard(
        hazard_id=h["hazard_id"],
        hazard_type=h["hazard_type"],
        position=Vector3(h["position_x"], h["position_y"], h["position_z"]),
        velocity=Vector3(h["velocity_x"], h["velocity_y"], h["velocity_z"]),
        radius_km=h["radius_km"],
    ))

print(f"[SHIP] Active hazards in range: {len(active_hazards)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load Autopilot State

# COMMAND ----------

try:
    ap_row = lakebase_client.fetch_one(
        "SELECT * FROM spacecraft_autopilot_state WHERE autopilot_id = %(autopilot_id)s",
        {"autopilot_id": 1},
    )
    if ap_row:
        autopilot_state = AutopilotState(
            mode=ap_row["mode"],
            total_corrections=int(ap_row["total_corrections"]),
            total_evasions=int(ap_row["total_evasions"]),
            total_commands_executed=int(ap_row["total_commands_executed"]),
            total_commands_rejected=int(ap_row["total_commands_rejected"]),
            fuel_used_by_autopilot_kg=float(ap_row["fuel_used_by_autopilot_kg"]),
            last_decision_time_s=float(ap_row["last_decision_time_s"]),
            ticks_processed=int(ap_row["ticks_processed"]),
        )
    else:
        autopilot_state = AutopilotState()
except Exception:
    autopilot_state = AutopilotState()

autopilot = SpacecraftAutopilot(autopilot_state=autopilot_state)
print(f"[SHIP] Autopilot mode: {autopilot.state.mode} | Ticks processed: {autopilot.state.ticks_processed}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Per-Second Inner Loop — Physics, Autopilot, Telemetry

# COMMAND ----------

import random

effective_duration = int(tick_duration_s * time_scale)
state = current_state

telemetry_rows = []
autopilot_decisions = []
read_count = 0
write_count = 0
tick_wall_start = time.time()

# Elapsed mission days for meteor shower calculations
elapsed_days = state.timestamp_s / 86400.0

# Identify active meteor shower hazards for hull damage
active_meteor_showers = [h for h in active_hazards if h.hazard_type == "meteor_shower"]

for second in range(effective_duration):
    # ---- (a) Run autopilot tick ----
    decision = autopilot.run_tick(
        spacecraft_state=state,
        active_hazards=active_hazards,
        pending_commands=pending_commands if second == 0 else [],  # process commands only on first second
        elapsed_days=elapsed_days + (second / 86400.0),
    )
    autopilot_decisions.append(decision)
    read_count += 1

    # ---- (b/c) Propagate state with or without thrust ----
    if decision.burn_vector and decision.burn_duration_s > 0:
        # Apply thrust for this 1-second step (or remainder of burn)
        burn_fraction = min(1.0, decision.burn_duration_s)
        fuel_burn_rate = decision.fuel_cost_kg / max(decision.burn_duration_s, 1.0)
        state = propagate_state(
            state, dt=1.0,
            thrust=decision.burn_vector,
            fuel_burn_rate_kg_per_s=fuel_burn_rate,
        )
    else:
        state = propagate_state(state, dt=1.0)

    # ---- (d) Apply hull damage from active meteor showers ----
    for shower_hazard in active_meteor_showers:
        # Simplified damage model: if within shower radius, apply small damage
        dist_to_shower = (state.position - shower_hazard.position).magnitude()
        if dist_to_shower < shower_hazard.radius_km:
            # Damage proportional to proximity and shower density
            proximity_factor = 1.0 - (dist_to_shower / shower_hazard.radius_km)
            damage_per_second = 0.001 * proximity_factor  # 0.1% max per second at center
            state = SpacecraftState(
                position=state.position,
                velocity=state.velocity,
                fuel_remaining_kg=state.fuel_remaining_kg,
                hull_integrity=max(0.0, state.hull_integrity - damage_per_second),
                engine_status=state.engine_status,
                timestamp_s=state.timestamp_s,
            )

    # ---- (e) Generate telemetry row with sensor noise ----
    noisy = add_sensor_noise(state)
    earth_pos = BODIES["earth"].position_at(state.timestamp_s)
    comm_delay = communication_delay(state.position, earth_pos)
    ts = sim_time + timedelta(seconds=second + 1)

    telemetry_rows.append({
        "telemetry_id": str(uuid.uuid4()),
        "timestamp": ts.isoformat(),
        "position_x": noisy["position_x"],
        "position_y": noisy["position_y"],
        "position_z": noisy["position_z"],
        "velocity_x": noisy["velocity_x"],
        "velocity_y": noisy["velocity_y"],
        "velocity_z": noisy["velocity_z"],
        "fuel_remaining_kg": state.fuel_remaining_kg,
        "hull_integrity": state.hull_integrity,
        "engine_status": state.engine_status,
        "communication_delay_s": comm_delay,
        "ingestion_timestamp": (ts + timedelta(seconds=comm_delay)).isoformat(),
    })

    # ---- (f) Write to Lakebase realtime telemetry (rolling buffer of 600) ----
    # We batch the Lakebase write — every 10 seconds or at end
    if (second + 1) % 10 == 0 or second == effective_duration - 1:
        batch_start = max(0, len(telemetry_rows) - 10)
        batch = telemetry_rows[batch_start:]
        for row in batch:
            lakebase_client.execute(
                """INSERT INTO telemetry_realtime (
                    telemetry_id, timestamp, position_x, position_y, position_z,
                    velocity_x, velocity_y, velocity_z,
                    fuel_remaining_kg, hull_integrity, engine_status,
                    communication_delay_s, ingestion_timestamp
                ) VALUES (
                    %(telemetry_id)s, %(timestamp)s,
                    %(position_x)s, %(position_y)s, %(position_z)s,
                    %(velocity_x)s, %(velocity_y)s, %(velocity_z)s,
                    %(fuel_remaining_kg)s, %(hull_integrity)s, %(engine_status)s,
                    %(communication_delay_s)s, %(ingestion_timestamp)s
                )""",
                row,
            )
            write_count += 1

        # Trim realtime table to latest 600 rows
        lakebase_client.execute("""
            DELETE FROM telemetry_realtime
            WHERE tick_seq NOT IN (
                SELECT tick_seq FROM telemetry_realtime
                ORDER BY simulation_time_s DESC
                LIMIT 600
            )
        """)
        read_count += 1

    # ---- (g) Track throughput ----
    read_count += 1  # state read per propagation

print(f"[SHIP] Inner loop complete: {effective_duration} seconds simulated")
print(f"[SHIP] Telemetry rows generated: {len(telemetry_rows)}")
print(f"[SHIP] Autopilot decisions: {len(autopilot_decisions)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Post-Loop: Bulk Writes and State Updates

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6a. Bulk write telemetry to Delta

# COMMAND ----------

if telemetry_rows:
    telem_df = spark.createDataFrame(telemetry_rows)
    telem_df = telem_df.withColumn("timestamp", F.to_timestamp("timestamp"))
    telem_df = telem_df.withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
    telem_df.write.mode("append").saveAsTable(f"`{catalog}`.telemetry.spacecraft_telemetry")
    write_count += len(telemetry_rows)
    print(f"[SHIP] Wrote {len(telemetry_rows)} telemetry rows to Delta")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6b. Update Lakebase mission state with final state

# COMMAND ----------

earth_pos = BODIES["earth"].position_at(state.timestamp_s)
final_delay = communication_delay(state.position, earth_pos)
new_sim_time = sim_time + timedelta(seconds=effective_duration)

lakebase_client.execute(
    """UPDATE mission_state SET
        timestamp = %(timestamp)s,
        position_x = %(position_x)s, position_y = %(position_y)s, position_z = %(position_z)s,
        velocity_x = %(velocity_x)s, velocity_y = %(velocity_y)s, velocity_z = %(velocity_z)s,
        fuel_remaining_kg = %(fuel_remaining_kg)s,
        hull_integrity = %(hull_integrity)s,
        engine_status = %(engine_status)s,
        communication_delay_s = %(communication_delay_s)s,
        mission_elapsed_s = %(mission_elapsed_s)s,
        updated_at = NOW()
    WHERE state_id = %(state_id)s""",
    {
        "timestamp": new_sim_time,
        "position_x": state.position.x, "position_y": state.position.y, "position_z": state.position.z,
        "velocity_x": state.velocity.x, "velocity_y": state.velocity.y, "velocity_z": state.velocity.z,
        "fuel_remaining_kg": state.fuel_remaining_kg,
        "hull_integrity": state.hull_integrity,
        "engine_status": state.engine_status,
        "communication_delay_s": final_delay,
        "mission_elapsed_s": state.timestamp_s,
        "state_id": 1,
    },
)
write_count += 1
print(f"[SHIP] Mission state updated — sim time: {new_sim_time}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6c. Update autopilot state in Lakebase

# COMMAND ----------

ap_state = autopilot.state
lakebase_client.upsert(
    "spacecraft_autopilot_state",
    pk={"autopilot_id": 1},
    data={
        "mode": ap_state.mode,
        "total_corrections": ap_state.total_corrections,
        "total_evasions": ap_state.total_evasions,
        "total_commands_executed": ap_state.total_commands_executed,
        "total_commands_rejected": ap_state.total_commands_rejected,
        "fuel_used_by_autopilot_kg": ap_state.fuel_used_by_autopilot_kg,
        "last_decision_time_s": ap_state.last_decision_time_s,
        "ticks_processed": ap_state.ticks_processed,
        "updated_at": datetime.now(timezone.utc),
    },
)
write_count += 1
print(f"[SHIP] Autopilot state saved — mode: {ap_state.mode}, evasions: {ap_state.total_evasions}, corrections: {ap_state.total_corrections}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6d. Update simulation clock

# COMMAND ----------

lakebase_client.execute(
    "UPDATE simulation_clock SET simulation_time = %(simulation_time)s, total_elapsed_s = %(total_elapsed_s)s WHERE clock_id = %(clock_id)s",
    {"simulation_time": new_sim_time, "total_elapsed_s": state.timestamp_s, "clock_id": 1},
)
write_count += 1
print(f"[SHIP] Simulation clock updated to {new_sim_time}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6e. Write autopilot decisions to Delta decision log

# COMMAND ----------

# Filter to non-coast decisions for the log (coast is too noisy), but always log at least one per tick
notable_decisions = [d for d in autopilot_decisions if d.action != "coast"]
if not notable_decisions and autopilot_decisions:
    # Log the last coast decision as a summary
    notable_decisions = [autopilot_decisions[-1]]

if notable_decisions:
    decision_rows = []
    for d in notable_decisions:
        raw = d.to_dict()
        ts = (sim_time + timedelta(seconds=d.timestamp_s - current_state.timestamp_s)).isoformat()
        decision_rows.append({
            "decision_id": raw["decision_id"],
            "agent_name": "autopilot",
            "decision_type": raw["action"],
            "timestamp": ts,
            "input_summary": f"priority={raw['priority_level']}, hazard={raw.get('target_hazard_id', 'none')}, cmd={raw.get('command_id', 'none')}",
            "reasoning": raw["reasoning"],
            "recommendation": json.dumps({k: raw[k] for k in ("burn_duration_s", "delta_v", "fuel_cost_kg", "burn_vector_x", "burn_vector_y", "burn_vector_z") if k in raw}),
            "confidence_score": 1.0 if raw["action"] in ("evasion_burn", "execute_command") else 0.8,
            "accepted": True,
            "metadata": json.dumps({k: raw[k] for k in ("priority_level", "elapsed_ms", "target_hazard_id", "command_id") if raw.get(k) is not None}),
        })

    dec_df = spark.createDataFrame(decision_rows)
    dec_df = dec_df.withColumn("timestamp", F.to_timestamp("timestamp"))
    dec_df.write.mode("append").saveAsTable(f"`{catalog}`.agents.decision_log")
    write_count += len(notable_decisions)
    print(f"[SHIP] Wrote {len(notable_decisions)} autopilot decisions to agents.decision_log")

# Mark executed/rejected commands in the command queue
for cmd in pending_commands:
    # Check if this command was executed or rejected
    cmd_decisions = [d for d in autopilot_decisions if d.command_id == cmd.command_id]
    if cmd_decisions:
        final_status = "executed" if cmd_decisions[0].action == "execute_command" else "rejected"
        reason = cmd_decisions[0].reasoning
        lakebase_client.execute(
            "UPDATE command_queue SET status = %(status)s, response_reason = %(reason)s WHERE command_id = %(command_id)s",
            {"status": final_status, "reason": reason, "command_id": cmd.command_id},
        )
        write_count += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6f. Write throughput metrics

# COMMAND ----------

tick_wall_elapsed = time.time() - tick_wall_start
ops_per_second = (read_count + write_count) / max(tick_wall_elapsed, 0.001)

lakebase_client.execute(
    """INSERT INTO throughput_metrics (
        metric_id, source, timestamp, wall_time_s,
        read_count, write_count, total_ops, ops_per_second,
        simulated_seconds, telemetry_rows, decision_rows
    ) VALUES (
        %(metric_id)s, %(source)s, NOW(), %(wall_time_s)s,
        %(read_count)s, %(write_count)s, %(total_ops)s, %(ops_per_second)s,
        %(simulated_seconds)s, %(telemetry_rows)s, %(decision_rows)s
    )""",
    {
        "metric_id": str(uuid.uuid4()),
        "source": "spacecraft_tick",
        "wall_time_s": tick_wall_elapsed,
        "read_count": read_count,
        "write_count": write_count,
        "total_ops": read_count + write_count,
        "ops_per_second": ops_per_second,
        "simulated_seconds": effective_duration,
        "telemetry_rows": len(telemetry_rows),
        "decision_rows": len(notable_decisions),
    },
)
write_count += 1
print(f"[SHIP] Throughput: {ops_per_second:.1f} ops/s | Wall time: {tick_wall_elapsed:.2f}s | Reads: {read_count} | Writes: {write_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Tick Summary

# COMMAND ----------

print(f"\n{'='*60}")
print(f"  SPACECRAFT TICK COMPLETE")
print(f"{'='*60}")
print(f"  Simulation time:  {new_sim_time}")
print(f"  Duration:         {effective_duration}s ({effective_duration/60:.1f} min)")
print(f"  Position:         ({state.position.x:.0f}, {state.position.y:.0f}, {state.position.z:.0f}) km")
print(f"  Speed:            {state.speed:.2f} km/s")
print(f"  Fuel:             {state.fuel_remaining_kg:.1f} kg")
print(f"  Hull:             {state.hull_integrity:.1f}%")
print(f"  Comm delay:       {final_delay:.1f}s ({final_delay/60:.1f} min)")
print(f"  Autopilot mode:   {autopilot.state.mode}")
print(f"  Evasions:         {autopilot.state.total_evasions}")
print(f"  Commands exec:    {autopilot.state.total_commands_executed}")
print(f"  Commands reject:  {autopilot.state.total_commands_rejected}")
print(f"  Corrections:      {autopilot.state.total_corrections}")
print(f"  Throughput:       {ops_per_second:.1f} ops/s")
print(f"{'='*60}")
