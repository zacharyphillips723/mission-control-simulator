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

catalog = dbutils.widgets.get("catalog")
tick_duration_s = int(dbutils.widgets.get("tick_duration_s"))

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
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, IntegerType,
)
from datetime import datetime, timezone, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Current Spacecraft State

# COMMAND ----------

state_row = spark.sql(f"""
    SELECT * FROM `{catalog}`.ops.mission_state WHERE state_id = 1
""").collect()[0]

clock_row = spark.sql(f"""
    SELECT * FROM `{catalog}`.ops.simulation_clock WHERE clock_id = 1
""").collect()[0]

current_state = SpacecraftState(
    position=Vector3(state_row.position_x, state_row.position_y, state_row.position_z),
    velocity=Vector3(state_row.velocity_x, state_row.velocity_y, state_row.velocity_z),
    fuel_remaining_kg=state_row.fuel_remaining_kg,
    hull_integrity=state_row.hull_integrity,
    engine_status=state_row.engine_status,
    timestamp_s=state_row.mission_elapsed_s,
)

sim_time = clock_row.simulation_time
time_scale = clock_row.time_scale

print(f"[SHIP] Current position: ({current_state.position.x:.0f}, {current_state.position.y:.0f}, {current_state.position.z:.0f})")
print(f"[SHIP] Speed: {current_state.speed:.2f} km/s | Fuel: {current_state.fuel_remaining_kg:.1f} kg | Hull: {current_state.hull_integrity:.1f}%")
print(f"[SHIP] Simulation time: {sim_time} | Time scale: {time_scale}x")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Check Command Queue for Received Commands

# COMMAND ----------

# Commands whose estimated_receive_time has passed are now "received" by the ship
received_commands_rows = spark.sql(f"""
    UPDATE `{catalog}`.ops.command_queue
    SET status = 'received'
    WHERE status = 'in_flight'
      AND estimated_receive_time <= TIMESTAMP '{sim_time.strftime("%Y-%m-%d %H:%M:%S")}'
""")

# Fetch all commands that are now in 'received' status and ready for autopilot
pending_cmd_rows = spark.sql(f"""
    SELECT * FROM `{catalog}`.ops.command_queue
    WHERE status = 'received'
    ORDER BY priority DESC, estimated_receive_time ASC
""").collect()

pending_commands = []
for row in pending_cmd_rows:
    pending_commands.append(GroundCommand(
        command_id=row.command_id,
        command_type=row.command_type,
        burn_vector_x=float(row.burn_vector_x) if hasattr(row, 'burn_vector_x') and row.burn_vector_x else 0.0,
        burn_vector_y=float(row.burn_vector_y) if hasattr(row, 'burn_vector_y') and row.burn_vector_y else 0.0,
        burn_vector_z=float(row.burn_vector_z) if hasattr(row, 'burn_vector_z') and row.burn_vector_z else 0.0,
        burn_duration_s=float(row.burn_duration_s) if hasattr(row, 'burn_duration_s') and row.burn_duration_s else 0.0,
        delta_v=float(row.delta_v) if hasattr(row, 'delta_v') and row.delta_v else 0.0,
        fuel_cost_kg=float(row.fuel_cost_kg) if hasattr(row, 'fuel_cost_kg') and row.fuel_cost_kg else 0.0,
        priority=row.priority if hasattr(row, 'priority') and row.priority else "normal",
        status="received",
    ))

print(f"[SHIP] Pending commands: {len(pending_commands)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read Active Hazards for Autopilot Awareness

# COMMAND ----------

from physics_engine import Hazard

hazard_rows = spark.sql(f"""
    SELECT * FROM `{catalog}`.ops.active_hazards
""").collect()

active_hazards = []
for h in hazard_rows:
    active_hazards.append(Hazard(
        hazard_id=h.hazard_id,
        hazard_type=h.hazard_type,
        position=Vector3(h.position_x, h.position_y, h.position_z),
        velocity=Vector3(h.velocity_x, h.velocity_y, h.velocity_z),
        radius_km=h.radius_km,
    ))

print(f"[SHIP] Active hazards in range: {len(active_hazards)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load Autopilot State

# COMMAND ----------

try:
    ap_row = spark.sql(f"""
        SELECT * FROM `{catalog}`.ops.spacecraft_autopilot_state WHERE autopilot_id = 1
    """).collect()
    if ap_row:
        ap_row = ap_row[0]
        autopilot_state = AutopilotState(
            mode=ap_row.mode,
            total_corrections=int(ap_row.total_corrections),
            total_evasions=int(ap_row.total_evasions),
            total_commands_executed=int(ap_row.total_commands_executed),
            total_commands_rejected=int(ap_row.total_commands_rejected),
            fuel_used_by_autopilot_kg=float(ap_row.fuel_used_by_autopilot_kg),
            last_decision_time_s=float(ap_row.last_decision_time_s),
            ticks_processed=int(ap_row.ticks_processed),
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
            spark.sql(f"""
                INSERT INTO `{catalog}`.ops.telemetry_realtime VALUES (
                    '{row["telemetry_id"]}',
                    TIMESTAMP '{row["timestamp"]}',
                    {row["position_x"]}, {row["position_y"]}, {row["position_z"]},
                    {row["velocity_x"]}, {row["velocity_y"]}, {row["velocity_z"]},
                    {row["fuel_remaining_kg"]}, {row["hull_integrity"]},
                    '{row["engine_status"]}',
                    {row["communication_delay_s"]},
                    TIMESTAMP '{row["ingestion_timestamp"]}'
                )
            """)
            write_count += 1

        # Trim realtime table to latest 600 rows
        spark.sql(f"""
            DELETE FROM `{catalog}`.ops.telemetry_realtime
            WHERE timestamp < (
                SELECT MIN(timestamp) FROM (
                    SELECT timestamp FROM `{catalog}`.ops.telemetry_realtime
                    ORDER BY timestamp DESC
                    LIMIT 600
                )
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

spark.sql(f"""
    UPDATE `{catalog}`.ops.mission_state
    SET
        timestamp = TIMESTAMP '{new_sim_time.strftime("%Y-%m-%d %H:%M:%S")}',
        position_x = {state.position.x},
        position_y = {state.position.y},
        position_z = {state.position.z},
        velocity_x = {state.velocity.x},
        velocity_y = {state.velocity.y},
        velocity_z = {state.velocity.z},
        fuel_remaining_kg = {state.fuel_remaining_kg},
        hull_integrity = {state.hull_integrity},
        engine_status = '{state.engine_status}',
        communication_delay_s = {final_delay},
        mission_elapsed_s = {state.timestamp_s},
        updated_at = CURRENT_TIMESTAMP()
    WHERE state_id = 1
""")
write_count += 1
print(f"[SHIP] Mission state updated — sim time: {new_sim_time}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6c. Update autopilot state in Lakebase

# COMMAND ----------

ap_state = autopilot.state
spark.sql(f"""
    MERGE INTO `{catalog}`.ops.spacecraft_autopilot_state AS target
    USING (SELECT 1 AS autopilot_id) AS source
    ON target.autopilot_id = source.autopilot_id
    WHEN MATCHED THEN UPDATE SET
        mode = '{ap_state.mode}',
        total_corrections = {ap_state.total_corrections},
        total_evasions = {ap_state.total_evasions},
        total_commands_executed = {ap_state.total_commands_executed},
        total_commands_rejected = {ap_state.total_commands_rejected},
        fuel_used_by_autopilot_kg = {ap_state.fuel_used_by_autopilot_kg},
        last_decision_time_s = {ap_state.last_decision_time_s},
        ticks_processed = {ap_state.ticks_processed},
        updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        autopilot_id, mode, total_corrections, total_evasions,
        total_commands_executed, total_commands_rejected,
        fuel_used_by_autopilot_kg, last_decision_time_s, ticks_processed, updated_at
    ) VALUES (
        1, '{ap_state.mode}', {ap_state.total_corrections}, {ap_state.total_evasions},
        {ap_state.total_commands_executed}, {ap_state.total_commands_rejected},
        {ap_state.fuel_used_by_autopilot_kg}, {ap_state.last_decision_time_s},
        {ap_state.ticks_processed}, CURRENT_TIMESTAMP()
    )
""")
write_count += 1
print(f"[SHIP] Autopilot state saved — mode: {ap_state.mode}, evasions: {ap_state.total_evasions}, corrections: {ap_state.total_corrections}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6d. Update simulation clock

# COMMAND ----------

spark.sql(f"""
    UPDATE `{catalog}`.ops.simulation_clock
    SET
        simulation_time = TIMESTAMP '{new_sim_time.strftime("%Y-%m-%d %H:%M:%S")}',
        total_elapsed_s = {state.timestamp_s}
    WHERE clock_id = 1
""")
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
        spark.sql(f"""
            UPDATE `{catalog}`.ops.command_queue
            SET status = '{final_status}',
                response_reason = '{reason.replace("'", "''")}'
            WHERE command_id = '{cmd.command_id}'
        """)
        write_count += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6f. Write throughput metrics

# COMMAND ----------

tick_wall_elapsed = time.time() - tick_wall_start
ops_per_second = (read_count + write_count) / max(tick_wall_elapsed, 0.001)

spark.sql(f"""
    INSERT INTO `{catalog}`.ops.throughput_metrics VALUES (
        '{str(uuid.uuid4())}',
        'spacecraft_tick',
        CURRENT_TIMESTAMP(),
        {tick_wall_elapsed},
        {read_count},
        {write_count},
        {read_count + write_count},
        {ops_per_second},
        {effective_duration},
        {len(telemetry_rows)},
        {len(notable_decisions)}
    )
""")
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
