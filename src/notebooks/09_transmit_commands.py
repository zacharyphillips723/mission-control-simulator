# Databricks notebook source
# MAGIC %md
# MAGIC # Mission Control — Command Transmission
# MAGIC
# MAGIC Transmits approved commands from mission control to the spacecraft.
# MAGIC Commands travel at the speed of light — there is a real delay between
# MAGIC transmission and receipt based on spacecraft distance from Earth.
# MAGIC
# MAGIC **Flow:**
# MAGIC 1. Read approved commands from `ops.command_queue`
# MAGIC 2. Validate each against predicted state at receipt time
# MAGIC 3. Mark as `in_flight` with `estimated_receive_time`
# MAGIC 4. Log transmission events to Delta `commands.command_log`
# MAGIC 5. Track throughput metrics

# COMMAND ----------

dbutils.widgets.text("catalog", "mission_control_dev", "Catalog Name")
dbutils.widgets.text("lakebase_project_id", "mission-control", "Lakebase Project ID")
catalog = dbutils.widgets.get("catalog")
lakebase_project_id = dbutils.widgets.get("lakebase_project_id")

# COMMAND ----------

import sys, os, json, uuid, time
from datetime import datetime, timezone, timedelta

notebook_path = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
repo_root = "/".join(notebook_path.split("/")[:-2])
sys.path.insert(0, os.path.join("/Workspace", repo_root, "src", "python"))

from physics_engine import (
    SpacecraftState, Vector3, BODIES,
    communication_delay, propagate_trajectory, estimate_fuel_cost,
)
from command_executor import CommandExecutor, Command
from pyspark.sql import functions as F

import lakebase_client
lakebase_client.init(lakebase_project_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Current State & Pending Commands

# COMMAND ----------

tick_start = time.time()
read_ops = 0
write_ops = 0

# Read current spacecraft state for validation (Lakebase)
state_row = lakebase_client.fetch_one("SELECT * FROM mission_state WHERE state_id = 1")
read_ops += 1

current_state = SpacecraftState(
    position=Vector3(state_row["position_x"], state_row["position_y"], state_row["position_z"]),
    velocity=Vector3(state_row["velocity_x"], state_row["velocity_y"], state_row["velocity_z"]),
    fuel_remaining_kg=state_row["fuel_remaining_kg"],
    hull_integrity=state_row["hull_integrity"],
    engine_status=state_row["engine_status"],
    timestamp_s=state_row["mission_elapsed_s"],
)

# Calculate current comm delay
earth_pos = BODIES["earth"].position_at(current_state.timestamp_s)
comm_delay_s = communication_delay(current_state.position, earth_pos)

# Read approved commands awaiting transmission (Lakebase)
approved_commands = lakebase_client.fetch_all(
    "SELECT * FROM command_queue WHERE status = 'approved' ORDER BY priority ASC, created_at ASC"
)
read_ops += 1

print(f"[COMMS] Communication delay: {comm_delay_s:.1f}s ({comm_delay_s/60:.1f} min)")
print(f"[COMMS] Approved commands awaiting transmission: {len(approved_commands)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate & Transmit Commands

# COMMAND ----------

executor = CommandExecutor(catalog=catalog)
sim_time = state_row["timestamp"]

transmitted = 0
rejected = 0

for cmd_row in approved_commands:
    cmd_id = cmd_row["command_id"]
    cmd_type = cmd_row["command_type"]
    raw_payload = cmd_row["payload"]
    payload = json.loads(raw_payload) if isinstance(raw_payload, str) and raw_payload else (raw_payload or {})
    priority = cmd_row["priority"]

    print(f"\n  Processing command {cmd_id[:8]}... (type={cmd_type}, priority={priority})")

    # Build Command object
    cmd = Command(
        command_id=cmd_id,
        command_type=cmd_type,
        maneuver_id=None,
        payload=payload,
        priority=priority,
        created_at=cmd_row["created_at"],
        approved_by=cmd_row["approved_by"],
        status="approved",
    )

    # Validate: will this command be safe when it arrives?
    is_valid, reason = CommandExecutor.validate_command(cmd, current_state, comm_delay_s)

    if not is_valid:
        # Reject the command (Lakebase)
        lakebase_client.execute(
            "UPDATE command_queue SET status = 'failed', updated_at = NOW() WHERE command_id = %(cmd_id)s",
            {"cmd_id": cmd_id},
        )
        write_ops += 1
        rejected += 1
        print(f"    ✗ REJECTED: {reason}")

        # Log rejection to Delta (stays with Spark)
        cmd.status = "rejected"
        cmd.rejection_reason = reason
        spark.sql(executor.log_to_delta_sql(cmd))
        write_ops += 1
        continue

    # Transmit the command
    # Calculate when the spacecraft will receive it
    estimated_receive_sim_time = sim_time + timedelta(seconds=comm_delay_s)

    # Update command_queue in Lakebase
    lakebase_client.execute(
        """UPDATE command_queue
           SET status = 'in_flight',
               transmit_time = NOW(),
               estimated_receive_time = %(est_receive)s,
               updated_at = NOW()
           WHERE command_id = %(cmd_id)s""",
        {
            "cmd_id": cmd_id,
            "est_receive": estimated_receive_sim_time,
        },
    )
    write_ops += 1

    # Log transmission to Delta (stays with Spark)
    cmd.status = "in_flight"
    cmd.transmit_time = datetime.now(timezone.utc)
    cmd.estimated_receive_time = estimated_receive_sim_time
    spark.sql(executor.log_to_delta_sql(cmd))
    write_ops += 1

    transmitted += 1
    print(f"    ✓ TRANSMITTED — ETA at spacecraft: {estimated_receive_sim_time.strftime('%H:%M:%S')} "
          f"(in {comm_delay_s:.0f}s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check In-Flight Command Status

# COMMAND ----------

# Report on all in-flight commands (Lakebase)
in_flight = lakebase_client.fetch_all(
    """SELECT command_id, command_type, priority, transmit_time, estimated_receive_time
       FROM command_queue
       WHERE status = 'in_flight'
       ORDER BY estimated_receive_time ASC"""
)
read_ops += 1

if in_flight:
    print(f"\n[COMMS] Commands currently in flight: {len(in_flight)}")
    for cmd in in_flight:
        eta = cmd["estimated_receive_time"]
        print(f"  - {cmd['command_id'][:8]}... ({cmd['command_type']}) → ETA: {eta}")
else:
    print("\n[COMMS] No commands currently in flight")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Throughput Metrics

# COMMAND ----------

tick_elapsed = time.time() - tick_start
total_ops = read_ops + write_ops
ops_per_sec = total_ops / max(tick_elapsed, 0.001)

try:
    lakebase_client.execute(
        """INSERT INTO throughput_metrics
           (metric_id, component, recorded_at, elapsed_s, model_calls,
            db_operations, total_operations, ops_per_second,
            error_count, retry_count, cache_hits)
           VALUES (%(metric_id)s, %(component)s, NOW(), %(elapsed_s)s, %(model_calls)s,
                   %(db_operations)s, %(total_operations)s, %(ops_per_second)s,
                   0, 0, 0)""",
        {
            "metric_id": str(uuid.uuid4()),
            "component": "command_transmission",
            "elapsed_s": tick_elapsed,
            "model_calls": read_ops,
            "db_operations": write_ops,
            "total_operations": total_ops,
            "ops_per_second": ops_per_sec,
        },
    )
except Exception:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"\n{'='*60}")
print(f"  COMMAND TRANSMISSION COMPLETE")
print(f"{'='*60}")
print(f"  Commands transmitted: {transmitted}")
print(f"  Commands rejected:    {rejected}")
print(f"  Commands in flight:   {len(in_flight)}")
print(f"  Comm delay:           {comm_delay_s:.1f}s ({comm_delay_s/60:.1f} min)")
print(f"  Throughput:           {ops_per_sec:.1f} ops/s")
print(f"  Wall time:            {tick_elapsed:.2f}s")
print(f"{'='*60}")
