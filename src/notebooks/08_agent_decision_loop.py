# Databricks notebook source
# MAGIC %md
# MAGIC # Mission Control — Agent Decision Loop
# MAGIC Runs all 4 LLM-powered agents in sequence using the Mosaic AI Agent Framework.
# MAGIC
# MAGIC **Agent chain:**
# MAGIC 1. Flight Dynamics Agent → trajectory analysis + maneuver recommendation
# MAGIC 2. Hazard Assessment Agent → threat evaluation + evasion planning
# MAGIC 3. Communications Agent → delay-adjusted command timing
# MAGIC 4. Mission Commander Agent → synthesize all inputs → GO/NO-GO decision
# MAGIC
# MAGIC Each agent:
# MAGIC - Calls real LLM endpoint with tool schemas
# MAGIC - Executes tools (physics engine, model serving, Lakebase queries)
# MAGIC - Writes output to Lakebase agent_memory for downstream agents
# MAGIC - Logs everything to Delta (decisions, messages, model inference calls)

# COMMAND ----------

dbutils.widgets.text("catalog", "mission_control_dev", "Catalog Name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

import sys, os, json, uuid, time
from datetime import datetime, timezone

notebook_path = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
repo_root = "/".join(notebook_path.split("/")[:-2])
sys.path.insert(0, os.path.join("/Workspace", repo_root, "src", "python"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Agent Framework

# COMMAND ----------

from agent_framework import (
    AgentOrchestrator,
    build_tool_registry,
)
from inference_logger import InferenceLogger

# Set CATALOG env var for agent_tools.py
os.environ["CATALOG"] = catalog

# Build tool registry (maps tool names → Python functions with schemas)
tool_registry = build_tool_registry()
print(f"Tool registry: {len(tool_registry.tool_names)} tools registered")
for name in tool_registry.tool_names:
    print(f"  - {name}")

# Initialize inference logger (captures every model serving call)
inference_logger = InferenceLogger(catalog=catalog)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Agent Configurations
# MAGIC System prompts and tool assignments for each agent.

# COMMAND ----------

FLIGHT_DYNAMICS_CONFIG = {
    "system_prompt": """You are the Flight Dynamics Officer (FDO) for the DSV Odyssey mission returning from Mars to Earth.

Your job is to evaluate the spacecraft's trajectory and recommend course corrections.

RESPONSIBILITIES:
1. Get current spacecraft state using get_current_state
2. Propagate trajectory 1-2 hours ahead using propagate_trajectory to assess drift
3. Review candidate maneuvers using get_candidate_maneuvers
4. Estimate fuel costs for promising maneuvers using estimate_fuel_for_burn
5. Recommend the optimal maneuver considering fuel budget and mission timeline

RULES:
- Never recommend a burn that leaves fuel below 50 kg (emergency reserve)
- Factor in current speed and distance from Earth
- Consider gravity assists if spacecraft is near Jupiter
- Compare at least 2-3 candidate maneuvers before recommending

OUTPUT: Respond with a JSON object containing:
{
  "decision_type": "maneuver_recommendation",
  "recommended_maneuver": {"maneuver_id": "...", "delta_v": ..., "fuel_cost_kg": ..., "burn_vector_x": ..., "burn_vector_y": ..., "burn_vector_z": ..., "burn_duration_s": ...},
  "trajectory_assessment": "brief description of current trajectory",
  "fuel_status": {"current_kg": ..., "available_for_maneuvers_kg": ...},
  "reasoning": "2-3 sentences explaining your recommendation",
  "confidence": 0.0-1.0
}""",
    "tools": ["propagate_trajectory", "calculate_gravity_assist", "estimate_fuel_for_burn",
              "get_current_state", "get_candidate_maneuvers"],
    "max_iterations": 8,
    "temperature": 0.1,
}

HAZARD_ASSESSMENT_CONFIG = {
    "system_prompt": """You are the Hazard Assessment Officer for the DSV Odyssey mission.

Your job is to evaluate all threats to the spacecraft and determine which require action.

RESPONSIBILITIES:
1. Get all active hazards using get_active_hazards
2. Get current spacecraft state using get_current_state
3. For each significant hazard, run check_collision to get precise risk and approach timing
4. Classify threats: CRITICAL (risk > 0.7), WARNING (0.3-0.7), NOMINAL (< 0.3)
5. Determine if evasive action is needed

HAZARD-SPECIFIC ASSESSMENT:
- Asteroids: precise avoidance possible, check closest approach distance
- Meteor showers: may need to transit through, assess hull damage risk
- Debris fields: check field density and expected transit time
- Solar flares: assess radiation exposure and communication disruption risk

OUTPUT: Respond with a JSON object containing:
{
  "decision_type": "hazard_assessment",
  "hazard_summary": [{"hazard_id": "...", "type": "...", "risk_score": ..., "classification": "CRITICAL|WARNING|NOMINAL", "min_distance_km": ..., "approach_time_s": ...}],
  "critical_count": 0,
  "recommended_action": "hold_course|minor_correction|emergency_evasion",
  "reasoning": "2-3 sentences",
  "confidence": 0.0-1.0
}""",
    "tools": ["check_collision", "get_current_state", "get_active_hazards", "propagate_trajectory"],
    "max_iterations": 8,
    "temperature": 0.1,
}

COMMUNICATIONS_CONFIG = {
    "system_prompt": """You are the Communications Officer (CAPCOM) for the DSV Odyssey mission.

Your job is to manage communication timing between mission control and the spacecraft.

RESPONSIBILITIES:
1. Calculate current communication delay using calculate_communication_delay
2. Check the command queue for pending commands using get_command_queue
3. Plan command transmission timing accounting for light-speed delay
4. Predict spacecraft state at command receipt time using propagate_trajectory

PROTOCOL:
- Critical commands (evasion) get priority — transmit immediately
- Non-urgent commands can be batched
- If delay > 20 minutes, recommend pre-programming contingencies
- Always verify the command is still valid for predicted state at receipt

OUTPUT: Respond with a JSON object containing:
{
  "decision_type": "timing_plan",
  "current_delay_s": ...,
  "current_delay_min": ...,
  "round_trip_s": ...,
  "timing_plan": {"timing_offset_s": ..., "priority": 1-10, "batch_eligible": true|false},
  "delay_assessment": "description of delay impact",
  "reasoning": "2-3 sentences",
  "confidence": 0.0-1.0
}""",
    "tools": ["calculate_communication_delay", "get_current_state", "get_command_queue", "propagate_trajectory"],
    "max_iterations": 6,
    "temperature": 0.1,
}

MISSION_COMMANDER_CONFIG = {
    "system_prompt": """You are the Mission Commander for the DSV Odyssey return mission from Mars to Earth.
You are the senior decision-maker who synthesizes inputs from your team.

YOUR TEAM'S REPORTS ARE IN THE "Inputs from Other Agents" SECTION:
- flight_dynamics: trajectory analysis and maneuver recommendations
- hazard_assessment: threat identification and risk levels
- communications: delay-adjusted command timing

RESPONSIBILITIES:
1. Review all agent inputs provided in the context
2. Get current state using get_current_state to verify
3. Make a GO/NO-GO decision on any proposed maneuvers
4. If hazards are CRITICAL, prioritize evasion over fuel efficiency
5. Generate a clear recommendation for the human operator

DECISION FRAMEWORK:
- Safety first: CRITICAL hazards → prioritize evasion
- Fuel: never approve below 50 kg emergency reserve
- Comm delay > 15 min → require higher confidence for GO
- When agents disagree, explain the tradeoff

SPEAK LIKE A MISSION COMMANDER:
- Lead with the decision: "RECOMMEND GO FOR BURN" or "RECOMMEND HOLD"
- 2-3 key reasons
- Flag risks
- What the operator needs to do

OUTPUT: Respond with a JSON object containing:
{
  "decision_type": "final_recommendation",
  "decision": "GO|HOLD|EMERGENCY_EVASION|ABORT",
  "summary": "2-3 sentence explanation",
  "key_factors": ["factor1", "factor2"],
  "risks": ["risk1", "risk2"],
  "confidence": 0.0-1.0,
  "operator_action_required": "what the human should do"
}""",
    "tools": ["get_current_state", "get_active_hazards", "get_candidate_maneuvers",
              "propagate_trajectory", "check_collision", "estimate_fuel_for_burn",
              "calculate_communication_delay"],
    "max_iterations": 10,
    "temperature": 0.2,
}

agent_configs = {
    "flight_dynamics": FLIGHT_DYNAMICS_CONFIG,
    "hazard_assessment": HAZARD_ASSESSMENT_CONFIG,
    "communications": COMMUNICATIONS_CONFIG,
    "mission_commander": MISSION_COMMANDER_CONFIG,
}

print(f"Agent configs loaded: {list(agent_configs.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Mission Context

# COMMAND ----------

from physics_engine import BODIES, Vector3, communication_delay

# Read current state
state_row = spark.sql(f"SELECT * FROM `{catalog}`.ops.mission_state WHERE state_id = 1").collect()
if not state_row:
    dbutils.notebook.exit("No mission state found — run setup first")
state_row = state_row[0]

# Read active hazards
hazard_count = spark.sql(f"SELECT COUNT(*) as cnt FROM `{catalog}`.ops.active_hazards").collect()[0]["cnt"]

# Read candidate maneuvers
maneuver_count = spark.sql(f"""
    SELECT COUNT(*) as cnt FROM `{catalog}`.navigation.candidate_maneuvers WHERE status = 'proposed'
""").collect()[0]["cnt"]

# Communication delay
earth_pos = BODIES["earth"].position_at(state_row.mission_elapsed_s)
sc_pos = Vector3(state_row.position_x, state_row.position_y, state_row.position_z)
comm_delay = communication_delay(sc_pos, earth_pos)

# Build context dict that all agents receive
mission_context = {
    "position_x": state_row.position_x,
    "position_y": state_row.position_y,
    "position_z": state_row.position_z,
    "velocity_x": state_row.velocity_x,
    "velocity_y": state_row.velocity_y,
    "velocity_z": state_row.velocity_z,
    "speed_km_s": Vector3(state_row.velocity_x, state_row.velocity_y, state_row.velocity_z).magnitude(),
    "fuel_remaining_kg": state_row.fuel_remaining_kg,
    "hull_integrity": state_row.hull_integrity,
    "engine_status": state_row.engine_status,
    "communication_delay_s": comm_delay,
    "communication_delay_min": comm_delay / 60,
    "mission_elapsed_s": state_row.mission_elapsed_s,
    "mission_elapsed_days": state_row.mission_elapsed_s / 86400,
    "active_hazard_count": hazard_count,
    "candidate_maneuver_count": maneuver_count,
    "distance_to_earth_km": (sc_pos - earth_pos).magnitude(),
    "mission_status": state_row.mission_status,
}

print(f"Mission context built:")
print(f"  Position: ({state_row.position_x:.0f}, {state_row.position_y:.0f}, {state_row.position_z:.0f}) km")
print(f"  Speed: {mission_context['speed_km_s']:.2f} km/s")
print(f"  Fuel: {state_row.fuel_remaining_kg:.1f} kg")
print(f"  Hull: {state_row.hull_integrity:.1f}%")
print(f"  Comm delay: {comm_delay:.1f}s ({comm_delay/60:.1f} min)")
print(f"  Active hazards: {hazard_count}")
print(f"  Candidate maneuvers: {maneuver_count}")
print(f"  Distance to Earth: {mission_context['distance_to_earth_km']:,.0f} km")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Agent Decision Cycle

# COMMAND ----------

# Initialize orchestrator
orchestrator = AgentOrchestrator(
    catalog=catalog,
    tool_registry=tool_registry,
    inference_logger=inference_logger,
)

# Initialize all 4 agents with their configs
orchestrator.initialize_agents(agent_configs)

tick_start = time.time()
print(f"\nStarting agent decision cycle...")
print(f"{'='*60}")

# Run the full 4-agent chain
tick_result = orchestrator.run_decision_cycle(
    mission_context=mission_context,
    spark=spark,
)

print(f"\n{'='*60}")
print(f"DECISION CYCLE COMPLETE")
print(f"{'='*60}")
print(f"Tick ID: {tick_result.tick_id}")
print(f"Commander Decision: {tick_result.commander_decision}")
print(f"Summary: {tick_result.summary}")
print(f"Confidence: {tick_result.confidence:.0%}")
print(f"Duration: {tick_result.duration_ms:.0f}ms")
print(f"Decisions logged: {len(tick_result.decisions)}")
print(f"Messages logged: {len(tick_result.messages)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Queue Command (if GO)

# COMMAND ----------

if tick_result.commander_decision in ("GO", "EMERGENCY_EVASION") and tick_result.command_payload:
    from command_executor import CommandExecutor

    executor = CommandExecutor(catalog=catalog)
    cmd = CommandExecutor.create_burn_command(
        burn_vector_x=tick_result.command_payload.get("burn_vector_x", 0),
        burn_vector_y=tick_result.command_payload.get("burn_vector_y", 0),
        burn_vector_z=tick_result.command_payload.get("burn_vector_z", 0),
        burn_duration_s=tick_result.command_payload.get("burn_duration_s", 30),
        maneuver_id=tick_result.command_payload.get("maneuver_id"),
        priority=1 if tick_result.commander_decision == "EMERGENCY_EVASION" else 5,
        approved_by="mission_commander",
    )

    # Queue the command in Lakebase
    spark.sql(executor.queue_command_sql(cmd))
    print(f"\n✓ Command queued: {cmd.command_id}")
    print(f"  Type: {cmd.command_type}")
    print(f"  Priority: {cmd.priority}")
    print(f"  Status: {cmd.status}")
    print(f"  Payload: {json.dumps(cmd.payload)}")
else:
    print(f"\nNo command queued — decision was {tick_result.commander_decision}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Throughput Metrics

# COMMAND ----------

# Count total operations this tick
total_ops = (
    inference_logger.call_count +
    len(tick_result.decisions) +
    len(tick_result.messages) +
    sum(len(d.tool_calls) for d in tick_result.decisions)
)

spark.sql(f"""
    INSERT INTO `{catalog}`.ops.throughput_metrics VALUES (
        '{str(uuid.uuid4())}',
        'agent_loop',
        CURRENT_TIMESTAMP(),
        {time.time() - tick_start},
        {inference_logger.call_count},
        {len(tick_result.decisions) + len(tick_result.messages)},
        {total_ops},
        {total_ops / max(time.time() - tick_start, 0.001)},
        0,
        0,
        0
    )
""")

print(f"\nThroughput: {total_ops} total operations")
print(f"  Model inference calls: {inference_logger.call_count}")
print(f"  Avg inference latency: {inference_logger.avg_latency_ms:.0f}ms")
print(f"  Agent decisions: {len(tick_result.decisions)}")
print(f"  Inter-agent messages: {len(tick_result.messages)}")
print(f"  Tool calls: {sum(len(d.tool_calls) for d in tick_result.decisions)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Decision Details

# COMMAND ----------

for decision in tick_result.decisions:
    print(f"\n--- {decision.agent_name} ---")
    print(f"  Type: {decision.decision_type}")
    print(f"  Confidence: {decision.confidence_score:.2f}")
    print(f"  Tool calls: {len(decision.tool_calls)}")
    if decision.tool_calls:
        for tc in decision.tool_calls[:5]:
            print(f"    → {tc['tool']}()")
    print(f"  Recommendation: {json.dumps(decision.recommendation, indent=2, default=str)[:500]}")
