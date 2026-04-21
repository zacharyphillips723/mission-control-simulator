# Databricks notebook source
# MAGIC %md
# MAGIC # Mission Control — Deploy AI Agents
# MAGIC Defines and registers 4 Mosaic AI agents using the Agent Framework:
# MAGIC 1. **Flight Dynamics Agent** — Evaluates trajectory options, suggests corrections
# MAGIC 2. **Hazard Assessment Agent** — Identifies and ranks threats
# MAGIC 3. **Communications Agent** — Calculates delay-adjusted commands
# MAGIC 4. **Mission Commander Agent** — Orchestrates all agents, generates final recommendations

# COMMAND ----------

dbutils.widgets.text("catalog", "mission_control_dev", "Catalog Name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

import sys, os
notebook_path = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
repo_root = "/".join(notebook_path.split("/")[:-2])
sys.path.insert(0, os.path.join("/Workspace", repo_root, "src", "python"))

import mlflow
from mlflow.models import ModelConfig

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Tool Schemas
# MAGIC These are the UC function signatures that agents can call.

# COMMAND ----------

from dataclasses import dataclass

# Tools available to agents — these map to the functions in agent_tools.py
PHYSICS_TOOLS = [
    {
        "name": "propagate_trajectory",
        "description": "Propagate spacecraft trajectory forward using N-body orbital mechanics. Returns predicted positions over time.",
        "parameters": {
            "position_x": "float — Current X position in km",
            "position_y": "float — Current Y position in km",
            "position_z": "float — Current Z position in km",
            "velocity_x": "float — Current X velocity in km/s",
            "velocity_y": "float — Current Y velocity in km/s",
            "velocity_z": "float — Current Z velocity in km/s",
            "fuel_remaining_kg": "float — Remaining fuel",
            "duration_s": "float — Prediction horizon in seconds",
        }
    },
    {
        "name": "calculate_gravity_assist",
        "description": "Calculate velocity change from a gravity assist around a celestial body.",
        "parameters": {
            "spacecraft_vel_x": "float", "spacecraft_vel_y": "float", "spacecraft_vel_z": "float",
            "body_name": "string — earth, mars, or jupiter",
            "periapsis_km": "float — Closest approach distance",
        }
    },
    {
        "name": "check_collision",
        "description": "Check collision risk between spacecraft and a hazard. Returns risk score 0-1.",
        "parameters": {
            "spacecraft_pos_x": "float", "spacecraft_pos_y": "float", "spacecraft_pos_z": "float",
            "hazard_pos_x": "float", "hazard_pos_y": "float", "hazard_pos_z": "float",
            "hazard_radius_km": "float",
        }
    },
    {
        "name": "calculate_communication_delay",
        "description": "Calculate one-way and round-trip communication delay to Earth.",
        "parameters": {
            "spacecraft_pos_x": "float", "spacecraft_pos_y": "float", "spacecraft_pos_z": "float",
        }
    },
    {
        "name": "estimate_fuel_for_burn",
        "description": "Estimate fuel required for a maneuver using the Tsiolkovsky equation.",
        "parameters": {"delta_v_km_s": "float", "spacecraft_mass_kg": "float (optional, default 5000)"}
    },
]

DATA_TOOLS = [
    {
        "name": "get_current_state",
        "description": "Get the current spacecraft state (position, velocity, fuel, hull, engine status) from Lakebase.",
    },
    {
        "name": "get_active_hazards",
        "description": "Get all currently active hazards with risk scores, sorted by danger level.",
    },
    {
        "name": "get_candidate_maneuvers",
        "description": "Get top-ranked candidate maneuvers with delta-v, fuel cost, and scores.",
    },
    {
        "name": "get_command_queue",
        "description": "Get pending commands awaiting transmission.",
    },
]

MODEL_TOOLS = [
    {
        "name": "predict_trajectory",
        "description": "ML model prediction of future spacecraft position (60s lookahead).",
    },
    {
        "name": "score_hazard_risk",
        "description": "ML model scoring of hazard collision probability.",
    },
    {
        "name": "rank_maneuver",
        "description": "ML model scoring of maneuver quality (composite of feasibility, risk reduction, fuel efficiency).",
    },
    {
        "name": "get_optimal_timing",
        "description": "ML model recommendation for command timing offset accounting for communication delay.",
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent 1: Flight Dynamics Agent
# MAGIC Evaluates trajectory options and suggests orbital corrections.

# COMMAND ----------

FLIGHT_DYNAMICS_SYSTEM_PROMPT = """You are the Flight Dynamics Officer (FDO) for the DSV Odyssey mission.
Your job is to evaluate the spacecraft's trajectory and recommend course corrections.

Your responsibilities:
1. Monitor the current trajectory using propagate_trajectory
2. Evaluate gravity assist opportunities using calculate_gravity_assist
3. Review candidate maneuvers and score them
4. Recommend the optimal maneuver considering fuel budget and risk

When analyzing trajectory:
- Always check the current state first via get_current_state
- Propagate trajectory 1 hour ahead to assess drift
- Compare candidate maneuvers using rank_maneuver ML model
- Factor in fuel reserves — never recommend a burn that leaves < 50 kg fuel
- Use estimate_fuel_for_burn for precise consumption estimates

Output format your recommendations as structured JSON with:
- recommended_maneuver_id
- reasoning (2-3 sentences)
- fuel_impact_kg
- risk_assessment
- confidence (0-1)
"""

flight_dynamics_config = {
    "agent_name": "flight_dynamics",
    "llm_endpoint": "databricks-meta-llama-3-3-70b-instruct",
    "system_prompt": FLIGHT_DYNAMICS_SYSTEM_PROMPT,
    "tools": ["propagate_trajectory", "calculate_gravity_assist", "estimate_fuel_for_burn",
              "get_current_state", "get_candidate_maneuvers", "predict_trajectory", "rank_maneuver"],
    "max_iterations": 8,
    "temperature": 0.1,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent 2: Hazard Assessment Agent
# MAGIC Identifies, tracks, and ranks all threats to the spacecraft.

# COMMAND ----------

HAZARD_ASSESSMENT_SYSTEM_PROMPT = """You are the Hazard Assessment Officer for the DSV Odyssey mission.
Your job is to evaluate all threats and determine which require immediate action.

Your responsibilities:
1. Review all active hazards via get_active_hazards
2. For each hazard, run check_collision to get precise risk scores
3. Cross-reference with the ML hazard risk model via score_hazard_risk
4. Classify threats as CRITICAL (risk > 0.7), WARNING (0.3-0.7), or NOMINAL (< 0.3)
5. Determine if any hazard requires evasive maneuver

Assessment protocol:
- Check all active hazards every tick
- For CRITICAL hazards: immediately flag for evasion, include time-to-impact
- For WARNING hazards: monitor closely, project if risk is increasing
- Consider meteor showers and debris fields differently than single asteroids:
  * Asteroids: precise avoidance possible, check closest approach
  * Meteor showers: may need to transit through, assess cumulative hull damage risk
  * Debris fields: check field density and transit time
  * Solar flares: assess radiation exposure vs. shield capability

Output your assessment as structured JSON with:
- hazard_summary: list of active hazards with classifications
- critical_alerts: any hazards requiring immediate action
- recommended_action: "hold_course", "minor_correction", or "emergency_evasion"
- confidence (0-1)
"""

hazard_assessment_config = {
    "agent_name": "hazard_assessment",
    "llm_endpoint": "databricks-meta-llama-3-3-70b-instruct",
    "system_prompt": HAZARD_ASSESSMENT_SYSTEM_PROMPT,
    "tools": ["check_collision", "get_current_state", "get_active_hazards",
              "propagate_trajectory", "score_hazard_risk"],
    "max_iterations": 8,
    "temperature": 0.1,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent 3: Communications Agent
# MAGIC Manages delay-adjusted command timing and signal planning.

# COMMAND ----------

COMMUNICATIONS_SYSTEM_PROMPT = """You are the Communications Officer (CAPCOM) for the DSV Odyssey mission.
Your job is to manage communication timing between mission control and the spacecraft.

Your responsibilities:
1. Calculate current communication delay via calculate_communication_delay
2. Determine optimal command transmission timing via get_optimal_timing ML model
3. Account for signal delay when scheduling commands
4. Ensure commands arrive with enough lead time for spacecraft to execute

Communication protocol:
- Every command must account for one-way signal delay
- Critical commands (evasion) get priority transmission
- Predict spacecraft state at command receipt time using propagate_trajectory
- If delay exceeds 20 minutes, recommend pre-programmed contingencies
- Batch non-urgent commands to reduce transmission overhead

When a maneuver is approved:
1. Get current delay via calculate_communication_delay
2. Get optimal timing offset via get_optimal_timing
3. Calculate: transmit_time = now, receive_time = now + delay, execute_time = receive_time + timing_offset
4. Validate that spacecraft state at execute_time still supports the maneuver

Output as structured JSON:
- current_delay_s
- recommended_transmit_time
- estimated_receive_time
- execution_window
- confidence (0-1)
"""

communications_config = {
    "agent_name": "communications",
    "llm_endpoint": "databricks-meta-llama-3-3-70b-instruct",
    "system_prompt": COMMUNICATIONS_SYSTEM_PROMPT,
    "tools": ["calculate_communication_delay", "get_current_state", "get_command_queue",
              "propagate_trajectory", "get_optimal_timing"],
    "max_iterations": 6,
    "temperature": 0.1,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent 4: Mission Commander Agent
# MAGIC Orchestrates all other agents and generates final recommendations for the operator.

# COMMAND ----------

MISSION_COMMANDER_SYSTEM_PROMPT = """You are the Mission Commander for the DSV Odyssey return mission.
You are the senior decision-maker who synthesizes inputs from all flight officers.

Your team:
- Flight Dynamics Officer: trajectory analysis and maneuver recommendations
- Hazard Assessment Officer: threat identification and risk scoring
- Communications Officer: delay-adjusted command timing

Your responsibilities:
1. Review current mission state via get_current_state
2. Assess the flight dynamics recommendation (trajectory + maneuvers)
3. Assess the hazard assessment (threats + recommended action)
4. Assess the communications plan (timing feasibility)
5. Make a GO/NO-GO decision on proposed maneuvers
6. Generate a clear, actionable recommendation for the human operator

Decision framework:
- Safety first: if any hazard is CRITICAL, prioritize evasion over fuel efficiency
- Fuel conservation: never approve a maneuver that leaves fuel below emergency reserve (50 kg)
- Communication reliability: if delay is >15 min, require higher confidence thresholds
- When agents disagree, explain the tradeoff clearly to the operator
- Always provide your confidence level and the key factors driving your decision

You speak concisely and clearly, like a real mission commander:
- Lead with the decision: "RECOMMEND GO FOR BURN" or "RECOMMEND HOLD"
- Follow with 2-3 key reasons
- Flag any risks or dissenting inputs
- State confidence level

Output as structured JSON:
- decision: "GO" | "HOLD" | "EMERGENCY_EVASION" | "ABORT"
- summary: 2-3 sentence explanation
- key_factors: list of driving considerations
- risks: any flagged concerns
- confidence (0-1)
- operator_action_required: what the human needs to do
"""

mission_commander_config = {
    "agent_name": "mission_commander",
    "llm_endpoint": "databricks-meta-llama-3-3-70b-instruct",
    "system_prompt": MISSION_COMMANDER_SYSTEM_PROMPT,
    "tools": ["get_current_state", "get_active_hazards", "get_candidate_maneuvers",
              "get_command_queue", "propagate_trajectory", "check_collision",
              "calculate_communication_delay", "estimate_fuel_for_burn"],
    "max_iterations": 10,
    "temperature": 0.2,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Agent Configs
# MAGIC Save agent configurations to Unity Catalog volumes for deployment.

# COMMAND ----------

import json

spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.agents.configs")

agent_configs = {
    "flight_dynamics": flight_dynamics_config,
    "hazard_assessment": hazard_assessment_config,
    "communications": communications_config,
    "mission_commander": mission_commander_config,
}

for name, config in agent_configs.items():
    path = f"/Volumes/{catalog}/agents/configs/{name}_config.json"
    dbutils.fs.put(path, json.dumps(config, indent=2), overwrite=True)
    print(f"✓ Saved {name} config to {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Agents with MLflow
# MAGIC Register each agent as an MLflow model for versioning and deployment.

# COMMAND ----------

from agent_framework import build_tool_registry

# Validate tool registry matches agent configs
tool_registry = build_tool_registry()
all_tool_names = set(tool_registry.tool_names)
print(f"Tool registry: {len(all_tool_names)} tools available")

for name, config in agent_configs.items():
    missing = set(config["tools"]) - all_tool_names
    if missing:
        print(f"  ⚠ Agent {name} references missing tools: {missing}")
    else:
        print(f"  ✓ Agent {name}: all {len(config['tools'])} tools found")

# Register each agent config with MLflow for versioning, lineage, and experiment tracking
# Actual execution happens in 08_agent_decision_loop via AgentOrchestrator + MissionControlAgent
experiment_name = f"/Users/{spark.sql('SELECT current_user()').collect()[0][0]}/mission-control-agents"
mlflow.set_experiment(experiment_name)

for name, config in agent_configs.items():
    with mlflow.start_run(run_name=f"agent_{name}"):
        mlflow.log_dict(config, "agent_config.json")
        mlflow.log_param("agent_name", name)
        mlflow.log_param("llm_endpoint", config["llm_endpoint"])
        mlflow.log_param("num_tools", len(config["tools"]))
        mlflow.log_param("max_iterations", config["max_iterations"])
        mlflow.log_param("temperature", config["temperature"])

        # Log tool schemas for each agent
        agent_tool_schemas = tool_registry.get_schemas_for_agent(config["tools"])
        mlflow.log_dict({"tools": agent_tool_schemas}, "tool_schemas.json")
        mlflow.log_text(config["system_prompt"], "system_prompt.txt")

        print(f"✓ Agent registered: {name} (model: {config['llm_endpoint']}, tools: {len(config['tools'])})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"\n{'='*60}")
print("AGENT DEPLOYMENT COMPLETE")
print(f"{'='*60}")
for name, config in agent_configs.items():
    print(f"\n  Agent: {name}")
    print(f"  LLM: {config['llm_endpoint']}")
    print(f"  Tools: {', '.join(config['tools'])}")
    print(f"  Max iterations: {config['max_iterations']}")
print(f"\nConfigs saved to: /Volumes/{catalog}/agents/configs/")
print(f"MLflow experiment: {experiment_name}")
