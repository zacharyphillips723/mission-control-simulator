"""
Mission Control — Agent Framework

Wraps the Mosaic AI Agent Framework (databricks-agents SDK) to implement
4 LLM-powered mission control agents that reason, call tools, communicate
with each other, and produce structured decisions.

Architecture:
  - MissionControlAgent: Single agent with system prompt, tools, and LLM endpoint
  - AgentOrchestrator: Runs the 4-agent chain with shared context and message passing
  - All communication via shared Lakebase state (ops.agent_memory)
  - All I/O logged to Delta (agents.decision_log, agents.message_log, models.inference_log)

Agent chain per tick:
  1. Flight Dynamics → trajectory + maneuver analysis
  2. Hazard Assessment → threat evaluation (reads Flight Dynamics output)
  3. Communications → delay-adjusted timing (reads both above)
  4. Mission Commander → GO/NO-GO decision (reads all three)
"""

import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import lakebase_client
from inference_logger import InferenceLogger


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class AgentMessage:
    """A message between agents or from agent to operator."""
    message_id: str
    from_agent: str
    to_agent: str  # specific agent name or "broadcast" or "operator"
    message_type: str  # analysis, alert, timing_plan, order, acknowledgment
    content: dict
    timestamp: datetime
    tick_id: str


@dataclass
class AgentDecision:
    """A structured decision from an agent."""
    decision_id: str
    agent_name: str
    decision_type: str
    input_summary: str
    reasoning: str
    recommendation: dict
    confidence_score: float
    timestamp: datetime
    tick_id: str
    tool_calls: list[dict] = field(default_factory=list)
    metadata: Optional[dict] = None


@dataclass
class TickResult:
    """Aggregated result of one full agent decision cycle."""
    tick_id: str
    commander_decision: str  # GO, HOLD, EMERGENCY_EVASION, ABORT
    summary: str
    decisions: list[AgentDecision]
    messages: list[AgentMessage]
    command_payload: Optional[dict] = None  # If GO: the command to queue
    confidence: float = 0.0
    duration_ms: float = 0.0


# ---------------------------------------------------------------------------
# Tool registry
# ---------------------------------------------------------------------------

@dataclass
class ToolDefinition:
    """A tool available to agents."""
    name: str
    description: str
    parameters: dict  # JSON Schema style
    function: Callable  # The actual Python function to call


class ToolRegistry:
    """Registry of tools available to agents."""

    def __init__(self):
        self._tools: dict[str, ToolDefinition] = {}

    def register(self, tool_def: ToolDefinition):
        self._tools[tool_def.name] = tool_def

    def get(self, name: str) -> Optional[ToolDefinition]:
        return self._tools.get(name)

    def get_schemas_for_agent(self, tool_names: list[str]) -> list[dict]:
        """Get OpenAI-compatible tool schemas for a set of tool names."""
        schemas = []
        for name in tool_names:
            tool = self._tools.get(name)
            if tool:
                schemas.append({
                    "type": "function",
                    "function": {
                        "name": tool.name,
                        "description": tool.description,
                        "parameters": tool.parameters,
                    }
                })
        return schemas

    def execute(self, name: str, arguments: dict) -> str:
        """Execute a tool by name with given arguments."""
        tool = self._tools.get(name)
        if not tool:
            return json.dumps({"error": f"Unknown tool: {name}"})
        try:
            result = tool.function(**arguments)
            return result if isinstance(result, str) else json.dumps(result)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @property
    def tool_names(self) -> list[str]:
        return list(self._tools.keys())


def build_tool_registry() -> ToolRegistry:
    """Build the full tool registry from agent_tools.py functions."""
    import agent_tools

    registry = ToolRegistry()

    # Physics tools
    registry.register(ToolDefinition(
        name="propagate_trajectory",
        description="Propagate spacecraft trajectory forward using N-body orbital mechanics. Returns predicted positions over time.",
        parameters={
            "type": "object",
            "properties": {
                "position_x": {"type": "number", "description": "Current X position in km"},
                "position_y": {"type": "number", "description": "Current Y position in km"},
                "position_z": {"type": "number", "description": "Current Z position in km"},
                "velocity_x": {"type": "number", "description": "Current X velocity in km/s"},
                "velocity_y": {"type": "number", "description": "Current Y velocity in km/s"},
                "velocity_z": {"type": "number", "description": "Current Z velocity in km/s"},
                "fuel_remaining_kg": {"type": "number", "description": "Remaining fuel in kg"},
                "duration_s": {"type": "number", "description": "Prediction horizon in seconds"},
            },
            "required": ["position_x", "position_y", "position_z", "velocity_x", "velocity_y", "velocity_z", "fuel_remaining_kg", "duration_s"],
        },
        function=agent_tools.tool_propagate_trajectory,
    ))

    registry.register(ToolDefinition(
        name="calculate_gravity_assist",
        description="Calculate velocity change from a gravity assist around a celestial body.",
        parameters={
            "type": "object",
            "properties": {
                "spacecraft_vel_x": {"type": "number"}, "spacecraft_vel_y": {"type": "number"}, "spacecraft_vel_z": {"type": "number"},
                "body_name": {"type": "string", "description": "earth, mars, or jupiter"},
                "periapsis_km": {"type": "number", "description": "Closest approach distance to body center"},
                "body_timestamp_s": {"type": "number", "description": "Simulation time of encounter"},
            },
            "required": ["spacecraft_vel_x", "spacecraft_vel_y", "spacecraft_vel_z", "body_name", "periapsis_km", "body_timestamp_s"],
        },
        function=agent_tools.tool_calculate_gravity_assist,
    ))

    registry.register(ToolDefinition(
        name="check_collision",
        description="Check collision risk between spacecraft and a hazard. Returns risk score 0-1, minimum distance, and closest approach time.",
        parameters={
            "type": "object",
            "properties": {
                "spacecraft_pos_x": {"type": "number"}, "spacecraft_pos_y": {"type": "number"}, "spacecraft_pos_z": {"type": "number"},
                "spacecraft_vel_x": {"type": "number"}, "spacecraft_vel_y": {"type": "number"}, "spacecraft_vel_z": {"type": "number"},
                "hazard_pos_x": {"type": "number"}, "hazard_pos_y": {"type": "number"}, "hazard_pos_z": {"type": "number"},
                "hazard_vel_x": {"type": "number"}, "hazard_vel_y": {"type": "number"}, "hazard_vel_z": {"type": "number"},
                "hazard_radius_km": {"type": "number"},
                "fuel_remaining_kg": {"type": "number"},
            },
            "required": ["spacecraft_pos_x", "spacecraft_pos_y", "spacecraft_pos_z",
                         "spacecraft_vel_x", "spacecraft_vel_y", "spacecraft_vel_z",
                         "hazard_pos_x", "hazard_pos_y", "hazard_pos_z",
                         "hazard_vel_x", "hazard_vel_y", "hazard_vel_z",
                         "hazard_radius_km"],
        },
        function=agent_tools.tool_check_collision,
    ))

    registry.register(ToolDefinition(
        name="calculate_communication_delay",
        description="Calculate one-way and round-trip communication delay between spacecraft and Earth.",
        parameters={
            "type": "object",
            "properties": {
                "spacecraft_pos_x": {"type": "number"}, "spacecraft_pos_y": {"type": "number"}, "spacecraft_pos_z": {"type": "number"},
            },
            "required": ["spacecraft_pos_x", "spacecraft_pos_y", "spacecraft_pos_z"],
        },
        function=agent_tools.tool_calculate_communication_delay,
    ))

    registry.register(ToolDefinition(
        name="estimate_fuel_for_burn",
        description="Estimate fuel required for a given delta-v using the Tsiolkovsky rocket equation.",
        parameters={
            "type": "object",
            "properties": {
                "delta_v_km_s": {"type": "number", "description": "Required velocity change in km/s"},
                "spacecraft_mass_kg": {"type": "number", "description": "Current spacecraft mass (default 5000)"},
            },
            "required": ["delta_v_km_s"],
        },
        function=agent_tools.tool_estimate_fuel_for_burn,
    ))

    # Data tools
    registry.register(ToolDefinition(
        name="get_current_state",
        description="Get the current spacecraft state (position, velocity, fuel, hull, engine status) from Lakebase.",
        parameters={"type": "object", "properties": {}},
        function=agent_tools.tool_get_current_state,
    ))

    registry.register(ToolDefinition(
        name="get_active_hazards",
        description="Get all currently active hazards with risk scores, sorted by danger level.",
        parameters={"type": "object", "properties": {}},
        function=agent_tools.tool_get_active_hazards,
    ))

    registry.register(ToolDefinition(
        name="get_candidate_maneuvers",
        description="Get top-ranked candidate maneuvers with delta-v, fuel cost, and scores.",
        parameters={
            "type": "object",
            "properties": {"top_n": {"type": "integer", "description": "Number of candidates to return (default 5)"}},
        },
        function=agent_tools.tool_get_candidate_maneuvers,
    ))

    registry.register(ToolDefinition(
        name="get_command_queue",
        description="Get pending commands awaiting transmission.",
        parameters={"type": "object", "properties": {}},
        function=agent_tools.tool_get_command_queue,
    ))

    return registry


# ---------------------------------------------------------------------------
# Single Agent
# ---------------------------------------------------------------------------

class MissionControlAgent:
    """
    A single LLM-powered mission control agent.

    Uses the Foundation Model API (pay-per-token) to call an LLM with tools.
    The LLM reasons about the mission state, calls tools, and produces
    a structured JSON decision.
    """

    def __init__(
        self,
        agent_name: str,
        system_prompt: str,
        tool_names: list[str],
        tool_registry: ToolRegistry,
        llm_endpoint: str = "databricks-meta-llama-3-3-70b-instruct",
        max_iterations: int = 8,
        temperature: float = 0.1,
        inference_logger: Optional[InferenceLogger] = None,
    ):
        self.agent_name = agent_name
        self.system_prompt = system_prompt
        self.tool_names = tool_names
        self.tool_registry = tool_registry
        self.llm_endpoint = llm_endpoint
        self.max_iterations = max_iterations
        self.temperature = temperature
        self.inference_logger = inference_logger
        self._tool_calls_made: list[dict] = []

    def run(
        self,
        context: dict,
        peer_context: Optional[dict] = None,
        tick_id: str = "",
    ) -> AgentDecision:
        """
        Run the agent for one decision cycle.

        Args:
            context: Current mission context (state, hazards, maneuvers, etc.)
            peer_context: Output from other agents (from agent_memory)
            tick_id: Correlation ID for this tick

        Returns:
            AgentDecision with reasoning and recommendation
        """
        self._tool_calls_made = []
        start_time = time.perf_counter()

        # Build the conversation
        messages = self._build_messages(context, peer_context)
        tool_schemas = self.tool_registry.get_schemas_for_agent(self.tool_names)

        # ReAct loop: LLM → tool calls → LLM → ... → final answer
        final_response = None
        for iteration in range(self.max_iterations):
            response = self._call_llm(messages, tool_schemas, tick_id)

            if not response:
                break

            # Check if LLM wants to call tools
            tool_calls = response.get("tool_calls", [])
            if not tool_calls:
                # No more tool calls — this is the final answer
                final_response = response.get("content", "")
                break

            # Execute tool calls
            messages.append({"role": "assistant", "content": None, "tool_calls": tool_calls})

            for tc in tool_calls:
                fn_name = tc["function"]["name"]
                fn_args = json.loads(tc["function"]["arguments"]) if isinstance(tc["function"]["arguments"], str) else tc["function"]["arguments"]

                result = self.tool_registry.execute(fn_name, fn_args)
                self._tool_calls_made.append({
                    "tool": fn_name,
                    "args": fn_args,
                    "result_preview": result[:200] if isinstance(result, str) else str(result)[:200],
                })

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": result,
                })

        elapsed_ms = (time.perf_counter() - start_time) * 1000

        # Parse structured output
        recommendation = self._parse_recommendation(final_response or "")
        confidence = recommendation.get("confidence", 0.5)

        return AgentDecision(
            decision_id=str(uuid.uuid4()),
            agent_name=self.agent_name,
            decision_type=recommendation.get("decision_type", "analysis"),
            input_summary=self._summarize_context(context, peer_context),
            reasoning=final_response or "No response from LLM",
            recommendation=recommendation,
            confidence_score=confidence,
            timestamp=datetime.now(timezone.utc),
            tick_id=tick_id,
            tool_calls=self._tool_calls_made,
            metadata={"iterations": iteration + 1, "elapsed_ms": elapsed_ms},
        )

    def _build_messages(self, context: dict, peer_context: Optional[dict]) -> list[dict]:
        """Build the message list for the LLM."""
        messages = [{"role": "system", "content": self.system_prompt}]

        # Add mission context
        context_text = "## Current Mission State\n"
        context_text += json.dumps(context, indent=2, default=str)

        if peer_context:
            context_text += "\n\n## Inputs from Other Agents\n"
            for agent_name, output in peer_context.items():
                context_text += f"\n### {agent_name}\n{json.dumps(output, indent=2, default=str)}\n"

        messages.append({"role": "user", "content": context_text})
        return messages

    def _call_llm(
        self,
        messages: list[dict],
        tool_schemas: list[dict],
        tick_id: str,
    ) -> Optional[dict]:
        """Call the LLM endpoint via Foundation Model API."""
        import requests
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        host = w.config.host.rstrip("/")
        token = w.config.token

        payload = {
            "messages": messages,
            "temperature": self.temperature,
            "max_tokens": 2048,
        }
        if tool_schemas:
            payload["tools"] = tool_schemas
            payload["tool_choice"] = "auto"

        url = f"{host}/serving-endpoints/{self.llm_endpoint}/invocations"

        def _do_call():
            resp = requests.post(
                url,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json=payload,
                timeout=120,
            )
            resp.raise_for_status()
            return resp.json()

        # Log the LLM call if logger is available
        if self.inference_logger:
            result = self.inference_logger.log_call(
                endpoint_name=self.llm_endpoint,
                caller=self.agent_name,
                input_features={"message_count": len(messages), "has_tools": bool(tool_schemas)},
                call_fn=_do_call,
                tick_id=tick_id,
            )
        else:
            result = _do_call()

        choices = result.get("choices", [])
        if not choices:
            return None

        message = choices[0].get("message", {})
        return message

    def _parse_recommendation(self, response_text: str) -> dict:
        """Extract structured JSON from the LLM response."""
        if not response_text:
            return {"decision_type": "no_response", "confidence": 0.0}

        # Try to extract JSON from the response
        try:
            # Look for JSON block
            start = response_text.find("{")
            end = response_text.rfind("}") + 1
            if start >= 0 and end > start:
                return json.loads(response_text[start:end])
        except json.JSONDecodeError:
            pass

        # Fallback: return the raw text as recommendation
        return {
            "decision_type": "analysis",
            "raw_response": response_text,
            "confidence": 0.5,
        }

    def _summarize_context(self, context: dict, peer_context: Optional[dict]) -> str:
        """Create a brief summary of inputs for logging."""
        parts = []
        if "speed_km_s" in context:
            parts.append(f"Speed={context['speed_km_s']:.2f}")
        if "fuel_remaining_kg" in context:
            parts.append(f"Fuel={context['fuel_remaining_kg']:.1f}")
        if "active_hazard_count" in context:
            parts.append(f"Hazards={context['active_hazard_count']}")
        if peer_context:
            parts.append(f"Peer inputs: {', '.join(peer_context.keys())}")
        return " | ".join(parts) if parts else "Standard context"


# ---------------------------------------------------------------------------
# Agent Orchestrator
# ---------------------------------------------------------------------------

class AgentOrchestrator:
    """
    Runs the 4-agent decision chain for one simulation tick.

    Manages:
    - Agent execution order (flight_dynamics → hazard → comms → commander)
    - Context passing between agents via Lakebase agent_memory
    - Message logging to both Lakebase (real-time) and Delta (audit)
    - Tick-level correlation via tick_id
    """

    def __init__(
        self,
        catalog: str,
        tool_registry: ToolRegistry,
        inference_logger: InferenceLogger,
        llm_endpoint: str = "databricks-meta-llama-3-3-70b-instruct",
    ):
        self.catalog = catalog
        self.tool_registry = tool_registry
        self.inference_logger = inference_logger
        self.llm_endpoint = llm_endpoint

        # Import system prompts from config
        self.agents: dict[str, MissionControlAgent] = {}
        self.messages: list[AgentMessage] = []
        self.decisions: list[AgentDecision] = []

    def initialize_agents(self, agent_configs: dict[str, dict]):
        """
        Initialize agents from configuration dicts.

        Args:
            agent_configs: Dict of agent_name -> config dict with keys:
                system_prompt, tools, max_iterations, temperature
        """
        for name, config in agent_configs.items():
            self.agents[name] = MissionControlAgent(
                agent_name=name,
                system_prompt=config["system_prompt"],
                tool_names=config["tools"],
                tool_registry=self.tool_registry,
                llm_endpoint=config.get("llm_endpoint", self.llm_endpoint),
                max_iterations=config.get("max_iterations", 8),
                temperature=config.get("temperature", 0.1),
                inference_logger=self.inference_logger,
            )

    def run_decision_cycle(
        self,
        mission_context: dict,
        spark=None,
    ) -> TickResult:
        """
        Run one full decision cycle with all 4 agents.

        Args:
            mission_context: Current mission state dict with keys like
                position_x/y/z, velocity_x/y/z, fuel_remaining_kg, etc.
            spark: SparkSession for writing to Lakebase/Delta

        Returns:
            TickResult with commander decision, all agent outputs, and messages
        """
        tick_id = f"tick-{uuid.uuid4().hex[:12]}"
        start_time = time.perf_counter()
        self.messages = []
        self.decisions = []
        peer_context: dict[str, dict] = {}

        # Agent execution order
        agent_order = ["flight_dynamics", "hazard_assessment", "communications", "mission_commander"]

        for agent_name in agent_order:
            agent = self.agents.get(agent_name)
            if not agent:
                print(f"  ⚠ Agent '{agent_name}' not initialized, skipping")
                continue

            print(f"  Running {agent_name}...")

            try:
                # Run the agent
                decision = agent.run(
                    context=mission_context,
                    peer_context=peer_context if peer_context else None,
                    tick_id=tick_id,
                )
                self.decisions.append(decision)

                # Store output for downstream agents
                peer_context[agent_name] = decision.recommendation

                # Create inter-agent message
                to_agent = {
                    "flight_dynamics": "hazard_assessment",
                    "hazard_assessment": "communications",
                    "communications": "mission_commander",
                    "mission_commander": "operator",
                }[agent_name]

                msg = AgentMessage(
                    message_id=str(uuid.uuid4()),
                    from_agent=agent_name,
                    to_agent=to_agent,
                    message_type=decision.decision_type,
                    content=decision.recommendation,
                    timestamp=datetime.now(timezone.utc),
                    tick_id=tick_id,
                )
                self.messages.append(msg)

                # Write to Lakebase agent_memory for downstream agents
                if spark:
                    self._write_agent_memory(spark, agent_name, decision.recommendation, tick_id)
                    self._write_realtime_message(spark, msg)

                print(f"    ✓ {agent_name}: confidence={decision.confidence_score:.2f}, "
                      f"tools={len(decision.tool_calls)}")

            except Exception as e:
                print(f"    ✗ {agent_name} failed: {e}")
                # Store error so downstream agents know
                peer_context[agent_name] = {"error": str(e), "agent": agent_name}

        # Extract commander decision
        commander_decision = self._extract_commander_decision(peer_context)
        elapsed_ms = (time.perf_counter() - start_time) * 1000

        # Build command payload if GO
        command_payload = None
        if commander_decision["decision"] in ("GO", "EMERGENCY_EVASION"):
            command_payload = self._build_command_payload(peer_context)

        result = TickResult(
            tick_id=tick_id,
            commander_decision=commander_decision["decision"],
            summary=commander_decision.get("summary", ""),
            decisions=self.decisions,
            messages=self.messages,
            command_payload=command_payload,
            confidence=commander_decision.get("confidence", 0.0),
            duration_ms=elapsed_ms,
        )

        # Flush all logs to Delta
        if spark:
            self._flush_to_delta(spark, result)

        return result

    def _extract_commander_decision(self, peer_context: dict) -> dict:
        """Extract the GO/HOLD/EVADE decision from the commander's output."""
        commander_output = peer_context.get("mission_commander", {})

        if isinstance(commander_output, dict):
            decision = commander_output.get("decision", "HOLD")
            return {
                "decision": decision,
                "summary": commander_output.get("summary", ""),
                "confidence": commander_output.get("confidence", 0.5),
            }

        return {"decision": "HOLD", "summary": "Commander output not parseable", "confidence": 0.3}

    def _build_command_payload(self, peer_context: dict) -> Optional[dict]:
        """Build a command payload from the flight dynamics recommendation."""
        fd_output = peer_context.get("flight_dynamics", {})
        recommended = fd_output.get("recommended_maneuver") or fd_output.get("recommended_maneuver_id")

        if isinstance(recommended, dict):
            return {
                "command_type": "burn",
                "maneuver_id": recommended.get("maneuver_id"),
                "burn_vector_x": recommended.get("burn_vector_x", 0),
                "burn_vector_y": recommended.get("burn_vector_y", 0),
                "burn_vector_z": recommended.get("burn_vector_z", 0),
                "burn_duration_s": recommended.get("burn_duration_s", 30),
            }
        return None

    def _write_agent_memory(self, spark, agent_name: str, output: dict, tick_id: str):
        """Write agent output to Lakebase for downstream agents to read."""
        safe_value = json.dumps(output, default=str)
        try:
            lakebase_client.upsert(
                "agent_memory",
                {"agent_name": agent_name, "memory_key": "latest_analysis"},
                {"memory_value": safe_value, "created_at": datetime.now(timezone.utc), "updated_at": datetime.now(timezone.utc)}
            )
        except Exception as e:
            print(f"    ⚠ Could not write agent_memory for {agent_name}: {e}")

    def _write_realtime_message(self, spark, msg: AgentMessage):
        """Write a message to Lakebase for real-time display."""
        safe_content = json.dumps(msg.content, default=str)
        try:
            lakebase_client.execute(
                "INSERT INTO agent_messages_realtime (message_id, from_agent, to_agent, message_type, content, tick_id, created_at) "
                "VALUES (%(message_id)s, %(from_agent)s, %(to_agent)s, %(message_type)s, %(content)s, %(tick_id)s, %(created_at)s)",
                {
                    "message_id": msg.message_id,
                    "from_agent": msg.from_agent,
                    "to_agent": msg.to_agent,
                    "message_type": msg.message_type,
                    "content": safe_content,
                    "tick_id": msg.tick_id,
                    "created_at": datetime.now(timezone.utc),
                }
            )
        except Exception as e:
            print(f"    ⚠ Could not write realtime message: {e}")

    def _flush_to_delta(self, spark, result: TickResult):
        """Write all decisions and messages to Delta Lake for audit trail."""
        from pyspark.sql import functions as F

        # Write decisions
        if result.decisions:
            decision_rows = [
                {
                    "decision_id": d.decision_id,
                    "agent_name": d.agent_name,
                    "decision_type": d.decision_type,
                    "timestamp": d.timestamp.isoformat(),
                    "input_summary": d.input_summary,
                    "reasoning": d.reasoning[:4000] if d.reasoning else "",
                    "recommendation": json.dumps(d.recommendation, default=str),
                    "confidence_score": d.confidence_score,
                    "accepted": None,
                    "metadata": json.dumps({
                        "tick_id": d.tick_id,
                        "tool_calls": d.tool_calls,
                        **(d.metadata or {}),
                    }, default=str),
                }
                for d in result.decisions
            ]
            df = spark.createDataFrame(decision_rows)
            df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
            df.write.mode("append").saveAsTable(f"`{self.catalog}`.agents.decision_log")

        # Write messages
        if result.messages:
            msg_rows = [
                {
                    "message_id": m.message_id,
                    "from_agent": m.from_agent,
                    "to_agent": m.to_agent,
                    "message_type": m.message_type,
                    "content": json.dumps(m.content, default=str),
                    "timestamp": m.timestamp.isoformat(),
                    "tick_id": m.tick_id,
                    "metadata": None,
                }
                for m in result.messages
            ]
            df = spark.createDataFrame(msg_rows)
            df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
            df.write.mode("append").saveAsTable(f"`{self.catalog}`.agents.message_log")

        # Flush inference logs
        count = self.inference_logger.flush_to_delta(spark)
        print(f"    ✓ Flushed {len(result.decisions)} decisions, "
              f"{len(result.messages)} messages, {count} inference logs to Delta")
