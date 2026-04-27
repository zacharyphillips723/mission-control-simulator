"""
Mission Control — Spacecraft Autopilot

Deterministic state machine that runs onboard the spacecraft. Makes autonomous
decisions based on a strict priority stack. This is NOT an LLM — it is a rule-based
controller with optional model-serving callouts for trajectory prediction and
hazard risk scoring.

Priority stack (highest first):
  1. EMERGENCY EVASION — Hazard within 500km and closing
  2. EXECUTE GROUND COMMANDS — Process validated command queue
  3. STATION KEEPING — Correct trajectory drift
  4. FUEL CONSERVATION — Enter safe/emergency modes at low fuel
  5. COAST — No action needed
"""

import math
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional

from mission_constants import EXHAUST_VELOCITY_KM_S, DRY_MASS_KG
from physics_engine import (
    SpacecraftState,
    Vector3,
    Hazard,
    estimate_fuel_cost,
    propagate_state,
    propagate_trajectory,
    closest_approach,
    collision_risk,
)


# ---------------------------------------------------------------------------
# Autopilot Constants
# ---------------------------------------------------------------------------

EVASION_TRIGGER_DISTANCE_KM = 500.0
MAX_EVASION_DELTA_V = 0.5  # km/s — autopilot authority limit
STATION_KEEPING_MAX_DV = 0.01  # km/s per tick
FUEL_SAFE_MODE_KG = 100.0
FUEL_EMERGENCY_ONLY_KG = 50.0
DRIFT_THRESHOLD_KM = 50.0  # position drift before station keeping kicks in


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------

@dataclass
class AutopilotState:
    """Persistent state tracked across ticks by the autopilot."""
    mode: str = "nominal"  # nominal, safe_mode, emergency_only
    total_corrections: int = 0
    total_evasions: int = 0
    total_commands_executed: int = 0
    total_commands_rejected: int = 0
    fuel_used_by_autopilot_kg: float = 0.0
    last_decision_time_s: float = 0.0
    ticks_processed: int = 0

    def to_dict(self) -> dict:
        return {
            "mode": self.mode,
            "total_corrections": self.total_corrections,
            "total_evasions": self.total_evasions,
            "total_commands_executed": self.total_commands_executed,
            "total_commands_rejected": self.total_commands_rejected,
            "fuel_used_by_autopilot_kg": self.fuel_used_by_autopilot_kg,
            "last_decision_time_s": self.last_decision_time_s,
            "ticks_processed": self.ticks_processed,
        }


@dataclass
class AutopilotDecision:
    """The output of a single autopilot tick — always produced, even for coast."""
    decision_id: str
    timestamp_s: float
    priority_level: int  # 1=evasion, 2=command, 3=station_keeping, 4=fuel_conservation, 5=coast
    action: str  # evasion_burn, execute_command, station_keeping_burn, enter_safe_mode, enter_emergency_only, coast
    reasoning: str
    burn_vector: Optional[Vector3] = None
    burn_duration_s: float = 0.0
    delta_v: float = 0.0
    fuel_cost_kg: float = 0.0
    target_hazard_id: Optional[str] = None
    command_id: Optional[str] = None
    elapsed_ms: float = 0.0  # wall-clock time to compute decision

    def to_dict(self) -> dict:
        d = {
            "decision_id": self.decision_id,
            "timestamp_s": self.timestamp_s,
            "priority_level": self.priority_level,
            "action": self.action,
            "reasoning": self.reasoning,
            "burn_duration_s": self.burn_duration_s,
            "delta_v": self.delta_v,
            "fuel_cost_kg": self.fuel_cost_kg,
            "target_hazard_id": self.target_hazard_id,
            "command_id": self.command_id,
            "elapsed_ms": self.elapsed_ms,
        }
        if self.burn_vector:
            d["burn_vector_x"] = self.burn_vector.x
            d["burn_vector_y"] = self.burn_vector.y
            d["burn_vector_z"] = self.burn_vector.z
        else:
            d["burn_vector_x"] = 0.0
            d["burn_vector_y"] = 0.0
            d["burn_vector_z"] = 0.0
        return d


@dataclass
class GroundCommand:
    """A command received from ground control."""
    command_id: str
    command_type: str  # burn, abort_burn, adjust_attitude, system_check
    burn_vector_x: float = 0.0
    burn_vector_y: float = 0.0
    burn_vector_z: float = 0.0
    burn_duration_s: float = 0.0
    delta_v: float = 0.0
    fuel_cost_kg: float = 0.0
    priority: str = "normal"  # normal, high, critical
    status: str = "received"


# ---------------------------------------------------------------------------
# Autopilot Engine
# ---------------------------------------------------------------------------

class SpacecraftAutopilot:
    """
    Deterministic state machine for autonomous spacecraft control.

    Evaluates a strict priority stack each tick and produces exactly one
    AutopilotDecision. The decision may include a thrust vector or may
    simply be "coast" if nothing requires action.
    """

    def __init__(
        self,
        autopilot_state: Optional[AutopilotState] = None,
        planned_trajectory: Optional[list[SpacecraftState]] = None,
        model_caller: Optional[Callable] = None,
    ):
        """
        Args:
            autopilot_state: Persistent state from previous ticks (or fresh).
            planned_trajectory: The nominal trajectory the spacecraft should follow.
                Used for station-keeping drift detection.
            model_caller: Optional async function to call model serving endpoints.
                Signature: model_caller(endpoint_name: str, payload: dict) -> dict
                If None, autopilot uses only deterministic logic.
        """
        self.state = autopilot_state or AutopilotState()
        self.planned_trajectory = planned_trajectory or []
        self.model_caller = model_caller
        self._ops_count = 0
        self._tick_start_time = 0.0

    def run_tick(
        self,
        spacecraft_state: SpacecraftState,
        active_hazards: list[Hazard],
        pending_commands: list[GroundCommand],
        elapsed_days: float,
    ) -> AutopilotDecision:
        """
        Run one autopilot tick through the full priority stack.

        Returns an AutopilotDecision regardless of whether action is taken.
        """
        self._tick_start_time = time.time()
        self._ops_count = 0

        # Update fuel-based mode first
        self._update_fuel_mode(spacecraft_state)
        self._ops_count += 1

        # Priority 1: Emergency Evasion
        decision = self._check_emergency_evasion(spacecraft_state, active_hazards)
        if decision:
            return self._finalize(decision)

        # Priority 2: Execute Ground Commands
        decision = self._check_ground_commands(spacecraft_state, pending_commands, active_hazards)
        if decision:
            return self._finalize(decision)

        # Priority 3: Station Keeping
        decision = self._check_station_keeping(spacecraft_state)
        if decision:
            return self._finalize(decision)

        # Priority 4: Fuel Conservation (mode transitions logged)
        decision = self._check_fuel_conservation(spacecraft_state)
        if decision:
            return self._finalize(decision)

        # Priority 5: Coast
        return self._finalize(AutopilotDecision(
            decision_id=str(uuid.uuid4()),
            timestamp_s=spacecraft_state.timestamp_s,
            priority_level=5,
            action="coast",
            reasoning="All systems nominal. No hazards, commands, or drift requiring action.",
        ))

    # ------------------------------------------------------------------
    # Priority 1: Emergency Evasion
    # ------------------------------------------------------------------

    def _check_emergency_evasion(
        self,
        state: SpacecraftState,
        hazards: list[Hazard],
    ) -> Optional[AutopilotDecision]:
        """Check for hazards within 500km and closing — compute perpendicular burn."""
        if self.state.mode == "emergency_only" and state.fuel_remaining_kg < 10:
            return None  # Not enough fuel even for emergencies

        closest_threat = None
        closest_dist = float("inf")

        for hazard in hazards:
            self._ops_count += 1
            rel_pos = hazard.position - state.position
            distance = rel_pos.magnitude() - hazard.radius_km

            if distance > EVASION_TRIGGER_DISTANCE_KM:
                continue

            # Check if closing (relative velocity toward spacecraft)
            rel_vel = hazard.velocity - state.velocity
            closing_rate = -rel_pos.normalized().dot(rel_vel)

            if closing_rate <= 0:
                continue  # Moving away, not a threat

            if distance < closest_dist:
                closest_dist = distance
                closest_threat = (hazard, distance, closing_rate)

        if closest_threat is None:
            return None

        hazard, distance, closing_rate = closest_threat
        self._ops_count += 5  # evasion computation ops

        # Compute perpendicular burn vector
        to_hazard = (hazard.position - state.position).normalized()
        vel_dir = state.velocity.normalized()

        # Cross product to get perpendicular direction
        perp = Vector3(
            vel_dir.y * to_hazard.z - vel_dir.z * to_hazard.y,
            vel_dir.z * to_hazard.x - vel_dir.x * to_hazard.z,
            vel_dir.x * to_hazard.y - vel_dir.y * to_hazard.x,
        )
        perp_mag = perp.magnitude()
        if perp_mag < 1e-10:
            # Hazard directly ahead — dodge in Z
            perp = Vector3(0, 0, 1)
        else:
            perp = perp.normalized()

        # Scale delta-v based on urgency (closer = harder burn)
        time_to_impact = max(1.0, distance / max(closing_rate, 0.001))
        needed_dv = min(
            MAX_EVASION_DELTA_V,
            max(0.01, (hazard.radius_km + 100.0) / time_to_impact),
        )

        # Compute burn parameters
        burn_duration = min(60.0, max(1.0, needed_dv / 0.01))  # max 60s burn
        burn_accel_mag = needed_dv / burn_duration
        burn_vector = perp * burn_accel_mag

        fuel_cost = estimate_fuel_cost(needed_dv)

        # Check fuel availability — use proper Tsiolkovsky inverse
        if fuel_cost > state.fuel_remaining_kg:
            available_fuel = state.fuel_remaining_kg * 0.8  # reserve 20%
            fuel_cost = min(fuel_cost, available_fuel)
            # Inverse rocket equation: dv = ve * ln(1 + fuel / dry_mass)
            needed_dv = min(
                MAX_EVASION_DELTA_V,
                EXHAUST_VELOCITY_KM_S * math.log(1 + max(0, available_fuel) / DRY_MASS_KG),
            )
            burn_accel_mag = needed_dv / burn_duration
            burn_vector = perp * burn_accel_mag

        # Optional model call for risk assessment
        model_risk = None
        if self.model_caller:
            try:
                model_risk = self.model_caller("hazard_risk_scorer", {
                    "distance_km": distance,
                    "closing_rate_km_s": closing_rate,
                    "hazard_type": hazard.hazard_type,
                    "hazard_radius_km": hazard.radius_km,
                })
            except Exception:
                pass  # Fallback to deterministic logic

        self.state.total_evasions += 1
        self.state.fuel_used_by_autopilot_kg += fuel_cost

        reasoning = (
            f"EMERGENCY: {hazard.hazard_type} '{hazard.hazard_id[:8]}' at {distance:.1f}km "
            f"and closing at {closing_rate:.2f}km/s. Time to impact: {time_to_impact:.0f}s. "
            f"Executing perpendicular burn of {needed_dv:.4f}km/s delta-v."
        )
        if model_risk:
            reasoning += f" Model risk score: {model_risk.get('risk', 'N/A')}."

        return AutopilotDecision(
            decision_id=str(uuid.uuid4()),
            timestamp_s=state.timestamp_s,
            priority_level=1,
            action="evasion_burn",
            reasoning=reasoning,
            burn_vector=burn_vector,
            burn_duration_s=burn_duration,
            delta_v=needed_dv,
            fuel_cost_kg=fuel_cost,
            target_hazard_id=hazard.hazard_id,
        )

    # ------------------------------------------------------------------
    # Priority 2: Execute Ground Commands
    # ------------------------------------------------------------------

    def _check_ground_commands(
        self,
        state: SpacecraftState,
        commands: list[GroundCommand],
        active_hazards: list[Hazard],
    ) -> Optional[AutopilotDecision]:
        """Process the highest-priority received command, validating against current state."""
        if not commands:
            return None

        # Sort by priority (critical > high > normal)
        priority_order = {"critical": 0, "high": 1, "normal": 2}
        commands_sorted = sorted(commands, key=lambda c: priority_order.get(c.priority, 2))
        self._ops_count += len(commands)

        for cmd in commands_sorted:
            # Validate: do we have enough fuel?
            if cmd.fuel_cost_kg > state.fuel_remaining_kg:
                self.state.total_commands_rejected += 1
                return AutopilotDecision(
                    decision_id=str(uuid.uuid4()),
                    timestamp_s=state.timestamp_s,
                    priority_level=2,
                    action="reject_command",
                    reasoning=(
                        f"Command '{cmd.command_id[:8]}' rejected: requires {cmd.fuel_cost_kg:.1f}kg fuel "
                        f"but only {state.fuel_remaining_kg:.1f}kg available."
                    ),
                    command_id=cmd.command_id,
                )

            # Validate: are we in safe mode and this is not critical?
            if self.state.mode == "safe_mode" and cmd.priority != "critical":
                self.state.total_commands_rejected += 1
                return AutopilotDecision(
                    decision_id=str(uuid.uuid4()),
                    timestamp_s=state.timestamp_s,
                    priority_level=2,
                    action="reject_command",
                    reasoning=(
                        f"Command '{cmd.command_id[:8]}' rejected: autopilot in safe_mode, "
                        f"only critical commands accepted. Fuel: {state.fuel_remaining_kg:.1f}kg."
                    ),
                    command_id=cmd.command_id,
                )

            # Validate: emergency_only mode blocks all non-evasion commands
            if self.state.mode == "emergency_only":
                self.state.total_commands_rejected += 1
                return AutopilotDecision(
                    decision_id=str(uuid.uuid4()),
                    timestamp_s=state.timestamp_s,
                    priority_level=2,
                    action="reject_command",
                    reasoning=(
                        f"Command '{cmd.command_id[:8]}' rejected: autopilot in emergency_only mode. "
                        f"Fuel critically low at {state.fuel_remaining_kg:.1f}kg."
                    ),
                    command_id=cmd.command_id,
                )

            # Validate: would this burn move us toward a known hazard?
            if cmd.command_type == "burn":
                burn_vec = Vector3(cmd.burn_vector_x, cmd.burn_vector_y, cmd.burn_vector_z)
                would_endanger = self._would_burn_endanger(state, burn_vec, cmd.burn_duration_s, active_hazards)
                self._ops_count += 3

                if would_endanger:
                    self.state.total_commands_rejected += 1
                    return AutopilotDecision(
                        decision_id=str(uuid.uuid4()),
                        timestamp_s=state.timestamp_s,
                        priority_level=2,
                        action="reject_command",
                        reasoning=(
                            f"Command '{cmd.command_id[:8]}' rejected: executing this burn would "
                            f"bring spacecraft closer to an active hazard."
                        ),
                        command_id=cmd.command_id,
                    )

                # Optional: ask model for trajectory prediction
                if self.model_caller:
                    try:
                        self.model_caller("trajectory_predictor", {
                            "current_state": {
                                "px": state.position.x, "py": state.position.y, "pz": state.position.z,
                                "vx": state.velocity.x, "vy": state.velocity.y, "vz": state.velocity.z,
                            },
                            "burn_vector": {"x": burn_vec.x, "y": burn_vec.y, "z": burn_vec.z},
                            "burn_duration_s": cmd.burn_duration_s,
                        })
                    except Exception:
                        pass

                # Execute the burn command
                self.state.total_commands_executed += 1
                self.state.fuel_used_by_autopilot_kg += cmd.fuel_cost_kg

                return AutopilotDecision(
                    decision_id=str(uuid.uuid4()),
                    timestamp_s=state.timestamp_s,
                    priority_level=2,
                    action="execute_command",
                    reasoning=(
                        f"Executing ground command '{cmd.command_id[:8]}' ({cmd.command_type}). "
                        f"Delta-v: {cmd.delta_v:.4f}km/s, fuel cost: {cmd.fuel_cost_kg:.1f}kg. "
                        f"Command validated against current hazards and fuel budget."
                    ),
                    burn_vector=burn_vec,
                    burn_duration_s=cmd.burn_duration_s,
                    delta_v=cmd.delta_v,
                    fuel_cost_kg=cmd.fuel_cost_kg,
                    command_id=cmd.command_id,
                )

            # Non-burn commands (system_check, adjust_attitude, etc.)
            self.state.total_commands_executed += 1
            return AutopilotDecision(
                decision_id=str(uuid.uuid4()),
                timestamp_s=state.timestamp_s,
                priority_level=2,
                action="execute_command",
                reasoning=(
                    f"Executing ground command '{cmd.command_id[:8]}' ({cmd.command_type}). "
                    f"Non-propulsive command acknowledged."
                ),
                command_id=cmd.command_id,
            )

        return None

    def _would_burn_endanger(
        self,
        state: SpacecraftState,
        burn_vector: Vector3,
        burn_duration_s: float,
        hazards: list[Hazard],
    ) -> bool:
        """Check if a proposed burn would move the spacecraft toward a known hazard."""
        if not hazards:
            return False

        # Propagate with the proposed burn for a short lookahead
        future_states = propagate_trajectory(
            state, duration_s=min(burn_duration_s * 3, 300), dt=10.0,
            thrust=burn_vector,
        )

        for hazard in hazards:
            min_dist, _ = closest_approach(future_states, hazard)
            risk = collision_risk(min_dist, hazard.radius_km)
            if risk > 0.5:
                return True

        return False

    # ------------------------------------------------------------------
    # Priority 3: Station Keeping
    # ------------------------------------------------------------------

    def _check_station_keeping(
        self,
        state: SpacecraftState,
    ) -> Optional[AutopilotDecision]:
        """Apply small corrections if trajectory has drifted from the plan."""
        if not self.planned_trajectory:
            return None

        if self.state.mode in ("safe_mode", "emergency_only"):
            return None  # No station keeping when conserving fuel

        self._ops_count += 2

        # Find the closest planned state by timestamp
        closest_planned = min(
            self.planned_trajectory,
            key=lambda s: abs(s.timestamp_s - state.timestamp_s),
        )

        drift = (state.position - closest_planned.position).magnitude()

        if drift < DRIFT_THRESHOLD_KM:
            return None  # Within tolerance

        # Compute correction vector toward planned position
        correction_dir = (closest_planned.position - state.position).normalized()
        correction_dv = min(STATION_KEEPING_MAX_DV, drift * 1e-6)  # proportional
        burn_duration = max(1.0, correction_dv / 0.001)
        burn_accel = correction_dv / burn_duration
        burn_vector = correction_dir * burn_accel
        fuel_cost = estimate_fuel_cost(correction_dv)

        if fuel_cost > state.fuel_remaining_kg * 0.1:
            return None  # Don't spend more than 10% of remaining fuel on station keeping

        self.state.total_corrections += 1
        self.state.fuel_used_by_autopilot_kg += fuel_cost

        return AutopilotDecision(
            decision_id=str(uuid.uuid4()),
            timestamp_s=state.timestamp_s,
            priority_level=3,
            action="station_keeping_burn",
            reasoning=(
                f"Trajectory drift of {drift:.1f}km exceeds {DRIFT_THRESHOLD_KM}km threshold. "
                f"Applying correction burn of {correction_dv:.6f}km/s toward planned trajectory."
            ),
            burn_vector=burn_vector,
            burn_duration_s=burn_duration,
            delta_v=correction_dv,
            fuel_cost_kg=fuel_cost,
        )

    # ------------------------------------------------------------------
    # Priority 4: Fuel Conservation
    # ------------------------------------------------------------------

    def _update_fuel_mode(self, state: SpacecraftState) -> None:
        """Update the autopilot mode based on current fuel level."""
        if state.fuel_remaining_kg < FUEL_EMERGENCY_ONLY_KG:
            self.state.mode = "emergency_only"
        elif state.fuel_remaining_kg < FUEL_SAFE_MODE_KG:
            self.state.mode = "safe_mode"
        else:
            self.state.mode = "nominal"

    def _check_fuel_conservation(
        self,
        state: SpacecraftState,
    ) -> Optional[AutopilotDecision]:
        """Log mode transitions when fuel thresholds are crossed."""
        self._ops_count += 1

        if self.state.mode == "emergency_only":
            return AutopilotDecision(
                decision_id=str(uuid.uuid4()),
                timestamp_s=state.timestamp_s,
                priority_level=4,
                action="enter_emergency_only",
                reasoning=(
                    f"Fuel critically low at {state.fuel_remaining_kg:.1f}kg "
                    f"(threshold: {FUEL_EMERGENCY_ONLY_KG}kg). Emergency-only mode active. "
                    f"Only evasion burns permitted."
                ),
            )

        if self.state.mode == "safe_mode":
            return AutopilotDecision(
                decision_id=str(uuid.uuid4()),
                timestamp_s=state.timestamp_s,
                priority_level=4,
                action="enter_safe_mode",
                reasoning=(
                    f"Fuel low at {state.fuel_remaining_kg:.1f}kg "
                    f"(threshold: {FUEL_SAFE_MODE_KG}kg). Safe mode active. "
                    f"Station keeping disabled, only critical commands accepted."
                ),
            )

        return None

    # ------------------------------------------------------------------
    # Finalization & Metrics
    # ------------------------------------------------------------------

    def _finalize(self, decision: AutopilotDecision) -> AutopilotDecision:
        """Attach timing metrics and update persistent state."""
        elapsed = (time.time() - self._tick_start_time) * 1000.0
        decision.elapsed_ms = elapsed
        self.state.last_decision_time_s = decision.timestamp_s
        self.state.ticks_processed += 1
        return decision

    @property
    def operations_per_second(self) -> float:
        """Throughput metric: operations evaluated in the last tick."""
        elapsed = time.time() - self._tick_start_time if self._tick_start_time else 1.0
        return self._ops_count / max(elapsed, 0.001)

    @property
    def ops_count(self) -> int:
        return self._ops_count
