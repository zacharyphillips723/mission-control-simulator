"""
Mission Control — Ship Captain Agent

Autonomous onboard agent that runs with zero communication delay. The Captain
has authority to override mission control commands when local conditions
(hazards, fuel, hull) make those commands unsafe.

Authority hierarchy:
  - Full authority: emergency evasion, micro-corrections (< 0.05 km/s), fuel mode
  - Override authority: can VETO or MODIFY incoming MC commands (must log reasoning)
  - Defers to MC for: major course changes (> 0.5 km/s), mission objective changes

The Captain runs every tick as part of the command-arrival pipeline, BEFORE
burn execution — so it can intercept and modify MC commands in real time.
"""

import json
import math
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from mission_constants import EXHAUST_VELOCITY_KM_S, DRY_MASS_KG
from physics_engine import (
    Vector3,
    SpacecraftState,
    Hazard,
    estimate_fuel_cost,
    collision_risk,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CAPTAIN_MICRO_DV_LIMIT = 0.05       # km/s — max autonomous micro-correction
CAPTAIN_EVASION_DV_LIMIT = 0.5      # km/s — max emergency evasion burn
FUEL_RESERVE_KG = 50.0              # never burn below this
HAZARD_OVERRIDE_DISTANCE_KM = 800.0 # override MC commands if hazard within this
HULL_SAFE_MODE_PCT = 40.0           # enter safe mode below this hull %
FUEL_CAUTION_PCT = 25.0             # scale down MC burns below this fuel %


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------

@dataclass
class CaptainDecision:
    """A single decision made by the ship captain."""
    decision_id: str
    simulation_time_s: float
    action: str  # emergency_evasion, micro_correction, override_command,
                 # approve_command, modify_command, enter_safe_mode, await_orders
    priority_level: int  # 1=evasion, 2=override, 3=correction, 4=approve, 5=nominal
    reasoning: str
    override_of_command_id: Optional[str] = None
    original_command_summary: Optional[str] = None
    captain_alternative_summary: Optional[str] = None
    burn_vector: Optional[Vector3] = None
    delta_v: float = 0.0
    fuel_cost_kg: float = 0.0
    alert_level: str = "green"  # green, yellow, red
    confidence: float = 0.85
    elapsed_ms: float = 0.0


@dataclass
class CaptainState:
    """Persistent state tracked across ticks."""
    alert_level: str = "green"
    total_overrides: int = 0
    total_evasions: int = 0
    total_micro_corrections: int = 0
    total_commands_approved: int = 0
    fuel_used_by_captain_kg: float = 0.0
    last_decision_action: str = "await_orders"
    last_decision_reasoning: str = ""
    ticks_since_mc_contact: int = 0
    safe_mode_announced: bool = False


# ---------------------------------------------------------------------------
# Ship Captain
# ---------------------------------------------------------------------------

class ShipCaptain:
    """Autonomous onboard decision-maker for the spacecraft."""

    def __init__(self):
        self.state = CaptainState()
        self._recent_hazard_ids: set[str] = set()

    def evaluate_tick(
        self,
        ship: SpacecraftState,
        hazards: list[Hazard],
        pending_mc_commands: list[dict],
        onboard_predictions: list[dict],
        elapsed_days: float,
    ) -> CaptainDecision:
        """
        Run the captain's decision logic for this tick.

        Called BEFORE command execution in the mini-tick loop so the captain
        can intercept, approve, modify, or veto MC commands.

        Returns a CaptainDecision describing the action taken.
        """
        import time as _time
        t0 = _time.perf_counter()

        # Update alert level based on conditions
        self._update_alert_level(ship, hazards)

        # Priority 1: Emergency evasion (imminent hazard, no delay)
        decision = self._check_emergency_evasion(ship, hazards)
        if decision:
            decision.elapsed_ms = (_time.perf_counter() - t0) * 1000
            return decision

        # Priority 2: Evaluate pending MC commands (override/modify/approve)
        for cmd in pending_mc_commands:
            decision = self._evaluate_mc_command(cmd, ship, hazards, onboard_predictions)
            if decision:
                decision.elapsed_ms = (_time.perf_counter() - t0) * 1000
                return decision

        # Priority 3: Autonomous micro-corrections from onboard predictions
        decision = self._check_micro_correction(ship, onboard_predictions)
        if decision:
            decision.elapsed_ms = (_time.perf_counter() - t0) * 1000
            return decision

        # Priority 4: Hull/fuel safe mode
        decision = self._check_safe_mode(ship)
        if decision:
            decision.elapsed_ms = (_time.perf_counter() - t0) * 1000
            return decision

        # Priority 5: Nominal — await orders
        self.state.last_decision_action = "await_orders"
        self.state.last_decision_reasoning = "Situation nominal — maintaining course."
        self.state.ticks_since_mc_contact += 1

        decision = CaptainDecision(
            decision_id=str(uuid.uuid4()),
            simulation_time_s=ship.timestamp_s,
            action="await_orders",
            priority_level=5,
            reasoning="Situation nominal — maintaining course.",
            alert_level=self.state.alert_level,
            confidence=0.92,
        )
        decision.elapsed_ms = (_time.perf_counter() - t0) * 1000
        return decision

    # ------------------------------------------------------------------
    # Priority 1: Emergency Evasion
    # ------------------------------------------------------------------

    def _check_emergency_evasion(
        self, ship: SpacecraftState, hazards: list[Hazard]
    ) -> Optional[CaptainDecision]:
        """Immediate autonomous evasion if hazard is imminent."""
        if not hazards:
            return None

        for hazard in hazards:
            to_hazard = Vector3(
                hazard.position.x - ship.position.x,
                hazard.position.y - ship.position.y,
                hazard.position.z - ship.position.z,
            )
            distance = to_hazard.magnitude()

            if distance > HAZARD_OVERRIDE_DISTANCE_KM:
                continue

            # Check if closing
            rel_vel = Vector3(
                hazard.velocity.x - ship.velocity.x,
                hazard.velocity.y - ship.velocity.y,
                hazard.velocity.z - ship.velocity.z,
            )
            closing_rate = -(
                to_hazard.x * rel_vel.x + to_hazard.y * rel_vel.y + to_hazard.z * rel_vel.z
            ) / max(distance, 1.0)

            if closing_rate <= 0:
                continue  # receding — not a threat

            # Imminent collision — act immediately
            time_to_impact = distance / max(closing_rate, 0.001)

            # Compute perpendicular evasion vector
            vel_dir = ship.velocity.normalized()
            to_hazard_dir = to_hazard.normalized()
            perp = Vector3(
                vel_dir.y * to_hazard_dir.z - vel_dir.z * to_hazard_dir.y,
                vel_dir.z * to_hazard_dir.x - vel_dir.x * to_hazard_dir.z,
                vel_dir.x * to_hazard_dir.y - vel_dir.y * to_hazard_dir.x,
            )
            if perp.magnitude() < 1e-10:
                perp = Vector3(0, 0, 1)
            perp = perp.normalized()

            needed_dv = min(
                CAPTAIN_EVASION_DV_LIMIT,
                max(0.01, (hazard.radius_km + 100.0) / time_to_impact),
            )

            fuel_cost = estimate_fuel_cost(needed_dv)
            available = max(0, ship.fuel_remaining_kg - FUEL_RESERVE_KG)
            if fuel_cost > available:
                needed_dv = min(
                    CAPTAIN_EVASION_DV_LIMIT,
                    EXHAUST_VELOCITY_KM_S * math.log(1 + available / DRY_MASS_KG),
                )
                fuel_cost = estimate_fuel_cost(needed_dv)

            burn_duration = max(1.0, min(60.0, needed_dv / 0.01))
            burn_accel = needed_dv / burn_duration
            burn_vec = perp * burn_accel

            self.state.total_evasions += 1
            self.state.fuel_used_by_captain_kg += fuel_cost
            self.state.alert_level = "red"

            return CaptainDecision(
                decision_id=str(uuid.uuid4()),
                simulation_time_s=ship.timestamp_s,
                action="emergency_evasion",
                priority_level=1,
                reasoning=(
                    f"EMERGENCY: {hazard.hazard_type} at {distance:.0f}km, "
                    f"closing at {closing_rate:.1f}km/s. Time to impact: {time_to_impact:.0f}s. "
                    f"Executing autonomous evasion burn — no time to wait for MC."
                ),
                burn_vector=burn_vec,
                delta_v=needed_dv,
                fuel_cost_kg=fuel_cost,
                alert_level="red",
                confidence=0.95,
            )

        return None

    # ------------------------------------------------------------------
    # Priority 2: Evaluate MC Commands
    # ------------------------------------------------------------------

    def _evaluate_mc_command(
        self,
        cmd: dict,
        ship: SpacecraftState,
        hazards: list[Hazard],
        predictions: list[dict],
    ) -> Optional[CaptainDecision]:
        """Evaluate an incoming MC command — approve, modify, or veto."""
        cmd_id = cmd.get("command_id", "")
        cmd_type = cmd.get("command_type", "")
        payload = cmd.get("payload", {})
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except (json.JSONDecodeError, TypeError):
                payload = {}

        # Reset MC contact counter
        self.state.ticks_since_mc_contact = 0

        # --- Check 1: Would this burn endanger us given current hazards? ---
        if cmd_type == "burn" and hazards:
            bvx = float(payload.get("burn_vector_x", 0))
            bvy = float(payload.get("burn_vector_y", 0))
            bvz = float(payload.get("burn_vector_z", 0))
            dur = float(payload.get("burn_duration_s", 0))
            dv = math.sqrt(bvx**2 + bvy**2 + bvz**2) * dur

            # Check if burn puts us closer to any hazard
            post_vx = ship.velocity.x + bvx * dur
            post_vy = ship.velocity.y + bvy * dur
            post_vz = ship.velocity.z + bvz * dur

            for hazard in hazards:
                to_hazard = Vector3(
                    hazard.position.x - ship.position.x,
                    hazard.position.y - ship.position.y,
                    hazard.position.z - ship.position.z,
                )
                dist = to_hazard.magnitude()
                if dist > HAZARD_OVERRIDE_DISTANCE_KM * 2:
                    continue

                # Would post-burn velocity bring us closer?
                post_closing = -(
                    to_hazard.x * (hazard.velocity.x - post_vx)
                    + to_hazard.y * (hazard.velocity.y - post_vy)
                    + to_hazard.z * (hazard.velocity.z - post_vz)
                ) / max(dist, 1.0)

                if post_closing > 0 and dist < HAZARD_OVERRIDE_DISTANCE_KM:
                    self.state.total_overrides += 1
                    return CaptainDecision(
                        decision_id=str(uuid.uuid4()),
                        simulation_time_s=ship.timestamp_s,
                        action="override_command",
                        priority_level=2,
                        reasoning=(
                            f"VETO: MC burn command would increase closing rate with "
                            f"{hazard.hazard_type} at {dist:.0f}km. MC data was "
                            f"{self.state.ticks_since_mc_contact * 2}s stale. "
                            f"Holding position until hazard passes."
                        ),
                        override_of_command_id=cmd_id,
                        original_command_summary=f"burn dv={dv:.3f} km/s",
                        captain_alternative_summary="HOLD — hazard proximity",
                        alert_level="yellow",
                        confidence=0.88,
                    )

        # --- Check 2: Fuel critically low — scale down burn ---
        fuel_pct = (ship.fuel_remaining_kg / 500.0) * 100  # assume 500kg capacity
        if cmd_type == "burn" and fuel_pct < FUEL_CAUTION_PCT:
            bvx = float(payload.get("burn_vector_x", 0))
            bvy = float(payload.get("burn_vector_y", 0))
            bvz = float(payload.get("burn_vector_z", 0))
            dur = float(payload.get("burn_duration_s", 0))
            dv = math.sqrt(bvx**2 + bvy**2 + bvz**2) * dur

            if dv > 0.1:  # only scale down significant burns
                scale = max(0.3, fuel_pct / FUEL_CAUTION_PCT)
                self.state.total_overrides += 1
                return CaptainDecision(
                    decision_id=str(uuid.uuid4()),
                    simulation_time_s=ship.timestamp_s,
                    action="modify_command",
                    priority_level=2,
                    reasoning=(
                        f"MODIFY: Fuel at {fuel_pct:.0f}% — scaling MC burn from "
                        f"{dv:.3f} to {dv * scale:.3f} km/s to preserve reserves."
                    ),
                    override_of_command_id=cmd_id,
                    original_command_summary=f"burn dv={dv:.3f} km/s",
                    captain_alternative_summary=f"burn dv={dv * scale:.3f} km/s (scaled {scale:.0%})",
                    alert_level="yellow",
                    confidence=0.82,
                )

        # --- Check 3: Hull damage — reject non-essential commands ---
        if ship.hull_integrity < HULL_SAFE_MODE_PCT and cmd_type == "burn":
            bvx = float(payload.get("burn_vector_x", 0))
            bvy = float(payload.get("burn_vector_y", 0))
            bvz = float(payload.get("burn_vector_z", 0))
            dur = float(payload.get("burn_duration_s", 0))
            dv = math.sqrt(bvx**2 + bvy**2 + bvz**2) * dur
            if dv > 0.05:
                self.state.total_overrides += 1
                return CaptainDecision(
                    decision_id=str(uuid.uuid4()),
                    simulation_time_s=ship.timestamp_s,
                    action="override_command",
                    priority_level=2,
                    reasoning=(
                        f"VETO: Hull integrity at {ship.hull_integrity:.0f}% — "
                        f"refusing large burn ({dv:.3f} km/s) to avoid structural stress. "
                        f"Requesting MC guidance for safer approach."
                    ),
                    override_of_command_id=cmd_id,
                    original_command_summary=f"burn dv={dv:.3f} km/s",
                    captain_alternative_summary="HOLD — hull integrity concern",
                    alert_level="red",
                    confidence=0.90,
                )

        # --- Default: Approve the command ---
        self.state.total_commands_approved += 1
        return None  # None means "approved — let it execute normally"

    # ------------------------------------------------------------------
    # Priority 3: Micro-corrections
    # ------------------------------------------------------------------

    def _check_micro_correction(
        self, ship: SpacecraftState, predictions: list[dict]
    ) -> Optional[CaptainDecision]:
        """Apply small autonomous corrections if onboard predictions show drift."""
        if not predictions:
            return None

        # Look at the most recent prediction
        latest = predictions[0] if predictions else None
        if not latest:
            return None

        assessment = latest.get("assessment", "on_course")
        error_km = latest.get("prediction_error_km")

        if assessment == "correction_needed" and error_km and error_km > 5000:
            # Compute a small correction toward predicted path
            pred_x = latest.get("predicted_pos_x")
            pred_y = latest.get("predicted_pos_y")
            pred_z = latest.get("predicted_pos_z", 0)

            if pred_x is not None and pred_y is not None:
                correction_dir = Vector3(
                    pred_x - ship.position.x,
                    pred_y - ship.position.y,
                    (pred_z or 0) - ship.position.z,
                ).normalized()

                needed_dv = min(CAPTAIN_MICRO_DV_LIMIT, error_km * 1e-7)
                if needed_dv < 0.0001:
                    return None

                fuel_cost = estimate_fuel_cost(needed_dv)
                if fuel_cost > ship.fuel_remaining_kg - FUEL_RESERVE_KG:
                    return None

                burn_duration = max(1.0, needed_dv / 0.001)
                burn_accel = needed_dv / burn_duration
                burn_vec = correction_dir * burn_accel

                self.state.total_micro_corrections += 1
                self.state.fuel_used_by_captain_kg += fuel_cost

                return CaptainDecision(
                    decision_id=str(uuid.uuid4()),
                    simulation_time_s=ship.timestamp_s,
                    action="micro_correction",
                    priority_level=3,
                    reasoning=(
                        f"Onboard prediction shows {error_km:.0f}km drift — "
                        f"applying autonomous micro-correction of {needed_dv:.5f} km/s."
                    ),
                    burn_vector=burn_vec,
                    delta_v=needed_dv,
                    fuel_cost_kg=fuel_cost,
                    alert_level=self.state.alert_level,
                    confidence=0.80,
                )

        return None

    # ------------------------------------------------------------------
    # Priority 4: Safe Mode
    # ------------------------------------------------------------------

    def _check_safe_mode(self, ship: SpacecraftState) -> Optional[CaptainDecision]:
        """Enter safe mode if hull or fuel is critically low."""
        if ship.hull_integrity < HULL_SAFE_MODE_PCT and not self.state.safe_mode_announced:
            self.state.safe_mode_announced = True
            self.state.alert_level = "red"
            return CaptainDecision(
                decision_id=str(uuid.uuid4()),
                simulation_time_s=ship.timestamp_s,
                action="enter_safe_mode",
                priority_level=4,
                reasoning=(
                    f"Hull integrity at {ship.hull_integrity:.0f}% — "
                    f"entering safe mode. All non-essential systems offline. "
                    f"Requesting MC guidance."
                ),
                alert_level="red",
                confidence=0.95,
            )
        return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _update_alert_level(self, ship: SpacecraftState, hazards: list[Hazard]):
        """Update alert level based on current conditions."""
        fuel_pct = (ship.fuel_remaining_kg / 500.0) * 100

        if ship.hull_integrity < HULL_SAFE_MODE_PCT:
            self.state.alert_level = "red"
        elif any(
            Vector3(
                h.position.x - ship.position.x,
                h.position.y - ship.position.y,
                h.position.z - ship.position.z,
            ).magnitude() < HAZARD_OVERRIDE_DISTANCE_KM
            for h in hazards
        ):
            self.state.alert_level = "red"
        elif fuel_pct < FUEL_CAUTION_PCT or ship.hull_integrity < 60:
            self.state.alert_level = "yellow"
        else:
            self.state.alert_level = "green"

    def get_state_summary(self) -> dict:
        """Return captain state for API consumption."""
        return {
            "alert_level": self.state.alert_level,
            "total_overrides": self.state.total_overrides,
            "total_evasions": self.state.total_evasions,
            "total_micro_corrections": self.state.total_micro_corrections,
            "total_commands_approved": self.state.total_commands_approved,
            "fuel_used_by_captain_kg": round(self.state.fuel_used_by_captain_kg, 2),
            "last_decision_action": self.state.last_decision_action,
            "last_decision_reasoning": self.state.last_decision_reasoning,
            "ticks_since_mc_contact": self.state.ticks_since_mc_contact,
        }
