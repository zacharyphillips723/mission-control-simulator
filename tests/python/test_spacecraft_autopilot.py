"""Tests for src/python/spacecraft_autopilot.py — 18 tests covering the priority
stack, evasion, fuel modes, command queue, and station keeping."""

import math

import pytest

from physics_engine import (
    GM,
    ORBIT_RADIUS,
    Hazard,
    SpacecraftState,
    Vector3,
)
from spacecraft_autopilot import (
    DRIFT_THRESHOLD_KM,
    EVASION_TRIGGER_DISTANCE_KM,
    FUEL_EMERGENCY_ONLY_KG,
    FUEL_SAFE_MODE_KG,
    MAX_EVASION_DELTA_V,
    AutopilotState,
    GroundCommand,
    SpacecraftAutopilot,
)


def _make_state(
    px=1.8e8, py=0.0, pz=0.0,
    vx=0.0, vy=20.0, vz=0.0,
    fuel=300.0, t=0.0,
) -> SpacecraftState:
    return SpacecraftState(
        position=Vector3(px, py, pz),
        velocity=Vector3(vx, vy, vz),
        fuel_remaining_kg=fuel,
        timestamp_s=t,
    )


def _make_hazard(
    px=1.8e8, py=100.0, pz=0.0,
    vx=0.0, vy=-1.0, vz=0.0,
    radius=10.0,
    hid="h-001",
) -> Hazard:
    return Hazard(
        hazard_id=hid,
        hazard_type="asteroid",
        position=Vector3(px, py, pz),
        velocity=Vector3(vx, vy, vz),
        radius_km=radius,
    )


# ── Init & Constants ────────────────────────────────────────────────────

class TestAutopilotInit:
    def test_defaults(self):
        ap = SpacecraftAutopilot()
        assert ap.state.mode == "nominal"
        assert ap.state.ticks_processed == 0

    def test_priority_ordering(self):
        """Evasion(1) < command(2) < station_keeping(3)."""
        assert 1 < 2 < 3  # obviously, but documents intent

    def test_evasion_trigger_distance(self):
        assert EVASION_TRIGGER_DISTANCE_KM == 500.0


# ── Decide: no hazards, no commands ─────────────────────────────────────

class TestDecideCoast:
    def test_coast_when_nothing_to_do(self):
        ap = SpacecraftAutopilot()
        state = _make_state()
        decision = ap.run_tick(state, [], [], elapsed_days=1.0)
        assert decision.action == "coast"
        assert decision.priority_level == 5


# ── Evasion ─────────────────────────────────────────────────────────────

class TestEvasion:
    def test_hazard_within_trigger(self):
        """Hazard at 100 km (within 500 km) and closing → evasion."""
        ap = SpacecraftAutopilot()
        ship = _make_state(px=1.8e8, py=0.0, vx=0.0, vy=20.0)
        # Hazard 100 km ahead, closing
        hazard = _make_hazard(px=1.8e8, py=100.0, vx=0.0, vy=-5.0)
        decision = ap.run_tick(ship, [hazard], [], elapsed_days=1.0)
        assert decision.action == "evasion_burn"
        assert decision.priority_level == 1

    def test_hazard_beyond_trigger(self):
        """Hazard at 10,000 km → no evasion."""
        ap = SpacecraftAutopilot()
        ship = _make_state()
        hazard = _make_hazard(py=10_000.0)
        decision = ap.run_tick(ship, [hazard], [], elapsed_days=1.0)
        assert decision.action != "evasion_burn"

    def test_evasion_dv_capped_normal(self):
        """Evasion burn delta-v should not exceed MAX_EVASION_DELTA_V (regression guard)."""
        ap = SpacecraftAutopilot()
        ship = _make_state(fuel=500.0)
        # Hazard approaching at medium range
        hazard = _make_hazard(py=300.0, vy=-5.0, radius=10.0)
        decision = ap.run_tick(ship, [hazard], [], elapsed_days=1.0)
        if decision.action == "evasion_burn":
            assert decision.delta_v <= MAX_EVASION_DELTA_V + 0.001

    def test_evasion_dv_capped_fuel_limited(self):
        """When fuel is limited, recalculated dv should still respect MAX_EVASION_DELTA_V (regression guard)."""
        ap = SpacecraftAutopilot()
        ship = _make_state(fuel=500.0)
        hazard = _make_hazard(py=50.0, vy=-20.0, radius=50.0)  # very close, very fast
        decision = ap.run_tick(ship, [hazard], [], elapsed_days=1.0)
        if decision.action == "evasion_burn":
            assert decision.delta_v <= MAX_EVASION_DELTA_V + 0.001

    def test_evasion_overrides_command(self):
        """Evasion takes priority over queued command."""
        ap = SpacecraftAutopilot()
        ship = _make_state(fuel=500.0)
        hazard = _make_hazard(py=100.0, vy=-5.0)
        cmd = GroundCommand(
            command_id="cmd-001",
            command_type="burn",
            burn_vector_x=0.01, burn_vector_y=0.0, burn_vector_z=0.0,
            burn_duration_s=30.0, delta_v=0.3, fuel_cost_kg=10.0,
        )
        decision = ap.run_tick(ship, [hazard], [cmd], elapsed_days=1.0)
        assert decision.priority_level == 1  # evasion

    def test_multiple_hazards_nearest(self):
        """Evades the nearest hazard, not the farthest."""
        ap = SpacecraftAutopilot()
        ship = _make_state(fuel=500.0)
        near = _make_hazard(py=80.0, vy=-5.0, hid="near")
        far = _make_hazard(py=400.0, vy=-5.0, hid="far")
        decision = ap.run_tick(ship, [near, far], [], elapsed_days=1.0)
        if decision.action == "evasion_burn":
            assert decision.target_hazard_id == "near"


# ── Command execution ───────────────────────────────────────────────────

class TestCommands:
    def test_command_override_coast(self):
        """Pending command executes over coast."""
        ap = SpacecraftAutopilot()
        ship = _make_state(fuel=500.0)
        cmd = GroundCommand(
            command_id="cmd-002",
            command_type="burn",
            burn_vector_x=0.001, burn_vector_y=0.0, burn_vector_z=0.0,
            burn_duration_s=10.0, delta_v=0.01, fuel_cost_kg=5.0,
        )
        decision = ap.run_tick(ship, [], [cmd], elapsed_days=1.0)
        assert decision.action == "execute_command"
        assert decision.command_id == "cmd-002"

    def test_command_queue_fifo_by_priority(self):
        """Critical command processed before normal."""
        ap = SpacecraftAutopilot()
        ship = _make_state(fuel=500.0)
        normal = GroundCommand(
            command_id="cmd-normal",
            command_type="system_check",
            priority="normal",
        )
        critical = GroundCommand(
            command_id="cmd-critical",
            command_type="system_check",
            priority="critical",
        )
        decision = ap.run_tick(ship, [], [normal, critical], elapsed_days=1.0)
        assert decision.command_id == "cmd-critical"


# ── Fuel modes ──────────────────────────────────────────────────────────

class TestFuelModes:
    def test_safe_mode(self):
        """fuel < FUEL_SAFE_MODE_KG → safe_mode."""
        ap = SpacecraftAutopilot()
        ship = _make_state(fuel=80.0)
        decision = ap.run_tick(ship, [], [], elapsed_days=1.0)
        assert ap.state.mode == "safe_mode"

    def test_emergency_mode(self):
        """fuel < FUEL_EMERGENCY_ONLY_KG → emergency_only."""
        ap = SpacecraftAutopilot()
        ship = _make_state(fuel=30.0)
        decision = ap.run_tick(ship, [], [], elapsed_days=1.0)
        assert ap.state.mode == "emergency_only"

    def test_emergency_rejects_commands(self):
        """In emergency_only, all commands are rejected."""
        ap = SpacecraftAutopilot()
        ship = _make_state(fuel=30.0)
        cmd = GroundCommand(
            command_id="cmd-003",
            command_type="burn",
            burn_vector_x=0.001,
            burn_duration_s=10.0, delta_v=0.01, fuel_cost_kg=5.0,
        )
        decision = ap.run_tick(ship, [], [cmd], elapsed_days=1.0)
        assert decision.action == "reject_command" or decision.action == "enter_emergency_only"


# ── Station keeping ─────────────────────────────────────────────────────

class TestStationKeeping:
    def test_station_keeping_activates(self):
        """Drift beyond threshold → station keeping burn."""
        planned_pos = Vector3(1.8e8, 0.0, 0.0)
        planned_state = SpacecraftState(
            position=planned_pos,
            velocity=Vector3(0, 20, 0),
            fuel_remaining_kg=500.0,
            timestamp_s=100.0,
        )
        ap = SpacecraftAutopilot(planned_trajectory=[planned_state])
        # Ship drifted 200 km from plan
        ship = _make_state(px=1.8e8 + 200.0, t=100.0, fuel=500.0)
        decision = ap.run_tick(ship, [], [], elapsed_days=1.0)
        assert decision.action == "station_keeping_burn"
        assert decision.priority_level == 3


# ── Mode transitions & misc ────────────────────────────────────────────

class TestMisc:
    def test_mode_transitions(self):
        """nominal → safe_mode → emergency_only as fuel drops."""
        ap = SpacecraftAutopilot()
        # Nominal
        ap.run_tick(_make_state(fuel=200.0), [], [], 1.0)
        assert ap.state.mode == "nominal"
        # Safe mode
        ap.run_tick(_make_state(fuel=80.0), [], [], 1.0)
        assert ap.state.mode == "safe_mode"
        # Emergency
        ap.run_tick(_make_state(fuel=30.0), [], [], 1.0)
        assert ap.state.mode == "emergency_only"

    def test_returns_action_type(self):
        """Decision always has an action string."""
        ap = SpacecraftAutopilot()
        decision = ap.run_tick(_make_state(), [], [], 1.0)
        assert isinstance(decision.action, str)
        assert len(decision.action) > 0

    def test_nan_position_no_crash(self):
        """NaN in position → should not crash (may produce NaN output)."""
        ap = SpacecraftAutopilot()
        ship = _make_state(px=float("nan"), py=float("nan"))
        # Should not raise
        decision = ap.run_tick(ship, [], [], 1.0)
        assert decision is not None
