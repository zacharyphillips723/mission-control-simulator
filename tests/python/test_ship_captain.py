"""Tests for src/python/ship_captain.py — Ship Captain agent."""

import math
import sys
import os

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src", "python"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "app"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
import _mock_deps  # noqa: F401

from physics_engine import Vector3, SpacecraftState, Hazard
from ship_captain import (
    ShipCaptain,
    CaptainDecision,
    CaptainState,
    CAPTAIN_MICRO_DV_LIMIT,
    CAPTAIN_EVASION_DV_LIMIT,
    HAZARD_OVERRIDE_DISTANCE_KM,
)


def _make_ship(px=1.8e8, py=0, pz=0, vx=0, vy=20, vz=0, fuel=300, hull=100, t=0):
    return SpacecraftState(
        position=Vector3(px, py, pz),
        velocity=Vector3(vx, vy, vz),
        fuel_remaining_kg=fuel,
        timestamp_s=t,
        hull_integrity=hull,
    )


def _make_hazard(px=1.8e8, py=100, pz=0, vx=0, vy=-5, vz=0, radius=10, hid="h-1"):
    return Hazard(
        hazard_id=hid,
        hazard_type="asteroid",
        position=Vector3(px, py, pz),
        velocity=Vector3(vx, vy, vz),
        radius_km=radius,
    )


class TestCaptainInit:
    def test_default_state(self):
        cap = ShipCaptain()
        assert cap.state.alert_level == "green"
        assert cap.state.total_overrides == 0

    def test_get_state_summary(self):
        cap = ShipCaptain()
        summary = cap.get_state_summary()
        assert "alert_level" in summary
        assert "total_overrides" in summary


class TestEmergencyEvasion:
    def test_close_hazard_triggers_evasion(self):
        cap = ShipCaptain()
        ship = _make_ship()
        hazard = _make_hazard(py=100, vy=-10)  # close and closing
        decision = cap.evaluate_tick(ship, [hazard], [], [], 1.0)
        assert decision.action == "emergency_evasion"
        assert decision.priority_level == 1
        assert decision.delta_v > 0
        assert decision.delta_v <= CAPTAIN_EVASION_DV_LIMIT

    def test_far_hazard_no_evasion(self):
        cap = ShipCaptain()
        ship = _make_ship()
        hazard = _make_hazard(py=10000)  # far away
        decision = cap.evaluate_tick(ship, [hazard], [], [], 1.0)
        assert decision.action != "emergency_evasion"

    def test_receding_hazard_no_evasion(self):
        cap = ShipCaptain()
        ship = _make_ship(vy=0)
        hazard = _make_hazard(py=500, vy=10)  # moving away from stationary ship
        decision = cap.evaluate_tick(ship, [hazard], [], [], 1.0)
        assert decision.action != "emergency_evasion"

    def test_evasion_fuel_limited(self):
        cap = ShipCaptain()
        ship = _make_ship(fuel=60)  # very low fuel
        hazard = _make_hazard(py=100, vy=-10)
        decision = cap.evaluate_tick(ship, [hazard], [], [], 1.0)
        if decision.action == "emergency_evasion":
            assert decision.fuel_cost_kg <= ship.fuel_remaining_kg


class TestMCCommandEvaluation:
    def test_approve_safe_command(self):
        cap = ShipCaptain()
        ship = _make_ship()
        cmd = {
            "command_id": "cmd-001",
            "command_type": "burn",
            "payload": {"burn_vector_x": 0.001, "burn_vector_y": 0, "burn_vector_z": 0, "burn_duration_s": 10},
        }
        decision = cap.evaluate_tick(ship, [], [cmd], [], 1.0)
        # Should either approve (return await_orders) or no override
        assert decision.action in ("await_orders",)

    def test_veto_burn_near_hazard(self):
        cap = ShipCaptain()
        ship = _make_ship()
        hazard = _make_hazard(py=500, vy=-1)  # within HAZARD_OVERRIDE_DISTANCE
        # Command that would move toward hazard
        cmd = {
            "command_id": "cmd-002",
            "command_type": "burn",
            "payload": {"burn_vector_x": 0, "burn_vector_y": 1.0, "burn_vector_z": 0, "burn_duration_s": 10},
        }
        decision = cap.evaluate_tick(ship, [hazard], [cmd], [], 1.0)
        # Captain should either evade the hazard (P1) or override the command (P2)
        assert decision.priority_level <= 2

    def test_modify_burn_low_fuel(self):
        cap = ShipCaptain()
        ship = _make_ship(fuel=80)  # 16% — below FUEL_CAUTION_PCT (25%)
        cmd = {
            "command_id": "cmd-003",
            "command_type": "burn",
            "payload": {"burn_vector_x": 0.1, "burn_vector_y": 0, "burn_vector_z": 0, "burn_duration_s": 10},
        }
        decision = cap.evaluate_tick(ship, [], [cmd], [], 1.0)
        assert decision.action == "modify_command"
        assert decision.override_of_command_id == "cmd-003"

    def test_veto_burn_hull_damaged(self):
        cap = ShipCaptain()
        ship = _make_ship(hull=30)  # below HULL_SAFE_MODE_PCT (40%)
        cmd = {
            "command_id": "cmd-004",
            "command_type": "burn",
            "payload": {"burn_vector_x": 0.1, "burn_vector_y": 0, "burn_vector_z": 0, "burn_duration_s": 10},
        }
        decision = cap.evaluate_tick(ship, [], [cmd], [], 1.0)
        # Captain should either veto or enter safe mode
        assert decision.action in ("override_command", "enter_safe_mode")


class TestMicroCorrection:
    def test_correction_on_drift(self):
        cap = ShipCaptain()
        ship = _make_ship()
        predictions = [{
            "assessment": "correction_needed",
            "prediction_error_km": 10000,
            "predicted_pos_x": ship.position.x + 100,
            "predicted_pos_y": ship.position.y + 100,
            "predicted_pos_z": 0,
        }]
        decision = cap.evaluate_tick(ship, [], [], predictions, 1.0)
        assert decision.action == "micro_correction"
        assert decision.delta_v <= CAPTAIN_MICRO_DV_LIMIT

    def test_no_correction_on_course(self):
        cap = ShipCaptain()
        ship = _make_ship()
        predictions = [{"assessment": "on_course", "prediction_error_km": 10}]
        decision = cap.evaluate_tick(ship, [], [], predictions, 1.0)
        assert decision.action != "micro_correction"


class TestSafeMode:
    def test_hull_damage_triggers_safe_mode(self):
        cap = ShipCaptain()
        ship = _make_ship(hull=30)
        decision = cap.evaluate_tick(ship, [], [], [], 1.0)
        assert decision.action == "enter_safe_mode"
        assert cap.state.alert_level == "red"

    def test_nominal_hull_no_safe_mode(self):
        cap = ShipCaptain()
        ship = _make_ship(hull=90)
        decision = cap.evaluate_tick(ship, [], [], [], 1.0)
        assert decision.action != "enter_safe_mode"


class TestAlertLevel:
    def test_green_nominal(self):
        cap = ShipCaptain()
        ship = _make_ship(fuel=300, hull=90)
        cap.evaluate_tick(ship, [], [], [], 1.0)
        assert cap.state.alert_level == "green"

    def test_yellow_low_fuel(self):
        cap = ShipCaptain()
        ship = _make_ship(fuel=80)  # 16%
        cap.evaluate_tick(ship, [], [], [], 1.0)
        assert cap.state.alert_level == "yellow"

    def test_red_hull_damage(self):
        cap = ShipCaptain()
        ship = _make_ship(hull=30)
        cap.evaluate_tick(ship, [], [], [], 1.0)
        assert cap.state.alert_level == "red"


class TestNominal:
    def test_await_orders(self):
        cap = ShipCaptain()
        ship = _make_ship()
        decision = cap.evaluate_tick(ship, [], [], [], 1.0)
        assert decision.action == "await_orders"
        assert decision.priority_level == 5
        assert decision.confidence > 0

    def test_decision_has_id(self):
        cap = ShipCaptain()
        ship = _make_ship()
        decision = cap.evaluate_tick(ship, [], [], [], 1.0)
        assert decision.decision_id
        assert len(decision.decision_id) > 0
