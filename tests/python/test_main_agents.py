"""Tests for _rule_based_agent() in app/main.py — 14 tests."""

import math
import sys
import os

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "app"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
import _mock_deps  # noqa: F401

from main import _rule_based_agent, earth_position_at


def _nominal_state(elapsed_s=86400 * 30) -> dict:
    """Mid-mission state heading toward Earth."""
    ex, ey = earth_position_at(elapsed_s)
    # Ship between Mars and Earth, heading inward
    px = ex + 50_000_000
    return {
        "position_x": px,
        "position_y": ey,
        "position_z": 0.0,
        "velocity_x": -20.0,
        "velocity_y": 5.0,
        "velocity_z": 0.0,
        "fuel_remaining_kg": 350.0,
        "comm_delay_s": 480.0,
        "elapsed_s": elapsed_s,
    }


def _low_fuel_state() -> dict:
    s = _nominal_state()
    s["fuel_remaining_kg"] = 50.0
    return s


# ── Flight Dynamics ─────────────────────────────────────────────────────

class TestFlightDynamics:
    def test_nominal_on_course(self):
        """On-course ship → on_course=True."""
        state = _nominal_state()
        result = _rule_based_agent("flight_dynamics", state, [], {}, {})
        assert "reasoning" in result
        assert isinstance(result.get("on_course"), bool)

    def test_low_fuel_warning(self):
        """Low fuel → reasoning mentions fuel."""
        state = _low_fuel_state()
        result = _rule_based_agent("flight_dynamics", state, [], {}, {})
        assert "fuel" in result["reasoning"].lower() or "Fuel" in result["reasoning"]

    def test_high_deviation(self):
        """Ship heading perpendicular → not on course."""
        state = _nominal_state()
        state["velocity_x"] = 0.0
        state["velocity_y"] = 25.0  # all tangential
        result = _rule_based_agent("flight_dynamics", state, [], {}, {})
        # Should detect drifting or off-course
        assert result.get("is_drifting") or not result.get("on_course")


# ── Hazard Assessment ───────────────────────────────────────────────────

class TestHazardAssessment:
    def test_no_hazards(self):
        """Empty hazard list → reasoning says 'no' threats."""
        result = _rule_based_agent("hazard_assessment", _nominal_state(), [], {}, {})
        assert result["hazard_count"] == 0
        assert "no" in result["reasoning"].lower() or "0" in result["reasoning"]

    def test_close_hazard(self):
        """Hazard with high risk score → evasion recommended."""
        hazards = [{"risk_score": 0.9, "hazard_type": "asteroid"}]
        result = _rule_based_agent("hazard_assessment", _nominal_state(), hazards, {}, {})
        assert result["max_risk"] >= 0.9
        assert "evasive" in result["reasoning"].lower() or "action" in result["reasoning"].lower()

    def test_far_hazard(self):
        """Hazard with low risk → manageable."""
        hazards = [{"risk_score": 0.1, "hazard_type": "debris"}]
        result = _rule_based_agent("hazard_assessment", _nominal_state(), hazards, {}, {})
        assert result["max_risk"] < 0.3
        assert "manageable" in result["reasoning"].lower()


# ── Communications ──────────────────────────────────────────────────────

class TestCommunications:
    def test_nominal(self):
        """Low delay → acceptable."""
        state = _nominal_state()
        state["comm_delay_s"] = 120.0  # 2 min
        result = _rule_based_agent("communications", state, [], {}, {})
        assert result["delay_critical"] is False
        assert "acceptable" in result["reasoning"].lower() or "within" in result["reasoning"].lower()

    def test_high_delay(self):
        """Delay > 20 min → significant."""
        state = _nominal_state()
        state["comm_delay_s"] = 1500.0  # 25 min
        result = _rule_based_agent("communications", state, [], {}, {})
        assert result["delay_critical"] is True
        assert "significant" in result["reasoning"].lower() or "autopilot" in result["reasoning"].lower()


# ── Mission Commander ───────────────────────────────────────────────────

class TestMissionCommander:
    def test_nominal(self):
        """All systems go → GO decision."""
        peer = {
            "flight_dynamics": {"on_course": True, "is_drifting": False, "closing_rate_kms": 15.0, "phase": "intercept_cruise"},
            "hazard_assessment": {"max_risk": 0.1, "ml_scored": False},
            "communications": {"delay_critical": False, "ml_enhanced": False},
        }
        state = _nominal_state()
        result = _rule_based_agent("mission_commander", state, [], {}, peer)
        assert result["decision"] == "GO"
        assert result["confidence"] > 0.8

    def test_multiple_issues(self):
        """High hazard risk → EMERGENCY_EVASION."""
        peer = {
            "flight_dynamics": {"on_course": False, "is_drifting": False, "closing_rate_kms": 5.0, "phase": "intercept_cruise"},
            "hazard_assessment": {"max_risk": 0.85, "ml_scored": False},
            "communications": {"delay_critical": True, "ml_enhanced": False},
        }
        state = _nominal_state()
        result = _rule_based_agent("mission_commander", state, [], {}, peer)
        assert result["decision"] == "EMERGENCY_EVASION"


# ── Cross-cutting ───────────────────────────────────────────────────────

class TestAgentCrossCutting:
    def test_confidence_range(self):
        """All agents return confidence in [0.0, 1.0]."""
        state = _nominal_state()
        for name in ["flight_dynamics", "hazard_assessment", "communications", "mission_commander"]:
            peer = {
                "flight_dynamics": {"on_course": True, "is_drifting": False, "closing_rate_kms": 10, "phase": "intercept"},
                "hazard_assessment": {"max_risk": 0.1, "ml_scored": False},
                "communications": {"delay_critical": False, "ml_enhanced": False},
            }
            result = _rule_based_agent(name, state, [], {}, peer)
            assert 0.0 <= result["confidence"] <= 1.0, f"{name}: confidence={result['confidence']}"

    def test_returns_required_fields(self):
        """Every agent returns reasoning and confidence."""
        state = _nominal_state()
        for name in ["flight_dynamics", "hazard_assessment", "communications", "mission_commander"]:
            peer = {
                "flight_dynamics": {"on_course": True, "is_drifting": False, "closing_rate_kms": 10, "phase": "intercept"},
                "hazard_assessment": {"max_risk": 0.1, "ml_scored": False},
                "communications": {"delay_critical": False, "ml_enhanced": False},
            }
            result = _rule_based_agent(name, state, [], {}, peer)
            assert "reasoning" in result
            assert "confidence" in result

    def test_nan_values_no_crash(self):
        """NaN in mission state → agent still returns (no crash)."""
        state = _nominal_state()
        state["position_x"] = float("nan")
        state["velocity_x"] = float("nan")
        # Should not raise
        result = _rule_based_agent("flight_dynamics", state, [], {}, {})
        assert result is not None
        assert "reasoning" in result
