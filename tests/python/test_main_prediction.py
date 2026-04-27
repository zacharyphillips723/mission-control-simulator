"""Tests for _physics_predict() in app/main.py — 8 tests."""

import math
import sys
import os

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "app"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
import _mock_deps  # noqa: F401

from main import GM_SUN, EARTH_ORBIT_RADIUS_KM, _physics_predict


class TestPhysicsPredict:
    def test_forward(self):
        """Predicts position 1 hour ahead — should move from initial position."""
        r = EARTH_ORBIT_RADIUS_KM
        vx, vy, vz = 0.0, math.sqrt(GM_SUN / r), 0.0
        px, py, pz = r, 0.0, 0.0
        fx, fy, fz = _physics_predict(px, py, pz, vx, vy, vz, 3600.0)
        dist = math.sqrt((fx - px)**2 + (fy - py)**2 + (fz - pz)**2)
        assert dist > 1000  # moved more than 1000 km in 1 hour

    def test_zero_dt(self):
        """dt=0 → same position."""
        px, py, pz = 1.5e8, 0.0, 0.0
        fx, fy, fz = _physics_predict(px, py, pz, 0, 29.78, 0, 0.0)
        # With 0 horizon, should still be at same position (1 step, 0 dt)
        # Actually steps = max(1, int(0/2)) = 1, step_dt = 0
        assert fx == pytest.approx(px, abs=1.0)

    def test_includes_gravity(self):
        """Prediction curves (not straight line) — gravity bends the path."""
        r = EARTH_ORBIT_RADIUS_KM
        v = math.sqrt(GM_SUN / r)
        # Predict with gravity
        fx, fy, fz = _physics_predict(r, 0, 0, 0, v, 0, 86400.0)  # 1 day
        final_r = math.sqrt(fx**2 + fy**2 + fz**2)
        # With gravity on a circular orbit, should stay near original radius
        # Without gravity, the ship would fly off in a straight line
        straight_line_x = r
        straight_line_y = v * 86400
        straight_line_r = math.sqrt(straight_line_x**2 + straight_line_y**2)
        # With gravity, the radius should be MUCH closer to r than the straight line distance
        assert abs(final_r - r) < abs(straight_line_r - r)

    def test_fuel_estimate_concept(self):
        """The prediction function itself doesn't return fuel, but we can check
        that a predicted deviance from actual position implies a correction cost."""
        r = EARTH_ORBIT_RADIUS_KM
        v = math.sqrt(GM_SUN / r)
        fx, fy, fz = _physics_predict(r, 0, 0, 0, v, 0, 3600.0)
        # Prediction should be roughly on orbit
        final_r = math.sqrt(fx**2 + fy**2 + fz**2)
        assert final_r == pytest.approx(r, rel=0.01)

    def test_predict_vs_actual_consistency(self):
        """Predict 100 steps, then simulate 100 steps — compare."""
        r = EARTH_ORBIT_RADIUS_KM
        v = math.sqrt(GM_SUN / r)
        horizon = 600.0  # 10 minutes

        # Single call prediction
        fx1, fy1, fz1 = _physics_predict(r, 0, 0, 0, v, 0, horizon)

        # Manual step-by-step (same algorithm)
        steps = max(1, int(horizon / 2.0))
        step_dt = horizon / steps
        x, y, z = r, 0.0, 0.0
        _vx, _vy, _vz = 0.0, v, 0.0
        for _ in range(steps):
            rr = math.sqrt(x*x + y*y + z*z) or 1.0
            a_mag = -GM_SUN / (rr * rr)
            _vx += a_mag * x / rr * step_dt
            _vy += a_mag * y / rr * step_dt
            _vz += a_mag * z / rr * step_dt
            x += _vx * step_dt
            y += _vy * step_dt
            z += _vz * step_dt

        assert fx1 == pytest.approx(x, abs=1.0)
        assert fy1 == pytest.approx(y, abs=1.0)

    def test_prediction_with_radial_velocity(self):
        """Ship with inward velocity — prediction should show approach to Sun."""
        r = EARTH_ORBIT_RADIUS_KM
        fx, fy, fz = _physics_predict(r, 0, 0, -10.0, 20.0, 0, 3600.0)
        final_r = math.sqrt(fx**2 + fy**2 + fz**2)
        # With inward velocity, should end up closer to Sun
        assert final_r < r

    def test_assessment_on_course_concept(self):
        """Small prediction error → 'on_course' assessment concept."""
        r = EARTH_ORBIT_RADIUS_KM
        v = math.sqrt(GM_SUN / r)
        fx, fy, fz = _physics_predict(r, 0, 0, 0, v, 0, 60.0)  # 1 min
        error = math.sqrt((fx - r)**2 + fy**2 + fz**2)
        # For a short prediction on a circular orbit, error should be small
        assert error < 5000  # within 5000 km for 1-minute prediction

    def test_assessment_correction_concept(self):
        """Large deviation from target → 'correction_needed' concept."""
        # Ship heading away from Earth at high speed
        fx, fy, fz = _physics_predict(2.5e8, 0, 0, 20.0, 0, 0, 86400.0)
        # After 1 day at 20 km/s outward, should be much farther
        final_r = math.sqrt(fx**2 + fy**2 + fz**2)
        assert final_r > 2.5e8  # moved outward
