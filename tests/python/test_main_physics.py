"""Tests for pure physics functions in app/main.py — 16 tests."""

import math
import sys
import os

import pytest

# Add paths and mock heavy deps before importing main
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "app"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
import _mock_deps  # noqa: F401 — sets up all mocks

from main import (
    EARTH_ANGULAR_VEL,
    EARTH_ORBIT_PERIOD_S,
    EARTH_ORBIT_RADIUS_KM,
    GM_SUN,
    SPEED_OF_LIGHT_KM_S,
    compute_comm_delay,
    earth_position_at,
    earth_velocity_at,
    estimate_intercept,
)


class TestEarthPosition:
    def test_t0(self):
        """At t=0, Earth is at (ORBIT_RADIUS, 0)."""
        x, y = earth_position_at(0.0)
        assert x == pytest.approx(EARTH_ORBIT_RADIUS_KM, rel=1e-10)
        assert y == pytest.approx(0.0, abs=1e-6)

    def test_quarter_year(self):
        """At t=T/4, Earth is 90° around orbit."""
        t = EARTH_ORBIT_PERIOD_S / 4
        x, y = earth_position_at(t)
        assert x == pytest.approx(0.0, abs=1e3)  # within 1000 km of 0
        assert y == pytest.approx(EARTH_ORBIT_RADIUS_KM, rel=1e-3)

    def test_full_year(self):
        """At t=T, Earth returns to start."""
        t = EARTH_ORBIT_PERIOD_S
        x, y = earth_position_at(t)
        assert x == pytest.approx(EARTH_ORBIT_RADIUS_KM, rel=1e-6)
        assert y == pytest.approx(0.0, abs=1.0)

    def test_radius_constant(self):
        """Distance from origin is always ORBIT_RADIUS."""
        for frac in [0.0, 0.1, 0.25, 0.5, 0.75, 1.0]:
            t = EARTH_ORBIT_PERIOD_S * frac
            x, y = earth_position_at(t)
            r = math.sqrt(x**2 + y**2)
            assert r == pytest.approx(EARTH_ORBIT_RADIUS_KM, rel=1e-10)


class TestCommDelay:
    def test_at_earth(self):
        """Delay ≈ 0 when spacecraft is at Earth's position."""
        ex, ey = earth_position_at(0.0)
        delay = compute_comm_delay(ex, ey, 0.0, ex, ey)
        assert delay == pytest.approx(0.0, abs=0.01)

    def test_at_mars(self):
        """Delay should be realistic (3-22 min) when at Mars distance."""
        # Mars at opposition: ~0.52 AU away; conjunction: ~2.52 AU
        mars_x = 2.279e8
        ex, ey = earth_position_at(0.0)
        delay = compute_comm_delay(mars_x, 0.0, 0.0, ex, ey)
        delay_min = delay / 60.0
        # At t=0, Earth is at (1.496e8, 0), Mars dist from Earth ≈ 0.783e8 km
        assert 1.0 < delay_min < 25.0  # reasonable range

    def test_never_negative(self):
        """Comm delay is always ≥ 0."""
        for px in [-1e9, 0, 1e9]:
            for py in [-1e9, 0, 1e9]:
                delay = compute_comm_delay(px, py, 0.0, 0.0, 0.0)
                assert delay >= 0.0


class TestEarthVelocity:
    def test_t0(self):
        """At t=0, velocity is perpendicular to (1,0) → (0, v)."""
        vx, vy = earth_velocity_at(0.0)
        assert vx == pytest.approx(0.0, abs=0.01)
        assert vy > 0  # positive y direction

    def test_magnitude(self):
        """Speed ≈ 29.78 km/s (Earth's orbital speed)."""
        vx, vy = earth_velocity_at(0.0)
        speed = math.sqrt(vx**2 + vy**2)
        assert speed == pytest.approx(29.78, rel=0.01)

    def test_perpendicular_to_position(self):
        """v · r ≈ 0 (velocity perpendicular to radius)."""
        t = 1e6  # arbitrary time
        ex, ey = earth_position_at(t)
        vx, vy = earth_velocity_at(t)
        dot = ex * vx + ey * vy
        # Normalize by magnitudes
        r_mag = math.sqrt(ex**2 + ey**2)
        v_mag = math.sqrt(vx**2 + vy**2)
        cos_angle = dot / (r_mag * v_mag)
        assert cos_angle == pytest.approx(0.0, abs=1e-10)


class TestEstimateIntercept:
    def test_stationary_target(self):
        """Ship moving toward Earth's current position."""
        ex, ey = earth_position_at(0.0)
        # Ship at Mars, heading toward Earth
        px, py = 2.0e8, 0.0
        vx = -20.0  # heading sunward
        vy = 0.0
        ix, iy, evx, evy, t_arr = estimate_intercept(px, py, vx, vy, 0.0)
        assert t_arr > 0  # positive time to arrival

    def test_comoving(self):
        """Ship already at Earth → t ≈ 0 (or very small)."""
        ex, ey = earth_position_at(0.0)
        vx, vy = earth_velocity_at(0.0)
        ix, iy, evx, evy, t_arr = estimate_intercept(ex, ey, vx, vy, 0.0)
        # Should converge to near-zero or very small
        assert t_arr < 1000  # less than ~17 minutes

    def test_diverging(self):
        """Ship moving away → large t."""
        ex, ey = earth_position_at(0.0)
        px = 2.0e8
        vx = 20.0  # moving away from Sun/Earth
        vy = 0.0
        ix, iy, evx, evy, t_arr = estimate_intercept(px, 0.0, vx, vy, 0.0)
        assert t_arr > 1e6  # very large arrival time


class TestConstants:
    def test_gm_sun(self):
        assert GM_SUN == pytest.approx(1.32712440018e11)

    def test_orbit_radius(self):
        assert EARTH_ORBIT_RADIUS_KM == pytest.approx(1.496e8, rel=0.001)

    def test_speed_of_light(self):
        assert SPEED_OF_LIGHT_KM_S == pytest.approx(299792.458)
