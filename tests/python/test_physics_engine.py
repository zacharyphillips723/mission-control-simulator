"""Tests for src/python/physics_engine.py — 28 tests covering Vector3, gravity,
propagation, fuel estimation, collision risk, and gravity assists."""

import math

import pytest

from physics_engine import (
    BODIES,
    GM,
    ORBIT_PERIOD,
    ORBIT_RADIUS,
    CelestialBody,
    Hazard,
    SpacecraftState,
    Vector3,
    closest_approach,
    collision_risk,
    communication_delay,
    compute_delta_v,
    create_initial_state,
    estimate_fuel_cost,
    gravitational_acceleration,
    gravity_assist_delta_v,
    propagate_state,
    propagate_trajectory,
    total_acceleration,
)


# ── Vector3 operations ──────────────────────────────────────────────────

class TestVector3:
    def test_add(self):
        a = Vector3(1, 2, 3)
        b = Vector3(4, 5, 6)
        c = a + b
        assert c.x == 5 and c.y == 7 and c.z == 9

    def test_sub(self):
        a = Vector3(10, 20, 30)
        b = Vector3(1, 2, 3)
        c = a - b
        assert c.x == 9 and c.y == 18 and c.z == 27

    def test_mul_scalar(self):
        v = Vector3(2, 3, 4)
        r = v * 3
        assert r.x == 6 and r.y == 9 and r.z == 12

    def test_rmul_scalar(self):
        v = Vector3(2, 3, 4)
        r = 3 * v
        assert r.x == 6 and r.y == 9 and r.z == 12

    def test_magnitude(self):
        v = Vector3(3, 4, 0)
        assert v.magnitude() == pytest.approx(5.0)

    def test_magnitude_zero(self):
        v = Vector3(0, 0, 0)
        assert v.magnitude() == 0.0

    def test_normalize(self):
        v = Vector3(3, 4, 0)
        n = v.normalized()
        assert n.magnitude() == pytest.approx(1.0, abs=1e-12)
        assert n.x == pytest.approx(0.6)
        assert n.y == pytest.approx(0.8)

    def test_normalize_zero(self):
        v = Vector3(0, 0, 0)
        n = v.normalized()
        assert n.x == 0 and n.y == 0 and n.z == 0  # no crash

    def test_dot(self):
        a = Vector3(1, 2, 3)
        b = Vector3(4, 5, 6)
        assert a.dot(b) == 32  # 4+10+18


# ── Gravitational acceleration ──────────────────────────────────────────

class TestGravitationalAcceleration:
    def test_earth_orbit(self):
        """Earth at 1 AU from Sun: a ≈ GM_sun / r^2 ≈ 0.00593 km/s²."""
        pos = Vector3(ORBIT_RADIUS["earth"], 0, 0)
        sun = Vector3(0, 0, 0)
        a = gravitational_acceleration(pos, sun, GM["sun"])
        expected = GM["sun"] / ORBIT_RADIUS["earth"] ** 2
        assert a.magnitude() == pytest.approx(expected, rel=0.01)

    def test_inverse_square(self):
        """Double the distance → quarter the acceleration."""
        pos1 = Vector3(1e8, 0, 0)
        pos2 = Vector3(2e8, 0, 0)
        sun = Vector3(0, 0, 0)
        a1 = gravitational_acceleration(pos1, sun, GM["sun"]).magnitude()
        a2 = gravitational_acceleration(pos2, sun, GM["sun"]).magnitude()
        assert a1 / a2 == pytest.approx(4.0, rel=0.01)

    def test_zero_pos_no_crash(self):
        """Position at origin → clamped to r=1, no divide-by-zero."""
        pos = Vector3(0, 0, 0)
        body = Vector3(0, 0, 0)
        a = gravitational_acceleration(pos, body, GM["sun"])
        # Should not raise; magnitude is finite
        assert math.isfinite(a.magnitude())


# ── Total acceleration ──────────────────────────────────────────────────

class TestTotalAcceleration:
    def test_sun_only_dominates(self):
        """At 1 AU, total accel is dominated by Sun's gravity."""
        pos = Vector3(ORBIT_RADIUS["earth"], 0, 0)
        a_total = total_acceleration(pos, 0.0)
        a_sun = gravitational_acceleration(pos, Vector3(0, 0, 0), GM["sun"])
        # Sun contribution should be >99% of total
        assert a_total.magnitude() == pytest.approx(a_sun.magnitude(), rel=0.05)

    def test_multiple_bodies(self):
        """Total acceleration with no thrust should differ from sun-only due to planets."""
        pos = Vector3(ORBIT_RADIUS["earth"] * 0.5, 0, 0)
        a_total = total_acceleration(pos, 0.0)
        a_sun = gravitational_acceleration(pos, Vector3(0, 0, 0), GM["sun"])
        # Should be close but not identical (planets add small perturbation)
        assert a_total.magnitude() != a_sun.magnitude()


# ── Propagation ─────────────────────────────────────────────────────────

class TestPropagation:
    def test_circular_orbit_stays_at_1au(self):
        """Propagate orbit at 1 AU (away from planets) for 1 day, stays near 1 AU."""
        # Start at 1 AU but offset so we're not ON any planet (use t=T/3)
        r = ORBIT_RADIUS["earth"]
        v = math.sqrt(GM["sun"] / r)
        # Position at 60° — far from Earth (which is at 0°) and Mars
        angle = math.pi / 3
        state = SpacecraftState(
            position=Vector3(r * math.cos(angle), r * math.sin(angle), 0),
            velocity=Vector3(-v * math.sin(angle), v * math.cos(angle), 0),
            fuel_remaining_kg=0.0,
            timestamp_s=0.0,
        )
        duration = 86400  # 1 day
        dt = 60.0
        current = state
        for _ in range(int(duration / dt)):
            current = propagate_state(current, dt)
        rad = current.position.magnitude()
        assert rad == pytest.approx(r, rel=0.01)

    def test_energy_conservation(self):
        """Velocity Verlet conserves energy over 100 steps when away from planets."""
        r = ORBIT_RADIUS["earth"]
        v = math.sqrt(GM["sun"] / r)
        # Start at 60° offset to avoid planetary close encounters
        angle = math.pi / 3
        state = SpacecraftState(
            position=Vector3(r * math.cos(angle), r * math.sin(angle), 0),
            velocity=Vector3(-v * math.sin(angle), v * math.cos(angle), 0),
            fuel_remaining_kg=0.0,
            timestamp_s=0.0,
        )
        dt = 60.0

        def orbital_energy(s: SpacecraftState) -> float:
            rr = s.position.magnitude()
            vv = s.velocity.magnitude()
            return 0.5 * vv**2 - GM["sun"] / rr

        e0 = orbital_energy(state)
        current = state
        for _ in range(100):
            current = propagate_state(current, dt)
        e1 = orbital_energy(current)

        # Energy drift < 1% (multi-body Verlet has some drift from planetary perturbations)
        assert abs((e1 - e0) / e0) < 0.01

    def test_zero_dt(self, spacecraft_at_earth):
        """dt=0 → no state change."""
        state = spacecraft_at_earth
        result = propagate_state(state, 0.0)
        assert result.position.x == state.position.x
        assert result.velocity.x == state.velocity.x


# ── Hohmann transfer (using estimate_fuel_cost indirectly) ──────────────

class TestHohmannAndFuel:
    def test_estimate_fuel_cost_small_dv(self):
        """Small dv → fuel ≈ mass * dv / ve (linear regime of Tsiolkovsky)."""
        dv = 0.01  # very small
        mass = 5000.0
        ve = 3.0
        fuel = estimate_fuel_cost(dv, mass, ve)
        # For small dv/ve, exp(dv/ve) ≈ 1 + dv/ve, so fuel ≈ mass * dv/ve
        expected = mass * dv / ve
        assert fuel == pytest.approx(expected, rel=0.05)

    def test_estimate_fuel_cost_zero_dv(self):
        """dv=0 → fuel=0."""
        fuel = estimate_fuel_cost(0.0)
        assert fuel == pytest.approx(0.0, abs=1e-10)

    def test_estimate_fuel_cost_large_dv(self):
        """Large dv → exponential fuel growth."""
        small = estimate_fuel_cost(1.0)
        large = estimate_fuel_cost(5.0)
        # 5x the dv should need WAY more than 5x the fuel (exponential)
        assert large > small * 5


# ── Collision risk ──────────────────────────────────────────────────────

class TestCollisionRisk:
    def test_direct_hit(self):
        """Zero separation → risk = 1.0."""
        risk = collision_risk(0.0, hazard_radius_km=10.0)
        assert risk == pytest.approx(1.0)

    def test_far_away(self):
        """Large separation → risk ≈ 0.0."""
        risk = collision_risk(1e6, hazard_radius_km=10.0)
        assert risk == pytest.approx(0.0, abs=0.01)

    def test_closing_vs_far(self):
        """Closer approach → higher risk."""
        risk_close = collision_risk(50.0, hazard_radius_km=10.0)
        risk_far = collision_risk(500.0, hazard_radius_km=10.0)
        assert risk_close > risk_far


# ── Gravity assist ──────────────────────────────────────────────────────

class TestGravityAssist:
    def test_reasonable_delta_v(self):
        """Known flyby scenario produces a non-zero dv."""
        sc_vel = Vector3(20.0, 0.0, 0.0)
        body_vel = Vector3(0.0, 13.0, 0.0)
        dv = gravity_assist_delta_v(sc_vel, body_vel, periapsis_km=50000.0, body_gm=GM["jupiter"])
        assert dv.magnitude() > 0.1  # should get measurable assist

    def test_comoving_no_assist(self):
        """If spacecraft matches body velocity, effectively no relative velocity → no assist."""
        vel = Vector3(10.0, 5.0, 0.0)
        dv = gravity_assist_delta_v(vel, vel, periapsis_km=50000.0, body_gm=GM["jupiter"])
        assert dv.magnitude() < 0.01


# ── Dataclass construction ──────────────────────────────────────────────

class TestDataclasses:
    def test_spacecraft_state(self):
        s = SpacecraftState(
            position=Vector3(1, 2, 3),
            velocity=Vector3(4, 5, 6),
            fuel_remaining_kg=100.0,
        )
        assert s.speed == pytest.approx(Vector3(4, 5, 6).magnitude())
        assert s.distance_from_sun == pytest.approx(Vector3(1, 2, 3).magnitude())

    def test_celestial_body(self):
        b = CelestialBody("test", gm=100.0, orbit_radius_km=1000.0, orbit_period_s=3600.0)
        pos = b.position_at(0.0)
        assert pos.x == pytest.approx(1000.0)
        assert pos.y == pytest.approx(0.0)
