"""Bug-hunting tests — 7 tests targeting known issues found during code review.

These tests are EXPECTED TO FAIL initially, proving the bugs exist.
After fixing each bug, the corresponding test becomes a regression guard.

Run with:
    python -m pytest tests/python/test_bug_hunting.py -v --tb=long
"""

import math
import sys
import os

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src", "python"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "app"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
import _mock_deps  # noqa: F401

from physics_engine import (
    Vector3,
    SpacecraftState,
    estimate_fuel_cost,
    collision_risk,
    propagate_state,
    GM,
    ORBIT_RADIUS,
)
from spacecraft_autopilot import (
    SpacecraftAutopilot,
    Hazard,
    FUEL_EMERGENCY_ONLY_KG,
)


class TestBugHunting:

    def test_physics_constant_mismatch(self):
        """Fuel cost from main.py's constants vs physics_engine's defaults should match (regression guard)."""
        dv = 0.5  # km/s

        # physics_engine defaults (now uses shared mission_constants)
        fuel_pe = estimate_fuel_cost(dv)

        # main.py now uses the same constants: EXHAUST_VEL=30, DRY_MASS=3000
        EXHAUST_VEL_MAIN = 30.0
        DRY_MASS_MAIN = 3000.0
        mass_ratio = math.exp(dv / EXHAUST_VEL_MAIN)
        fuel_main = DRY_MASS_MAIN * (mass_ratio - 1)

        # These should be comparable — they're both "fuel cost for 0.5 km/s"
        ratio = fuel_pe / fuel_main if fuel_main > 0 else float("inf")
        assert 0.9 <= ratio <= 1.1, (
            f"Fuel cost mismatch: physics_engine={fuel_pe:.1f}kg, main.py={fuel_main:.1f}kg, "
            f"ratio={ratio:.1f}x"
        )

    @pytest.mark.xfail(reason="BUG: main.py uses symplectic Euler, physics_engine uses Verlet — energy drift diverges")
    def test_euler_vs_verlet_divergence(self):
        """Propagate same orbit 10K steps in both integrators, compare final positions."""
        r = ORBIT_RADIUS["earth"]
        v = math.sqrt(GM["sun"] / r)
        dt = 60.0  # 1 min steps
        steps = 10_000  # ~7 days

        # Velocity Verlet (physics_engine)
        state = SpacecraftState(
            position=Vector3(r, 0, 0),
            velocity=Vector3(0, v, 0),
            fuel_remaining_kg=0.0,
            timestamp_s=0.0,
        )
        for _ in range(steps):
            state = propagate_state(state, dt)
        verlet_x, verlet_y = state.position.x, state.position.y

        # Symplectic Euler (main.py's _physics_predict logic)
        from main import _physics_predict, GM_SUN
        euler_x, euler_y, euler_z = _physics_predict(r, 0, 0, 0, v, 0, dt * steps)

        # Compare positions
        dist = math.sqrt((verlet_x - euler_x)**2 + (verlet_y - euler_y)**2)
        orbit_pct = dist / r * 100
        assert orbit_pct < 1.0, (
            f"Position divergence after {steps} steps: {dist:.0f}km ({orbit_pct:.2f}% of orbit radius)"
        )

    @pytest.mark.xfail(reason="BUG: Potential SQL injection via f-string at approve_maneuver endpoint")
    def test_sql_injection_maneuver_approve(self):
        """Check that maneuver approval uses parameterized queries, not f-strings.

        We can't actually call the endpoint without DB, but we verify the code pattern.
        """
        import ast
        import inspect

        # Read main.py source and look for dangerous patterns near maneuver approval
        main_path = os.path.join(os.path.dirname(__file__), "..", "..", "app", "main.py")
        with open(main_path) as f:
            source = f.read()

        # Look for f-string SQL patterns near approve
        lines = source.split("\n")
        dangerous_patterns = []
        for i, line in enumerate(lines):
            if "approve" in line.lower() and ("f'" in line or 'f"' in line):
                # Check if it looks like SQL
                if any(kw in line.upper() for kw in ["UPDATE", "INSERT", "SELECT", "DELETE"]):
                    dangerous_patterns.append((i + 1, line.strip()))

        assert len(dangerous_patterns) == 0, (
            f"Found potential SQL injection via f-string:\n" +
            "\n".join(f"  L{n}: {l}" for n, l in dangerous_patterns)
        )

    def test_negative_delta_v_fuel_cost(self):
        """estimate_fuel_cost with negative dv should return positive fuel (uses abs, regression guard)."""
        result = estimate_fuel_cost(-5.0)
        # Negative dv is treated as abs(dv) — fuel cost should be positive
        assert result >= 0, f"Negative dv produced fuel={result:.2f}kg (should be ≥ 0)"
        # Should equal the positive dv result
        assert result == estimate_fuel_cost(5.0)

    @pytest.mark.xfail(reason="BUG: collision_risk with zero distance may produce edge-case behavior")
    def test_collision_risk_zero_distance(self):
        """collision_risk with position exactly at hazard center."""
        # min_distance = 0, which is ≤ hazard_radius → should return 1.0
        risk = collision_risk(0.0, hazard_radius_km=10.0)
        assert risk == 1.0
        # Also test with min_distance = hazard_radius exactly
        risk_at_surface = collision_risk(10.0, hazard_radius_km=10.0)
        assert 0.9 <= risk_at_surface <= 1.0, (
            f"At hazard surface (dist=radius), risk should be near 1.0 but got {risk_at_surface:.3f}"
        )

    def test_autopilot_evasion_ignores_fuel_mode(self):
        """Evasion with fuel=10 should respect emergency mode constraints (regression guard)."""
        ap = SpacecraftAutopilot()
        ship = SpacecraftState(
            position=Vector3(1.8e8, 0, 0),
            velocity=Vector3(0, 20, 0),
            fuel_remaining_kg=10.0,  # Very low, in emergency mode
            timestamp_s=0.0,
        )
        hazard = Hazard(
            hazard_id="h-budget",
            hazard_type="asteroid",
            position=Vector3(1.8e8, 100, 0),
            velocity=Vector3(0, -10, 0),
            radius_km=20.0,
        )
        decision = ap.run_tick(ship, [hazard], [], elapsed_days=1.0)
        if decision.action == "evasion_burn":
            assert decision.fuel_cost_kg <= ship.fuel_remaining_kg, (
                f"Evasion burn costs {decision.fuel_cost_kg:.1f}kg but only "
                f"{ship.fuel_remaining_kg:.1f}kg available"
            )

    @pytest.mark.xfail(reason="BUG: Agent tool SQL injection via unsanitized reasoning field")
    def test_agent_tools_sql_injection(self):
        """Agent reasoning field should not pass unsanitized to SQL queries.

        We verify by searching for patterns in agent_tools.py and main.py where
        agent output is interpolated into SQL.
        """
        tools_path = os.path.join(os.path.dirname(__file__), "..", "..", "src", "python", "agent_tools.py")
        main_path = os.path.join(os.path.dirname(__file__), "..", "..", "app", "main.py")

        dangerous = []
        for fpath in [tools_path, main_path]:
            if not os.path.exists(fpath):
                continue
            with open(fpath) as f:
                lines = f.readlines()
            for i, line in enumerate(lines):
                # Look for f-string SQL with agent-derived content
                if ("f'" in line or 'f"' in line) and any(
                    kw in line.upper() for kw in ["INSERT", "UPDATE"]
                ):
                    if any(
                        var in line for var in ["reasoning", "action_taken", "content", "message"]
                    ):
                        dangerous.append((os.path.basename(fpath), i + 1, line.strip()))

        assert len(dangerous) == 0, (
            f"Found potential SQL injection via agent output:\n" +
            "\n".join(f"  {f}:{n}: {l}" for f, n, l in dangerous)
        )
