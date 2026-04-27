"""Shared fixtures for Mission Control test suite."""

import math
import sys
import os

import pytest

# Ensure src/python is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src", "python"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "app"))

from physics_engine import (
    CelestialBody,
    Hazard,
    SpacecraftState,
    Vector3,
    GM,
    ORBIT_PERIOD,
    ORBIT_RADIUS,
)
from spacecraft_autopilot import (
    AutopilotState,
    GroundCommand,
    SpacecraftAutopilot,
)


@pytest.fixture
def spacecraft_at_earth() -> SpacecraftState:
    """Ship at Earth's orbit with circular velocity."""
    r = ORBIT_RADIUS["earth"]
    v = math.sqrt(GM["sun"] / r)  # ~29.78 km/s
    return SpacecraftState(
        position=Vector3(r, 0, 0),
        velocity=Vector3(0, v, 0),
        fuel_remaining_kg=500.0,
        hull_integrity=100.0,
        engine_status="nominal",
        timestamp_s=0.0,
    )


@pytest.fixture
def spacecraft_at_mars() -> SpacecraftState:
    """Ship at Mars orbit for intercept tests."""
    r = ORBIT_RADIUS["mars"]
    v = math.sqrt(GM["sun"] / r)
    return SpacecraftState(
        position=Vector3(r, 0, 0),
        velocity=Vector3(0, v, 0),
        fuel_remaining_kg=500.0,
        hull_integrity=100.0,
        engine_status="nominal",
        timestamp_s=0.0,
    )


@pytest.fixture
def sun_body() -> CelestialBody:
    """Sun at origin with GM_SUN."""
    return CelestialBody(
        name="sun",
        gm=GM["sun"],
        orbit_radius_km=0.0,
        orbit_period_s=1.0,  # not used for Sun
    )


@pytest.fixture
def sample_hazard() -> Hazard:
    """Asteroid at known position for collision tests."""
    return Hazard(
        hazard_id="hazard-test-001",
        hazard_type="asteroid",
        position=Vector3(1.5e8, 100.0, 0.0),
        velocity=Vector3(-5.0, 0.0, 0.0),
        radius_km=10.0,
    )


@pytest.fixture
def nominal_mission_state() -> dict:
    """A healthy mid-mission state dict for agent/autopilot tests."""
    elapsed_s = 86400 * 30  # 30 days
    # Ship roughly mid-way between Mars and Earth
    px = 1.8e8
    py = 5.0e7
    pz = 0.0
    vx = -15.0
    vy = -10.0
    return {
        "position_x": px,
        "position_y": py,
        "position_z": pz,
        "velocity_x": vx,
        "velocity_y": vy,
        "velocity_z": 0.0,
        "fuel_remaining_kg": 350.0,
        "comm_delay_s": 480.0,
        "elapsed_s": elapsed_s,
        "speed_km_s": math.sqrt(vx**2 + vy**2),
        "distance_to_earth_km": 1.0e8,
    }


@pytest.fixture
def autopilot_fresh() -> SpacecraftAutopilot:
    """Fresh autopilot with no prior state or planned trajectory."""
    return SpacecraftAutopilot()


@pytest.fixture
def fastapi_client():
    """httpx.AsyncClient bound to the FastAPI app.

    Skipped if Databricks SDK / Lakebase aren't available (CI environment).
    """
    pytest.importorskip("httpx")
    try:
        from main import app
    except Exception:
        pytest.skip("FastAPI app cannot be imported without Databricks environment")

    from httpx import ASGITransport, AsyncClient

    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://testserver")
