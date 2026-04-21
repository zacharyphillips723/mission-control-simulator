"""
Mission Control — Multi-Profile Mission Definitions

Defines diverse mission profiles that vary departure timing, speed, fuel,
trajectory, and hazard density. Used to generate robust, multi-scenario
training data for ML models.
"""

import math
from dataclasses import dataclass, field
from typing import Optional

from physics_engine import (
    BODIES,
    SpacecraftState,
    Vector3,
    create_initial_state,
)
from telemetry_generator import generate_telemetry_batch, generate_candidate_maneuvers


@dataclass
class MissionProfile:
    """A parameterized mission scenario for training data diversity."""
    name: str
    description: str
    departure_body: str = "mars"
    t_start_offset_days: float = 0.0
    extra_departure_dv: float = 0.0  # additional departure burn km/s
    initial_fuel_kg: float = 500.0
    gravity_assist_body: Optional[str] = None
    hazard_probability: float = 0.001
    solar_flare_probability: float = 0.001
    duration_hours: int = 24


# ---------------------------------------------------------------------------
# 11 canonical mission profiles
# ---------------------------------------------------------------------------
PROFILES: list[MissionProfile] = [
    MissionProfile(
        name="early_window",
        description="Standard early departure window — Mars at nominal orbital position",
        t_start_offset_days=0.0,
        duration_hours=24,
    ),
    MissionProfile(
        name="late_window",
        description="Late departure window — Mars 30 days further in orbit",
        t_start_offset_days=30.0,
        duration_hours=24,
    ),
    MissionProfile(
        name="optimal_window",
        description="Optimal Hohmann-like departure window — Mars 60 days into transfer arc",
        t_start_offset_days=60.0,
        duration_hours=24,
    ),
    MissionProfile(
        name="fast_departure",
        description="High-energy departure with extra delta-v; trades fuel for speed",
        extra_departure_dv=3.0,
        initial_fuel_kg=450.0,
        duration_hours=24,
    ),
    MissionProfile(
        name="slow_departure",
        description="Low-energy departure — minimal delta-v, fuel-conserving trajectory",
        extra_departure_dv=0.5,
        duration_hours=24,
    ),
    MissionProfile(
        name="low_fuel",
        description="Emergency low-fuel scenario — limited margin for maneuvers",
        initial_fuel_kg=200.0,
        duration_hours=24,
    ),
    MissionProfile(
        name="high_fuel",
        description="Fully fueled mission — maximum maneuver flexibility",
        initial_fuel_kg=800.0,
        duration_hours=24,
    ),
    MissionProfile(
        name="direct_transfer",
        description="Direct Mars-to-Earth transfer with no gravity assist",
        gravity_assist_body=None,
        duration_hours=24,
    ),
    MissionProfile(
        name="jupiter_assist",
        description="Jupiter gravity-assist trajectory — longer path, higher final speed",
        gravity_assist_body="jupiter",
        duration_hours=36,
    ),
    MissionProfile(
        name="asteroid_belt_dense",
        description="Route through dense asteroid belt region — elevated hazard frequency",
        hazard_probability=0.005,
        duration_hours=24,
    ),
    MissionProfile(
        name="solar_storm_active",
        description="Active solar storm period — frequent solar flare hazards",
        solar_flare_probability=0.01,
        hazard_probability=0.002,
        duration_hours=24,
    ),
]


def create_initial_state_for_profile(profile: MissionProfile) -> SpacecraftState:
    """
    Create an initial SpacecraftState customised for a mission profile.

    Starts from the base ``create_initial_state`` and then modifies the
    departure velocity by adding ``extra_departure_dv`` in the anti-sunward
    (radially outward-from-Sun) direction.  The ``t_start_offset_days`` shifts
    Mars's orbital position before departure.
    """
    t_start_s = profile.t_start_offset_days * 86400.0

    # Base state from physics engine
    state = create_initial_state(
        departure_body=profile.departure_body,
        t_start=t_start_s,
        fuel_kg=profile.initial_fuel_kg,
    )

    # Apply extra departure delta-v in the anti-sunward direction
    if profile.extra_departure_dv != 0.0:
        # Anti-sunward = radially away from the Sun = along position vector
        anti_sun = state.position.normalized()
        # Negate to push *toward* inner solar system (anti-sunward = inward)
        dv_vec = anti_sun * (-profile.extra_departure_dv)
        state = SpacecraftState(
            position=state.position,
            velocity=state.velocity + dv_vec,
            fuel_remaining_kg=state.fuel_remaining_kg,
            hull_integrity=state.hull_integrity,
            engine_status=state.engine_status,
            timestamp_s=state.timestamp_s,
        )

    return state


def generate_profile_telemetry(
    profile: MissionProfile,
) -> tuple[list[dict], list[dict], SpacecraftState]:
    """
    Generate telemetry for a single mission profile.

    Returns:
        (telemetry_rows, hazard_rows, final_state)
        Every telemetry row includes a ``mission_profile`` field set to
        the profile's name.
    """
    initial_state = create_initial_state_for_profile(profile)

    duration_s = profile.duration_hours * 3600.0

    # Combine base hazard probability with solar flare probability
    effective_hazard_prob = profile.hazard_probability + profile.solar_flare_probability

    telemetry_rows, hazard_rows, final_state = generate_telemetry_batch(
        initial_state=initial_state,
        duration_s=duration_s,
        dt=1.0,
        hazard_probability=effective_hazard_prob,
    )

    # Tag every row with the mission profile name
    for row in telemetry_rows:
        row["mission_profile"] = profile.name

    for row in hazard_rows:
        row["mission_profile"] = profile.name

    return telemetry_rows, hazard_rows, final_state


def generate_all_profiles() -> dict[str, tuple[list[dict], list[dict], SpacecraftState]]:
    """
    Generate telemetry for every profile in ``PROFILES``.

    Returns:
        A dict keyed by profile name, each value is
        ``(telemetry_rows, hazard_rows, final_state)``.
    """
    results: dict[str, tuple[list[dict], list[dict], SpacecraftState]] = {}
    for profile in PROFILES:
        print(f"  Generating profile: {profile.name} — {profile.description}")
        telemetry, hazards, final_state = generate_profile_telemetry(profile)
        results[profile.name] = (telemetry, hazards, final_state)
        print(f"    -> {len(telemetry):,} telemetry rows, {len(hazards):,} hazards")
    return results
