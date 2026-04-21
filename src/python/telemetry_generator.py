"""
Mission Control — Synthetic Telemetry Generator

Generates realistic spacecraft telemetry data using the physics engine.
Includes random perturbations, hazard generation, and communication delay simulation.
"""

import json
import math
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from physics_engine import (
    BODIES,
    Hazard,
    SpacecraftState,
    Vector3,
    closest_approach,
    collision_risk,
    communication_delay,
    create_initial_state,
    propagate_state,
)


def generate_hazard(
    spacecraft_state: SpacecraftState,
    t_seconds: float,
    difficulty: str = "medium",
) -> Hazard:
    """Generate a random hazard near the spacecraft's projected path."""
    # Distance from spacecraft where hazard spawns
    spawn_distances = {"easy": 5000, "medium": 2000, "hard": 500}
    spawn_dist = spawn_distances.get(difficulty, 2000)

    # Random offset from spacecraft position
    angle = random.uniform(0, 2 * math.pi)
    offset_z = random.uniform(-200, 200)

    hazard_types = ["asteroid", "meteor_shower", "debris_field", "solar_flare"]
    weights = [0.4, 0.3, 0.2, 0.1]
    hazard_type = random.choices(hazard_types, weights=weights, k=1)[0]

    # Size depends on type
    radii = {
        "asteroid": random.uniform(1, 50),
        "meteor_shower": random.uniform(100, 500),
        "debris_field": random.uniform(50, 200),
        "solar_flare": random.uniform(1000, 5000),
    }

    # Hazards move slowly relative to spacecraft
    speed = random.uniform(0.5, 5.0)
    vel_angle = random.uniform(0, 2 * math.pi)

    return Hazard(
        hazard_id=str(uuid.uuid4()),
        hazard_type=hazard_type,
        position=Vector3(
            spacecraft_state.position.x + spawn_dist * math.cos(angle),
            spacecraft_state.position.y + spawn_dist * math.sin(angle),
            spacecraft_state.position.z + offset_z,
        ),
        velocity=Vector3(
            speed * math.cos(vel_angle),
            speed * math.sin(vel_angle),
            random.uniform(-0.1, 0.1),
        ),
        radius_km=radii[hazard_type],
    )


def add_sensor_noise(state: SpacecraftState, noise_level: float = 0.001) -> dict:
    """Add realistic sensor noise to telemetry readings."""
    return {
        "position_x": state.position.x + random.gauss(0, abs(state.position.x) * noise_level),
        "position_y": state.position.y + random.gauss(0, abs(state.position.y) * noise_level),
        "position_z": state.position.z + random.gauss(0, max(1.0, abs(state.position.z)) * noise_level),
        "velocity_x": state.velocity.x + random.gauss(0, abs(state.velocity.x) * noise_level),
        "velocity_y": state.velocity.y + random.gauss(0, abs(state.velocity.y) * noise_level),
        "velocity_z": state.velocity.z + random.gauss(0, max(0.001, abs(state.velocity.z)) * noise_level),
    }


def generate_telemetry_batch(
    initial_state: Optional[SpacecraftState] = None,
    duration_s: float = 3600,
    dt: float = 1.0,
    hazard_probability: float = 0.001,
    mission_start_time: Optional[datetime] = None,
) -> tuple[list[dict], list[dict], SpacecraftState]:
    """
    Generate a batch of telemetry readings and any hazards encountered.

    Args:
        initial_state: Starting spacecraft state (defaults to Mars departure)
        duration_s: How many seconds of telemetry to generate
        dt: Time step between readings
        hazard_probability: Probability of hazard per time step
        mission_start_time: Wall-clock start time for timestamps

    Returns:
        (telemetry_rows, hazard_rows, final_state)
    """
    if initial_state is None:
        initial_state = create_initial_state()

    if mission_start_time is None:
        mission_start_time = datetime.now(timezone.utc)

    state = initial_state
    telemetry_rows = []
    hazard_rows = []

    earth = BODIES["earth"]
    elapsed = 0.0

    while elapsed < duration_s:
        # Propagate physics
        state = propagate_state(state, dt)
        elapsed += dt

        # Sensor readings with noise
        noisy = add_sensor_noise(state)

        # Communication delay
        earth_pos = earth.position_at(state.timestamp_s)
        delay = communication_delay(state.position, earth_pos)

        # Timestamp
        ts = mission_start_time + timedelta(seconds=elapsed)

        # Occasional engine degradation
        engine_status = state.engine_status
        if random.random() < 0.0001:
            engine_status = random.choice(["nominal", "nominal", "degraded"])
            state = SpacecraftState(
                position=state.position,
                velocity=state.velocity,
                fuel_remaining_kg=state.fuel_remaining_kg,
                hull_integrity=state.hull_integrity,
                engine_status=engine_status,
                timestamp_s=state.timestamp_s,
            )

        # Build telemetry row
        telemetry_rows.append({
            "telemetry_id": str(uuid.uuid4()),
            "timestamp": ts.isoformat(),
            "position_x": noisy["position_x"],
            "position_y": noisy["position_y"],
            "position_z": noisy["position_z"],
            "velocity_x": noisy["velocity_x"],
            "velocity_y": noisy["velocity_y"],
            "velocity_z": noisy["velocity_z"],
            "fuel_remaining_kg": state.fuel_remaining_kg,
            "hull_integrity": state.hull_integrity,
            "engine_status": engine_status,
            "communication_delay_s": delay,
            "ingestion_timestamp": (ts + timedelta(seconds=delay)).isoformat(),
        })

        # Random hazard generation
        if random.random() < hazard_probability:
            hazard = generate_hazard(state, state.timestamp_s)

            # Calculate risk
            future_states = [state]
            temp = state
            for _ in range(60):
                temp = propagate_state(temp, 60.0)
                future_states.append(temp)

            min_dist, approach_time = closest_approach(future_states, hazard)
            risk = collision_risk(min_dist, hazard.radius_km)

            hazard_rows.append({
                "hazard_id": hazard.hazard_id,
                "detected_at": ts.isoformat(),
                "hazard_type": hazard.hazard_type,
                "position_x": hazard.position.x,
                "position_y": hazard.position.y,
                "position_z": hazard.position.z,
                "velocity_x": hazard.velocity.x,
                "velocity_y": hazard.velocity.y,
                "velocity_z": hazard.velocity.z,
                "radius_km": hazard.radius_km,
                "risk_score": risk,
                "closest_approach_time": (
                    mission_start_time + timedelta(seconds=approach_time)
                ).isoformat(),
                "closest_approach_km": min_dist,
                "time_window_start": ts.isoformat(),
                "time_window_end": (ts + timedelta(hours=2)).isoformat(),
                "status": "active",
            })

    return telemetry_rows, hazard_rows, state


def generate_candidate_maneuvers(
    state: SpacecraftState,
    num_candidates: int = 5,
) -> list[dict]:
    """Generate ranked candidate maneuver options for the current state."""
    candidates = []

    for i in range(num_candidates):
        # Random burn direction biased toward Earth
        earth_dir = (BODIES["earth"].position_at(state.timestamp_s) - state.position).normalized()

        # Add randomness
        burn_vec = Vector3(
            earth_dir.x + random.gauss(0, 0.3),
            earth_dir.y + random.gauss(0, 0.3),
            earth_dir.z + random.gauss(0, 0.1),
        ).normalized()

        # Random burn magnitude
        burn_mag = random.uniform(0.0001, 0.001)  # km/s^2
        burn_duration = random.uniform(10, 120)  # seconds
        delta_v = burn_mag * burn_duration
        fuel_cost = random.uniform(5, 50)

        candidates.append({
            "maneuver_id": str(uuid.uuid4()),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "burn_vector_x": burn_vec.x * burn_mag,
            "burn_vector_y": burn_vec.y * burn_mag,
            "burn_vector_z": burn_vec.z * burn_mag,
            "burn_duration_s": burn_duration,
            "delta_v": delta_v,
            "fuel_cost_kg": fuel_cost,
            "risk_reduction_score": random.uniform(0.1, 0.9),
            "feasibility_score": random.uniform(0.3, 1.0),
            "ranking": i + 1,
            "status": "proposed",
        })

    # Sort by a composite score
    candidates.sort(
        key=lambda c: c["feasibility_score"] * 0.4 + c["risk_reduction_score"] * 0.6,
        reverse=True,
    )
    for i, c in enumerate(candidates):
        c["ranking"] = i + 1

    return candidates
