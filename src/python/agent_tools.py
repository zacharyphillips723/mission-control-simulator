"""
Mission Control — Agent Tools

Wraps the deterministic physics engine and model serving endpoints as callable
tools for Mosaic AI agents. These are the tools agents call — NOT the agents themselves.

Tool categories:
  1. Physics tools (deterministic — orbital mechanics, collision detection)
  2. Model tools (ML inference via Model Serving endpoints)
  3. Data tools (Lakebase/Delta queries for current state)
"""

import json
import os
from datetime import datetime, timezone
from typing import Optional

import lakebase_client
import mlflow
import requests
from databricks.sdk import WorkspaceClient

CATALOG = os.environ.get("CATALOG", "mission_control_dev")
LAKEBASE_PROJECT_ID = os.environ.get("LAKEBASE_PROJECT_ID", "mission-control")

_lakebase_initialized = False


def _ensure_lakebase():
    """Initialize Lakebase connection if not already connected."""
    global _lakebase_initialized
    if not _lakebase_initialized:
        lakebase_client.init(LAKEBASE_PROJECT_ID)
        _lakebase_initialized = True


# ---------------------------------------------------------------------------
# Helper: Call Model Serving endpoint
# ---------------------------------------------------------------------------

def _call_endpoint(endpoint_name: str, payload: dict) -> dict:
    """Call a Databricks Model Serving endpoint."""
    w = WorkspaceClient()
    host = w.config.host.rstrip("/")
    token = w.config.token

    url = f"{host}/serving-endpoints/{endpoint_name}/invocations"
    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _query_sql(query: str) -> list[dict]:
    """Execute a SQL query and return results."""
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active Spark session")
    rows = spark.sql(query).collect()
    return [row.asDict() for row in rows]


# ---------------------------------------------------------------------------
# Physics Tools (Deterministic)
# ---------------------------------------------------------------------------

def tool_propagate_trajectory(
    position_x: float, position_y: float, position_z: float,
    velocity_x: float, velocity_y: float, velocity_z: float,
    fuel_remaining_kg: float, duration_s: float, dt: float = 60.0,
    timestamp_s: float = 0.0,
) -> str:
    """
    Propagate spacecraft trajectory forward using N-body orbital mechanics.
    Returns predicted positions at each time step.

    Args:
        position_x/y/z: Current position in km (heliocentric)
        velocity_x/y/z: Current velocity in km/s
        fuel_remaining_kg: Remaining fuel
        duration_s: How far ahead to predict (seconds)
        dt: Time step between predictions (seconds)
        timestamp_s: Current simulation elapsed time

    Returns:
        JSON with predicted trajectory points
    """
    from physics_engine import SpacecraftState, Vector3, propagate_trajectory

    state = SpacecraftState(
        position=Vector3(position_x, position_y, position_z),
        velocity=Vector3(velocity_x, velocity_y, velocity_z),
        fuel_remaining_kg=fuel_remaining_kg,
        timestamp_s=timestamp_s,
    )

    states = propagate_trajectory(state, duration_s, dt)

    trajectory = [
        {
            "t_offset_s": s.timestamp_s - timestamp_s,
            "position_x": s.position.x,
            "position_y": s.position.y,
            "position_z": s.position.z,
            "speed_km_s": s.speed,
        }
        for s in states[::max(1, len(states) // 50)]  # Sample ~50 points
    ]

    return json.dumps({
        "trajectory_points": len(trajectory),
        "duration_s": duration_s,
        "final_position": trajectory[-1] if trajectory else None,
        "trajectory": trajectory,
    })


def tool_calculate_gravity_assist(
    spacecraft_vel_x: float, spacecraft_vel_y: float, spacecraft_vel_z: float,
    body_name: str, periapsis_km: float, body_timestamp_s: float,
) -> str:
    """
    Calculate velocity change from a gravity assist maneuver.

    Args:
        spacecraft_vel_x/y/z: Spacecraft velocity at encounter (km/s)
        body_name: Celestial body name (earth, mars, jupiter)
        periapsis_km: Closest approach distance to body center (km)
        body_timestamp_s: Simulation time of encounter

    Returns:
        JSON with delta-v vector and magnitude
    """
    from physics_engine import BODIES, Vector3, gravity_assist_delta_v

    body = BODIES.get(body_name)
    if not body:
        return json.dumps({"error": f"Unknown body: {body_name}"})

    body_pos = body.position_at(body_timestamp_s)
    # Approximate body velocity from orbital motion
    body_pos_dt = body.position_at(body_timestamp_s + 1.0)
    body_vel = body_pos_dt - body_pos

    sc_vel = Vector3(spacecraft_vel_x, spacecraft_vel_y, spacecraft_vel_z)
    dv = gravity_assist_delta_v(sc_vel, body_vel, periapsis_km, body.gm)

    return json.dumps({
        "delta_v_x": dv.x,
        "delta_v_y": dv.y,
        "delta_v_z": dv.z,
        "delta_v_magnitude_km_s": dv.magnitude(),
        "body": body_name,
        "periapsis_km": periapsis_km,
    })


def tool_check_collision(
    spacecraft_pos_x: float, spacecraft_pos_y: float, spacecraft_pos_z: float,
    spacecraft_vel_x: float, spacecraft_vel_y: float, spacecraft_vel_z: float,
    hazard_pos_x: float, hazard_pos_y: float, hazard_pos_z: float,
    hazard_vel_x: float, hazard_vel_y: float, hazard_vel_z: float,
    hazard_radius_km: float, fuel_remaining_kg: float = 500.0,
    timestamp_s: float = 0.0, lookahead_s: float = 3600.0,
) -> str:
    """
    Check collision risk between spacecraft and a hazard object over a time window.

    Returns:
        JSON with minimum distance, closest approach time, and risk score (0-1)
    """
    from physics_engine import (
        Hazard, SpacecraftState, Vector3,
        closest_approach, collision_risk, propagate_trajectory,
    )

    state = SpacecraftState(
        position=Vector3(spacecraft_pos_x, spacecraft_pos_y, spacecraft_pos_z),
        velocity=Vector3(spacecraft_vel_x, spacecraft_vel_y, spacecraft_vel_z),
        fuel_remaining_kg=fuel_remaining_kg,
        timestamp_s=timestamp_s,
    )

    hazard = Hazard(
        hazard_id="check",
        hazard_type="unknown",
        position=Vector3(hazard_pos_x, hazard_pos_y, hazard_pos_z),
        velocity=Vector3(hazard_vel_x, hazard_vel_y, hazard_vel_z),
        radius_km=hazard_radius_km,
    )

    states = propagate_trajectory(state, lookahead_s, dt=60.0)
    min_dist, approach_time = closest_approach(states, hazard)
    risk = collision_risk(min_dist, hazard_radius_km)

    return json.dumps({
        "minimum_distance_km": min_dist,
        "closest_approach_time_s": approach_time,
        "risk_score": risk,
        "hazard_radius_km": hazard_radius_km,
        "is_critical": risk > 0.7,
        "is_warning": risk > 0.3,
    })


def tool_calculate_communication_delay(
    spacecraft_pos_x: float, spacecraft_pos_y: float, spacecraft_pos_z: float,
    timestamp_s: float = 0.0,
) -> str:
    """
    Calculate current one-way communication delay between spacecraft and Earth.

    Returns:
        JSON with delay in seconds and minutes, plus round-trip time
    """
    from physics_engine import BODIES, Vector3, communication_delay

    earth_pos = BODIES["earth"].position_at(timestamp_s)
    sc_pos = Vector3(spacecraft_pos_x, spacecraft_pos_y, spacecraft_pos_z)
    delay = communication_delay(sc_pos, earth_pos)

    return json.dumps({
        "one_way_delay_s": delay,
        "one_way_delay_min": delay / 60,
        "round_trip_s": delay * 2,
        "round_trip_min": delay * 2 / 60,
        "distance_to_earth_km": (sc_pos - earth_pos).magnitude(),
    })


def tool_estimate_fuel_for_burn(
    delta_v_km_s: float,
    spacecraft_mass_kg: float = 5000.0,
) -> str:
    """
    Estimate fuel required for a given delta-v using the Tsiolkovsky equation.

    Args:
        delta_v_km_s: Required velocity change in km/s
        spacecraft_mass_kg: Current spacecraft mass (dry + fuel)

    Returns:
        JSON with estimated fuel consumption and mass ratio
    """
    from physics_engine import estimate_fuel_cost
    import math

    fuel = estimate_fuel_cost(delta_v_km_s, spacecraft_mass_kg)
    mass_ratio = math.exp(delta_v_km_s / 3.0)  # exhaust velocity ~3 km/s

    return json.dumps({
        "fuel_cost_kg": fuel,
        "mass_ratio": mass_ratio,
        "delta_v_km_s": delta_v_km_s,
    })


# ---------------------------------------------------------------------------
# Model Tools (ML Inference)
# ---------------------------------------------------------------------------

def tool_predict_trajectory(
    position_x: float, position_y: float, position_z: float,
    velocity_x: float, velocity_y: float, velocity_z: float,
    fuel_remaining_kg: float, communication_delay_s: float,
    endpoint_name: str = "mission-control-trajectory-dev",
) -> str:
    """
    Call the trajectory prediction ML model to predict future position.

    Returns:
        JSON with predicted future position (60 seconds ahead)
    """
    payload = {
        "dataframe_records": [{
            "pos_x": position_x, "pos_y": position_y, "pos_z": position_z,
            "vel_x": velocity_x, "vel_y": velocity_y, "vel_z": velocity_z,
            "fuel": fuel_remaining_kg, "comm_delay": communication_delay_s,
        }]
    }

    result = _call_endpoint(endpoint_name, payload)
    return json.dumps({"prediction": result, "lookahead_seconds": 60})


def tool_score_hazard_risk(
    position_x: float, position_y: float, position_z: float,
    velocity_x: float, velocity_y: float, velocity_z: float,
    radius_km: float, closest_approach_km: float,
    endpoint_name: str = "mission-control-hazard-risk-dev",
) -> str:
    """
    Score hazard collision risk using the ML model.

    Returns:
        JSON with predicted risk score (0-1)
    """
    payload = {
        "dataframe_records": [{
            "position_x": position_x, "position_y": position_y, "position_z": position_z,
            "velocity_x": velocity_x, "velocity_y": velocity_y, "velocity_z": velocity_z,
            "radius_km": radius_km, "closest_approach_km": closest_approach_km,
        }]
    }

    result = _call_endpoint(endpoint_name, payload)
    return json.dumps({"risk_prediction": result})


def tool_rank_maneuver(
    burn_vector_x: float, burn_vector_y: float, burn_vector_z: float,
    burn_duration_s: float, delta_v: float, fuel_cost_kg: float,
    risk_reduction_score: float,
    endpoint_name: str = "mission-control-maneuver-rank-dev",
) -> str:
    """
    Score a candidate maneuver using the ML ranking model.

    Returns:
        JSON with composite quality score
    """
    payload = {
        "dataframe_records": [{
            "burn_vector_x": burn_vector_x, "burn_vector_y": burn_vector_y,
            "burn_vector_z": burn_vector_z, "burn_duration_s": burn_duration_s,
            "delta_v": delta_v, "fuel_cost_kg": fuel_cost_kg,
            "risk_reduction_score": risk_reduction_score,
        }]
    }

    result = _call_endpoint(endpoint_name, payload)
    return json.dumps({"ranking_score": result})


def tool_get_optimal_timing(
    speed: float, fuel_remaining: float, distance_to_earth: float,
    comm_delay_s: float, hazard_risk: float, maneuver_urgency: float,
    endpoint_name: str = "mission-control-delay-policy-dev",
) -> str:
    """
    Get optimal command timing offset accounting for communication delay.
    The delay-aware policy model recommends how far in advance to send commands.

    Returns:
        JSON with recommended timing offset in seconds
    """
    payload = {
        "dataframe_records": [{
            "speed": speed, "fuel_remaining": fuel_remaining,
            "distance_to_earth": distance_to_earth, "comm_delay_s": comm_delay_s,
            "hazard_risk": hazard_risk, "maneuver_urgency": maneuver_urgency,
        }]
    }

    result = _call_endpoint(endpoint_name, payload)
    return json.dumps({"timing_offset": result})


# ---------------------------------------------------------------------------
# Data Tools (State Queries)
# ---------------------------------------------------------------------------

def tool_get_current_state() -> str:
    """Get the current spacecraft state from Lakebase."""
    _ensure_lakebase()
    row = lakebase_client.fetch_one("SELECT * FROM mission_state WHERE state_id = 1")
    if row:
        # Convert non-serializable types
        return json.dumps({k: str(v) if isinstance(v, datetime) else v for k, v in row.items()})
    return json.dumps({"error": "No mission state found"})


def tool_get_active_hazards() -> str:
    """Get all currently active hazards from Lakebase."""
    _ensure_lakebase()
    rows = lakebase_client.fetch_all("SELECT * FROM active_hazards ORDER BY risk_score DESC")
    return json.dumps([
        {k: str(v) if isinstance(v, datetime) else v for k, v in row.items()}
        for row in rows
    ])


def tool_get_candidate_maneuvers(top_n: int = 5) -> str:
    """Get top-ranked candidate maneuvers."""
    rows = _query_sql(f"""
        SELECT * FROM `{CATALOG}`.navigation.candidate_maneuvers
        WHERE status = 'proposed'
        ORDER BY ranking ASC
        LIMIT {top_n}
    """)
    return json.dumps([
        {k: str(v) if isinstance(v, datetime) else v for k, v in row.items()}
        for row in rows
    ])


def tool_get_command_queue() -> str:
    """Get pending commands from Lakebase."""
    _ensure_lakebase()
    rows = lakebase_client.fetch_all(
        "SELECT * FROM command_queue WHERE status IN ('pending', 'approved', 'transmitting') ORDER BY priority ASC, created_at ASC"
    )
    return json.dumps([
        {k: str(v) if isinstance(v, datetime) else v for k, v in row.items()}
        for row in rows
    ])


def tool_get_onboard_predictions(limit: int = 5) -> str:
    """
    Get the spacecraft's recent onboard trajectory predictions from Lakebase.
    These are predictions the ship made autonomously during communication delays
    using either the ML model or physics fallback. Check these BEFORE proposing
    a maneuver to avoid conflicting with the ship's autonomous corrections.

    Returns:
        JSON with recent predictions, assessments, actions taken, and accuracy
    """
    _ensure_lakebase()
    rows = lakebase_client.fetch_all(
        f"SELECT prediction_id, simulation_time_s, "
        f"  current_pos_x, current_pos_y, current_pos_z, "
        f"  predicted_pos_x, predicted_pos_y, predicted_pos_z, "
        f"  prediction_horizon_s, prediction_source, "
        f"  assessment, action_taken, correction_dv, "
        f"  actual_pos_x, actual_pos_y, actual_pos_z, "
        f"  prediction_error_km "
        f"FROM onboard_predictions "
        f"ORDER BY simulation_time_s DESC LIMIT {min(limit, 20)}"
    )
    # Compute summary stats
    errors = [float(r["prediction_error_km"]) for r in rows if r.get("prediction_error_km") is not None]
    corrections = [r for r in rows if r.get("action_taken") not in (None, "none")]
    ml_count = sum(1 for r in rows if r.get("prediction_source") == "model_serving")

    summary = {
        "total_predictions": len(rows),
        "ml_model_predictions": ml_count,
        "physics_fallback_predictions": len(rows) - ml_count,
        "predictions_with_accuracy": len(errors),
        "avg_prediction_error_km": sum(errors) / len(errors) if errors else None,
        "max_prediction_error_km": max(errors) if errors else None,
        "autonomous_corrections_applied": len(corrections),
        "latest_assessment": rows[0].get("assessment") if rows else None,
    }

    return json.dumps({
        "summary": summary,
        "predictions": [
            {k: str(v) if isinstance(v, datetime) else v for k, v in row.items()}
            for row in rows
        ],
    })
