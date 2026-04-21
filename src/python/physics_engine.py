"""
Mission Control — Orbital Mechanics & Physics Engine

Deterministic physics calculations for spacecraft trajectory, gravity assists,
and collision detection. These are NOT handled by LLMs — they require exact math.

Coordinate system: Heliocentric, units in km and km/s.
"""

import math
from dataclasses import dataclass, field
from typing import Optional


# Gravitational constant * mass for key bodies (km^3/s^2)
GM = {
    "sun": 1.32712440018e11,
    "earth": 3.986004418e5,
    "mars": 4.282837e4,
    "jupiter": 1.26686534e8,
    "moon": 4.9048695e3,
}

# Mean orbital radii in km (simplified circular orbits for demo)
ORBIT_RADIUS = {
    "earth": 1.496e8,    # 1 AU
    "mars": 2.279e8,     # 1.524 AU
    "jupiter": 7.785e8,  # 5.203 AU
}

# Orbital periods in seconds
ORBIT_PERIOD = {
    "earth": 365.25 * 86400,
    "mars": 687.0 * 86400,
    "jupiter": 4332.59 * 86400,
}


@dataclass
class Vector3:
    """3D vector for positions and velocities."""
    x: float = 0.0
    y: float = 0.0
    z: float = 0.0

    def magnitude(self) -> float:
        return math.sqrt(self.x**2 + self.y**2 + self.z**2)

    def __add__(self, other: "Vector3") -> "Vector3":
        return Vector3(self.x + other.x, self.y + other.y, self.z + other.z)

    def __sub__(self, other: "Vector3") -> "Vector3":
        return Vector3(self.x - other.x, self.y - other.y, self.z - other.z)

    def __mul__(self, scalar: float) -> "Vector3":
        return Vector3(self.x * scalar, self.y * scalar, self.z * scalar)

    def __rmul__(self, scalar: float) -> "Vector3":
        return self.__mul__(scalar)

    def dot(self, other: "Vector3") -> float:
        return self.x * other.x + self.y * other.y + self.z * other.z

    def normalized(self) -> "Vector3":
        mag = self.magnitude()
        if mag == 0:
            return Vector3(0, 0, 0)
        return Vector3(self.x / mag, self.y / mag, self.z / mag)

    def to_dict(self) -> dict:
        return {"x": self.x, "y": self.y, "z": self.z}


@dataclass
class SpacecraftState:
    """Complete spacecraft state at a point in time."""
    position: Vector3
    velocity: Vector3
    fuel_remaining_kg: float
    hull_integrity: float = 100.0
    engine_status: str = "nominal"
    timestamp_s: float = 0.0  # simulation seconds elapsed

    @property
    def speed(self) -> float:
        return self.velocity.magnitude()

    @property
    def distance_from_sun(self) -> float:
        return self.position.magnitude()


@dataclass
class CelestialBody:
    """A body with gravitational influence."""
    name: str
    gm: float  # G * mass in km^3/s^2
    orbit_radius_km: float
    orbit_period_s: float
    radius_km: float = 0.0

    def position_at(self, t_seconds: float) -> Vector3:
        """Get body position at simulation time t (simplified circular orbit in XY plane)."""
        angular_velocity = 2 * math.pi / self.orbit_period_s
        angle = angular_velocity * t_seconds
        return Vector3(
            self.orbit_radius_km * math.cos(angle),
            self.orbit_radius_km * math.sin(angle),
            0.0,
        )


@dataclass
class Hazard:
    """A hazard object (asteroid, debris, etc.)."""
    hazard_id: str
    hazard_type: str
    position: Vector3
    velocity: Vector3
    radius_km: float


# Pre-defined celestial bodies
BODIES = {
    "earth": CelestialBody("earth", GM["earth"], ORBIT_RADIUS["earth"], ORBIT_PERIOD["earth"], 6371.0),
    "mars": CelestialBody("mars", GM["mars"], ORBIT_RADIUS["mars"], ORBIT_PERIOD["mars"], 3389.5),
    "jupiter": CelestialBody("jupiter", GM["jupiter"], ORBIT_RADIUS["jupiter"], ORBIT_PERIOD["jupiter"], 69911.0),
}


def gravitational_acceleration(
    spacecraft_pos: Vector3,
    body_pos: Vector3,
    body_gm: float,
) -> Vector3:
    """Calculate gravitational acceleration from a body on the spacecraft."""
    r_vec = body_pos - spacecraft_pos
    r_mag = r_vec.magnitude()
    if r_mag < 1.0:  # avoid singularity
        r_mag = 1.0
    accel_magnitude = body_gm / (r_mag**2)
    return r_vec.normalized() * accel_magnitude


def total_acceleration(
    spacecraft_pos: Vector3,
    t_seconds: float,
    thrust: Optional[Vector3] = None,
) -> Vector3:
    """Calculate total acceleration on spacecraft from all gravitational sources + thrust."""
    # Sun gravity (at origin)
    sun_pos = Vector3(0, 0, 0)
    accel = gravitational_acceleration(spacecraft_pos, sun_pos, GM["sun"])

    # Planetary gravity
    for body in BODIES.values():
        body_pos = body.position_at(t_seconds)
        accel = accel + gravitational_acceleration(spacecraft_pos, body_pos, body.gm)

    # Thrust acceleration (if engines firing)
    if thrust:
        accel = accel + thrust

    return accel


def propagate_state(
    state: SpacecraftState,
    dt: float,
    thrust: Optional[Vector3] = None,
    fuel_burn_rate_kg_per_s: float = 0.0,
) -> SpacecraftState:
    """
    Advance spacecraft state by dt seconds using velocity Verlet integration.

    Args:
        state: Current spacecraft state
        dt: Time step in seconds
        thrust: Optional thrust acceleration vector (km/s^2)
        fuel_burn_rate_kg_per_s: Fuel consumption rate during thrust
    """
    # Check if we have fuel for thrust
    effective_thrust = thrust
    effective_burn_rate = fuel_burn_rate_kg_per_s
    if thrust and state.fuel_remaining_kg <= 0:
        effective_thrust = None
        effective_burn_rate = 0.0

    # Velocity Verlet integration
    a1 = total_acceleration(state.position, state.timestamp_s, effective_thrust)

    new_pos = state.position + state.velocity * dt + a1 * (0.5 * dt * dt)
    new_time = state.timestamp_s + dt

    a2 = total_acceleration(new_pos, new_time, effective_thrust)
    new_vel = state.velocity + (a1 + a2) * (0.5 * dt)

    new_fuel = max(0.0, state.fuel_remaining_kg - effective_burn_rate * dt)

    return SpacecraftState(
        position=new_pos,
        velocity=new_vel,
        fuel_remaining_kg=new_fuel,
        hull_integrity=state.hull_integrity,
        engine_status=state.engine_status,
        timestamp_s=new_time,
    )


def propagate_trajectory(
    state: SpacecraftState,
    duration_s: float,
    dt: float = 60.0,
    thrust: Optional[Vector3] = None,
    fuel_burn_rate_kg_per_s: float = 0.0,
) -> list[SpacecraftState]:
    """Propagate trajectory forward, returning states at each time step."""
    states = [state]
    current = state
    elapsed = 0.0

    while elapsed < duration_s:
        step = min(dt, duration_s - elapsed)
        current = propagate_state(current, step, thrust, fuel_burn_rate_kg_per_s)
        states.append(current)
        elapsed += step

    return states


def compute_delta_v(burn_vector: Vector3, burn_duration_s: float) -> float:
    """Compute total delta-v from a burn."""
    return burn_vector.magnitude() * burn_duration_s


def estimate_fuel_cost(
    delta_v: float,
    spacecraft_mass_kg: float = 5000.0,
    exhaust_velocity_km_s: float = 3.0,
) -> float:
    """Estimate fuel consumption using the Tsiolkovsky rocket equation."""
    mass_ratio = math.exp(delta_v / exhaust_velocity_km_s)
    fuel_kg = spacecraft_mass_kg * (mass_ratio - 1)
    return fuel_kg


def closest_approach(
    spacecraft_states: list[SpacecraftState],
    hazard: Hazard,
    dt: float = 60.0,
) -> tuple[float, float]:
    """
    Find closest approach distance and time between spacecraft trajectory and a hazard.

    Returns:
        (min_distance_km, time_of_closest_approach_s)
    """
    min_dist = float("inf")
    min_time = 0.0

    for state in spacecraft_states:
        # Propagate hazard position
        t = state.timestamp_s
        hazard_pos = hazard.position + hazard.velocity * t
        dist = (state.position - hazard_pos).magnitude()

        if dist < min_dist:
            min_dist = dist
            min_time = t

    return min_dist, min_time


def collision_risk(
    min_distance_km: float,
    hazard_radius_km: float,
    safety_margin_km: float = 100.0,
) -> float:
    """
    Calculate collision risk score (0-1) based on closest approach distance.
    Uses an exponential decay model.
    """
    danger_zone = hazard_radius_km + safety_margin_km
    if min_distance_km <= hazard_radius_km:
        return 1.0
    if min_distance_km >= danger_zone * 10:
        return 0.0
    return math.exp(-((min_distance_km - hazard_radius_km) / danger_zone))


def communication_delay(spacecraft_pos: Vector3, earth_pos: Vector3) -> float:
    """Calculate one-way light-speed communication delay in seconds."""
    distance_km = (spacecraft_pos - earth_pos).magnitude()
    speed_of_light_km_s = 299792.458
    return distance_km / speed_of_light_km_s


def gravity_assist_delta_v(
    spacecraft_velocity: Vector3,
    body_velocity: Vector3,
    periapsis_km: float,
    body_gm: float,
) -> Vector3:
    """
    Estimate velocity change from a gravity assist (patched conic approximation).

    The spacecraft gains/loses speed relative to the Sun by leveraging
    the body's orbital velocity.
    """
    # Relative velocity at infinity
    v_inf = spacecraft_velocity - body_velocity
    v_inf_mag = v_inf.magnitude()

    if v_inf_mag < 0.001:
        return Vector3(0, 0, 0)

    # Turn angle
    turn_angle = 2 * math.asin(1.0 / (1.0 + periapsis_km * v_inf_mag**2 / body_gm))

    # Rotate v_inf by turn angle in the orbital plane
    cos_a = math.cos(turn_angle)
    sin_a = math.sin(turn_angle)

    # Simplified 2D rotation in XY plane
    new_v_inf = Vector3(
        v_inf.x * cos_a - v_inf.y * sin_a,
        v_inf.x * sin_a + v_inf.y * cos_a,
        v_inf.z,
    )

    # Delta-v is the difference
    delta_v = (new_v_inf + body_velocity) - spacecraft_velocity
    return delta_v


def create_initial_state(
    departure_body: str = "mars",
    t_start: float = 0.0,
    fuel_kg: float = 500.0,
) -> SpacecraftState:
    """
    Create initial spacecraft state departing from Mars orbit toward Earth.
    Starts at Mars position with a slight velocity offset to begin return trajectory.
    """
    body = BODIES[departure_body]
    pos = body.position_at(t_start)

    # Initial velocity: Mars orbital velocity + departure burn toward inner solar system
    angular_vel = 2 * math.pi / body.orbit_period_s
    orbital_speed = angular_vel * body.orbit_radius_km

    # Tangential velocity (orbital) + small radial component toward Sun
    velocity = Vector3(
        -orbital_speed * math.sin(math.atan2(pos.y, pos.x)) - 2.0,  # slight inward push
        orbital_speed * math.cos(math.atan2(pos.y, pos.x)),
        0.0,
    )

    return SpacecraftState(
        position=pos,
        velocity=velocity,
        fuel_remaining_kg=fuel_kg,
        hull_integrity=100.0,
        engine_status="nominal",
        timestamp_s=t_start,
    )
