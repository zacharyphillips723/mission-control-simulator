"""
Mission Control — Hazard Simulation Engine

Generates realistic asteroid fields, meteor showers, debris fields, and solar flares
that the spacecraft must detect and evade during its Mars-to-Earth return trajectory.

Hazard types:
  1. Asteroids — Single rocky bodies on defined orbits. Predictable, avoidable with burns.
  2. Meteor Showers — Periodic particle streams from comet debris. Wide area, cumulative
     hull damage risk. Must be transited or waited out.
  3. Debris Fields — Clusters of small fragments from collisions. Moderate area, variable density.
  4. Solar Flares — Radiation bursts from the Sun. No physical collision, but radiation
     exposure and communication disruption risk.

The spacecraft's return trajectory (Mars → Earth) crosses the asteroid belt region
between 2.0 AU and 3.3 AU, making hazard encounters realistic and frequent.
"""

import math
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from physics_engine import BODIES, Hazard, SpacecraftState, Vector3


# ---------------------------------------------------------------------------
# Asteroid Belt Parameters
# ---------------------------------------------------------------------------

# The main asteroid belt lies between Mars (1.52 AU) and Jupiter (5.2 AU)
# with the densest region between 2.1 - 3.3 AU
AU_KM = 1.496e8  # 1 AU in km
BELT_INNER_KM = 2.1 * AU_KM
BELT_OUTER_KM = 3.3 * AU_KM

# Kirkwood gaps — orbital resonances with Jupiter that clear out asteroids
# These are semi-major axes where asteroid density is LOW
KIRKWOOD_GAPS_AU = [2.06, 2.50, 2.82, 2.96, 3.28]  # 4:1, 3:1, 5:2, 7:3, 2:1 resonances
KIRKWOOD_WIDTH_AU = 0.05  # Half-width of each gap


@dataclass
class AsteroidBody:
    """A single asteroid with orbital elements."""
    asteroid_id: str
    name: str
    semi_major_axis_km: float
    eccentricity: float
    inclination_deg: float
    mass_kg: float
    radius_km: float
    rotation_period_h: float
    spectral_type: str  # C-type (carbonaceous), S-type (siliceous), M-type (metallic)
    is_potentially_hazardous: bool

    def position_at(self, t_seconds: float) -> Vector3:
        """Simplified position on elliptical orbit in the ecliptic plane."""
        period_s = 2 * math.pi * math.sqrt(self.semi_major_axis_km**3 / 1.32712440018e11)
        mean_anomaly = (2 * math.pi * t_seconds / period_s) % (2 * math.pi)
        # Simplified: use mean anomaly as true anomaly (valid for low eccentricity)
        r = self.semi_major_axis_km * (1 - self.eccentricity**2) / (1 + self.eccentricity * math.cos(mean_anomaly))
        incl_rad = math.radians(self.inclination_deg)
        x = r * math.cos(mean_anomaly)
        y = r * math.sin(mean_anomaly) * math.cos(incl_rad)
        z = r * math.sin(mean_anomaly) * math.sin(incl_rad)
        return Vector3(x, y, z)

    def velocity_at(self, t_seconds: float, dt: float = 1.0) -> Vector3:
        """Numerical velocity via finite difference."""
        p1 = self.position_at(t_seconds)
        p2 = self.position_at(t_seconds + dt)
        return Vector3((p2.x - p1.x) / dt, (p2.y - p1.y) / dt, (p2.z - p1.z) / dt)

    def to_hazard(self, t_seconds: float) -> Hazard:
        """Convert to a Hazard object at a given time."""
        pos = self.position_at(t_seconds)
        vel = self.velocity_at(t_seconds)
        return Hazard(
            hazard_id=self.asteroid_id,
            hazard_type="asteroid",
            position=pos,
            velocity=vel,
            radius_km=self.radius_km,
        )


@dataclass
class MeteorShower:
    """A periodic meteor shower from cometary debris."""
    shower_id: str
    name: str
    parent_comet: str
    radiant_position: Vector3  # Direction meteors appear to come from
    stream_width_km: float  # Cross-sectional width of the stream
    particle_density: float  # Particles per km^3
    avg_particle_mass_kg: float
    avg_particle_speed_km_s: float
    peak_date_offset_days: float  # Days from simulation start to shower peak
    duration_days: float  # How long the shower lasts
    hull_damage_per_particle: float  # % hull integrity lost per impact

    def is_active(self, elapsed_days: float) -> bool:
        """Check if this shower is currently active."""
        half_dur = self.duration_days / 2
        return abs(elapsed_days - self.peak_date_offset_days) <= half_dur

    def intensity_at(self, elapsed_days: float) -> float:
        """Shower intensity as fraction of peak (Gaussian profile)."""
        if not self.is_active(elapsed_days):
            return 0.0
        sigma = self.duration_days / 4
        offset = elapsed_days - self.peak_date_offset_days
        return math.exp(-(offset**2) / (2 * sigma**2))

    def spacecraft_exposure(
        self,
        spacecraft_pos: Vector3,
        spacecraft_vel: Vector3,
        elapsed_days: float,
    ) -> dict:
        """
        Calculate spacecraft exposure to this meteor shower.

        Returns dict with:
            - is_in_stream: whether spacecraft is within the stream
            - intensity: current shower intensity (0-1)
            - impact_rate: estimated impacts per hour
            - hull_damage_rate: % hull damage per hour
            - transit_time_hours: estimated time to cross the stream
        """
        intensity = self.intensity_at(elapsed_days)
        if intensity == 0:
            return {"is_in_stream": False, "intensity": 0}

        # Check if spacecraft is near the shower's stream
        # Simplified: stream is a cylinder along the radiant direction
        radiant_norm = self.radiant_position.normalized()
        to_spacecraft = spacecraft_pos  # relative to stream center at origin
        # Project onto radiant direction
        along = to_spacecraft.dot(radiant_norm)
        perp_vec = to_spacecraft - radiant_norm * along
        perp_dist = perp_vec.magnitude()

        is_in_stream = perp_dist < self.stream_width_km

        if not is_in_stream:
            return {"is_in_stream": False, "intensity": intensity, "distance_to_stream_km": perp_dist}

        # Effective density at current intensity
        eff_density = self.particle_density * intensity

        # Impact rate: cross-section * relative velocity * density
        spacecraft_cross_section_km2 = 0.001  # ~1000 m^2
        rel_speed = self.avg_particle_speed_km_s + spacecraft_vel.magnitude()
        impact_rate_per_s = spacecraft_cross_section_km2 * rel_speed * eff_density
        impact_rate_per_hour = impact_rate_per_s * 3600

        hull_damage_per_hour = impact_rate_per_hour * self.hull_damage_per_particle

        # Transit time estimate
        transit_km = self.stream_width_km * 2  # diameter
        transit_speed = max(spacecraft_vel.magnitude(), 1.0)
        transit_hours = transit_km / (transit_speed * 3600)

        return {
            "is_in_stream": True,
            "intensity": intensity,
            "impact_rate_per_hour": impact_rate_per_hour,
            "hull_damage_per_hour": hull_damage_per_hour,
            "transit_time_hours": transit_hours,
            "total_hull_damage": hull_damage_per_hour * transit_hours,
        }


@dataclass
class DebrisField:
    """A field of debris from a collision or breakup event."""
    field_id: str
    origin_event: str  # Description of what caused the debris
    center_position: Vector3
    center_velocity: Vector3
    field_radius_km: float
    expansion_rate_km_s: float  # How fast the field is expanding
    particle_count: int
    avg_particle_radius_km: float
    created_at_s: float  # Simulation time when field was created

    def current_radius(self, t_seconds: float) -> float:
        """Field radius grows over time as debris disperses."""
        age = t_seconds - self.created_at_s
        return self.field_radius_km + self.expansion_rate_km_s * age

    def density_at(self, t_seconds: float) -> float:
        """Particle density decreases as field expands (volume increases as r^3)."""
        r = self.current_radius(t_seconds)
        volume = (4/3) * math.pi * r**3
        return self.particle_count / max(volume, 1.0)

    def center_at(self, t_seconds: float) -> Vector3:
        """Field center moves with initial velocity."""
        dt = t_seconds - self.created_at_s
        return self.center_position + self.center_velocity * dt

    def to_hazard(self, t_seconds: float) -> Hazard:
        return Hazard(
            hazard_id=self.field_id,
            hazard_type="debris_field",
            position=self.center_at(t_seconds),
            velocity=self.center_velocity,
            radius_km=self.current_radius(t_seconds),
        )


@dataclass
class SolarFlare:
    """A solar flare / coronal mass ejection event."""
    flare_id: str
    classification: str  # C, M, X class
    origin_time_s: float  # Simulation time of eruption
    speed_km_s: float  # CME propagation speed
    angular_width_deg: float  # How wide the CME cone is
    direction: Vector3  # Direction of CME propagation (from Sun)
    radiation_intensity: float  # Relative radiation intensity
    duration_hours: float  # How long the radiation persists

    def wavefront_radius(self, t_seconds: float) -> float:
        """Distance the CME wavefront has traveled from the Sun."""
        dt = t_seconds - self.origin_time_s
        if dt < 0:
            return 0.0
        return self.speed_km_s * dt

    def is_spacecraft_exposed(
        self,
        spacecraft_pos: Vector3,
        t_seconds: float,
    ) -> dict:
        """Check if spacecraft is within the CME cone and wavefront has reached it."""
        dt = t_seconds - self.origin_time_s
        if dt < 0 or dt > self.duration_hours * 3600:
            return {"exposed": False}

        wavefront = self.wavefront_radius(t_seconds)
        sc_dist = spacecraft_pos.magnitude()  # distance from Sun

        # Has wavefront reached the spacecraft?
        if wavefront < sc_dist * 0.9:
            return {"exposed": False, "wavefront_eta_s": (sc_dist - wavefront) / self.speed_km_s}

        # Is spacecraft within the angular cone?
        sc_dir = spacecraft_pos.normalized()
        flare_dir = self.direction.normalized()
        cos_angle = sc_dir.dot(flare_dir)
        angle_deg = math.degrees(math.acos(max(-1, min(1, cos_angle))))

        if angle_deg > self.angular_width_deg / 2:
            return {"exposed": False, "angle_offset_deg": angle_deg}

        # Radiation intensity decreases with distance squared
        intensity_at_sc = self.radiation_intensity / max((sc_dist / AU_KM)**2, 0.01)

        return {
            "exposed": True,
            "radiation_intensity": intensity_at_sc,
            "classification": self.classification,
            "communication_disrupted": intensity_at_sc > 0.5,
            "hull_damage_rate_per_hour": intensity_at_sc * 0.1,
            "remaining_duration_hours": max(0, self.duration_hours - dt / 3600),
        }


# ---------------------------------------------------------------------------
# Hazard Generators
# ---------------------------------------------------------------------------

def generate_asteroid_field(
    num_asteroids: int = 50,
    seed: int = 42,
) -> list[AsteroidBody]:
    """Generate a realistic distribution of asteroids in the main belt."""
    rng = random.Random(seed)
    asteroids = []

    spectral_types = ["C", "C", "C", "S", "S", "M"]  # C-type most common
    names_prefix = ["Ceres", "Pallas", "Vesta", "Hygiea", "Davida", "Interamnia",
                    "Europa", "Sylvia", "Cybele", "Eunomia"]

    for i in range(num_asteroids):
        # Semi-major axis: weighted toward 2.5-2.8 AU (most dense region)
        sma_au = rng.gauss(2.7, 0.4)
        sma_au = max(2.0, min(3.5, sma_au))

        # Avoid Kirkwood gaps
        for gap in KIRKWOOD_GAPS_AU:
            if abs(sma_au - gap) < KIRKWOOD_WIDTH_AU:
                sma_au += KIRKWOOD_WIDTH_AU * 2 * (1 if rng.random() > 0.5 else -1)

        # Larger asteroids are rarer
        if rng.random() < 0.05:
            radius = rng.uniform(50, 500)   # Large (Ceres-class)
        elif rng.random() < 0.2:
            radius = rng.uniform(5, 50)     # Medium
        else:
            radius = rng.uniform(0.1, 5)    # Small

        asteroids.append(AsteroidBody(
            asteroid_id=str(uuid.uuid4()),
            name=f"{rng.choice(names_prefix)}-{i+1:03d}",
            semi_major_axis_km=sma_au * AU_KM,
            eccentricity=rng.uniform(0.0, 0.3),
            inclination_deg=rng.gauss(7, 5),  # Most belt asteroids have low inclination
            mass_kg=rng.uniform(1e10, 1e18) * (radius / 10)**3,
            radius_km=radius,
            rotation_period_h=rng.uniform(2, 24),
            spectral_type=rng.choice(spectral_types),
            is_potentially_hazardous=radius > 50 and rng.random() < 0.3,
        ))

    return asteroids


def generate_meteor_showers(
    mission_duration_days: float = 180,
    seed: int = 42,
) -> list[MeteorShower]:
    """
    Generate meteor showers the spacecraft may encounter during transit.
    Based on real shower patterns but placed along the spacecraft's path.
    """
    rng = random.Random(seed)

    # Major showers (inspired by real ones but at interplanetary scale)
    showers = [
        MeteorShower(
            shower_id=str(uuid.uuid4()),
            name="Persei Stream",
            parent_comet="Comet Swift-Tuttle",
            radiant_position=Vector3(1.5 * AU_KM, 0.8 * AU_KM, 0.1 * AU_KM),
            stream_width_km=rng.uniform(5e5, 2e6),
            particle_density=rng.uniform(1e-15, 1e-13),
            avg_particle_mass_kg=rng.uniform(0.001, 0.1),
            avg_particle_speed_km_s=rng.uniform(30, 60),
            peak_date_offset_days=rng.uniform(20, 50),
            duration_days=rng.uniform(5, 15),
            hull_damage_per_particle=0.001,
        ),
        MeteorShower(
            shower_id=str(uuid.uuid4()),
            name="Leonid Curtain",
            parent_comet="Comet Tempel-Tuttle",
            radiant_position=Vector3(-1.0 * AU_KM, 1.8 * AU_KM, -0.05 * AU_KM),
            stream_width_km=rng.uniform(3e5, 1e6),
            particle_density=rng.uniform(1e-14, 1e-12),  # Denser stream
            avg_particle_mass_kg=rng.uniform(0.01, 1.0),
            avg_particle_speed_km_s=rng.uniform(50, 72),  # Fast
            peak_date_offset_days=rng.uniform(60, 90),
            duration_days=rng.uniform(3, 8),
            hull_damage_per_particle=0.003,
        ),
        MeteorShower(
            shower_id=str(uuid.uuid4()),
            name="Geminid Wall",
            parent_comet="Asteroid 3200 Phaethon",
            radiant_position=Vector3(0.5 * AU_KM, -1.2 * AU_KM, 0.2 * AU_KM),
            stream_width_km=rng.uniform(8e5, 3e6),
            particle_density=rng.uniform(1e-14, 5e-13),
            avg_particle_mass_kg=rng.uniform(0.005, 0.5),
            avg_particle_speed_km_s=rng.uniform(25, 35),
            peak_date_offset_days=rng.uniform(100, 140),
            duration_days=rng.uniform(7, 20),
            hull_damage_per_particle=0.002,
        ),
    ]

    # Add some minor/random showers
    for i in range(rng.randint(2, 5)):
        angle = rng.uniform(0, 2 * math.pi)
        dist = rng.uniform(1.5, 3.0) * AU_KM
        showers.append(MeteorShower(
            shower_id=str(uuid.uuid4()),
            name=f"Stream-{chr(65+i)}{rng.randint(10,99)}",
            parent_comet=f"Unknown Comet {rng.randint(100,999)}",
            radiant_position=Vector3(dist * math.cos(angle), dist * math.sin(angle), rng.uniform(-0.2, 0.2) * AU_KM),
            stream_width_km=rng.uniform(1e5, 5e5),
            particle_density=rng.uniform(1e-16, 1e-14),
            avg_particle_mass_kg=rng.uniform(0.0001, 0.01),
            avg_particle_speed_km_s=rng.uniform(15, 50),
            peak_date_offset_days=rng.uniform(10, mission_duration_days - 10),
            duration_days=rng.uniform(2, 10),
            hull_damage_per_particle=0.0005,
        ))

    return showers


def generate_solar_flare(
    t_seconds: float,
    seed: Optional[int] = None,
) -> SolarFlare:
    """Generate a random solar flare event."""
    rng = random.Random(seed)

    # Class distribution: C (common), M (moderate), X (rare)
    classification = rng.choices(["C", "M", "X"], weights=[0.6, 0.3, 0.1], k=1)[0]
    intensity_map = {"C": rng.uniform(0.1, 0.5), "M": rng.uniform(0.5, 2.0), "X": rng.uniform(2.0, 10.0)}

    # CME direction: somewhat random but from Sun's surface
    angle = rng.uniform(0, 2 * math.pi)
    elev = rng.gauss(0, 0.3)

    return SolarFlare(
        flare_id=str(uuid.uuid4()),
        classification=classification,
        origin_time_s=t_seconds,
        speed_km_s=rng.uniform(300, 2500),  # Slow to fast CME
        angular_width_deg=rng.uniform(20, 120),
        direction=Vector3(math.cos(angle) * math.cos(elev), math.sin(angle) * math.cos(elev), math.sin(elev)),
        radiation_intensity=intensity_map[classification],
        duration_hours=rng.uniform(2, 48),
    )


def generate_debris_field(
    near_position: Vector3,
    t_seconds: float,
    seed: Optional[int] = None,
) -> DebrisField:
    """Generate a debris field near a given position (e.g., from a collision)."""
    rng = random.Random(seed)

    offset = Vector3(
        rng.gauss(0, 500),
        rng.gauss(0, 500),
        rng.gauss(0, 100),
    )

    return DebrisField(
        field_id=str(uuid.uuid4()),
        origin_event=rng.choice([
            "Asteroid collision",
            "Derelict spacecraft breakup",
            "Comet fragment disintegration",
            "Mining operation debris",
        ]),
        center_position=near_position + offset,
        center_velocity=Vector3(rng.gauss(0, 2), rng.gauss(0, 2), rng.gauss(0, 0.5)),
        field_radius_km=rng.uniform(50, 500),
        expansion_rate_km_s=rng.uniform(0.001, 0.05),
        particle_count=rng.randint(100, 10000),
        avg_particle_radius_km=rng.uniform(0.0001, 0.01),
        created_at_s=t_seconds - rng.uniform(0, 86400),  # Created up to 1 day ago
    )


# ---------------------------------------------------------------------------
# Evasion Planning
# ---------------------------------------------------------------------------

@dataclass
class EvasionManeuver:
    """A recommended evasion maneuver to avoid a hazard."""
    maneuver_id: str
    hazard_id: str
    hazard_type: str
    burn_vector: Vector3
    burn_duration_s: float
    delta_v: float
    fuel_cost_kg: float
    execute_by_s: float  # Latest time to execute this maneuver
    miss_distance_after_km: float  # Predicted miss distance if maneuver executed
    risk_before: float
    risk_after: float


def plan_asteroid_evasion(
    spacecraft_state: SpacecraftState,
    asteroid: AsteroidBody,
    t_seconds: float,
    safety_margin_km: float = 200.0,
) -> Optional[EvasionManeuver]:
    """
    Plan an evasion maneuver to avoid an asteroid.

    Strategy: Compute a burn perpendicular to the line between spacecraft and asteroid
    to deflect the trajectory enough for safe passage.
    """
    from physics_engine import closest_approach, collision_risk, estimate_fuel_cost, propagate_trajectory

    hazard = asteroid.to_hazard(t_seconds)
    states = propagate_trajectory(spacecraft_state, 7200, dt=60.0)  # 2 hour lookahead
    min_dist, approach_time = closest_approach(states, hazard)
    risk_before = collision_risk(min_dist, hazard.radius_km, safety_margin_km)

    if risk_before < 0.1:
        return None  # No evasion needed

    # Compute evasion vector: perpendicular to spacecraft-asteroid line
    to_asteroid = hazard.position - spacecraft_state.position
    to_asteroid_norm = to_asteroid.normalized()

    # Cross product with velocity to get perpendicular direction
    vel_norm = spacecraft_state.velocity.normalized()
    perp = Vector3(
        vel_norm.y * to_asteroid_norm.z - vel_norm.z * to_asteroid_norm.y,
        vel_norm.z * to_asteroid_norm.x - vel_norm.x * to_asteroid_norm.z,
        vel_norm.x * to_asteroid_norm.y - vel_norm.y * to_asteroid_norm.x,
    ).normalized()

    # Scale burn based on how close the approach is
    needed_deflection_km = (hazard.radius_km + safety_margin_km) - min_dist
    time_to_approach = approach_time - spacecraft_state.timestamp_s

    if time_to_approach <= 0:
        time_to_approach = 60  # emergency

    # Required perpendicular velocity change
    required_dv = max(0.001, needed_deflection_km / time_to_approach)
    burn_duration = min(120, max(5, required_dv / 0.001))  # Limit burn to 2 min
    burn_accel = required_dv / burn_duration

    burn_vector = perp * burn_accel
    delta_v = required_dv
    fuel_cost = estimate_fuel_cost(delta_v)

    return EvasionManeuver(
        maneuver_id=str(uuid.uuid4()),
        hazard_id=asteroid.asteroid_id,
        hazard_type="asteroid",
        burn_vector=burn_vector,
        burn_duration_s=burn_duration,
        delta_v=delta_v,
        fuel_cost_kg=fuel_cost,
        execute_by_s=spacecraft_state.timestamp_s + time_to_approach * 0.5,  # Execute by halfway
        miss_distance_after_km=hazard.radius_km + safety_margin_km,
        risk_before=risk_before,
        risk_after=max(0.01, risk_before * 0.1),
    )


def plan_shower_transit(
    spacecraft_state: SpacecraftState,
    shower: MeteorShower,
    elapsed_days: float,
) -> dict:
    """
    Assess options for dealing with a meteor shower:
    1. Transit through (take the damage)
    2. Accelerate to cross faster
    3. Decelerate/redirect to go around
    4. Wait for shower to pass
    """
    exposure = shower.spacecraft_exposure(
        spacecraft_state.position, spacecraft_state.velocity, elapsed_days,
    )

    if not exposure.get("is_in_stream", False):
        return {
            "situation": "not_in_stream",
            "shower": shower.name,
            "distance_to_stream_km": exposure.get("distance_to_stream_km", "unknown"),
        }

    transit_hours = exposure.get("transit_time_hours", 0)
    total_damage = exposure.get("total_hull_damage", 0)
    hull = spacecraft_state.hull_integrity

    options = []

    # Option 1: Transit through
    options.append({
        "strategy": "transit_through",
        "hull_damage_pct": total_damage,
        "hull_after": hull - total_damage,
        "fuel_cost_kg": 0,
        "time_hours": transit_hours,
        "viable": (hull - total_damage) > 20,  # Must keep > 20% hull
    })

    # Option 2: Accelerate through (2x speed = half damage)
    accel_fuel = 15.0  # Rough estimate
    options.append({
        "strategy": "accelerate_transit",
        "hull_damage_pct": total_damage * 0.5,
        "hull_after": hull - total_damage * 0.5,
        "fuel_cost_kg": accel_fuel,
        "time_hours": transit_hours * 0.5,
        "viable": spacecraft_state.fuel_remaining_kg > accel_fuel + 50,
    })

    # Option 3: Redirect around (higher fuel, no damage)
    redirect_fuel = 40.0
    detour_hours = transit_hours * 3
    options.append({
        "strategy": "redirect_around",
        "hull_damage_pct": 0,
        "hull_after": hull,
        "fuel_cost_kg": redirect_fuel,
        "time_hours": detour_hours,
        "viable": spacecraft_state.fuel_remaining_kg > redirect_fuel + 50,
    })

    # Option 4: Wait (if shower has limited duration)
    remaining_shower_hours = (shower.duration_days - (elapsed_days - shower.peak_date_offset_days + shower.duration_days / 2)) * 24
    options.append({
        "strategy": "wait_for_passage",
        "hull_damage_pct": 0,
        "hull_after": hull,
        "fuel_cost_kg": 5.0,  # Station-keeping
        "time_hours": max(0, remaining_shower_hours),
        "viable": remaining_shower_hours < 48 and remaining_shower_hours > 0,
    })

    # Rank by viability then minimize damage + fuel trade-off
    viable = [o for o in options if o["viable"]]
    viable.sort(key=lambda o: o["hull_damage_pct"] * 10 + o["fuel_cost_kg"])

    return {
        "situation": "in_meteor_shower",
        "shower": shower.name,
        "intensity": exposure["intensity"],
        "impact_rate_per_hour": exposure.get("impact_rate_per_hour", 0),
        "options": options,
        "recommended": viable[0]["strategy"] if viable else "emergency_shield",
    }
