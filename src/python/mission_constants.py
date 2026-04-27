"""
Mission Control — Shared Physical & Game-Balanced Constants

Single source of truth for all physics and spacecraft parameters used across
physics_engine.py, spacecraft_autopilot.py, main.py, and the frontend.

The "game-balanced" exhaust velocity (30 km/s) is intentionally higher than
real ion propulsion (~3 km/s) to keep the demo playable — large maneuvers
would otherwise consume unrealistic amounts of fuel at realistic exhaust
velocities. All modules MUST import from here to prevent constant drift.
"""

import math

# ---------------------------------------------------------------------------
# Universal Physical Constants
# ---------------------------------------------------------------------------

SPEED_OF_LIGHT_KM_S = 299792.458

# Gravitational parameter (GM = G * M) for key bodies in km^3/s^2
GM = {
    "sun": 1.32712440018e11,
    "earth": 3.986004418e5,
    "mars": 4.282837e4,
    "jupiter": 1.26686534e8,
    "moon": 4.9048695e3,
}
GM_SUN = GM["sun"]

# Mean orbital radii in km (simplified circular orbits for demo)
ORBIT_RADIUS = {
    "earth": 1.496e8,    # 1 AU
    "mars": 2.279e8,     # 1.524 AU
    "jupiter": 7.785e8,  # 5.203 AU
}
EARTH_ORBIT_RADIUS_KM = ORBIT_RADIUS["earth"]

# Orbital periods in seconds
ORBIT_PERIOD = {
    "earth": 365.25 * 86400,
    "mars": 687.0 * 86400,
    "jupiter": 4332.59 * 86400,
}
EARTH_ORBIT_PERIOD_S = ORBIT_PERIOD["earth"]
EARTH_ANGULAR_VEL = 2.0 * math.pi / EARTH_ORBIT_PERIOD_S

# ---------------------------------------------------------------------------
# Spacecraft Parameters (Game-Balanced)
# ---------------------------------------------------------------------------

# Exhaust velocity: 30 km/s is a compromise.
#   - Real ion thruster: ~3 km/s (too stingy for a demo)
#   - Previous main.py value: 100 km/s (too generous, hid fuel issues)
#   - Previous physics_engine.py value: 3 km/s (realistic but unplayable)
EXHAUST_VELOCITY_KM_S = 30.0

# Dry mass: mass of spacecraft without fuel
DRY_MASS_KG = 3000.0

# Initial fuel capacity — gives ~10.5 km/s total delta-v budget
# (enough for transfer corrections + orbit insertion at Earth)
FUEL_CAPACITY_KG = 1200.0
