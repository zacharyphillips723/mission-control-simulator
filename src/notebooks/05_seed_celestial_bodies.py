# Databricks notebook source
# MAGIC %md
# MAGIC # Mission Control — Seed Celestial Body Data
# MAGIC Populates the celestial bodies reference table with solar system data.

# COMMAND ----------

dbutils.widgets.text("catalog", "mission_control_dev", "Catalog Name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime, timezone

bodies = [
    Row(
        body_id="sun", name="Sun", body_type="star",
        mass_kg=1.989e30, radius_km=696340.0,
        orbit_semi_major_axis_km=0.0, orbit_eccentricity=0.0, orbit_period_days=0.0,
        current_position_x=0.0, current_position_y=0.0, current_position_z=0.0,
        updated_at=datetime.now(timezone.utc),
    ),
    Row(
        body_id="earth", name="Earth", body_type="planet",
        mass_kg=5.972e24, radius_km=6371.0,
        orbit_semi_major_axis_km=1.496e8, orbit_eccentricity=0.0167, orbit_period_days=365.25,
        current_position_x=1.496e8, current_position_y=0.0, current_position_z=0.0,
        updated_at=datetime.now(timezone.utc),
    ),
    Row(
        body_id="mars", name="Mars", body_type="planet",
        mass_kg=6.39e23, radius_km=3389.5,
        orbit_semi_major_axis_km=2.279e8, orbit_eccentricity=0.0934, orbit_period_days=687.0,
        current_position_x=2.279e8, current_position_y=0.0, current_position_z=0.0,
        updated_at=datetime.now(timezone.utc),
    ),
    Row(
        body_id="jupiter", name="Jupiter", body_type="planet",
        mass_kg=1.898e27, radius_km=69911.0,
        orbit_semi_major_axis_km=7.785e8, orbit_eccentricity=0.0489, orbit_period_days=4332.59,
        current_position_x=7.785e8, current_position_y=0.0, current_position_z=0.0,
        updated_at=datetime.now(timezone.utc),
    ),
    Row(
        body_id="moon", name="Moon", body_type="moon",
        mass_kg=7.342e22, radius_km=1737.4,
        orbit_semi_major_axis_km=3.844e5, orbit_eccentricity=0.0549, orbit_period_days=27.3,
        current_position_x=1.496e8 + 3.844e5, current_position_y=0.0, current_position_z=0.0,
        updated_at=datetime.now(timezone.utc),
    ),
]

df = spark.createDataFrame(bodies)
df.write.mode("overwrite").saveAsTable(f"`{catalog}`.navigation.celestial_bodies")

print(f"✓ Seeded {len(bodies)} celestial bodies")
display(df.select("name", "body_type", "radius_km", "orbit_semi_major_axis_km"))
