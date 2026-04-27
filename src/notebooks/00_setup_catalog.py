# Databricks notebook source
# MAGIC %md
# MAGIC # Mission Control Simulator — Catalog & Schema Setup
# MAGIC Creates the Unity Catalog catalog and schemas for the mission control system.

# COMMAND ----------

dbutils.widgets.text("catalog", "mission_control_dev", "Catalog Name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# Try standard CREATE CATALOG first; if workspace requires managed location, retry with default storage
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
except Exception as e:
    if "storage root URL" in str(e) or "MANAGED LOCATION" in str(e) or "Default Storage" in str(e):
        print("Workspace requires managed location — looking up default managed storage")
        rows = spark.sql("SHOW EXTERNAL LOCATIONS").collect()
        managed = [r for r in rows if r["name"] == "__databricks_managed_storage_location"]
        if managed:
            loc = managed[0]["url"].rstrip("/") + f"/{catalog}"
            spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}` MANAGED LOCATION '{loc}'")
        else:
            # Catalog may already exist — try USE CATALOG before failing
            try:
                spark.sql(f"USE CATALOG `{catalog}`")
                print(f"Catalog `{catalog}` already exists — using it")
            except Exception:
                raise RuntimeError(
                    "Cannot create catalog: no managed storage location found and catalog does not exist"
                ) from e
    else:
        raise
spark.sql(f"USE CATALOG `{catalog}`")

# Core schemas
schemas = [
    ("telemetry", "Real-time and historical spacecraft telemetry data"),
    ("navigation", "Trajectory, maneuver candidates, and orbital mechanics"),
    ("hazards", "Asteroid and meteor shower detection and risk scoring"),
    ("commands", "Command queue, transmission tracking, and execution logs"),
    ("agents", "Agent decisions, reasoning logs, and memory"),
    ("models", "Model metadata, evaluation metrics, and training data"),
]

for schema_name, comment in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema_name}` COMMENT '{comment}'")
    print(f"✓ Schema created: {catalog}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN `{catalog}`"))
