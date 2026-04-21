# Databricks notebook source
# MAGIC %md
# MAGIC # Mission Control Simulator — Catalog & Schema Setup
# MAGIC Creates the Unity Catalog catalog and schemas for the mission control system.

# COMMAND ----------

dbutils.widgets.text("catalog", "mission_control_dev", "Catalog Name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
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
