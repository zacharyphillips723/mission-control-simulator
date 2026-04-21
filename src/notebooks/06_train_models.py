# Databricks notebook source
# MAGIC %md
# MAGIC # Mission Control — Train & Register ML Models
# MAGIC Trains 5 ML models on synthetic telemetry data and registers them in Unity Catalog via MLflow:
# MAGIC 1. **Trajectory Prediction** — Predicts future spacecraft position
# MAGIC 2. **Fuel Estimation** — Estimates delta-v and fuel consumption for maneuvers
# MAGIC 3. **Hazard Risk** — Scores collision probability
# MAGIC 4. **Maneuver Ranking** — Evaluates and ranks candidate actions
# MAGIC 5. **Delay-Aware Policy** — Optimizes actions under communication delay

# COMMAND ----------

dbutils.widgets.text("catalog", "mission_control_dev", "Catalog Name")
catalog = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG `{catalog}`")

# COMMAND ----------

import mlflow
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, RandomForestClassifier, RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score, accuracy_score
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import json

mlflow.set_registry_uri("databricks-uc")
experiment_name = f"/Users/{spark.sql('SELECT current_user()').collect()[0][0]}/mission-control-models"
mlflow.set_experiment(experiment_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Training Data

# COMMAND ----------

telemetry_df = spark.sql(f"""
    SELECT *
    FROM `{catalog}`.telemetry.spacecraft_telemetry
    ORDER BY timestamp ASC
""").toPandas()

hazard_df = spark.sql(f"""
    SELECT *
    FROM `{catalog}`.hazards.detected_hazards
    ORDER BY detected_at ASC
""").toPandas()

maneuver_df = spark.sql(f"""
    SELECT *
    FROM `{catalog}`.navigation.candidate_maneuvers
    ORDER BY generated_at ASC
""").toPandas()

print(f"Telemetry rows: {len(telemetry_df):,}")
print(f"Hazard rows: {len(hazard_df):,}")
print(f"Maneuver rows: {len(maneuver_df):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detect & Encode Multi-Profile Data

# COMMAND ----------

from sklearn.preprocessing import LabelEncoder

has_multi_profile = "mission_profile" in telemetry_df.columns and telemetry_df["mission_profile"].nunique() > 1

if has_multi_profile:
    profile_encoder = LabelEncoder()
    telemetry_df["mission_profile_encoded"] = profile_encoder.fit_transform(
        telemetry_df["mission_profile"].fillna("unknown")
    )
    profile_names = list(profile_encoder.classes_)
    n_profiles = len(profile_names)
    print(f"Multi-profile data detected: {n_profiles} profiles")
    for pname in profile_names:
        cnt = (telemetry_df["mission_profile"] == pname).sum()
        print(f"  - {pname}: {cnt:,} rows")

    # Encode hazard profiles if present
    hazard_has_profile = "mission_profile" in hazard_df.columns and len(hazard_df) > 0
    if hazard_has_profile:
        hazard_df["mission_profile_encoded"] = profile_encoder.transform(
            hazard_df["mission_profile"].fillna("unknown")
        )

    # Encode maneuver profiles if present
    maneuver_has_profile = "mission_profile" in maneuver_df.columns and len(maneuver_df) > 0
    if maneuver_has_profile:
        maneuver_df["mission_profile_encoded"] = profile_encoder.transform(
            maneuver_df["mission_profile"].fillna("unknown")
        )
else:
    n_profiles = 1
    profile_names = ["default"]
    hazard_has_profile = False
    maneuver_has_profile = False
    print("Single-profile data — standard training mode")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model 1: Trajectory Prediction
# MAGIC Given current state (position, velocity), predict position N seconds ahead.

# COMMAND ----------

def build_trajectory_features(df: pd.DataFrame, lookahead_rows: int = 60, include_profile: bool = False) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build features for trajectory prediction: current state -> future position."""
    base_cols = ["position_x", "position_y", "position_z",
                 "velocity_x", "velocity_y", "velocity_z",
                 "fuel_remaining_kg", "communication_delay_s"]
    feature_col_names = ["pos_x", "pos_y", "pos_z", "vel_x", "vel_y", "vel_z", "fuel", "comm_delay"]

    if include_profile and "mission_profile_encoded" in df.columns:
        base_cols.append("mission_profile_encoded")
        feature_col_names.append("mission_profile_enc")

    features = []
    targets = []
    arr = df[base_cols].values

    for i in range(len(arr) - lookahead_rows):
        features.append(arr[i])
        targets.append(df[["position_x", "position_y", "position_z"]].values[i + lookahead_rows])

    target_cols = ["future_pos_x", "future_pos_y", "future_pos_z"]

    return pd.DataFrame(features, columns=feature_col_names), pd.DataFrame(targets, columns=target_cols)

# COMMAND ----------

X_traj, y_traj = build_trajectory_features(telemetry_df, lookahead_rows=60, include_profile=has_multi_profile)

# Stratify by mission profile when multi-profile data is available
if has_multi_profile and "mission_profile_enc" in X_traj.columns:
    strat_col = X_traj["mission_profile_enc"]
    X_train, X_test, y_train, y_test = train_test_split(
        X_traj, y_traj, test_size=0.2, random_state=42, stratify=strat_col
    )
else:
    X_train, X_test, y_train, y_test = train_test_split(X_traj, y_traj, test_size=0.2, random_state=42)

with mlflow.start_run(run_name="trajectory_prediction") as run:
    mlflow.log_param("model_type", "GradientBoostingRegressor")
    mlflow.log_param("lookahead_seconds", 60)
    mlflow.log_param("training_rows", len(X_train))
    mlflow.log_param("n_profiles", n_profiles)
    mlflow.log_param("multi_profile", has_multi_profile)
    if has_multi_profile:
        for pname in profile_names:
            cnt = int((telemetry_df["mission_profile"] == pname).sum())
            mlflow.log_param(f"profile_rows_{pname}", cnt)

    model = Pipeline([
        ("scaler", StandardScaler()),
        ("regressor", GradientBoostingRegressor(
            n_estimators=200,
            max_depth=6,
            learning_rate=0.1,
            random_state=42,
        ))
    ])

    # Train one model per output dimension
    models = {}
    for col in y_traj.columns:
        m = Pipeline([
            ("scaler", StandardScaler()),
            ("regressor", GradientBoostingRegressor(
                n_estimators=200, max_depth=6, learning_rate=0.1, random_state=42
            ))
        ])
        m.fit(X_train, y_train[col])
        models[col] = m

    # Evaluate
    for col, m in models.items():
        preds = m.predict(X_test)
        rmse = np.sqrt(mean_squared_error(y_test[col], preds))
        r2 = r2_score(y_test[col], preds)
        mlflow.log_metric(f"rmse_{col}", rmse)
        mlflow.log_metric(f"r2_{col}", r2)
        print(f"  {col}: RMSE={rmse:.2f}, R²={r2:.4f}")

    # Log as a single model artifact using a wrapper
    class TrajectoryModel(mlflow.pyfunc.PythonModel):
        def __init__(self, models):
            self.models = models

        def predict(self, context, model_input):
            results = {}
            for col, m in self.models.items():
                results[col] = m.predict(model_input)
            return pd.DataFrame(results)

    mlflow.pyfunc.log_model(
        artifact_path="trajectory_model",
        python_model=TrajectoryModel(models),
        input_example=X_test.head(1),
        registered_model_name=f"{catalog}.models.trajectory_prediction",
    )

    traj_run_id = run.info.run_id
    print(f"✓ Trajectory model registered: {catalog}.models.trajectory_prediction")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model 2: Fuel Estimation
# MAGIC Given a maneuver (burn vector, duration), estimate fuel consumption.

# COMMAND ----------

if len(maneuver_df) > 10:
    fuel_features = maneuver_df[["burn_vector_x", "burn_vector_y", "burn_vector_z",
                                  "burn_duration_s", "delta_v"]].copy()
    fuel_features["burn_magnitude"] = np.sqrt(
        maneuver_df["burn_vector_x"]**2 +
        maneuver_df["burn_vector_y"]**2 +
        maneuver_df["burn_vector_z"]**2
    )
    fuel_target = maneuver_df["fuel_cost_kg"]

    X_train_f, X_test_f, y_train_f, y_test_f = train_test_split(
        fuel_features, fuel_target, test_size=0.2, random_state=42
    )

    with mlflow.start_run(run_name="fuel_estimation") as run:
        mlflow.log_param("model_type", "GradientBoostingRegressor")
        mlflow.log_param("training_rows", len(X_train_f))

        fuel_model = Pipeline([
            ("scaler", StandardScaler()),
            ("regressor", GradientBoostingRegressor(
                n_estimators=150, max_depth=5, learning_rate=0.1, random_state=42
            ))
        ])
        fuel_model.fit(X_train_f, y_train_f)

        preds = fuel_model.predict(X_test_f)
        rmse = np.sqrt(mean_squared_error(y_test_f, preds))
        mae = mean_absolute_error(y_test_f, preds)
        r2 = r2_score(y_test_f, preds)

        mlflow.log_metrics({"rmse": rmse, "mae": mae, "r2": r2})
        print(f"  Fuel estimation: RMSE={rmse:.2f} kg, MAE={mae:.2f} kg, R²={r2:.4f}")

        mlflow.sklearn.log_model(
            fuel_model,
            artifact_path="fuel_model",
            input_example=X_test_f.head(1),
            registered_model_name=f"{catalog}.models.fuel_estimation",
        )
        print(f"✓ Fuel model registered: {catalog}.models.fuel_estimation")
else:
    print("⚠ Not enough maneuver data for fuel model training — run telemetry generation first")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model 3: Hazard Risk Scoring
# MAGIC Given hazard properties and spacecraft state, predict collision risk score.

# COMMAND ----------

if len(hazard_df) > 10:
    risk_feature_cols = [
        "position_x", "position_y", "position_z",
        "velocity_x", "velocity_y", "velocity_z",
        "radius_km", "closest_approach_km",
    ]
    # Add mission profile context if available
    if has_multi_profile and hazard_has_profile:
        risk_feature_cols.append("mission_profile_encoded")

    risk_features = hazard_df[risk_feature_cols].copy()
    risk_features = risk_features.fillna(0)
    risk_target = hazard_df["risk_score"].fillna(0)

    # Stratify by profile if available
    if has_multi_profile and hazard_has_profile:
        strat_haz = hazard_df["mission_profile_encoded"]
        X_train_r, X_test_r, y_train_r, y_test_r = train_test_split(
            risk_features, risk_target, test_size=0.2, random_state=42, stratify=strat_haz
        )
    else:
        X_train_r, X_test_r, y_train_r, y_test_r = train_test_split(
            risk_features, risk_target, test_size=0.2, random_state=42
        )

    with mlflow.start_run(run_name="hazard_risk") as run:
        mlflow.log_param("model_type", "GradientBoostingRegressor")
        mlflow.log_param("training_rows", len(X_train_r))
        mlflow.log_param("n_profiles", n_profiles)
        mlflow.log_param("multi_profile", has_multi_profile)

        risk_model = Pipeline([
            ("scaler", StandardScaler()),
            ("regressor", GradientBoostingRegressor(
                n_estimators=150, max_depth=5, learning_rate=0.1, random_state=42
            ))
        ])
        risk_model.fit(X_train_r, y_train_r)

        preds = risk_model.predict(X_test_r)
        rmse = np.sqrt(mean_squared_error(y_test_r, preds))
        r2 = r2_score(y_test_r, preds)

        mlflow.log_metrics({"rmse": rmse, "r2": r2})
        print(f"  Hazard risk: RMSE={rmse:.4f}, R²={r2:.4f}")

        mlflow.sklearn.log_model(
            risk_model,
            artifact_path="risk_model",
            input_example=X_test_r.head(1),
            registered_model_name=f"{catalog}.models.hazard_risk",
        )
        print(f"✓ Hazard risk model registered: {catalog}.models.hazard_risk")
else:
    print("⚠ Not enough hazard data for risk model training")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model 4: Maneuver Ranking
# MAGIC Given maneuver features, predict a composite quality score for ranking.

# COMMAND ----------

if len(maneuver_df) > 10:
    rank_features = maneuver_df[[
        "burn_vector_x", "burn_vector_y", "burn_vector_z",
        "burn_duration_s", "delta_v", "fuel_cost_kg",
        "risk_reduction_score",
    ]].copy()

    # Composite score as target (weighted combination)
    rank_target = (
        maneuver_df["feasibility_score"] * 0.4 +
        maneuver_df["risk_reduction_score"] * 0.4 +
        (1 - maneuver_df["fuel_cost_kg"] / maneuver_df["fuel_cost_kg"].max()) * 0.2
    )

    X_train_m, X_test_m, y_train_m, y_test_m = train_test_split(
        rank_features, rank_target, test_size=0.2, random_state=42
    )

    with mlflow.start_run(run_name="maneuver_ranking") as run:
        mlflow.log_param("model_type", "RandomForestRegressor")
        mlflow.log_param("training_rows", len(X_train_m))

        rank_model = Pipeline([
            ("scaler", StandardScaler()),
            ("regressor", RandomForestRegressor(
                n_estimators=100, max_depth=8, random_state=42
            ))
        ])
        rank_model.fit(X_train_m, y_train_m)

        preds = rank_model.predict(X_test_m)
        rmse = np.sqrt(mean_squared_error(y_test_m, preds))
        r2 = r2_score(y_test_m, preds)

        mlflow.log_metrics({"rmse": rmse, "r2": r2})
        print(f"  Maneuver ranking: RMSE={rmse:.4f}, R²={r2:.4f}")

        mlflow.sklearn.log_model(
            rank_model,
            artifact_path="rank_model",
            input_example=X_test_m.head(1),
            registered_model_name=f"{catalog}.models.maneuver_ranking",
        )
        print(f"✓ Maneuver ranking model registered: {catalog}.models.maneuver_ranking")
else:
    print("⚠ Not enough maneuver data for ranking model training")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model 5: Delay-Aware Policy
# MAGIC Given current state + communication delay, predict optimal action timing offset.
# MAGIC The model learns that longer delays require more conservative, earlier action.

# COMMAND ----------

# Build synthetic training data: state + delay -> optimal timing offset
np.random.seed(42)
n_samples = max(len(telemetry_df) // 10, 500)

delay_features = pd.DataFrame({
    "speed": np.random.uniform(10, 40, n_samples),
    "fuel_remaining": np.random.uniform(50, 500, n_samples),
    "distance_to_earth": np.random.uniform(5e7, 4e8, n_samples),
    "comm_delay_s": np.random.uniform(180, 1300, n_samples),  # 3-22 min
    "hazard_risk": np.random.uniform(0, 1, n_samples),
    "maneuver_urgency": np.random.uniform(0, 1, n_samples),
})

# Add mission_scenario feature when multi-profile data exists
if has_multi_profile:
    # Assign a random profile index to each synthetic sample to capture scenario diversity
    delay_features["mission_scenario"] = np.random.randint(0, n_profiles, n_samples)

# Target: how many seconds before estimated receipt should the command be sent
# Higher delay + higher risk = send earlier (larger offset)
delay_target = (
    delay_features["comm_delay_s"] * 1.5 +
    delay_features["hazard_risk"] * 300 +
    delay_features["maneuver_urgency"] * 200 +
    np.random.normal(0, 30, n_samples)
).clip(lower=0)

# When multi-profile, add scenario-dependent bias (e.g. low-fuel profiles demand earlier action)
if has_multi_profile:
    scenario_bias = delay_features["mission_scenario"].map(
        {i: (i % 5) * 20 for i in range(n_profiles)}
    )
    delay_target = delay_target + scenario_bias

X_train_d, X_test_d, y_train_d, y_test_d = train_test_split(
    delay_features, delay_target, test_size=0.2, random_state=42
)

with mlflow.start_run(run_name="delay_aware_policy") as run:
    mlflow.log_param("model_type", "GradientBoostingRegressor")
    mlflow.log_param("training_rows", len(X_train_d))
    mlflow.log_param("n_profiles", n_profiles)
    mlflow.log_param("multi_profile", has_multi_profile)

    delay_model = Pipeline([
        ("scaler", StandardScaler()),
        ("regressor", GradientBoostingRegressor(
            n_estimators=200, max_depth=6, learning_rate=0.1, random_state=42
        ))
    ])
    delay_model.fit(X_train_d, y_train_d)

    preds = delay_model.predict(X_test_d)
    rmse = np.sqrt(mean_squared_error(y_test_d, preds))
    r2 = r2_score(y_test_d, preds)

    mlflow.log_metrics({"rmse": rmse, "r2": r2})
    print(f"  Delay-aware policy: RMSE={rmse:.2f}s, R²={r2:.4f}")

    mlflow.sklearn.log_model(
        delay_model,
        artifact_path="delay_model",
        input_example=X_test_d.head(1),
        registered_model_name=f"{catalog}.models.delay_aware_policy",
    )
    print(f"✓ Delay-aware policy model registered: {catalog}.models.delay_aware_policy")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Evaluation Metrics to Delta

# COMMAND ----------

from datetime import datetime, timezone
import uuid

eval_rows = []
for run_info in mlflow.search_runs(experiment_names=[experiment_name]).itertuples():
    for metric_key in ["rmse", "r2", "mae", "rmse_future_pos_x", "rmse_future_pos_y",
                        "rmse_future_pos_z", "r2_future_pos_x", "r2_future_pos_y", "r2_future_pos_z"]:
        col_name = f"metrics.{metric_key}"
        if col_name in run_info._fields or hasattr(run_info, col_name.replace(".", "_")):
            val = getattr(run_info, col_name.replace(".", "_"), None)
            if val is not None and not pd.isna(val):
                eval_rows.append({
                    "eval_id": str(uuid.uuid4()),
                    "model_name": run_info._asdict().get("tags.mlflow.runName", "unknown"),
                    "model_version": "1",
                    "evaluated_at": datetime.now(timezone.utc).isoformat(),
                    "metric_name": metric_key,
                    "metric_value": float(val),
                    "dataset_name": "synthetic_telemetry_v1",
                    "notes": f"Initial training run",
                })

if eval_rows:
    eval_df = spark.createDataFrame(eval_rows)
    eval_df = eval_df.withColumn("evaluated_at", F.to_timestamp("evaluated_at"))
    eval_df.write.mode("append").saveAsTable(f"`{catalog}`.models.evaluation_metrics")
    print(f"✓ Logged {len(eval_rows)} evaluation metrics to Delta")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

from pyspark.sql import functions as F

print(f"\n{'='*60}")
print("MODEL TRAINING COMPLETE")
print(f"{'='*60}")
print(f"\nRegistered models in {catalog}.models:")
for name in ["trajectory_prediction", "fuel_estimation", "hazard_risk",
             "maneuver_ranking", "delay_aware_policy"]:
    print(f"  ✓ {catalog}.models.{name}")
print(f"\nMLflow experiment: {experiment_name}")
print(f"All models ready for deployment to Model Serving endpoints.")
