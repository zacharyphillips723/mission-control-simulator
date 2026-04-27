/* ───── Mission State ───── */
export interface Position {
  x: number;
  y: number;
  z: number;
}

export interface Velocity {
  vx: number;
  vy: number;
  vz: number;
  magnitude: number;
}

export interface MissionState {
  mission_id: string;
  spacecraft_name: string;
  status: "nominal" | "caution" | "warning" | "critical";
  position: Position;
  velocity: Velocity;
  fuel_remaining_pct: number;
  hull_integrity_pct: number;
  comm_delay_seconds: number;
  engine_status: "online" | "standby" | "offline" | "damaged";
  mission_elapsed_days: number;
  target: string;
  session_id: string | null;
  mission_outcome?: "arrived" | "fuel_exhausted" | "collision" | "hull_failure" | null;
  mission_outcome_detail?: string | null;
  distance_to_earth_km?: number;
  captain?: CaptainState | null;
}

/* ───── Clock ───── */
export interface MissionClock {
  mission_elapsed_time: string;
  utc_time: string;
  simulation_speed: number;
  is_running: boolean;
  session_id: string | null;
}

/* ───── Trajectory ───── */
export interface TrajectoryPoint {
  x: number;
  y: number;
  timestamp: string;
  label?: string;
}

export interface CelestialBody {
  name: string;
  x: number;
  y: number;
  color: string;
  radius: number;
}

export interface TrajectoryData {
  spacecraft_path: TrajectoryPoint[];
  celestial_bodies: CelestialBody[];
  hazard_positions: TrajectoryPoint[];
}

/* ───── Hazards ───── */
export interface Hazard {
  id: string;
  type: "asteroid" | "solar_flare" | "debris" | "radiation_belt" | "micro_meteoroid";
  name: string;
  risk_score: number;
  closest_approach_time: string;
  closest_approach_km: number;
  status: "tracking" | "approaching" | "imminent" | "passed";
  description: string;
  position_x?: number;
  position_y?: number;
}

/* ───── Maneuvers ───── */
export interface Maneuver {
  id: string;
  rank: number;
  name: string;
  delta_v_ms: number;
  fuel_cost_pct: number;
  risk_reduction: number;
  feasibility: "high" | "medium" | "low";
  status: "pending" | "proposed" | "approved" | "rejected" | "executed";
  description: string;
}

/* ───── Commands ───── */
export interface Command {
  id: string;
  type: string;
  transmit_time: string;
  estimated_receive_time: string;
  status: "pending" | "transmitted" | "received" | "failed";
  payload_summary: string;
}

/* ───── Agent Decisions ───── */
export interface AgentDecision {
  id: string;
  agent_name: string;
  timestamp: string;
  reasoning: string;
  confidence: number;
  action_taken: string;
}

/* ───── Onboard Predictions ───── */
export interface OnboardPrediction {
  id: string;
  simulation_time_s: number;
  current_position: Position;
  predicted_position: Position;
  prediction_horizon_s: number;
  source: "model_serving" | "physics_fallback";
  assessment: "on_course" | "minor_deviation" | "correction_needed";
  action_taken: string;
  correction_dv: number;
  actual_position: Position | null;
  prediction_error_km: number | null;
  created_at: string;
}

/* ───── Prediction Accuracy ───── */
export interface PredictionAccuracy {
  total_predictions: number;
  backfilled_count: number;
  avg_error_km: number | null;
  min_error_km: number | null;
  max_error_km: number | null;
  ml_predictions: number;
  physics_predictions: number;
  assessments: {
    on_course: number;
    minor_deviation: number;
    correction_needed: number;
  };
  corrections_applied: number;
  recent_error_trend: number[];
  model_drifting: boolean;
  active_model?: string;
  source_accuracy?: Record<string, { avg_error_km: number; backfilled_count: number }>;
  drift_retrain_triggered?: boolean;
}

/* ───── Agent Messages ───── */
export interface AgentMessage {
  message_id: string;
  from_agent: string;
  to_agent: string;
  message_type: string;
  content: string;
  tick_id: string | null;
  created_at: string;
}

/* ───── Sessions ───── */
export interface Session {
  session_id: string;
  session_name: string;
  scenario_type: string;
  started_at: string;
  ended_at: string | null;
  duration_sim_seconds: number | null;
  max_time_scale_used: number | null;
  total_burns_executed: number;
  total_corrections: number;
  total_fuel_used_kg: number;
  total_hazards_encountered: number;
  final_distance_to_earth_km: number | null;
  outcome: string | null;
  status: "active" | "completed" | "aborted";
}

/* ───── Ship Captain ───── */
export interface CaptainDecision {
  decision_id: string;
  simulation_time_s: number;
  action: string;
  priority_level: number;
  reasoning: string;
  override_of_command_id?: string | null;
  original_command_summary?: string | null;
  captain_alternative_summary?: string | null;
  delta_v: number;
  fuel_cost_kg: number;
  alert_level: "green" | "yellow" | "red";
  confidence: number;
  elapsed_ms: number;
}

export interface CaptainState {
  captain_available: boolean;
  alert_level: "green" | "yellow" | "red";
  total_overrides: number;
  total_evasions: number;
  total_micro_corrections: number;
  total_commands_approved: number;
  fuel_used_by_captain_kg: number;
  last_decision_action: string;
  last_decision_reasoning: string;
  ticks_since_mc_contact: number;
  recent_decisions: CaptainDecision[];
}

/* ───── Stats ───── */
export interface StatsSummary {
  total_maneuvers_executed: number;
  hazards_avoided: number;
  fuel_consumed_pct: number;
  distance_traveled_au: number;
  commands_sent: number;
  avg_agent_confidence: number;
}

/* ───── Throughput Metrics ───── */
export interface ThroughputCurrent {
  ops_per_second: number;
  reads: number;
  writes: number;
  total_ops: number;
  window_seconds: number;
  latency_p50_ms: number;
  latency_p99_ms: number;
  sub_ticks: number;
  telemetry_writes_per_tick: number;
  sparkline: number[];
  pool?: {
    pool_size: number;
    checked_in: number;
    checked_out: number;
    overflow: number;
  };
}

/* ───── Mission Events ───── */
export interface MissionEvent {
  event_id: string;
  event_type: string;
  summary: string;
  simulation_time_s: number;
  metadata: Record<string, unknown>;
  created_at: string;
}
