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
}

/* ───── Clock ───── */
export interface MissionClock {
  mission_elapsed_time: string;
  utc_time: string;
  simulation_speed: number;
  is_running: boolean;
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
  status: "pending" | "approved" | "rejected" | "executed";
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

/* ───── Stats ───── */
export interface StatsSummary {
  total_maneuvers_executed: number;
  hazards_avoided: number;
  fuel_consumed_pct: number;
  distance_traveled_au: number;
  commands_sent: number;
  avg_agent_confidence: number;
}
