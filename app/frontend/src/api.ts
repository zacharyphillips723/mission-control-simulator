import type {
  MissionState,
  MissionClock,
  TrajectoryData,
  Hazard,
  Maneuver,
  Command,
  AgentDecision,
  AgentMessage,
  OnboardPrediction,
  PredictionAccuracy,
  StatsSummary,
  Session,
  ThroughputCurrent,
  CaptainState,
  CaptainDecision,
} from "./types";

async function fetchJson<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json() as Promise<T>;
}

export const api = {
  getMissionState: () => fetchJson<MissionState>("/api/mission/state"),
  getMissionClock: () => fetchJson<MissionClock>("/api/mission/clock"),
  getTrajectory: (hours = 6) =>
    fetchJson<TrajectoryData>(`/api/telemetry/trajectory?hours=${hours}`),
  getActiveHazards: () =>
    fetchJson<{ hazards: Hazard[] }>("/api/hazards/active").then((r) => r.hazards ?? []),
  getCandidateManeuvers: () =>
    fetchJson<{ maneuvers: Maneuver[] }>("/api/maneuvers/candidates").then((r) => r.maneuvers ?? []),
  getCommandQueue: () =>
    fetchJson<{ commands: Command[] }>("/api/commands/queue").then((r) => r.commands ?? []),
  getAgentDecisions: () =>
    fetchJson<{ decisions: AgentDecision[] }>("/api/agents/decisions").then((r) => r.decisions ?? []),
  getOnboardPredictions: (limit = 5) =>
    fetchJson<{ predictions: OnboardPrediction[] }>(`/api/predictions/onboard?limit=${limit}`).then(
      (r) => r.predictions ?? []
    ),
  getPredictionAccuracy: () =>
    fetchJson<{ metrics: PredictionAccuracy | null }>("/api/predictions/accuracy").then(
      (r) => r.metrics
    ),
  warmupModels: () =>
    fetch("/api/models/warmup", { method: "POST" }).then((r) => r.json()),
  triggerRetrain: () =>
    fetch("/api/models/retrain", { method: "POST" }).then((r) => {
      if (!r.ok) return r.json().then((e) => Promise.reject(e));
      return r.json();
    }),
  getStatsSummary: () => fetchJson<StatsSummary>("/api/stats/summary"),
  repositionSimulation: (params: {
    position_x: number;
    position_y: number;
    position_z?: number;
    velocity_x: number;
    velocity_y: number;
    velocity_z?: number;
    fuel_remaining_kg?: number;
    scenario_name?: string;
  }) =>
    fetch("/api/simulation/reposition", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params),
    }).then((r) => {
      if (!r.ok) return r.json().then((e) => Promise.reject(e));
      return r.json();
    }),
  resetSimulation: () =>
    fetch("/api/simulation/reset", { method: "POST" }).then((r) => r.json()),
  startSimulation: () =>
    fetch("/api/simulation/start", { method: "POST" }).then((r) => r.json()),
  stopSimulation: () =>
    fetch("/api/simulation/stop", { method: "POST" }).then((r) => r.json()),
  setTimeScale: (scale: number) =>
    fetch("/api/simulation/timescale", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ scale }),
    }).then((r) => r.json()),
  getSessionList: () =>
    fetchJson<{ sessions: Session[]; count: number; active_session_id: string | null }>(
      "/api/sessions"
    ),
  getSession: (id: string) => fetchJson<Session>(`/api/sessions/${id}`),
  getThroughputCurrent: () => fetchJson<ThroughputCurrent>("/api/throughput/current"),
  getAgentMessages: () =>
    fetchJson<{ messages: AgentMessage[] }>("/api/agents/messages/latest").then(
      (r) => r.messages ?? []
    ),
  sendManualBurn: (params: {
    direction: string;
    delta_v: number;
    burn_duration_s: number;
  }) =>
    fetch("/api/commands/manual-burn", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params),
    }).then((r) => {
      if (!r.ok) return r.json().then((e) => Promise.reject(e));
      return r.json();
    }),
  approveManeuver: (maneuverId: string) =>
    fetch(`/api/maneuvers/${maneuverId}/approve`, { method: "POST" }).then((r) => {
      if (!r.ok) return r.json().then((e) => Promise.reject(e));
      return r.json();
    }),
  rejectManeuver: (maneuverId: string) =>
    fetch(`/api/maneuvers/${maneuverId}/reject`, { method: "POST" }).then((r) => {
      if (!r.ok) return r.json().then((e) => Promise.reject(e));
      return r.json();
    }),
  getMissionEvents: () =>
    fetchJson<{ event_id: string; event_type: string; summary: string; simulation_time_s: number; metadata: Record<string, unknown>; created_at: string }[]>("/api/events"),
  getAgentMemory: () =>
    fetchJson<Record<string, Record<string, unknown>>>("/api/agents/memory"),
  getCaptainState: () => fetchJson<CaptainState>("/api/captain/state"),
  getCaptainDecisions: (limit = 30) =>
    fetchJson<{ decisions: CaptainDecision[]; count: number }>(
      `/api/captain/decisions?limit=${limit}`
    ).then((r) => r.decisions ?? []),
  injectHazard: (params: {
    hazard_type: string;
    position_x: number;
    position_y: number;
    velocity_x: number;
    velocity_y: number;
    radius_km?: number;
  }) =>
    fetch("/api/hazards/inject", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params),
    }).then((r) => {
      if (!r.ok) return r.json().then((e) => Promise.reject(e));
      return r.json();
    }),
};
