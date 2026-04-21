import type {
  MissionState,
  MissionClock,
  TrajectoryData,
  Hazard,
  Maneuver,
  Command,
  AgentDecision,
  StatsSummary,
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
  getActiveHazards: () => fetchJson<Hazard[]>("/api/hazards/active"),
  getCandidateManeuvers: () => fetchJson<Maneuver[]>("/api/maneuvers/candidates"),
  getCommandQueue: () => fetchJson<Command[]>("/api/commands/queue"),
  getAgentDecisions: () => fetchJson<AgentDecision[]>("/api/agents/decisions"),
  getStatsSummary: () => fetchJson<StatsSummary>("/api/stats/summary"),
};
