import React, { useEffect, useState, useCallback, useRef } from "react";
import { Activity, Satellite, Radio } from "lucide-react";
import { api } from "./api";
import type {
  MissionState,
  MissionClock,
  TrajectoryData,
  Hazard,
  Maneuver,
  Command,
  AgentDecision,
} from "./types";
import MissionOverview from "./components/MissionOverview";
import NavigationMap from "./components/NavigationMap";
import HazardConsole from "./components/HazardConsole";
import ManeuverWorkbench from "./components/ManeuverWorkbench";
import AgentConsole from "./components/AgentConsole";
import CommandLog from "./components/CommandLog";

const REFRESH_MS = 5000;

/* ───── Keyframe injection (runs once) ───── */
const styleTag = document.createElement("style");
styleTag.textContent = `
  @keyframes pulse-green {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.4; }
  }
  @keyframes scanline {
    0% { transform: translateY(-100%); }
    100% { transform: translateY(100%); }
  }
  ::-webkit-scrollbar { width: 4px; }
  ::-webkit-scrollbar-track { background: #0a0e1a; }
  ::-webkit-scrollbar-thumb { background: #1a2040; border-radius: 4px; }
`;
document.head.appendChild(styleTag);

/* ───── Styles ───── */
const s: Record<string, React.CSSProperties> = {
  root: {
    width: "100vw",
    height: "100vh",
    background: "#0a0e1a",
    display: "flex",
    flexDirection: "column",
    overflow: "hidden",
    fontFamily: "'Courier New', Consolas, monospace",
  },
  header: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    padding: "10px 20px",
    background: "linear-gradient(180deg, #0f1530 0%, #0a0e1a 100%)",
    borderBottom: "1px solid #1a2040",
    flexShrink: 0,
  },
  headerLeft: {
    display: "flex",
    alignItems: "center",
    gap: 12,
  },
  logo: {
    fontSize: 14,
    fontWeight: 900,
    letterSpacing: 4,
    color: "#e0e8f0",
    textTransform: "uppercase" as const,
  },
  logoBrand: {
    color: "#ff3621",
    marginRight: 4,
  },
  statusPill: {
    fontSize: 10,
    fontWeight: 700,
    letterSpacing: 1,
    padding: "3px 10px",
    borderRadius: 12,
    textTransform: "uppercase" as const,
    display: "flex",
    alignItems: "center",
    gap: 6,
  },
  headerRight: {
    display: "flex",
    alignItems: "center",
    gap: 16,
  },
  clock: {
    fontVariantNumeric: "tabular-nums",
    fontSize: 13,
    color: "#00ff88",
    display: "flex",
    alignItems: "center",
    gap: 6,
  },
  live: {
    fontSize: 9,
    fontWeight: 700,
    letterSpacing: 2,
    color: "#00ff88",
    animation: "pulse-green 1.5s ease-in-out infinite",
    display: "flex",
    alignItems: "center",
    gap: 4,
  },
  grid: {
    flex: 1,
    display: "grid",
    gridTemplateColumns: "1fr 2fr 1fr",
    gridTemplateRows: "1fr 1fr",
    gap: 8,
    padding: 8,
    minHeight: 0,
  },
};

function missionStatusStyle(
  status: string | undefined
): { bg: string; fg: string } {
  switch (status) {
    case "nominal":
      return { bg: "#0a2a1a", fg: "#00ff88" };
    case "caution":
      return { bg: "#2a2800", fg: "#ffaa00" };
    case "warning":
      return { bg: "#2a1a00", fg: "#ff8800" };
    case "critical":
      return { bg: "#2a0a0a", fg: "#ff4444" };
    default:
      return { bg: "#1a2040", fg: "#6b7fa3" };
  }
}

const App: React.FC = () => {
  const [missionState, setMissionState] = useState<MissionState | null>(null);
  const [clock, setClock] = useState<MissionClock | null>(null);
  const [trajectory, setTrajectory] = useState<TrajectoryData | null>(null);
  const [hazards, setHazards] = useState<Hazard[]>([]);
  const [maneuvers, setManeuvers] = useState<Maneuver[]>([]);
  const [commands, setCommands] = useState<Command[]>([]);
  const [decisions, setDecisions] = useState<AgentDecision[]>([]);
  const [lastUpdate, setLastUpdate] = useState<Date | null>(null);
  const [connected, setConnected] = useState(false);

  const trajectoryHoursRef = useRef(6);

  const fetchAll = useCallback(async () => {
    try {
      const [st, cl, tr, hz, mn, cm, ag] = await Promise.all([
        api.getMissionState(),
        api.getMissionClock(),
        api.getTrajectory(trajectoryHoursRef.current),
        api.getActiveHazards(),
        api.getCandidateManeuvers(),
        api.getCommandQueue(),
        api.getAgentDecisions(),
      ]);
      setMissionState(st);
      setClock(cl);
      setTrajectory(tr);
      setHazards(hz);
      setManeuvers(mn);
      setCommands(cm);
      setDecisions(ag);
      setLastUpdate(new Date());
      setConnected(true);
    } catch {
      setConnected(false);
    }
  }, []);

  useEffect(() => {
    fetchAll();
    const id = setInterval(fetchAll, REFRESH_MS);
    return () => clearInterval(id);
  }, [fetchAll]);

  const handleTimeRange = (hours: number) => {
    trajectoryHoursRef.current = hours;
    api.getTrajectory(hours).then(setTrajectory).catch(() => {});
  };

  const ms = missionStatusStyle(missionState?.status);

  return (
    <div style={s.root}>
      {/* ── Header ── */}
      <header style={s.header}>
        <div style={s.headerLeft}>
          <Satellite size={18} color="#ff3621" />
          <span style={s.logo}>
            <span style={s.logoBrand}>DATABRICKS</span> MISSION CONTROL
          </span>
          <div
            style={{
              ...s.statusPill,
              background: ms.bg,
              color: ms.fg,
              border: `1px solid ${ms.fg}44`,
            }}
          >
            <span
              style={{
                width: 6,
                height: 6,
                borderRadius: "50%",
                background: ms.fg,
                boxShadow: `0 0 6px ${ms.fg}`,
              }}
            />
            {missionState?.status?.toUpperCase() ?? "CONNECTING"}
          </div>
        </div>

        <div style={s.headerRight}>
          {clock && (
            <div style={s.clock}>
              <Radio size={12} />
              MET {clock.mission_elapsed_time}
              <span style={{ color: "#6b7fa3", fontSize: 10, marginLeft: 8 }}>
                {clock.utc_time}
              </span>
              <span style={{ color: "#ffaa00", fontSize: 9 }}>
                x{clock.simulation_speed}
              </span>
            </div>
          )}
          {connected && (
            <div style={s.live}>
              <Activity size={10} />
              LIVE
            </div>
          )}
          {!connected && (
            <div style={{ ...s.live, color: "#ff4444", animation: "none" }}>
              OFFLINE
            </div>
          )}
          {lastUpdate && (
            <span style={{ fontSize: 9, color: "#4a5a7a" }}>
              Updated{" "}
              {lastUpdate.toLocaleTimeString([], {
                hour: "2-digit",
                minute: "2-digit",
                second: "2-digit",
              })}
            </span>
          )}
        </div>
      </header>

      {/* ── Panel Grid ── */}
      <main style={s.grid}>
        {/* Top row */}
        <MissionOverview state={missionState} />
        <NavigationMap data={trajectory} onTimeRangeChange={handleTimeRange} />
        <HazardConsole hazards={hazards} />

        {/* Bottom row */}
        <ManeuverWorkbench maneuvers={maneuvers} />
        <AgentConsole decisions={decisions} />
        <CommandLog commands={commands} />
      </main>
    </div>
  );
};

export default App;
