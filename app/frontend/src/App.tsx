import React, { useEffect, useState, useCallback } from "react";
import { Activity, Satellite, Radio, Play, Square, RotateCcw, BarChart3, Zap } from "lucide-react";
import { api } from "./api";
import type {
  MissionState,
  MissionClock,
  Hazard,
  Maneuver,
  Command,
  AgentDecision,
  AgentMessage,
  OnboardPrediction,
  PredictionAccuracy,
  ThroughputCurrent,
  MissionEvent,
} from "./types";
import MissionOverview from "./components/MissionOverview";
import SolarSystemMap from "./components/SolarSystemMap";
import HazardConsole from "./components/HazardConsole";
import ManeuverWorkbench from "./components/ManeuverWorkbench";
import AgentCommsFeed from "./components/AgentCommsFeed";
import AgentConsole from "./components/AgentConsole";
import CommandLog from "./components/CommandLog";
import EventFeed from "./components/EventFeed";
import ThroughputOverlay from "./components/ThroughputOverlay";
import CaptainPanel from "./components/CaptainPanel";
import MissionOutcome from "./components/MissionOutcome";

// Tiered polling: critical state fast, secondary data slower
const POLL_FAST_MS = 1500;   // mission state + clock
const POLL_MEDIUM_MS = 3000; // hazards, maneuvers, commands, predictions
const POLL_SLOW_MS = 6000;   // agent decisions, messages, memory, accuracy

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
  main > * { min-height: 0; overflow: hidden; }
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
    gridTemplateColumns: "1fr 1fr 1fr 1fr",
    gridTemplateRows: "1fr 1fr",
    gap: 8,
    padding: 8,
    minHeight: 0,
    overflow: "hidden",
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
  const [hazards, setHazards] = useState<Hazard[]>([]);
  const [maneuvers, setManeuvers] = useState<Maneuver[]>([]);
  const [commands, setCommands] = useState<Command[]>([]);
  const [decisions, setDecisions] = useState<AgentDecision[]>([]);
  const [predictions, setPredictions] = useState<OnboardPrediction[]>([]);
  const [predictionAccuracy, setPredictionAccuracy] = useState<PredictionAccuracy | null>(null);
  const [lastUpdate, setLastUpdate] = useState<Date | null>(null);
  const [connected, setConnected] = useState(false);
  const [simStarting, setSimStarting] = useState(false);
  const [warmingUp, setWarmingUp] = useState(false);
  const [warmupResult, setWarmupResult] = useState<{ ready: number; total: number } | null>(null);
  const [resetting, setResetting] = useState(false);
  const [resetCounter, setResetCounter] = useState(0);
  const [agentMessages, setAgentMessages] = useState<AgentMessage[]>([]);
  const [agentMemory, setAgentMemory] = useState<Record<string, Record<string, unknown>> | null>(null);
  const [missionEvents, setMissionEvents] = useState<MissionEvent[]>([]);
  const [showMetrics, setShowMetrics] = useState(false);
  const [outcomeDismissed, setOutcomeDismissed] = useState(false);
  const [throughput, setThroughput] = useState<ThroughputCurrent | null>(null);

  // Fast tier: mission state + clock (every 1.5s)
  const fetchFast = useCallback(async () => {
    try {
      const results = await Promise.allSettled([
        api.getMissionState(),
        api.getMissionClock(),
      ]);
      const val = <T,>(r: PromiseSettledResult<T>, fallback: T): T =>
        r.status === "fulfilled" ? r.value : fallback;
      setMissionState(val(results[0], missionState));
      setClock(val(results[1], clock));
      setLastUpdate(new Date());
      setConnected(results.some((r) => r.status === "fulfilled"));
    } catch {
      setConnected(false);
    }
  }, []);

  // Medium tier: hazards, maneuvers, commands, predictions (every 3s)
  const fetchMedium = useCallback(async () => {
    try {
      const results = await Promise.allSettled([
        api.getActiveHazards(),
        api.getCandidateManeuvers(),
        api.getCommandQueue(),
        api.getOnboardPredictions(),
        api.getMissionEvents(),
      ]);
      const val = <T,>(r: PromiseSettledResult<T>, fallback: T): T =>
        r.status === "fulfilled" ? r.value : fallback;
      setHazards(val(results[0], []));
      setManeuvers(val(results[1], []));
      setCommands(val(results[2], []));
      setPredictions(val(results[3], []));
      setMissionEvents(val(results[4], []));
    } catch { /* non-critical */ }
  }, []);

  // Slow tier: agents, messages, memory, accuracy (every 6s)
  const fetchSlow = useCallback(async () => {
    try {
      const results = await Promise.allSettled([
        api.getAgentDecisions(),
        api.getPredictionAccuracy(),
        api.getAgentMessages(),
        api.getAgentMemory(),
      ]);
      const val = <T,>(r: PromiseSettledResult<T>, fallback: T): T =>
        r.status === "fulfilled" ? r.value : fallback;
      setDecisions(val(results[0], []));
      setPredictionAccuracy(val(results[1], null));
      setAgentMessages(val(results[2], []));
      setAgentMemory(val(results[3], null));
    } catch { /* non-critical */ }
  }, []);

  // Combined fetch for initial load and manual refreshes
  const fetchAll = useCallback(async () => {
    await Promise.allSettled([fetchFast(), fetchMedium(), fetchSlow()]);
  }, [fetchFast, fetchMedium, fetchSlow]);

  useEffect(() => {
    fetchAll();
    const fast = setInterval(fetchFast, POLL_FAST_MS);
    const medium = setInterval(fetchMedium, POLL_MEDIUM_MS);
    const slow = setInterval(fetchSlow, POLL_SLOW_MS);
    return () => { clearInterval(fast); clearInterval(medium); clearInterval(slow); };
  }, [fetchAll, fetchFast, fetchMedium, fetchSlow]);

  // Throughput polling — only when overlay is visible
  useEffect(() => {
    if (!showMetrics) return;
    let cancelled = false;
    const poll = async () => {
      try {
        const data = await api.getThroughputCurrent();
        if (!cancelled) setThroughput(data);
      } catch { /* ignore */ }
    };
    poll();
    const id = setInterval(poll, 3000);
    return () => { cancelled = true; clearInterval(id); };
  }, [showMetrics]);

  const isRunning = clock?.is_running ?? false;

  const handleTimeScaleChange = async (scale: number) => {
    try {
      await api.setTimeScale(scale);
      const newClock = await api.getMissionClock();
      setClock(newClock);
    } catch (e) {
      console.error("Time scale change failed:", e);
    }
  };

  const handleWarmup = async () => {
    setWarmingUp(true);
    setWarmupResult(null);
    try {
      const res = await api.warmupModels();
      setWarmupResult({ ready: res.ready, total: res.total });
      // Clear the result after 8 seconds
      setTimeout(() => setWarmupResult(null), 8000);
    } catch (e) {
      console.error("Warmup failed:", e);
    } finally {
      setWarmingUp(false);
    }
  };

  const handleSimToggle = async () => {
    setSimStarting(true);
    try {
      if (isRunning) {
        await api.stopSimulation();
      } else {
        await api.startSimulation();
      }
      // Refresh clock state
      const newClock = await api.getMissionClock();
      setClock(newClock);
    } catch (e) {
      console.error("Simulation toggle failed:", e);
    } finally {
      setSimStarting(false);
    }
  };

  const handleReset = async () => {
    if (!window.confirm("Reset mission?")) return;
    setResetting(true);
    try {
      await api.resetSimulation();
      setResetCounter((c) => c + 1);
      setOutcomeDismissed(false);
      // Refresh all data
      await fetchAll();
    } catch (e) {
      console.error("Reset failed:", e);
    } finally {
      setResetting(false);
    }
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
          <button
            onClick={() => setShowMetrics((v) => !v)}
            title="Toggle Lakebase throughput overlay"
            style={{
              display: "flex",
              alignItems: "center",
              gap: 5,
              padding: "5px 12px",
              borderRadius: 6,
              border: showMetrics ? "1px solid #00aaff66" : "1px solid #1a2040",
              background: showMetrics ? "#0a1a2a" : "#0d1224",
              color: showMetrics ? "#00aaff" : "#6b7fa3",
              fontSize: 11,
              fontWeight: 700,
              letterSpacing: 1,
              cursor: "pointer",
              fontFamily: "inherit",
              textTransform: "uppercase" as const,
            }}
          >
            <BarChart3 size={11} />
            METRICS
          </button>
          <button
            onClick={handleWarmup}
            disabled={warmingUp || isRunning}
            title="Wake up all ML + LLM endpoints from scale-to-zero"
            style={{
              display: "flex",
              alignItems: "center",
              gap: 5,
              padding: "5px 12px",
              borderRadius: 6,
              border: warmupResult
                ? warmupResult.ready === warmupResult.total
                  ? "1px solid #00ff8866"
                  : "1px solid #ffaa0066"
                : "1px solid #00aaff44",
              background: warmupResult
                ? warmupResult.ready === warmupResult.total
                  ? "#0a2a1a"
                  : "#2a2800"
                : "#0a1a2a",
              color: warmupResult
                ? warmupResult.ready === warmupResult.total
                  ? "#00ff88"
                  : "#ffaa00"
                : "#00aaff",
              fontSize: 11,
              fontWeight: 700,
              letterSpacing: 1,
              cursor: warmingUp ? "wait" : "pointer",
              opacity: warmingUp ? 0.6 : 1,
              fontFamily: "inherit",
              textTransform: "uppercase" as const,
            }}
          >
            <Zap size={11} style={warmingUp ? { animation: "spin 1s linear infinite" } : undefined} />
            {warmingUp
              ? "WARMING UP..."
              : warmupResult
                ? `${warmupResult.ready}/${warmupResult.total} READY`
                : "WARM UP"}
          </button>
          <button
            onClick={handleReset}
            disabled={resetting || isRunning}
            title={isRunning ? "Stop simulation before resetting" : "Reset simulation to initial state"}
            style={{
              display: "flex",
              alignItems: "center",
              gap: 5,
              padding: "5px 12px",
              borderRadius: 6,
              border: "1px solid #ffaa0066",
              background: "#2a2800",
              color: "#ffaa00",
              fontSize: 11,
              fontWeight: 700,
              letterSpacing: 1,
              cursor: resetting || isRunning ? "not-allowed" : "pointer",
              opacity: resetting || isRunning ? 0.4 : 1,
              fontFamily: "inherit",
              textTransform: "uppercase" as const,
            }}
          >
            <RotateCcw size={11} />
            {resetting ? "..." : "RESET"}
          </button>
          <button
            onClick={handleSimToggle}
            disabled={simStarting}
            style={{
              display: "flex",
              alignItems: "center",
              gap: 6,
              padding: "5px 14px",
              borderRadius: 6,
              border: isRunning
                ? "1px solid #ff444466"
                : "1px solid #00ff8866",
              background: isRunning ? "#2a0a0a" : "#0a2a1a",
              color: isRunning ? "#ff4444" : "#00ff88",
              fontSize: 11,
              fontWeight: 700,
              letterSpacing: 1,
              cursor: simStarting ? "wait" : "pointer",
              opacity: simStarting ? 0.6 : 1,
              fontFamily: "inherit",
              textTransform: "uppercase" as const,
            }}
          >
            {isRunning ? <Square size={11} /> : <Play size={11} />}
            {simStarting
              ? "..."
              : isRunning
                ? "STOP SIM"
                : "START SIM"}
          </button>
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
          {missionState?.session_id && (
            <span
              style={{
                fontSize: 9,
                color: "#6b7fa3",
                fontFamily: "inherit",
                letterSpacing: 1,
              }}
              title={`Session: ${missionState.session_id}`}
            >
              SESSION {missionState.session_id.slice(0, 8).toUpperCase()}
            </span>
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

      {/* ── Throughput Overlay ── */}
      {showMetrics && (
        <ThroughputOverlay
          data={throughput}
          timeScale={clock?.simulation_speed ?? 1}
        />
      )}

      {/* ── Panel Grid ── */}
      <main style={s.grid}>
        {/* Top row */}
        <MissionOverview state={missionState} />
        <div style={{ gridColumn: "2 / 4", minHeight: 0, overflow: "hidden" }}>
          <SolarSystemMap
            state={missionState}
            hazards={hazards}
            predictions={predictions}
            elapsedSeconds={
              missionState
                ? missionState.mission_elapsed_days * 86400
                : 0
            }
            onTimeScaleChange={handleTimeScaleChange}
            currentTimeScale={clock?.simulation_speed ?? 1}
            resetCounter={resetCounter}
            isRunning={isRunning}
            onReposition={async (params) => {
              try {
                await api.repositionSimulation(params);
                setResetCounter((c) => c + 1); // clear trail
                await fetchAll();
              } catch (e: any) {
                console.error("Reposition failed:", e);
              }
            }}
          />
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 8, minHeight: 0, overflow: "hidden" }}>
          <div style={{ flex: "0 0 auto", maxHeight: "45%", overflow: "hidden" }}>
            <CaptainPanel captain={missionState?.captain} />
          </div>
          <div style={{ flex: 1, minHeight: 0, overflow: "hidden" }}>
            <HazardConsole hazards={hazards} shipPosition={missionState ? { x: missionState.position.x, y: missionState.position.y } : null} />
          </div>
        </div>

        {/* Bottom row */}
        <ManeuverWorkbench
          maneuvers={maneuvers}
          onManualBurn={async (params) => {
            await api.sendManualBurn(params);
            await fetchAll();
          }}
          onApprove={async (id) => {
            await api.approveManeuver(id);
            await fetchAll();
          }}
          onReject={async (id) => {
            await api.rejectManeuver(id);
            await fetchAll();
          }}
        />
        <AgentCommsFeed messages={agentMessages} />
        <AgentConsole
          decisions={decisions}
          predictionAccuracy={predictionAccuracy}
          agentMemory={agentMemory}
          onRetrain={async () => {
            await api.triggerRetrain();
            await fetchAll();
          }}
        />
        <div style={{ display: "flex", flexDirection: "column", gap: 8, minHeight: 0, overflow: "hidden" }}>
          <div style={{ flex: 1, minHeight: 0, overflow: "hidden" }}>
            <EventFeed events={missionEvents} />
          </div>
          <div style={{ flex: 1, minHeight: 0, overflow: "hidden" }}>
            <CommandLog commands={commands} />
          </div>
        </div>
      </main>

      {/* ── Mission Outcome Overlay ── */}
      {missionState?.mission_outcome && !outcomeDismissed && (
        <MissionOutcome
          state={missionState}
          onDismiss={() => setOutcomeDismissed(true)}
        />
      )}
    </div>
  );
};

export default App;
