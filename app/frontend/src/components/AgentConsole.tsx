import React, { useState } from "react";
import { Bot, Brain, RefreshCw } from "lucide-react";
import type { AgentDecision, PredictionAccuracy } from "../types";

const styles: Record<string, React.CSSProperties> = {
  panel: {
    background: "#0d1224",
    border: "1px solid #1a2040",
    borderRadius: 8,
    padding: 16,
    height: "100%",
    display: "flex",
    flexDirection: "column",
  },
  title: {
    fontSize: 11,
    fontWeight: 700,
    letterSpacing: 2,
    color: "#6b7fa3",
    textTransform: "uppercase" as const,
    marginBottom: 10,
  },
  log: {
    flex: 1,
    overflowY: "auto" as const,
    fontFamily: "'Courier New', Consolas, monospace",
    fontSize: 11,
    lineHeight: "1.6",
    display: "flex",
    flexDirection: "column" as const,
    gap: 8,
    background: "#080c16",
    borderRadius: 4,
    padding: 10,
    border: "1px solid #111828",
  },
  entry: {
    borderBottom: "1px solid #111828",
    paddingBottom: 6,
  },
  header: {
    display: "flex",
    alignItems: "center",
    gap: 6,
    marginBottom: 3,
  },
  agentTag: {
    fontSize: 9,
    fontWeight: 700,
    letterSpacing: 1,
    padding: "1px 5px",
    borderRadius: 3,
    background: "#1a2a4a",
    color: "#00aaff",
    textTransform: "uppercase" as const,
  },
  timestamp: {
    fontSize: 9,
    color: "#4a5a7a",
    marginLeft: "auto",
  },
  reasoning: {
    color: "#a0b0c8",
    fontSize: 11,
    wordBreak: "break-word" as const,
  },
  confidence: {
    fontSize: 9,
    color: "#6b7fa3",
    marginTop: 2,
  },
};

function confidenceColor(c: number): string {
  if (c >= 0.8) return "#00ff88";
  if (c >= 0.5) return "#ffaa00";
  return "#ff4444";
}

type AgentMemory = Record<string, Record<string, unknown>>;

interface Props {
  decisions: AgentDecision[];
  predictionAccuracy?: PredictionAccuracy | null;
  onRetrain?: () => Promise<void>;
  agentMemory?: AgentMemory | null;
}

function TrendSparkline({ data }: { data: number[] }) {
  if (data.length < 2) return null;
  const max = Math.max(...data, 1);
  const w = 80;
  const h = 20;
  const pts = data.map((v, i) => {
    const x = (i / (data.length - 1)) * w;
    const y = h - (v / max) * (h - 2) - 1;
    return `${x},${y}`;
  });
  return (
    <svg width={w} height={h} style={{ verticalAlign: "middle" }}>
      <polyline
        points={pts.join(" ")}
        fill="none"
        stroke="#00aaff"
        strokeWidth={1.5}
      />
    </svg>
  );
}

function assessmentColor(key: string): string {
  switch (key) {
    case "on_course": return "#00ff88";
    case "minor_deviation": return "#ffaa00";
    case "correction_needed": return "#ff4444";
    default: return "#6b7fa3";
  }
}

function memoryLabel(key: string): string {
  const labels: Record<string, string> = {
    correction_effectiveness: "Corr. Eff.",
    fuel_burn_rate_kg_per_day: "Burn Rate",
    hazard_pattern: "Hazard Pattern",
    total_encounters: "Encounters",
    delay_trend: "Delay Trend",
    command_success_rate: "Cmd Success",
    captain_overrides: "Capt. Overrides",
  };
  return labels[key] || key.replace(/_/g, " ");
}

function memoryValue(key: string, val: unknown): string {
  if (typeof val === "number") {
    if (key.includes("rate") || key.includes("effectiveness") || key.includes("success")) {
      return `${(val * 100).toFixed(0)}%`;
    }
    return val % 1 === 0 ? String(val) : val.toFixed(1);
  }
  if (typeof val === "string") return val;
  return "";
}

function memoryColor(key: string, val: unknown): string {
  if (key === "correction_effectiveness" && typeof val === "number") {
    return val >= 0.7 ? "#00ff88" : val >= 0.4 ? "#ffaa00" : "#ff4444";
  }
  if (key === "hazard_pattern") {
    return val === "cluster" ? "#ff4444" : val === "periodic" ? "#ffaa00" : "#00ff88";
  }
  if (key === "delay_trend") {
    return val === "increasing" ? "#ff4444" : val === "decreasing" ? "#00ff88" : "#a0b0c8";
  }
  if (key === "command_success_rate" && typeof val === "number") {
    return val >= 0.7 ? "#00ff88" : val >= 0.4 ? "#ffaa00" : "#ff4444";
  }
  return "#e0e8f0";
}

const MEMORY_DISPLAY_KEYS = [
  "correction_effectiveness",
  "fuel_burn_rate_kg_per_day",
  "hazard_pattern",
  "total_encounters",
  "delay_trend",
  "command_success_rate",
  "captain_overrides",
];

const AgentConsole: React.FC<Props> = ({ decisions, predictionAccuracy, onRetrain, agentMemory }) => {
  const [retraining, setRetraining] = useState(false);

  const handleRetrain = async () => {
    if (!onRetrain) return;
    setRetraining(true);
    try {
      await onRetrain();
    } finally {
      setRetraining(false);
    }
  };

  const pa = predictionAccuracy;

  return (
    <div style={styles.panel}>
      {/* ── Onboard AI Status ── */}
      {pa && pa.total_predictions > 0 && (
        <div
          style={{
            background: "#080c16",
            border: `1px solid ${pa.model_drifting ? "#ff444466" : "#111828"}`,
            borderRadius: 4,
            padding: 8,
            marginBottom: 8,
            fontSize: 10,
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 6,
              marginBottom: 6,
            }}
          >
            <Brain size={11} color="#00aaff" />
            <span
              style={{
                fontSize: 9,
                fontWeight: 700,
                letterSpacing: 2,
                color: "#6b7fa3",
                textTransform: "uppercase" as const,
              }}
            >
              Onboard AI
            </span>
            {pa.active_model && (
              <span
                style={{
                  fontSize: 8,
                  fontWeight: 700,
                  letterSpacing: 1,
                  color: pa.active_model === "model_serving" ? "#00aaff" : "#ffaa00",
                  background: pa.active_model === "model_serving" ? "#0a1a2a" : "#2a1a0a",
                  padding: "1px 5px",
                  borderRadius: 3,
                  textTransform: "uppercase" as const,
                }}
              >
                {pa.active_model === "model_serving" ? "ML" : "PHYSICS"}
              </span>
            )}
            {pa.model_drifting && (
              <span
                style={{
                  fontSize: 8,
                  fontWeight: 700,
                  letterSpacing: 1,
                  color: "#ff4444",
                  background: "#2a0a0a",
                  padding: "1px 5px",
                  borderRadius: 3,
                  textTransform: "uppercase" as const,
                }}
              >
                DRIFT DETECTED
              </span>
            )}
            {pa.drift_retrain_triggered && (
              <span
                style={{
                  fontSize: 8,
                  fontWeight: 700,
                  letterSpacing: 1,
                  color: "#00ff88",
                  background: "#0a2a1a",
                  padding: "1px 5px",
                  borderRadius: 3,
                  textTransform: "uppercase" as const,
                }}
              >
                RETRAIN QUEUED
              </span>
            )}
            {onRetrain && (
              <button
                onClick={handleRetrain}
                disabled={retraining}
                title="Retrain trajectory model on accumulated prediction data from this and prior missions. Runs training notebook via Databricks Workflows."
                style={{
                  marginLeft: "auto",
                  display: "flex",
                  alignItems: "center",
                  gap: 4,
                  fontSize: 8,
                  fontWeight: 700,
                  letterSpacing: 1,
                  padding: "2px 6px",
                  borderRadius: 3,
                  border: "1px solid #00aaff44",
                  background: "#0a1a2a",
                  color: "#00aaff",
                  cursor: retraining ? "wait" : "pointer",
                  opacity: retraining ? 0.5 : 1,
                  fontFamily: "inherit",
                  textTransform: "uppercase" as const,
                }}
              >
                <RefreshCw size={8} style={retraining ? { animation: "spin 1s linear infinite" } : undefined} />
                {retraining ? "..." : "RETRAIN"}
              </button>
            )}
          </div>

          {/* Metrics row */}
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap", color: "#a0b0c8" }}>
            <span>
              Predictions:{" "}
              <span style={{ color: "#e0e8f0" }}>{pa.total_predictions}</span>
              <span style={{ color: "#4a5a7a", marginLeft: 4 }}>
                ({pa.backfilled_count} verified)
              </span>
            </span>
            <span>
              Source:{" "}
              <span style={{ color: "#00aaff" }}>ML {pa.ml_predictions}</span>
              {" / "}
              <span style={{ color: "#ffaa00" }}>PHY {pa.physics_predictions}</span>
            </span>
          </div>

          {/* Error + trend */}
          {pa.avg_error_km != null && (
            <div style={{ display: "flex", alignItems: "center", gap: 12, marginTop: 4, color: "#a0b0c8" }}>
              <span>
                Avg Error:{" "}
                <span style={{ color: pa.avg_error_km > 5000 ? "#ff4444" : pa.avg_error_km > 2000 ? "#ffaa00" : "#00ff88" }}>
                  {pa.avg_error_km.toFixed(0)} km
                </span>
              </span>
              {pa.min_error_km != null && pa.max_error_km != null && (
                <span style={{ color: "#4a5a7a", fontSize: 9 }}>
                  ({pa.min_error_km.toFixed(0)}–{pa.max_error_km.toFixed(0)} km)
                </span>
              )}
              {pa.recent_error_trend.length >= 2 && (
                <TrendSparkline data={pa.recent_error_trend} />
              )}
            </div>
          )}

          {/* Assessment breakdown */}
          <div style={{ display: "flex", gap: 10, marginTop: 4, fontSize: 9 }}>
            {Object.entries(pa.assessments).map(([key, count]) => (
              <span key={key} style={{ color: assessmentColor(key) }}>
                {key.replace(/_/g, " ")}: {count}
              </span>
            ))}
            {pa.corrections_applied > 0 && (
              <span style={{ color: "#00aaff" }}>
                corrections: {pa.corrections_applied}
              </span>
            )}
          </div>

          {/* Source accuracy breakdown */}
          {pa.source_accuracy && Object.keys(pa.source_accuracy).length > 0 && (
            <div style={{ display: "flex", gap: 12, marginTop: 4, fontSize: 9, color: "#a0b0c8" }}>
              {Object.entries(pa.source_accuracy).map(([src, stats]) => (
                <span key={src}>
                  <span style={{ color: src === "model_serving" ? "#00aaff" : "#ffaa00" }}>
                    {src === "model_serving" ? "ML" : "PHY"}
                  </span>
                  {" err: "}
                  <span style={{ color: "#e0e8f0" }}>
                    {stats.avg_error_km.toFixed(0)} km
                  </span>
                  <span style={{ color: "#4a5a7a", marginLeft: 3 }}>
                    (n={stats.backfilled_count})
                  </span>
                </span>
              ))}
            </div>
          )}
        </div>
      )}

      {/* ── Agent Memory ── */}
      {agentMemory && Object.keys(agentMemory).length > 0 && (
        <div
          style={{
            background: "#080c16",
            border: "1px solid #111828",
            borderRadius: 4,
            padding: 8,
            marginBottom: 8,
            fontSize: 9,
          }}
        >
          <div style={{ fontSize: 9, fontWeight: 700, letterSpacing: 2, color: "#6b7fa3", textTransform: "uppercase" as const, marginBottom: 4 }}>
            <Brain size={10} color="#6b7fa3" style={{ verticalAlign: "middle", marginRight: 4 }} />
            Agent Memory
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
            {Object.entries(agentMemory).map(([agent, mem]) => {
              const displayItems = MEMORY_DISPLAY_KEYS
                .filter((k) => k in mem && mem[k] !== null && mem[k] !== undefined)
                .filter((k) => !(Array.isArray(mem[k]) || typeof mem[k] === "object"));
              if (displayItems.length === 0) return null;
              return (
                <div key={agent} style={{ minWidth: 120 }}>
                  <div style={{ color: "#00aaff", fontWeight: 700, letterSpacing: 1, textTransform: "uppercase" as const, marginBottom: 2 }}>
                    {agent.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase()).slice(0, 12)}
                  </div>
                  {displayItems.map((k) => (
                    <div key={k} style={{ display: "flex", justifyContent: "space-between", gap: 6, color: "#4a5a7a" }}>
                      <span>{memoryLabel(k)}</span>
                      <span style={{ color: memoryColor(k, mem[k]) }}>
                        {memoryValue(k, mem[k])}
                      </span>
                    </div>
                  ))}
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* ── Agent Log ── */}
      <div style={styles.title}>
        <Bot
          size={12}
          style={{ verticalAlign: "middle", marginRight: 6 }}
          color="#00aaff"
        />
        Agent Console
      </div>
      <div style={styles.log}>
        {decisions.length === 0 && (
          <div style={{ color: "#4a5a7a" }}>Awaiting agent activity...</div>
        )}
        {decisions.map((d) => (
          <div key={d.id} style={styles.entry}>
            <div style={styles.header}>
              <span style={styles.agentTag}>{d.agent_name}</span>
              <span style={{ fontSize: 10, color: "#e0e8f0" }}>
                {d.action_taken}
              </span>
              <span style={styles.timestamp}>{d.timestamp}</span>
            </div>
            <div style={styles.reasoning}>&gt; {d.reasoning}</div>
            <div style={styles.confidence}>
              Confidence:{" "}
              <span style={{ color: confidenceColor(d.confidence) }}>
                {(d.confidence * 100).toFixed(0)}%
              </span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default AgentConsole;
