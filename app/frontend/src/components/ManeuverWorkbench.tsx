import React, { useState } from "react";
import { Check, X, Flame, Send } from "lucide-react";
import type { Maneuver } from "../types";

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
  burnForm: {
    background: "#080c16",
    border: "1px solid #1a2040",
    borderRadius: 4,
    padding: 10,
    marginBottom: 10,
  },
  burnTitle: {
    fontSize: 9,
    fontWeight: 700,
    letterSpacing: 2,
    color: "#ff8800",
    textTransform: "uppercase" as const,
    marginBottom: 8,
    display: "flex",
    alignItems: "center",
    gap: 5,
  },
  formRow: {
    display: "flex",
    gap: 8,
    marginBottom: 6,
    alignItems: "center",
  },
  label: {
    fontSize: 9,
    color: "#6b7fa3",
    width: 60,
    flexShrink: 0,
  },
  select: {
    flex: 1,
    background: "#0d1224",
    border: "1px solid #1a2040",
    borderRadius: 3,
    color: "#e0e8f0",
    fontSize: 10,
    padding: "3px 6px",
    fontFamily: "'Courier New', Consolas, monospace",
  },
  input: {
    flex: 1,
    background: "#0d1224",
    border: "1px solid #1a2040",
    borderRadius: 3,
    color: "#e0e8f0",
    fontSize: 10,
    padding: "3px 6px",
    fontFamily: "'Courier New', Consolas, monospace",
    width: 60,
  },
  unit: {
    fontSize: 9,
    color: "#4a5a7a",
    flexShrink: 0,
  },
  burnBtn: {
    display: "flex",
    alignItems: "center",
    gap: 5,
    padding: "4px 10px",
    borderRadius: 4,
    border: "1px solid #ff880066",
    background: "#2a1a00",
    color: "#ff8800",
    fontSize: 10,
    fontWeight: 700,
    letterSpacing: 1,
    cursor: "pointer",
    fontFamily: "'Courier New', Consolas, monospace",
    textTransform: "uppercase" as const,
    marginLeft: "auto",
  },
  tableWrap: {
    flex: 1,
    overflowY: "auto" as const,
  },
  table: {
    width: "100%",
    borderCollapse: "collapse" as const,
    fontSize: 11,
  },
  th: {
    textAlign: "left" as const,
    color: "#4a5a7a",
    fontSize: 9,
    fontWeight: 700,
    letterSpacing: 1,
    textTransform: "uppercase" as const,
    padding: "6px 6px",
    borderBottom: "1px solid #1a2040",
  },
  td: {
    padding: "6px 6px",
    borderBottom: "1px solid #111828",
    color: "#c8d6e5",
    verticalAlign: "middle" as const,
  },
  actionBtn: {
    width: 22,
    height: 22,
    borderRadius: 4,
    border: "none",
    cursor: "pointer",
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    marginRight: 4,
  },
  feedback: {
    fontSize: 9,
    padding: "3px 8px",
    borderRadius: 3,
    marginTop: 4,
    textAlign: "center" as const,
  },
};

const DIRECTIONS = [
  { value: "prograde", label: "Accelerate (prograde)" },
  { value: "retrograde", label: "Decelerate (retrograde)" },
  { value: "earth", label: "Toward Earth" },
  { value: "brake_earth", label: "Brake for Earth orbit" },
  { value: "radial_in", label: "Radial In (sunward)" },
  { value: "radial_out", label: "Radial Out (anti-sun)" },
  { value: "normal", label: "Normal (out of plane)" },
];

function feasibilityColor(f: string) {
  switch (f) {
    case "high": return "#00ff88";
    case "medium": return "#ffaa00";
    default: return "#ff4444";
  }
}

function statusBadge(s: string): React.CSSProperties {
  const map: Record<string, { bg: string; fg: string }> = {
    pending: { bg: "#1a2040", fg: "#6b7fa3" },
    proposed: { bg: "#1a2040", fg: "#6b7fa3" },
    approved: { bg: "#0a2a1a", fg: "#00ff88" },
    rejected: { bg: "#2a0a0a", fg: "#ff4444" },
    executed: { bg: "#0a1a2a", fg: "#00aaff" },
  };
  const c = map[s] ?? map.pending;
  return {
    fontSize: 9,
    fontWeight: 700,
    letterSpacing: 1,
    padding: "2px 6px",
    borderRadius: 3,
    background: c.bg,
    color: c.fg,
    textTransform: "uppercase",
  };
}

interface Props {
  maneuvers: Maneuver[];
  onManualBurn?: (params: { direction: string; delta_v: number; burn_duration_s: number }) => Promise<void>;
  onApprove?: (id: string) => Promise<void>;
  onReject?: (id: string) => Promise<void>;
}

const ManeuverWorkbench: React.FC<Props> = ({ maneuvers, onManualBurn, onApprove, onReject }) => {
  const [direction, setDirection] = useState("earth");
  const [deltaV, setDeltaV] = useState(0.5);
  const [duration, setDuration] = useState(30);
  const [sending, setSending] = useState(false);
  const [feedback, setFeedback] = useState<{ msg: string; ok: boolean } | null>(null);

  const handleBurn = async () => {
    if (!onManualBurn) return;
    setSending(true);
    setFeedback(null);
    try {
      await onManualBurn({ direction, delta_v: deltaV, burn_duration_s: duration });
      setFeedback({ msg: "Burn command queued", ok: true });
    } catch (e: any) {
      setFeedback({ msg: e?.detail || "Failed to send burn", ok: false });
    } finally {
      setSending(false);
      setTimeout(() => setFeedback(null), 4000);
    }
  };

  const handleApprove = async (id: string) => {
    if (!onApprove) return;
    try {
      await onApprove(id);
      setFeedback({ msg: "Maneuver approved", ok: true });
    } catch (e: any) {
      setFeedback({ msg: e?.detail || "Failed to approve", ok: false });
    } finally {
      setTimeout(() => setFeedback(null), 4000);
    }
  };

  const handleReject = async (id: string) => {
    if (!onReject) return;
    try {
      await onReject(id);
      setFeedback({ msg: "Maneuver rejected", ok: true });
    } catch (e: any) {
      setFeedback({ msg: e?.detail || "Failed to reject", ok: false });
    } finally {
      setTimeout(() => setFeedback(null), 4000);
    }
  };

  return (
    <div style={styles.panel}>
      <div style={styles.title}>Maneuver Workbench</div>

      {/* Quick Burn Form */}
      <div style={styles.burnForm}>
        <div style={styles.burnTitle}>
          <Flame size={10} />
          Manual Burn Command
        </div>
        <div style={styles.formRow}>
          <span style={styles.label}>Direction</span>
          <select
            style={styles.select}
            value={direction}
            onChange={(e) => setDirection(e.target.value)}
          >
            {DIRECTIONS.map((d) => (
              <option key={d.value} value={d.value}>{d.label}</option>
            ))}
          </select>
        </div>
        <div style={styles.formRow}>
          <span style={styles.label}>Delta-V</span>
          <input
            type="number"
            style={styles.input}
            value={deltaV}
            step={0.1}
            min={0.01}
            max={5}
            onChange={(e) => setDeltaV(Math.min(5, parseFloat(e.target.value) || 0.5))}
          />
          <span style={styles.unit}>km/s</span>
          <span style={styles.label}>Duration</span>
          <input
            type="number"
            style={styles.input}
            value={duration}
            step={5}
            min={1}
            max={300}
            onChange={(e) => setDuration(Math.min(300, parseInt(e.target.value) || 30))}
          />
          <span style={styles.unit}>sec</span>
        </div>
        <div style={{ fontSize: 9, color: "#ff8800", marginBottom: 4, fontFamily: "'Courier New', Consolas, monospace" }}>
          Est. fuel: ~{Math.round(3000 * (Math.exp(deltaV / 30) - 1))} kg
          {deltaV > 5 && <span style={{ color: "#ff4444", marginLeft: 8 }}>EXCEEDS MAX (5 km/s)</span>}
        </div>
        <div style={{ display: "flex", alignItems: "center" }}>
          {feedback && (
            <span
              style={{
                ...styles.feedback,
                background: feedback.ok ? "#0a2a1a" : "#2a0a0a",
                color: feedback.ok ? "#00ff88" : "#ff4444",
              }}
            >
              {feedback.msg}
            </span>
          )}
          <button
            style={{
              ...styles.burnBtn,
              opacity: sending ? 0.5 : 1,
              cursor: sending ? "wait" : "pointer",
            }}
            onClick={handleBurn}
            disabled={sending}
          >
            <Send size={10} />
            {sending ? "Sending..." : "Execute Burn"}
          </button>
        </div>
      </div>

      {/* Agent-Generated Maneuver Candidates */}
      <div style={styles.tableWrap}>
        <table style={styles.table}>
          <thead>
            <tr>
              <th style={styles.th}>#</th>
              <th style={styles.th}>Name</th>
              <th style={styles.th}>&Delta;V</th>
              <th style={styles.th}>Fuel</th>
              <th style={styles.th}>Risk &darr;</th>
              <th style={styles.th}>Feas.</th>
              <th style={styles.th}>Status</th>
              <th style={styles.th}>Actions</th>
            </tr>
          </thead>
          <tbody>
            {maneuvers.length === 0 && (
              <tr>
                <td style={styles.td} colSpan={8}>
                  <span style={{ color: "#6b7fa3" }}>
                    No candidate maneuvers from agents
                  </span>
                </td>
              </tr>
            )}
            {maneuvers.map((m) => (
              <tr key={m.id}>
                <td style={{ ...styles.td, color: "#ffaa00", fontWeight: 700 }}>
                  {m.rank}
                </td>
                <td style={{ ...styles.td, maxWidth: 100, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {m.name}
                </td>
                <td style={styles.td}>{m.delta_v_ms.toFixed(1)} m/s</td>
                <td style={styles.td}>{m.fuel_cost_pct.toFixed(1)}%</td>
                <td style={styles.td}>
                  <span style={{ color: "#00ff88" }}>
                    -{(m.risk_reduction * 100).toFixed(0)}%
                  </span>
                </td>
                <td style={styles.td}>
                  <span style={{ color: feasibilityColor(m.feasibility) }}>
                    {m.feasibility.toUpperCase()}
                  </span>
                </td>
                <td style={styles.td}>
                  <span style={statusBadge(m.status)}>{m.status}</span>
                </td>
                <td style={styles.td}>
                  {(m.status === "pending" || m.status === "proposed") && (
                    <>
                      <button
                        style={{ ...styles.actionBtn, background: "#0a2a1a" }}
                        title="Approve"
                        onClick={() => handleApprove(m.id)}
                      >
                        <Check size={12} color="#00ff88" />
                      </button>
                      <button
                        style={{ ...styles.actionBtn, background: "#2a0a0a" }}
                        title="Reject"
                        onClick={() => handleReject(m.id)}
                      >
                        <X size={12} color="#ff4444" />
                      </button>
                    </>
                  )}
                  {m.status === "approved" && (
                    <span style={{ fontSize: 9, color: "#00ff88" }}>APPROVED</span>
                  )}
                  {m.status === "executed" && (
                    <span style={{ fontSize: 9, color: "#00aaff" }}>DONE</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default ManeuverWorkbench;
