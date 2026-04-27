import React from "react";
import { Shield, AlertTriangle, Anchor, Zap, ChevronRight } from "lucide-react";
import type { CaptainState } from "../types";

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
    display: "flex",
    alignItems: "center",
    gap: 6,
  },
  alertDot: {
    width: 8,
    height: 8,
    borderRadius: "50%",
    display: "inline-block",
  },
  statsRow: {
    display: "flex",
    gap: 10,
    marginBottom: 10,
    flexWrap: "wrap" as const,
  },
  stat: {
    background: "#080c16",
    border: "1px solid #1a2040",
    borderRadius: 4,
    padding: "6px 10px",
    flex: "1 1 auto",
    minWidth: 70,
  },
  statLabel: {
    fontSize: 8,
    color: "#4a5a7a",
    fontWeight: 700,
    letterSpacing: 1,
    textTransform: "uppercase" as const,
  },
  statValue: {
    fontSize: 14,
    fontWeight: 700,
    fontFamily: "'Courier New', Consolas, monospace",
    marginTop: 2,
  },
  decisionLog: {
    flex: 1,
    overflowY: "auto" as const,
    fontFamily: "'Courier New', Consolas, monospace",
    fontSize: 10,
    lineHeight: "1.5",
    display: "flex",
    flexDirection: "column" as const,
    gap: 6,
    background: "#080c16",
    borderRadius: 4,
    padding: 8,
    border: "1px solid #111828",
  },
  decisionEntry: {
    borderBottom: "1px solid #111828",
    paddingBottom: 5,
  },
  decisionHeader: {
    display: "flex",
    alignItems: "center",
    gap: 5,
    marginBottom: 2,
  },
  actionBadge: {
    fontSize: 8,
    fontWeight: 700,
    letterSpacing: 1,
    padding: "1px 5px",
    borderRadius: 3,
    textTransform: "uppercase" as const,
  },
  reasoning: {
    fontSize: 10,
    color: "#8a9ab5",
    lineHeight: "1.4",
  },
  lastAction: {
    background: "#080c16",
    border: "1px solid #1a2040",
    borderRadius: 4,
    padding: 8,
    marginBottom: 8,
    fontSize: 10,
  },
};

function alertColor(level: string): string {
  switch (level) {
    case "red": return "#ff4444";
    case "yellow": return "#ffaa00";
    default: return "#00ff88";
  }
}

function actionStyle(action: string): { bg: string; fg: string } {
  switch (action) {
    case "emergency_evasion":
      return { bg: "#2a0a0a", fg: "#ff4444" };
    case "override_command":
      return { bg: "#2a1a00", fg: "#ff8800" };
    case "modify_command":
      return { bg: "#2a2a00", fg: "#ffaa00" };
    case "micro_correction":
      return { bg: "#0a1a2a", fg: "#00aaff" };
    case "enter_safe_mode":
      return { bg: "#2a0a1a", fg: "#ff44aa" };
    case "approve_command":
      return { bg: "#0a2a1a", fg: "#00ff88" };
    default:
      return { bg: "#1a2040", fg: "#6b7fa3" };
  }
}

function actionIcon(action: string) {
  switch (action) {
    case "emergency_evasion": return <Zap size={10} color="#ff4444" />;
    case "override_command":
    case "modify_command": return <AlertTriangle size={10} color="#ffaa00" />;
    case "micro_correction": return <ChevronRight size={10} color="#00aaff" />;
    default: return <Anchor size={10} color="#6b7fa3" />;
  }
}

function formatAction(action: string): string {
  return action.replace(/_/g, " ").toUpperCase();
}

interface Props {
  captain: CaptainState | null | undefined;
}

const CaptainPanel: React.FC<Props> = ({ captain }) => {
  if (!captain || !captain.captain_available) {
    return (
      <div style={styles.panel}>
        <div style={styles.title}>
          <Shield size={12} /> Ship Captain
        </div>
        <div style={{ color: "#4a5a7a", fontSize: 10, fontFamily: "'Courier New', Consolas, monospace" }}>
          Captain module initializing...
        </div>
      </div>
    );
  }

  const decisions = captain.recent_decisions || [];
  const aColor = alertColor(captain.alert_level);
  const as = actionStyle(captain.last_decision_action);

  return (
    <div style={styles.panel}>
      <div style={styles.title}>
        <Shield size={12} color={aColor} />
        Ship Captain
        <span style={{ ...styles.alertDot, background: aColor, boxShadow: `0 0 6px ${aColor}` }} />
        <span style={{ fontSize: 9, color: aColor, fontWeight: 700 }}>
          {captain.alert_level.toUpperCase()}
        </span>
      </div>

      {/* Current status */}
      <div style={styles.lastAction}>
        <div style={{ display: "flex", alignItems: "center", gap: 5, marginBottom: 3 }}>
          {actionIcon(captain.last_decision_action)}
          <span style={{
            ...styles.actionBadge,
            background: as.bg,
            color: as.fg,
          }}>
            {formatAction(captain.last_decision_action)}
          </span>
        </div>
        <div style={{ color: "#8a9ab5", fontSize: 10 }}>
          {captain.last_decision_reasoning || "Awaiting orders from Mission Control."}
        </div>
      </div>

      {/* Stats */}
      <div style={styles.statsRow}>
        <div style={styles.stat}>
          <div style={styles.statLabel}>Overrides</div>
          <div style={{ ...styles.statValue, color: captain.total_overrides > 0 ? "#ff8800" : "#6b7fa3" }}>
            {captain.total_overrides}
          </div>
        </div>
        <div style={styles.stat}>
          <div style={styles.statLabel}>Evasions</div>
          <div style={{ ...styles.statValue, color: captain.total_evasions > 0 ? "#ff4444" : "#6b7fa3" }}>
            {captain.total_evasions}
          </div>
        </div>
        <div style={styles.stat}>
          <div style={styles.statLabel}>Corrections</div>
          <div style={{ ...styles.statValue, color: captain.total_micro_corrections > 0 ? "#00aaff" : "#6b7fa3" }}>
            {captain.total_micro_corrections}
          </div>
        </div>
        <div style={styles.stat}>
          <div style={styles.statLabel}>Approved</div>
          <div style={{ ...styles.statValue, color: "#00ff88" }}>
            {captain.total_commands_approved}
          </div>
        </div>
        <div style={styles.stat}>
          <div style={styles.statLabel}>Fuel Used</div>
          <div style={{ ...styles.statValue, color: "#c8d6e5" }}>
            {captain.fuel_used_by_captain_kg.toFixed(1)}
          </div>
        </div>
      </div>

      {/* Decision log */}
      <div style={{ fontSize: 9, color: "#4a5a7a", fontWeight: 700, letterSpacing: 1, marginBottom: 4, textTransform: "uppercase" }}>
        Decision Log
      </div>
      <div style={styles.decisionLog}>
        {decisions.length === 0 && (
          <div style={{ color: "#4a5a7a" }}>No decisions recorded yet</div>
        )}
        {[...decisions].reverse().slice(0, 15).map((d, i) => {
          const ds = actionStyle(d.action);
          return (
            <div key={d.decision_id || i} style={styles.decisionEntry}>
              <div style={styles.decisionHeader}>
                {actionIcon(d.action)}
                <span style={{ ...styles.actionBadge, background: ds.bg, color: ds.fg }}>
                  {formatAction(d.action)}
                </span>
                {d.delta_v > 0 && (
                  <span style={{ fontSize: 9, color: "#c8d6e5" }}>
                    dv={d.delta_v.toFixed(4)} km/s
                  </span>
                )}
                <span style={{ fontSize: 8, color: "#4a5a7a", marginLeft: "auto" }}>
                  P{d.priority_level}
                </span>
              </div>
              <div style={styles.reasoning}>{d.reasoning}</div>
            </div>
          );
        })}
      </div>
    </div>
  );
};

export default CaptainPanel;
