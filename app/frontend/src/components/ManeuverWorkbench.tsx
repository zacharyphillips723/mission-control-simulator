import React from "react";
import { Check, X } from "lucide-react";
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
};

function feasibilityColor(f: string) {
  switch (f) {
    case "high":
      return "#00ff88";
    case "medium":
      return "#ffaa00";
    default:
      return "#ff4444";
  }
}

function statusBadge(s: string): React.CSSProperties {
  const map: Record<string, { bg: string; fg: string }> = {
    pending: { bg: "#1a2040", fg: "#6b7fa3" },
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
}

const ManeuverWorkbench: React.FC<Props> = ({ maneuvers }) => {
  return (
    <div style={styles.panel}>
      <div style={styles.title}>Maneuver Workbench</div>
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
                    No candidate maneuvers
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
                  <button
                    style={{
                      ...styles.actionBtn,
                      background: "#0a2a1a",
                    }}
                    title="Approve"
                  >
                    <Check size={12} color="#00ff88" />
                  </button>
                  <button
                    style={{
                      ...styles.actionBtn,
                      background: "#2a0a0a",
                    }}
                    title="Reject"
                  >
                    <X size={12} color="#ff4444" />
                  </button>
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
