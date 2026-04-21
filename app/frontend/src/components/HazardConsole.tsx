import React from "react";
import {
  AlertTriangle,
  Radiation,
  Flame,
  CircleDot,
  Zap,
  Snowflake,
} from "lucide-react";
import type { Hazard } from "../types";

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
  list: {
    flex: 1,
    overflowY: "auto" as const,
    display: "flex",
    flexDirection: "column" as const,
    gap: 8,
  },
  card: {
    background: "#101830",
    border: "1px solid #1a2040",
    borderRadius: 6,
    padding: 10,
    display: "flex",
    flexDirection: "column" as const,
    gap: 6,
  },
  row: {
    display: "flex",
    alignItems: "center",
    gap: 8,
  },
  badge: {
    fontSize: 9,
    fontWeight: 700,
    letterSpacing: 1,
    padding: "2px 6px",
    borderRadius: 3,
    textTransform: "uppercase" as const,
  },
  riskTrack: {
    flex: 1,
    height: 6,
    background: "#1a2040",
    borderRadius: 3,
    overflow: "hidden",
  },
  meta: {
    fontSize: 10,
    color: "#4a5a7a",
  },
};

function riskColor(score: number): string {
  if (score < 0.3) return "#00ff88";
  if (score < 0.7) return "#ffaa00";
  return "#ff4444";
}

function statusBadge(status: string): React.CSSProperties {
  const colors: Record<string, { bg: string; fg: string }> = {
    tracking: { bg: "#1a2040", fg: "#6b7fa3" },
    approaching: { bg: "#2a2800", fg: "#ffaa00" },
    imminent: { bg: "#2a0a0a", fg: "#ff4444" },
    passed: { bg: "#0a2a1a", fg: "#00ff88" },
  };
  const c = colors[status] ?? colors.tracking;
  return { background: c.bg, color: c.fg };
}

function HazardIcon({ type }: { type: string }) {
  const size = 14;
  switch (type) {
    case "asteroid":
      return <CircleDot size={size} color="#ffaa00" />;
    case "solar_flare":
      return <Flame size={size} color="#ff6600" />;
    case "debris":
      return <Snowflake size={size} color="#6b7fa3" />;
    case "radiation_belt":
      return <Radiation size={size} color="#ff44ff" />;
    case "micro_meteoroid":
      return <Zap size={size} color="#ffaa00" />;
    default:
      return <AlertTriangle size={size} color="#ffaa00" />;
  }
}

interface Props {
  hazards: Hazard[];
}

const HazardConsole: React.FC<Props> = ({ hazards }) => {
  return (
    <div style={styles.panel}>
      <div style={styles.title}>
        Hazard Console{" "}
        <span style={{ color: "#ff4444", fontSize: 10 }}>
          ({hazards.length} active)
        </span>
      </div>

      <div style={styles.list}>
        {hazards.length === 0 && (
          <div style={{ color: "#00ff88", fontSize: 12 }}>
            No active hazards detected
          </div>
        )}
        {hazards.map((h) => {
          const rc = riskColor(h.risk_score);
          return (
            <div
              key={h.id}
              style={{
                ...styles.card,
                borderLeftColor: rc,
                borderLeftWidth: 3,
              }}
            >
              <div style={styles.row}>
                <HazardIcon type={h.type} />
                <span style={{ color: "#e0e8f0", fontSize: 12, fontWeight: 600 }}>
                  {h.name}
                </span>
                <span style={{ ...styles.badge, ...statusBadge(h.status) }}>
                  {h.status}
                </span>
              </div>

              <div style={styles.row}>
                <span style={{ fontSize: 10, color: "#6b7fa3", width: 60 }}>
                  Risk {(h.risk_score * 100).toFixed(0)}%
                </span>
                <div style={styles.riskTrack}>
                  <div
                    style={{
                      width: `${h.risk_score * 100}%`,
                      height: "100%",
                      background: rc,
                      borderRadius: 3,
                      boxShadow: `0 0 6px ${rc}66`,
                      transition: "width 0.4s ease",
                    }}
                  />
                </div>
              </div>

              <div style={styles.meta}>
                Closest approach: {h.closest_approach_km.toLocaleString()} km &mdash;{" "}
                {h.closest_approach_time}
              </div>
              <div style={{ ...styles.meta, color: "#5a6a8a" }}>
                {h.description}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};

export default HazardConsole;
