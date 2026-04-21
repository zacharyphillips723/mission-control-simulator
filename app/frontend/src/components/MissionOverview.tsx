import React from "react";
import { Fuel, Gauge, Radio, Shield, Rocket } from "lucide-react";
import type { MissionState } from "../types";

const styles: Record<string, React.CSSProperties> = {
  panel: {
    background: "#0d1224",
    border: "1px solid #1a2040",
    borderRadius: 8,
    padding: 16,
    height: "100%",
    display: "flex",
    flexDirection: "column",
    gap: 12,
  },
  title: {
    fontSize: 11,
    fontWeight: 700,
    letterSpacing: 2,
    color: "#6b7fa3",
    textTransform: "uppercase" as const,
    marginBottom: 4,
  },
  row: {
    display: "flex",
    alignItems: "center",
    gap: 8,
    fontSize: 13,
  },
  label: {
    color: "#6b7fa3",
    width: 90,
    flexShrink: 0,
  },
  value: {
    color: "#e0e8f0",
    fontVariantNumeric: "tabular-nums",
  },
  gaugeTrack: {
    flex: 1,
    height: 10,
    background: "#1a2040",
    borderRadius: 5,
    overflow: "hidden",
    position: "relative" as const,
  },
  statusDot: {
    width: 8,
    height: 8,
    borderRadius: "50%",
    display: "inline-block",
  },
};

function GaugeBar({ pct, color }: { pct: number; color: string }) {
  return (
    <div style={styles.gaugeTrack}>
      <div
        style={{
          width: `${Math.max(0, Math.min(100, pct))}%`,
          height: "100%",
          background: color,
          borderRadius: 5,
          transition: "width 0.6s ease",
          boxShadow: `0 0 8px ${color}66`,
        }}
      />
    </div>
  );
}

function engineColor(status: string) {
  switch (status) {
    case "online":
      return "#00ff88";
    case "standby":
      return "#ffaa00";
    case "offline":
      return "#6b7fa3";
    default:
      return "#ff4444";
  }
}

interface Props {
  state: MissionState | null;
}

const MissionOverview: React.FC<Props> = ({ state }) => {
  if (!state) {
    return (
      <div style={styles.panel}>
        <div style={styles.title}>Mission Overview</div>
        <div style={{ color: "#6b7fa3", fontSize: 12 }}>Awaiting telemetry...</div>
      </div>
    );
  }

  const pos = state.position;
  const vel = state.velocity;

  return (
    <div style={styles.panel}>
      <div style={styles.title}>Mission Overview</div>

      <div style={styles.row}>
        <Rocket size={14} color="#00ff88" />
        <span style={styles.label}>Spacecraft</span>
        <span style={{ ...styles.value, color: "#00ff88", fontWeight: 700 }}>
          {state.spacecraft_name}
        </span>
      </div>

      <div style={styles.row}>
        <span style={styles.label}>Position</span>
        <span style={styles.value}>
          X {pos.x.toFixed(3)} &nbsp; Y {pos.y.toFixed(3)} &nbsp; Z{" "}
          {pos.z.toFixed(3)} AU
        </span>
      </div>

      <div style={styles.row}>
        <Gauge size={14} color="#ffaa00" />
        <span style={styles.label}>Velocity</span>
        <span style={styles.value}>{vel.magnitude.toFixed(2)} km/s</span>
      </div>

      <div style={styles.row}>
        <Fuel size={14} color="#00ff88" />
        <span style={styles.label}>Fuel</span>
        <GaugeBar
          pct={state.fuel_remaining_pct}
          color={state.fuel_remaining_pct > 30 ? "#00ff88" : "#ff4444"}
        />
        <span style={{ ...styles.value, width: 40, textAlign: "right" }}>
          {state.fuel_remaining_pct.toFixed(1)}%
        </span>
      </div>

      <div style={styles.row}>
        <Shield size={14} color="#00aaff" />
        <span style={styles.label}>Hull</span>
        <GaugeBar
          pct={state.hull_integrity_pct}
          color={state.hull_integrity_pct > 50 ? "#00aaff" : "#ff4444"}
        />
        <span style={{ ...styles.value, width: 40, textAlign: "right" }}>
          {state.hull_integrity_pct.toFixed(1)}%
        </span>
      </div>

      <div style={styles.row}>
        <Radio size={14} color="#ffaa00" />
        <span style={styles.label}>Comm Delay</span>
        <span style={styles.value}>{state.comm_delay_seconds.toFixed(1)}s</span>
      </div>

      <div style={styles.row}>
        <span style={styles.label}>Engine</span>
        <span
          style={{
            ...styles.statusDot,
            background: engineColor(state.engine_status),
            boxShadow: `0 0 6px ${engineColor(state.engine_status)}`,
          }}
        />
        <span style={{ ...styles.value, textTransform: "uppercase" }}>
          {state.engine_status}
        </span>
      </div>

      <div style={styles.row}>
        <span style={styles.label}>Target</span>
        <span style={styles.value}>{state.target}</span>
      </div>
    </div>
  );
};

export default MissionOverview;
