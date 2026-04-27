import React from "react";
import { CheckCircle, XCircle, Fuel, Shield } from "lucide-react";
import type { MissionState } from "../types";

const OUTCOMES: Record<string, {
  title: string;
  subtitle: string;
  icon: React.ReactNode;
  bg: string;
  glow: string;
  fg: string;
  border: string;
}> = {
  arrived: {
    title: "MISSION COMPLETE",
    subtitle: "Odyssey has reached Earth orbit. Welcome home.",
    icon: <CheckCircle size={48} />,
    bg: "radial-gradient(ellipse at center, #0a2a1a 0%, #0a0e1a 70%)",
    glow: "#00ff88",
    fg: "#00ff88",
    border: "#00ff8866",
  },
  fuel_exhausted: {
    title: "MISSION FAILED",
    subtitle: "Fuel reserves depleted. Odyssey is adrift.",
    icon: <Fuel size={48} />,
    bg: "radial-gradient(ellipse at center, #2a1a00 0%, #0a0e1a 70%)",
    glow: "#ff8800",
    fg: "#ff8800",
    border: "#ff880066",
  },
  collision: {
    title: "MISSION LOST",
    subtitle: "Odyssey collided with a hazard. All hands lost.",
    icon: <XCircle size={48} />,
    bg: "radial-gradient(ellipse at center, #2a0a0a 0%, #0a0e1a 70%)",
    glow: "#ff4444",
    fg: "#ff4444",
    border: "#ff444466",
  },
  hull_failure: {
    title: "MISSION FAILED",
    subtitle: "Critical hull breach. Structural integrity lost.",
    icon: <Shield size={48} />,
    bg: "radial-gradient(ellipse at center, #2a0a0a 0%, #0a0e1a 70%)",
    glow: "#ff4444",
    fg: "#ff4444",
    border: "#ff444466",
  },
};

interface Props {
  state: MissionState;
  onDismiss?: () => void;
}

const MissionOutcome: React.FC<Props> = ({ state, onDismiss }) => {
  const outcome = state.mission_outcome;
  if (!outcome || !OUTCOMES[outcome]) return null;

  const o = OUTCOMES[outcome];

  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        zIndex: 9999,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        background: "rgba(0, 0, 0, 0.85)",
        backdropFilter: "blur(8px)",
        fontFamily: "'Courier New', Consolas, monospace",
      }}
    >
      <div
        style={{
          background: o.bg,
          border: `2px solid ${o.border}`,
          borderRadius: 16,
          padding: "48px 64px",
          textAlign: "center",
          maxWidth: 560,
          boxShadow: `0 0 80px ${o.glow}33, 0 0 40px ${o.glow}22`,
        }}
      >
        {/* Icon */}
        <div
          style={{
            color: o.fg,
            marginBottom: 20,
            filter: `drop-shadow(0 0 20px ${o.glow})`,
          }}
        >
          {o.icon}
        </div>

        {/* Title */}
        <div
          style={{
            fontSize: 28,
            fontWeight: 900,
            letterSpacing: 6,
            color: o.fg,
            marginBottom: 12,
            textShadow: `0 0 30px ${o.glow}`,
          }}
        >
          {o.title}
        </div>

        {/* Subtitle */}
        <div
          style={{
            fontSize: 14,
            color: "#c8d6e5",
            marginBottom: 24,
            lineHeight: 1.6,
          }}
        >
          {o.subtitle}
        </div>

        {/* Detail */}
        {state.mission_outcome_detail && (
          <div
            style={{
              fontSize: 11,
              color: "#a0b0c8",
              marginBottom: 24,
              padding: "12px 16px",
              background: "#0a0e1a",
              borderRadius: 6,
              border: "1px solid #1a2040",
              textAlign: "left",
              lineHeight: 1.5,
            }}
          >
            {state.mission_outcome_detail}
          </div>
        )}

        {/* Stats */}
        <div
          style={{
            display: "flex",
            justifyContent: "center",
            gap: 24,
            marginBottom: 28,
            fontSize: 10,
            color: "#6b7fa3",
            letterSpacing: 1,
          }}
        >
          <div>
            <div style={{ color: "#4a5a7a", fontSize: 8, textTransform: "uppercase", marginBottom: 2 }}>
              Duration
            </div>
            <div style={{ color: "#e0e8f0", fontSize: 13 }}>
              {state.mission_elapsed_days.toFixed(1)} days
            </div>
          </div>
          <div>
            <div style={{ color: "#4a5a7a", fontSize: 8, textTransform: "uppercase", marginBottom: 2 }}>
              Fuel Remaining
            </div>
            <div style={{ color: state.fuel_remaining_pct < 10 ? "#ff4444" : "#e0e8f0", fontSize: 13 }}>
              {state.fuel_remaining_pct.toFixed(1)}%
            </div>
          </div>
          <div>
            <div style={{ color: "#4a5a7a", fontSize: 8, textTransform: "uppercase", marginBottom: 2 }}>
              Distance to Earth
            </div>
            <div style={{ color: "#e0e8f0", fontSize: 13 }}>
              {state.distance_to_earth_km
                ? `${(state.distance_to_earth_km / 1.496e8).toFixed(3)} AU`
                : "—"}
            </div>
          </div>
        </div>

        {/* Dismiss */}
        {onDismiss && (
          <button
            onClick={onDismiss}
            style={{
              padding: "8px 24px",
              borderRadius: 6,
              border: `1px solid ${o.border}`,
              background: "#0d1224",
              color: o.fg,
              fontSize: 12,
              fontWeight: 700,
              letterSpacing: 2,
              cursor: "pointer",
              fontFamily: "inherit",
              textTransform: "uppercase",
            }}
          >
            {outcome === "arrived" ? "View Dashboard" : "Reset & Try Again"}
          </button>
        )}
      </div>
    </div>
  );
};

export default MissionOutcome;
