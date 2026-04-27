import React, { useRef, useEffect } from "react";
import { ScrollText } from "lucide-react";
import type { MissionEvent } from "../types";

const s: Record<string, React.CSSProperties> = {
  root: {
    height: "100%",
    display: "flex",
    flexDirection: "column",
    background: "#0d1224",
    border: "1px solid #1a2040",
    borderRadius: 8,
    overflow: "hidden",
    fontFamily: "'Courier New', Consolas, monospace",
  },
  header: {
    display: "flex",
    alignItems: "center",
    gap: 6,
    padding: "8px 12px",
    fontSize: 9,
    fontWeight: 700,
    letterSpacing: 2,
    color: "#6b7fa3",
    textTransform: "uppercase" as const,
    borderBottom: "1px solid #1a2040",
    flexShrink: 0,
  },
  list: {
    flex: 1,
    overflow: "auto",
    padding: "4px 0",
  },
  item: {
    display: "flex",
    gap: 8,
    padding: "3px 10px",
    fontSize: 9,
    lineHeight: 1.5,
    borderBottom: "1px solid #0a0e1a",
  },
  time: {
    flexShrink: 0,
    width: 48,
    color: "#4a5a7a",
    fontVariantNumeric: "tabular-nums",
    textAlign: "right" as const,
  },
  dot: {
    flexShrink: 0,
    width: 6,
    height: 6,
    borderRadius: 3,
    marginTop: 3,
  },
  summary: {
    color: "#a0b0c8",
    flex: 1,
    overflow: "hidden",
    textOverflow: "ellipsis",
    whiteSpace: "nowrap" as const,
  },
  empty: {
    padding: 16,
    color: "#4a5a7a",
    fontSize: 10,
    textAlign: "center" as const,
  },
};

const TYPE_COLORS: Record<string, string> = {
  mission_start: "#00ff88",
  mission_arrived: "#00ff88",
  mission_fuel_exhausted: "#ff4444",
  mission_hull_failure: "#ff4444",
  burn_executed: "#00aaff",
  captain_evasion: "#ff6644",
  captain_micro_correction: "#ffaa00",
  captain_override_command: "#ff2244",
  captain_modify_command: "#ff8844",
  captain_approve_command: "#00ff88",
  hazard_injected: "#ff6644",
  scenario_loaded: "#aa66ff",
  autopilot_correction: "#00aaff",
  model_drift_retrain: "#ffaa00",
};

function formatSimTime(s: number): string {
  const d = Math.floor(s / 86400);
  const h = Math.floor((s % 86400) / 3600);
  if (d > 0) return `${d}d${h}h`;
  const m = Math.floor((s % 3600) / 60);
  if (h > 0) return `${h}h${m}m`;
  return `${m}m`;
}

interface Props {
  events: MissionEvent[];
}

const EventFeed: React.FC<Props> = ({ events }) => {
  const listRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (listRef.current) {
      listRef.current.scrollTop = listRef.current.scrollHeight;
    }
  }, [events.length]);

  return (
    <div style={s.root}>
      <div style={s.header}>
        <ScrollText size={10} color="#6b7fa3" />
        Mission Events
        {events.length > 0 && (
          <span style={{ marginLeft: "auto", color: "#4a5a7a" }}>
            {events.length}
          </span>
        )}
      </div>
      <div style={s.list} ref={listRef}>
        {events.length === 0 ? (
          <div style={s.empty}>No events yet — start a mission</div>
        ) : (
          events.map((e) => (
            <div key={e.event_id} style={s.item}>
              <span style={s.time}>{formatSimTime(e.simulation_time_s)}</span>
              <span
                style={{
                  ...s.dot,
                  background: TYPE_COLORS[e.event_type] || "#6b7fa3",
                  boxShadow: `0 0 4px ${TYPE_COLORS[e.event_type] || "#6b7fa3"}44`,
                }}
              />
              <span style={s.summary} title={e.summary}>
                {e.summary}
              </span>
            </div>
          ))
        )}
      </div>
    </div>
  );
};

export default EventFeed;
