import React, { useEffect, useRef, useState } from "react";
import { MessageSquare, ArrowRight, Radio } from "lucide-react";
import type { AgentMessage } from "../types";

const AGENT_COLORS: Record<string, string> = {
  flight_dynamics: "#00aaff",
  hazard_assessment: "#ff8800",
  communications: "#aa66ff",
  mission_commander: "#ff3621",
  autopilot: "#00ff88",
  operator: "#e0e8f0",
  spacecraft: "#00ff88",
  broadcast: "#ffaa00",
};

const MSG_TYPE_ICONS: Record<string, { color: string; label: string }> = {
  analysis: { color: "#00aaff", label: "ANALYSIS" },
  alert: { color: "#ff4444", label: "ALERT" },
  timing_plan: { color: "#aa66ff", label: "TIMING" },
  order: { color: "#ff3621", label: "ORDER" },
  acknowledgment: { color: "#00ff88", label: "ACK" },
  recommendation: { color: "#ffaa00", label: "REC" },
  status: { color: "#6b7fa3", label: "STATUS" },
};

function agentLabel(name: string): string {
  return name
    .split("_")
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
}

function agentColor(name: string): string {
  return AGENT_COLORS[name] ?? "#6b7fa3";
}

function parseContent(raw: string): string {
  try {
    const obj = JSON.parse(raw);
    if (obj.summary) return obj.summary;
    if (obj.recommendation) return typeof obj.recommendation === "string" ? obj.recommendation : JSON.stringify(obj.recommendation);
    if (obj.message) return obj.message;
    if (obj.decision) return obj.decision;
    if (obj.action) return obj.action;
    // Fallback: first string value
    for (const v of Object.values(obj)) {
      if (typeof v === "string" && v.length > 10) return v.slice(0, 120);
    }
    return JSON.stringify(obj).slice(0, 100);
  } catch {
    return String(raw).slice(0, 120);
  }
}

function formatTime(ts: string): string {
  try {
    const d = new Date(ts);
    return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  } catch {
    return ts;
  }
}

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
  feed: {
    flex: 1,
    overflowY: "auto" as const,
    display: "flex",
    flexDirection: "column" as const,
    gap: 6,
    background: "#080c16",
    borderRadius: 4,
    padding: 8,
    border: "1px solid #111828",
  },
  msg: {
    borderBottom: "1px solid #111828",
    paddingBottom: 6,
    fontSize: 10,
  },
  msgHeader: {
    display: "flex",
    alignItems: "center",
    gap: 4,
    marginBottom: 3,
  },
  agentTag: {
    fontSize: 8,
    fontWeight: 700,
    letterSpacing: 1,
    padding: "1px 4px",
    borderRadius: 2,
    textTransform: "uppercase" as const,
  },
  arrow: {
    color: "#4a5a7a",
  },
  typeTag: {
    fontSize: 7,
    fontWeight: 700,
    letterSpacing: 1,
    padding: "1px 4px",
    borderRadius: 2,
    textTransform: "uppercase" as const,
    marginLeft: "auto",
  },
  content: {
    color: "#a0b0c8",
    fontSize: 10,
    lineHeight: 1.4,
    wordBreak: "break-word" as const,
    paddingLeft: 8,
    borderLeft: "2px solid #1a2040",
  },
  time: {
    fontSize: 8,
    color: "#4a5a7a",
    marginTop: 2,
    textAlign: "right" as const,
  },
  empty: {
    color: "#4a5a7a",
    fontSize: 11,
    padding: 12,
    textAlign: "center" as const,
  },
  tickDivider: {
    display: "flex",
    alignItems: "center",
    gap: 6,
    padding: "4px 0",
    fontSize: 8,
    color: "#4a5a7a",
    letterSpacing: 1,
  },
  tickLine: {
    flex: 1,
    height: 1,
    background: "#1a2040",
  },
};

interface Props {
  messages: AgentMessage[];
}

const STAGGER_DELAY_MS = 250; // delay between each message appearing

const AgentCommsFeed: React.FC<Props> = ({ messages }) => {
  // Track which message IDs have been revealed (for stagger animation)
  const revealedRef = useRef<Set<string>>(new Set());
  const [revealed, setRevealed] = useState<Set<string>>(new Set());

  useEffect(() => {
    // Find new message IDs not yet revealed
    const newIds = messages
      .map((m) => m.message_id)
      .filter((id) => !revealedRef.current.has(id));

    if (newIds.length === 0) return;

    // Stagger-reveal each new message
    const timers: ReturnType<typeof setTimeout>[] = [];
    newIds.forEach((id, i) => {
      const t = setTimeout(() => {
        revealedRef.current.add(id);
        setRevealed((prev) => new Set(prev).add(id));
      }, i * STAGGER_DELAY_MS);
      timers.push(t);
    });

    return () => timers.forEach(clearTimeout);
  }, [messages]);

  // Backend returns newest-first (ORDER BY created_at DESC) — use directly
  // Group messages by tick_id
  const grouped: { tick: string | null; msgs: AgentMessage[] }[] = [];
  let currentTick: string | null = null;
  for (const m of messages) {
    if (m.tick_id !== currentTick) {
      currentTick = m.tick_id;
      grouped.push({ tick: currentTick, msgs: [] });
    }
    grouped[grouped.length - 1].msgs.push(m);
  }

  return (
    <div style={styles.panel}>
      <div style={styles.title}>
        <MessageSquare size={12} color="#aa66ff" />
        Agent Communications
        <span style={{ marginLeft: "auto", fontSize: 9, color: "#4a5a7a", fontWeight: 400 }}>
          {messages.length} msgs
        </span>
      </div>
      <div style={styles.feed}>
        {messages.length === 0 && (
          <div style={styles.empty}>
            <Radio size={14} style={{ opacity: 0.4, marginBottom: 4 }} />
            <br />
            Awaiting agent communications...
          </div>
        )}
        {grouped.map((group, gi) => (
          <React.Fragment key={gi}>
            {group.tick && (
              <div style={styles.tickDivider}>
                <div style={styles.tickLine} />
                <span>TICK {group.tick.slice(0, 8).toUpperCase()}</span>
                <div style={styles.tickLine} />
              </div>
            )}
            {group.msgs.map((m) => {
              const mt = MSG_TYPE_ICONS[m.message_type] ?? { color: "#6b7fa3", label: m.message_type.toUpperCase() };
              const isVisible = revealed.has(m.message_id);
              return (
                <div
                  key={m.message_id}
                  style={{
                    ...styles.msg,
                    opacity: isVisible ? 1 : 0,
                    transform: isVisible ? "translateY(0)" : "translateY(6px)",
                    transition: "opacity 0.3s ease-out, transform 0.3s ease-out",
                  }}
                >
                  <div style={styles.msgHeader}>
                    <span
                      style={{
                        ...styles.agentTag,
                        background: agentColor(m.from_agent) + "22",
                        color: agentColor(m.from_agent),
                        border: `1px solid ${agentColor(m.from_agent)}44`,
                      }}
                    >
                      {agentLabel(m.from_agent)}
                    </span>
                    <ArrowRight size={10} style={styles.arrow} />
                    <span
                      style={{
                        ...styles.agentTag,
                        background: agentColor(m.to_agent) + "22",
                        color: agentColor(m.to_agent),
                        border: `1px solid ${agentColor(m.to_agent)}44`,
                      }}
                    >
                      {agentLabel(m.to_agent)}
                    </span>
                    <span
                      style={{
                        ...styles.typeTag,
                        background: mt.color + "22",
                        color: mt.color,
                        border: `1px solid ${mt.color}44`,
                      }}
                    >
                      {mt.label}
                    </span>
                  </div>
                  <div style={styles.content}>{parseContent(m.content)}</div>
                  <div style={styles.time}>{formatTime(m.created_at)}</div>
                </div>
              );
            })}
          </React.Fragment>
        ))}
      </div>
    </div>
  );
};

export default AgentCommsFeed;
