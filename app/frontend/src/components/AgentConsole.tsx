import React, { useEffect, useRef } from "react";
import { Bot } from "lucide-react";
import type { AgentDecision } from "../types";

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

interface Props {
  decisions: AgentDecision[];
}

const AgentConsole: React.FC<Props> = ({ decisions }) => {
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [decisions]);

  return (
    <div style={styles.panel}>
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
        <div ref={bottomRef} />
      </div>
    </div>
  );
};

export default AgentConsole;
