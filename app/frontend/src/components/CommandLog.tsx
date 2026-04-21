import React from "react";
import { Send, Clock } from "lucide-react";
import type { Command } from "../types";

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
    gap: 6,
  },
  card: {
    background: "#101830",
    border: "1px solid #1a2040",
    borderRadius: 6,
    padding: 8,
    display: "flex",
    flexDirection: "column" as const,
    gap: 4,
  },
  row: {
    display: "flex",
    alignItems: "center",
    gap: 6,
  },
  cmdType: {
    fontSize: 11,
    fontWeight: 700,
    color: "#e0e8f0",
  },
  meta: {
    fontSize: 10,
    color: "#4a5a7a",
    display: "flex",
    alignItems: "center",
    gap: 4,
  },
  summary: {
    fontSize: 10,
    color: "#6b7fa3",
    fontStyle: "italic" as const,
  },
};

function statusBadge(
  status: string
): { bg: string; fg: string; glow: string } {
  switch (status) {
    case "pending":
      return { bg: "#1a2040", fg: "#6b7fa3", glow: "none" };
    case "transmitted":
      return { bg: "#0a1a3a", fg: "#00aaff", glow: "0 0 6px #00aaff44" };
    case "received":
      return { bg: "#0a2a1a", fg: "#00ff88", glow: "0 0 6px #00ff8844" };
    case "failed":
      return { bg: "#2a0a0a", fg: "#ff4444", glow: "0 0 6px #ff444444" };
    default:
      return { bg: "#1a2040", fg: "#6b7fa3", glow: "none" };
  }
}

interface Props {
  commands: Command[];
}

const CommandLog: React.FC<Props> = ({ commands }) => {
  return (
    <div style={styles.panel}>
      <div style={styles.title}>
        <Send
          size={12}
          style={{ verticalAlign: "middle", marginRight: 6 }}
          color="#00aaff"
        />
        Command Log
      </div>
      <div style={styles.list}>
        {commands.length === 0 && (
          <div style={{ color: "#6b7fa3", fontSize: 12 }}>
            No commands in queue
          </div>
        )}
        {commands.map((c) => {
          const sb = statusBadge(c.status);
          return (
            <div key={c.id} style={styles.card}>
              <div style={styles.row}>
                <span style={styles.cmdType}>{c.type}</span>
                <span
                  style={{
                    fontSize: 9,
                    fontWeight: 700,
                    letterSpacing: 1,
                    padding: "2px 6px",
                    borderRadius: 3,
                    background: sb.bg,
                    color: sb.fg,
                    boxShadow: sb.glow,
                    textTransform: "uppercase",
                    marginLeft: "auto",
                  }}
                >
                  {c.status}
                </span>
              </div>
              <div style={styles.meta}>
                <Clock size={9} />
                TX: {c.transmit_time}
                <span style={{ margin: "0 4px", color: "#2a3060" }}>|</span>
                ETA: {c.estimated_receive_time}
              </div>
              <div style={styles.summary}>{c.payload_summary}</div>
            </div>
          );
        })}
      </div>
    </div>
  );
};

export default CommandLog;
