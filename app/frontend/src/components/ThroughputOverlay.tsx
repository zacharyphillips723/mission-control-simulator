import React from "react";
import { Activity, Database, Zap } from "lucide-react";
import type { ThroughputCurrent } from "../types";

const s: Record<string, React.CSSProperties> = {
  overlay: {
    position: "absolute",
    top: 52,
    right: 12,
    width: 260,
    background: "rgba(10, 14, 26, 0.95)",
    border: "1px solid #1a2040",
    borderRadius: 8,
    padding: 12,
    fontFamily: "'Courier New', Consolas, monospace",
    fontSize: 10,
    color: "#a0b0c8",
    zIndex: 100,
    backdropFilter: "blur(8px)",
  },
  title: {
    fontSize: 9,
    fontWeight: 700,
    letterSpacing: 2,
    color: "#6b7fa3",
    textTransform: "uppercase" as const,
    marginBottom: 8,
    display: "flex",
    alignItems: "center",
    gap: 6,
  },
  row: {
    display: "flex",
    justifyContent: "space-between",
    marginBottom: 3,
  },
  label: { color: "#6b7fa3" },
  value: { color: "#e0e8f0", fontWeight: 700 },
  accent: { fontWeight: 700 },
  divider: {
    borderTop: "1px solid #1a2040",
    margin: "6px 0",
  },
};

function opsColor(ops: number): string {
  if (ops >= 8) return "#00ff88";
  if (ops >= 4) return "#00aaff";
  return "#ffaa00";
}

function latencyColor(ms: number): string {
  if (ms < 5) return "#00ff88";
  if (ms < 15) return "#ffaa00";
  return "#ff4444";
}

function Sparkline({ data, color }: { data: number[]; color: string }) {
  if (data.length < 2) return null;
  const max = Math.max(...data, 1);
  const w = 100;
  const h = 24;
  const pts = data.map((v, i) => {
    const x = (i / (data.length - 1)) * w;
    const y = h - (v / max) * (h - 4) - 2;
    return `${x},${y}`;
  });
  return (
    <svg width={w} height={h} style={{ display: "block", margin: "4px 0" }}>
      {/* baseline */}
      <line x1={0} y1={h - 1} x2={w} y2={h - 1} stroke="#1a2040" strokeWidth={1} />
      <polyline
        points={pts.join(" ")}
        fill="none"
        stroke={color}
        strokeWidth={1.5}
      />
      {/* latest value dot */}
      {data.length > 0 && (
        <circle
          cx={w}
          cy={h - (data[data.length - 1] / max) * (h - 4) - 2}
          r={2.5}
          fill={color}
        />
      )}
    </svg>
  );
}

interface Props {
  data: ThroughputCurrent | null;
  timeScale: number;
}

const ThroughputOverlay: React.FC<Props> = ({ data, timeScale }) => {
  if (!data) {
    return (
      <div style={s.overlay}>
        <div style={s.title}>
          <Database size={10} color="#6b7fa3" />
          Lakebase Throughput
        </div>
        <div style={{ color: "#4a5a7a" }}>Awaiting metrics...</div>
      </div>
    );
  }

  const simPerSec = timeScale; // dt = 2s * scale, over 2 real seconds

  return (
    <div style={s.overlay}>
      <div style={s.title}>
        <Database size={10} color="#00aaff" />
        Lakebase Throughput
      </div>

      {/* Ops/sec gauge */}
      <div style={{ ...s.row, marginBottom: 6 }}>
        <span style={s.label}>
          <Zap size={9} style={{ verticalAlign: "middle", marginRight: 3 }} />
          OPS/SEC
        </span>
        <span style={{ ...s.accent, color: opsColor(data.ops_per_second), fontSize: 14 }}>
          {data.ops_per_second.toFixed(1)}
        </span>
      </div>

      {/* Sparkline */}
      {data.sparkline.length >= 2 && (
        <Sparkline data={data.sparkline} color={opsColor(data.ops_per_second)} />
      )}

      <div style={s.divider} />

      {/* Read/Write breakdown */}
      <div style={s.row}>
        <span style={s.label}>Reads</span>
        <span style={s.value}>{data.reads}</span>
      </div>
      <div style={s.row}>
        <span style={s.label}>Writes</span>
        <span style={s.value}>{data.writes}</span>
      </div>
      <div style={s.row}>
        <span style={s.label}>Total Ops (window)</span>
        <span style={s.value}>{data.total_ops}</span>
      </div>

      <div style={s.divider} />

      {/* Latency */}
      <div style={s.row}>
        <span style={s.label}>Latency p50</span>
        <span style={{ ...s.accent, color: latencyColor(data.latency_p50_ms) }}>
          {data.latency_p50_ms.toFixed(1)} ms
        </span>
      </div>
      <div style={s.row}>
        <span style={s.label}>Latency p99</span>
        <span style={{ ...s.accent, color: latencyColor(data.latency_p99_ms) }}>
          {data.latency_p99_ms.toFixed(1)} ms
        </span>
      </div>

      <div style={s.divider} />

      {/* Time scale info */}
      <div style={s.row}>
        <span style={s.label}>Time Scale</span>
        <span style={{ ...s.accent, color: "#ffaa00" }}>x{timeScale}</span>
      </div>
      <div style={s.row}>
        <span style={s.label}>Sim Time / Real Sec</span>
        <span style={s.value}>{simPerSec.toLocaleString()}s</span>
      </div>
      <div style={s.row}>
        <span style={s.label}>Sub-Ticks / Tick</span>
        <span style={s.value}>{data.sub_ticks}</span>
      </div>
      <div style={s.row}>
        <span style={s.label}>Telemetry Writes / Tick</span>
        <span style={s.value}>{data.telemetry_writes_per_tick}</span>
      </div>

      {/* Connection Pool */}
      {data.pool && (
        <>
          <div style={s.divider} />
          <div style={s.row}>
            <span style={s.label}>Pool Size</span>
            <span style={s.value}>{data.pool.pool_size}</span>
          </div>
          <div style={s.row}>
            <span style={s.label}>Active / Idle</span>
            <span style={s.value}>
              <span style={{ color: data.pool.checked_out > 0 ? "#00aaff" : "#6b7fa3" }}>
                {data.pool.checked_out}
              </span>
              {" / "}
              {data.pool.checked_in}
            </span>
          </div>
          {data.pool.overflow > 0 && (
            <div style={s.row}>
              <span style={s.label}>Overflow</span>
              <span style={{ ...s.accent, color: "#ffaa00" }}>{data.pool.overflow}</span>
            </div>
          )}
        </>
      )}

      {/* Footer hint */}
      <div style={{ marginTop: 6, fontSize: 8, color: "#4a5a7a", lineHeight: 1.4 }}>
        <Activity size={8} style={{ verticalAlign: "middle", marginRight: 3 }} />
        Increase time scale to see Lakebase throughput ramp up with sub-10ms latency
      </div>
    </div>
  );
};

export default ThroughputOverlay;
