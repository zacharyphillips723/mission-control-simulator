import React, { useState } from "react";
import {
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
  ZAxis,
} from "recharts";
import { ZoomIn, ZoomOut, Clock } from "lucide-react";
import type { TrajectoryData } from "../types";

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
  titleRow: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: 8,
  },
  title: {
    fontSize: 11,
    fontWeight: 700,
    letterSpacing: 2,
    color: "#6b7fa3",
    textTransform: "uppercase" as const,
  },
  controls: {
    display: "flex",
    gap: 6,
    alignItems: "center",
  },
  btn: {
    background: "#1a2040",
    border: "1px solid #2a3060",
    borderRadius: 4,
    color: "#6b7fa3",
    cursor: "pointer",
    padding: "3px 8px",
    fontSize: 11,
    display: "flex",
    alignItems: "center",
    gap: 4,
  },
  btnActive: {
    background: "#1a3040",
    border: "1px solid #00ff88",
    color: "#00ff88",
  },
};

const ZOOM_LEVELS = [1.0, 1.5, 2.0, 3.0, 5.0];
const TIME_RANGES = [6, 12, 24, 48];

interface Props {
  data: TrajectoryData | null;
  onTimeRangeChange?: (hours: number) => void;
}

const NavigationMap: React.FC<Props> = ({ data, onTimeRangeChange }) => {
  const [zoomIdx, setZoomIdx] = useState(0);
  const [timeRange, setTimeRange] = useState(6);

  const zoom = ZOOM_LEVELS[zoomIdx];
  const extent = 2.5 / zoom;

  const handleZoomIn = () =>
    setZoomIdx((i) => Math.min(i + 1, ZOOM_LEVELS.length - 1));
  const handleZoomOut = () => setZoomIdx((i) => Math.max(i - 1, 0));
  const handleTimeRange = (h: number) => {
    setTimeRange(h);
    onTimeRangeChange?.(h);
  };

  if (!data) {
    return (
      <div style={styles.panel}>
        <div style={styles.title}>Navigation Map</div>
        <div
          style={{
            flex: 1,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            color: "#6b7fa3",
            fontSize: 12,
          }}
        >
          Acquiring trajectory data...
        </div>
      </div>
    );
  }

  const bodies = data.celestial_bodies.map((b) => ({
    x: b.x,
    y: b.y,
    name: b.name,
    color: b.color,
    r: b.radius,
  }));

  return (
    <div style={styles.panel}>
      <div style={styles.titleRow}>
        <div style={styles.title}>Navigation Map</div>
        <div style={styles.controls}>
          {TIME_RANGES.map((h) => (
            <button
              key={h}
              style={{
                ...styles.btn,
                ...(timeRange === h ? styles.btnActive : {}),
              }}
              onClick={() => handleTimeRange(h)}
            >
              <Clock size={10} />
              {h}h
            </button>
          ))}
          <button style={styles.btn} onClick={handleZoomOut}>
            <ZoomOut size={12} />
          </button>
          <button style={styles.btn} onClick={handleZoomIn}>
            <ZoomIn size={12} />
          </button>
        </div>
      </div>

      <div style={{ flex: 1, minHeight: 0 }}>
        <ResponsiveContainer width="100%" height="100%">
          <ScatterChart margin={{ top: 10, right: 10, bottom: 10, left: 10 }}>
            <CartesianGrid
              stroke="#1a2040"
              strokeDasharray="3 3"
              fill="#080c18"
            />
            <XAxis
              type="number"
              dataKey="x"
              domain={[-extent, extent]}
              tick={{ fill: "#4a5a7a", fontSize: 10 }}
              tickLine={{ stroke: "#1a2040" }}
              axisLine={{ stroke: "#1a2040" }}
              name="X (AU)"
              unit=" AU"
            />
            <YAxis
              type="number"
              dataKey="y"
              domain={[-extent, extent]}
              tick={{ fill: "#4a5a7a", fontSize: 10 }}
              tickLine={{ stroke: "#1a2040" }}
              axisLine={{ stroke: "#1a2040" }}
              name="Y (AU)"
              unit=" AU"
            />
            <ZAxis range={[20, 200]} />
            <Tooltip
              cursor={{ strokeDasharray: "3 3", stroke: "#2a3060" }}
              contentStyle={{
                background: "#0d1224",
                border: "1px solid #1a2040",
                borderRadius: 6,
                fontSize: 11,
                color: "#c8d6e5",
              }}
            />

            {/* Spacecraft trajectory */}
            <Scatter
              name="Trajectory"
              data={data.spacecraft_path}
              line={{ stroke: "#00ff88", strokeWidth: 1.5 }}
              lineType="joint"
            >
              {data.spacecraft_path.map((_, i) => (
                <Cell
                  key={i}
                  fill="#00ff88"
                  r={i === data.spacecraft_path.length - 1 ? 5 : 2}
                />
              ))}
            </Scatter>

            {/* Celestial bodies */}
            <Scatter name="Bodies" data={bodies}>
              {bodies.map((b, i) => (
                <Cell key={i} fill={b.color} r={b.r} />
              ))}
            </Scatter>

            {/* Hazards */}
            <Scatter
              name="Hazards"
              data={data.hazard_positions}
              shape="diamond"
            >
              {data.hazard_positions.map((_, i) => (
                <Cell key={i} fill="#ff4444" r={4} />
              ))}
            </Scatter>
          </ScatterChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export default NavigationMap;
