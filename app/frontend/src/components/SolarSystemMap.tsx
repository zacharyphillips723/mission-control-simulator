import React, { useRef, useEffect, useState, useCallback } from "react";
import { ZoomIn, ZoomOut, Crosshair, Gauge, MapPin, X, Check, Flame } from "lucide-react";
import type { MissionState, Hazard, OnboardPrediction } from "../types";
import { api } from "../api";

/* ── Constants ── */
const AU_KM = 149_597_870.7;
const EARTH_ORBIT_AU = 1.0;
const MARS_ORBIT_AU = 1.524;
const EARTH_PERIOD_S = 365.25 * 86400;
const MARS_PERIOD_S = 687.0 * 86400;

const MAX_TRAIL = 2000; // max trail points to keep
const PROJECTED_STEPS = 60; // number of projected path steps
const PROJECTED_DT_S = 3600; // 1 hour per step

const GM_SUN_KM3_S2 = 1.32712440018e11;

/* ── Helpers ── */
function kmToAU(km: number): number {
  return km / AU_KM;
}

function auToKm(au: number): number {
  return au * AU_KM;
}

function circularOrbitSpeed(radius_km: number): number {
  return Math.sqrt(GM_SUN_KM3_S2 / radius_km);
}

function planetPos(orbitAU: number, periodS: number, elapsedS: number) {
  const angle = (2 * Math.PI * elapsedS) / periodS;
  return { x: orbitAU * Math.cos(angle), y: orbitAU * Math.sin(angle) };
}

/* ── Styles ── */
const styles: Record<string, React.CSSProperties> = {
  panel: {
    background: "#0d1224",
    border: "1px solid #1a2040",
    borderRadius: 8,
    padding: 16,
    height: "100%",
    display: "flex",
    flexDirection: "column",
    position: "relative",
  },
  titleRow: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: 8,
    flexShrink: 0,
  },
  title: {
    fontSize: 11,
    fontWeight: 700,
    letterSpacing: 2,
    color: "#6b7fa3",
    textTransform: "uppercase" as const,
  },
  controls: { display: "flex", gap: 6, alignItems: "center" },
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
    fontFamily: "inherit",
  },
  btnActive: {
    background: "#1a3040",
    border: "1px solid #00ff88",
    color: "#00ff88",
  },
  legend: {
    position: "absolute" as const,
    bottom: 24,
    left: 24,
    display: "flex",
    flexDirection: "column" as const,
    gap: 4,
    fontSize: 9,
    color: "#6b7fa3",
    pointerEvents: "none" as const,
  },
  legendItem: {
    display: "flex",
    alignItems: "center",
    gap: 6,
  },
  info: {
    position: "absolute" as const,
    bottom: 24,
    right: 24,
    fontSize: 9,
    color: "#4a5a7a",
    textAlign: "right" as const,
    pointerEvents: "none" as const,
    lineHeight: 1.6,
  },
};

const ZOOM_LEVELS = [0.5, 0.8, 1.0, 1.5, 2.0, 3.0, 5.0, 8.0];

interface RepositionParams {
  position_x: number;
  position_y: number;
  position_z?: number;
  velocity_x: number;
  velocity_y: number;
  velocity_z?: number;
  fuel_remaining_kg?: number;
  scenario_name?: string;
}

interface Props {
  state: MissionState | null;
  hazards: Hazard[];
  predictions?: OnboardPrediction[];
  elapsedSeconds: number;
  onTimeScaleChange?: (scale: number) => void;
  currentTimeScale?: number;
  resetCounter?: number;
  isRunning?: boolean;
  onReposition?: (params: RepositionParams) => Promise<void>;
}

/* ── Preset scenarios ── */
interface PresetScenario {
  label: string;
  desc: string;
  color: string;
  category: "standard" | "challenge";
  fn: () => RepositionParams;
}

const PRESETS: PresetScenario[] = [
  // --- Standard Scenarios ---
  {
    label: "Earth Departure",
    desc: "Near Earth, heading outward",
    color: "#4488ff",
    category: "standard",
    fn: () => {
      const r = AU_KM + 50_000;
      return {
        position_x: r, position_y: 0,
        velocity_x: 2.0, velocity_y: 29.78,
        scenario_name: "Earth Departure",
      };
    },
  },
  {
    label: "Mars Orbit",
    desc: "Circular orbit at Mars distance",
    color: "#ff4422",
    category: "standard",
    fn: () => {
      const r = 1.524 * AU_KM;
      const v = circularOrbitSpeed(r);
      return {
        position_x: r, position_y: 0,
        velocity_x: 0, velocity_y: v,
        scenario_name: "Mars Orbit",
      };
    },
  },
  {
    label: "Deep Space",
    desc: "Between Mars and Jupiter",
    color: "#aa66ff",
    category: "standard",
    fn: () => {
      const r = 3.0 * AU_KM;
      const v = circularOrbitSpeed(r);
      return {
        position_x: r * Math.cos(0.5), position_y: r * Math.sin(0.5),
        velocity_x: -v * Math.sin(0.5), velocity_y: v * Math.cos(0.5),
        scenario_name: "Deep Space",
      };
    },
  },
  {
    label: "Inner Solar System",
    desc: "Between Venus and Earth",
    color: "#ffaa00",
    category: "standard",
    fn: () => {
      const r = 0.8 * AU_KM;
      const v = circularOrbitSpeed(r);
      return {
        position_x: 0, position_y: r,
        velocity_x: -v, velocity_y: 0,
        scenario_name: "Inner Solar System",
      };
    },
  },
  // --- Challenge Scenarios ---
  {
    label: "Fuel Crisis",
    desc: "Reach Earth with only 100 kg fuel",
    color: "#ff6644",
    category: "challenge",
    fn: () => {
      // Same transfer orbit as default, but with minimal fuel
      return {
        position_x: -11335491, position_y: -216294050,
        velocity_x: 19.0, velocity_y: 11.8,
        fuel_remaining_kg: 100,
        scenario_name: "Fuel Crisis",
      };
    },
  },
  {
    label: "Wrong Way",
    desc: "Heading away from Earth at high speed",
    color: "#ff2244",
    category: "challenge",
    fn: () => {
      const r = 1.524 * AU_KM;
      const v = circularOrbitSpeed(r);
      return {
        position_x: -r * 0.7, position_y: r * 0.7,
        velocity_x: -v * 0.6, velocity_y: -v * 0.8,
        fuel_remaining_kg: 800,
        scenario_name: "Wrong Way",
      };
    },
  },
  {
    label: "Gravity Slingshot",
    desc: "Near Jupiter, use gravity to return",
    color: "#ff88cc",
    category: "challenge",
    fn: () => {
      const r = 5.2 * AU_KM;
      const v = circularOrbitSpeed(r);
      return {
        position_x: r * 0.7, position_y: r * 0.7,
        velocity_x: -v * 0.3, velocity_y: -v * 0.6,
        fuel_remaining_kg: 200,
        scenario_name: "Gravity Slingshot",
      };
    },
  },
  {
    label: "Sprint Home",
    desc: "Close to Earth but drifting past at high speed",
    color: "#44ddff",
    category: "challenge",
    fn: () => {
      const r = 1.1 * AU_KM;
      return {
        position_x: r, position_y: 0.2 * AU_KM,
        velocity_x: 5.0, velocity_y: 35.0,
        fuel_remaining_kg: 150,
        scenario_name: "Sprint Home",
      };
    },
  },
];

const TIME_SCALES = [1, 10, 60, 300, 600, 3600];
const TIME_SCALE_LABELS: Record<number, string> = {
  1: "1x",
  10: "10x",
  60: "1min/s",
  300: "5min/s",
  600: "10min/s",
  3600: "1hr/s",
};

const SolarSystemMap: React.FC<Props> = ({
  state,
  hazards,
  predictions = [],
  elapsedSeconds,
  onTimeScaleChange,
  currentTimeScale = 1,
  resetCounter = 0,
  isRunning = false,
  onReposition,
}) => {
  const svgRef = useRef<SVGSVGElement>(null);
  const trailRef = useRef<{ x: number; y: number }[]>([]);
  const [zoomIdx, setZoomIdx] = useState(2); // start at 1.0x
  const [followShip, setFollowShip] = useState(false);

  // Scenario Mode state
  const [scenarioMode, setScenarioMode] = useState(false);
  const [scenarioTool, setScenarioTool] = useState<"ship" | "hazard">("ship");
  const [scenarioPos, setScenarioPos] = useState<{ x: number; y: number } | null>(null); // AU
  const [scenarioVel, setScenarioVel] = useState<{ x: number; y: number } | null>(null); // km/s
  const [scenarioFuel, setScenarioFuel] = useState<number | null>(null); // custom fuel override
  const [scenarioLabel, setScenarioLabel] = useState<string>("Custom Scenario");
  const [dragging, setDragging] = useState(false);
  const [applying, setApplying] = useState(false);
  const [hazardType, setHazardType] = useState<"asteroid" | "micro_meteoroid">("asteroid");

  // Pan state (click-drag to pan the map)
  const [panOffset, setPanOffset] = useState<{ x: number; y: number }>({ x: 0, y: 0 });
  const [panning, setPanning] = useState(false);
  const panStartRef = useRef<{ mouseX: number; mouseY: number; panX: number; panY: number } | null>(null);

  const zoom = ZOOM_LEVELS[zoomIdx];
  const viewExtent = 2.2 / zoom; // AU extent from center

  // Convert mouse event to SVG AU coordinates
  const mouseToAU = useCallback(
    (e: React.MouseEvent<SVGSVGElement>): { x: number; y: number } | null => {
      const svg = svgRef.current;
      if (!svg) return null;
      const pt = svg.createSVGPoint();
      pt.x = e.clientX;
      pt.y = e.clientY;
      const ctm = svg.getScreenCTM();
      if (!ctm) return null;
      const svgPt = pt.matrixTransform(ctm.inverse());
      return { x: svgPt.x, y: -svgPt.y }; // flip Y back to AU
    },
    []
  );

  // SVG click: place ship position or drop hazard
  const handleSvgClick = useCallback(
    async (e: React.MouseEvent<SVGSVGElement>) => {
      if (!scenarioMode || dragging) return;
      const au = mouseToAU(e);
      if (!au) return;

      if (scenarioTool === "hazard") {
        // Drop a hazard at the clicked position
        const pos_x_km = auToKm(au.x);
        const pos_y_km = auToKm(au.y);
        // Give it a random velocity toward the general ship area
        const speed = 5 + Math.random() * 15; // 5-20 km/s
        const angle = Math.random() * 2 * Math.PI;
        try {
          await api.injectHazard({
            hazard_type: hazardType,
            position_x: pos_x_km,
            position_y: pos_y_km,
            velocity_x: speed * Math.cos(angle),
            velocity_y: speed * Math.sin(angle),
            radius_km: hazardType === "asteroid" ? 2 + Math.random() * 8 : 0.5 + Math.random() * 2,
          });
        } catch (err) {
          console.error("Failed to inject hazard:", err);
        }
        return;
      }

      // Ship placement mode
      setScenarioPos(au);
      // Default velocity: circular orbit tangent
      const r_km = Math.sqrt(auToKm(au.x) ** 2 + auToKm(au.y) ** 2) || 1;
      const v = circularOrbitSpeed(r_km);
      const angle = Math.atan2(au.y, au.x);
      setScenarioVel({ x: -v * Math.sin(angle), y: v * Math.cos(angle) });
    },
    [scenarioMode, scenarioTool, hazardType, dragging, mouseToAU]
  );

  // Drag from placed ship to set velocity direction + magnitude (or drop hazard)
  const handleSvgMouseDown = useCallback(
    (e: React.MouseEvent<SVGSVGElement>) => {
      if (!scenarioMode) return;

      // Hazard injection is handled by handleSvgClick — skip here to avoid duplicates

      // Ship velocity drag mode
      if (!scenarioPos) return;
      const au = mouseToAU(e);
      if (!au) return;
      const dx = au.x - scenarioPos.x;
      const dy = au.y - scenarioPos.y;
      if (Math.sqrt(dx * dx + dy * dy) < 0.05 * (viewExtent / 2.2) * 3.0) {
        setDragging(true);
        e.preventDefault();
      }
    },
    [scenarioMode, scenarioTool, hazardType, scenarioPos, mouseToAU, viewExtent]
  );

  const handleSvgMouseMove = useCallback(
    (e: React.MouseEvent<SVGSVGElement>) => {
      if (!dragging || !scenarioPos) return;
      const au = mouseToAU(e);
      if (!au) return;
      // Vector from ship to cursor = velocity direction
      const dx_au = au.x - scenarioPos.x;
      const dy_au = au.y - scenarioPos.y;
      const len_au = Math.sqrt(dx_au * dx_au + dy_au * dy_au) || 1e-10;
      // Map drag length to speed: 0.1 AU drag = 30 km/s (logarithmic feel)
      const speed = Math.min(60, len_au / 0.1 * 30);
      setScenarioVel({
        x: (dx_au / len_au) * speed,
        y: (dy_au / len_au) * speed,
      });
    },
    [dragging, scenarioPos, mouseToAU]
  );

  const handleSvgMouseUp = useCallback(() => {
    setDragging(false);
  }, []);

  // Pan handlers (active when NOT in scenario mode)
  const handlePanStart = useCallback(
    (e: React.MouseEvent<SVGSVGElement>) => {
      if (scenarioMode) return;
      setPanning(true);
      panStartRef.current = {
        mouseX: e.clientX,
        mouseY: e.clientY,
        panX: panOffset.x,
        panY: panOffset.y,
      };
      e.preventDefault();
    },
    [scenarioMode, panOffset]
  );

  const handlePanMove = useCallback(
    (e: React.MouseEvent<SVGSVGElement>) => {
      if (!panning || !panStartRef.current) return;
      const svg = svgRef.current;
      if (!svg) return;
      // Convert pixel delta to AU delta using the current viewBox scale
      const rect = svg.getBoundingClientRect();
      const scaleX = (viewExtent * 2) / rect.width;
      const scaleY = (viewExtent * 2) / rect.height;
      const dx = (e.clientX - panStartRef.current.mouseX) * scaleX;
      const dy = (e.clientY - panStartRef.current.mouseY) * scaleY;
      setPanOffset({
        x: panStartRef.current.panX - dx,
        y: panStartRef.current.panY + dy, // flip Y because SVG Y is inverted
      });
    },
    [panning, viewExtent]
  );

  const handlePanEnd = useCallback(() => {
    setPanning(false);
    panStartRef.current = null;
  }, []);

  // Apply scenario
  const applyScenario = useCallback(
    async (params?: RepositionParams) => {
      if (!onReposition) return;
      setApplying(true);
      try {
        if (params) {
          await onReposition(params);
        } else if (scenarioPos && scenarioVel) {
          const repoParams: RepositionParams = {
            position_x: auToKm(scenarioPos.x),
            position_y: auToKm(scenarioPos.y),
            velocity_x: scenarioVel.x,
            velocity_y: scenarioVel.y,
            scenario_name: scenarioLabel,
          };
          if (scenarioFuel !== null) {
            repoParams.fuel_remaining_kg = scenarioFuel;
          }
          await onReposition(repoParams);
        }
        setScenarioMode(false);
        setScenarioPos(null);
        setScenarioVel(null);
        setScenarioFuel(null);
        setScenarioLabel("Custom Scenario");
      } catch (e) {
        console.error("Reposition failed:", e);
      } finally {
        setApplying(false);
      }
    },
    [onReposition, scenarioPos, scenarioVel, scenarioFuel, scenarioLabel]
  );

  const cancelScenario = useCallback(() => {
    setScenarioMode(false);
    setScenarioTool("ship");
    setScenarioPos(null);
    setScenarioVel(null);
    setScenarioFuel(null);
    setScenarioLabel("Custom Scenario");
    setDragging(false);
  }, []);

  // Clear trail and pan on reset
  useEffect(() => {
    trailRef.current = [];
    setPanOffset({ x: 0, y: 0 });
  }, [resetCounter]);

  // Accumulate spacecraft trail
  useEffect(() => {
    if (!state) return;
    const x = kmToAU(state.position.x);
    const y = kmToAU(state.position.y);
    const trail = trailRef.current;
    // Only add if position changed
    if (
      trail.length === 0 ||
      Math.abs(trail[trail.length - 1].x - x) > 0.001 ||
      Math.abs(trail[trail.length - 1].y - y) > 0.001
    ) {
      trail.push({ x, y });
      if (trail.length > MAX_TRAIL) trail.shift();
    }
  }, [state?.position.x, state?.position.y]);

  const handleZoomIn = () =>
    setZoomIdx((i) => Math.min(i + 1, ZOOM_LEVELS.length - 1));
  const handleZoomOut = () => setZoomIdx((i) => Math.max(i - 1, 0));

  // Compute positions
  const earth = planetPos(EARTH_ORBIT_AU, EARTH_PERIOD_S, elapsedSeconds);
  const mars = planetPos(MARS_ORBIT_AU, MARS_PERIOD_S, elapsedSeconds);

  const shipX = state ? kmToAU(state.position.x) : 0;
  const shipY = state ? kmToAU(state.position.y) : 0;
  const shipVx = state ? kmToAU(state.velocity.vx) : 0;
  const shipVy = state ? kmToAU(state.velocity.vy) : 0;

  // Center point (follow ship or sun, plus pan offset)
  const cx = (followShip ? shipX : 0) + panOffset.x;
  const cy = (followShip ? shipY : 0) + panOffset.y;

  // Projected path (extrapolate velocity)
  const projectedPath: string[] = [];
  {
    let px = shipX,
      py = shipY;
    for (let i = 0; i < PROJECTED_STEPS; i++) {
      px += shipVx * PROJECTED_DT_S;
      py += shipVy * PROJECTED_DT_S;
      projectedPath.push(`${px},${-py}`);
    }
  }

  // SVG coordinate transform: x stays, y flips (SVG y is down)
  const vb = `${cx - viewExtent} ${-(cy + viewExtent)} ${viewExtent * 2} ${viewExtent * 2}`;

  const trail = trailRef.current;

  // Distance from Earth
  const distEarthAU = state
    ? Math.sqrt(
        (kmToAU(state.position.x) - earth.x) ** 2 +
          (kmToAU(state.position.y) - earth.y) ** 2
      )
    : 0;

  const distSunAU = state
    ? Math.sqrt(kmToAU(state.position.x) ** 2 + kmToAU(state.position.y) ** 2)
    : 0;

  // Orbit ring points helper
  const orbitRing = useCallback((radius: number, segments = 120) => {
    const pts: string[] = [];
    for (let i = 0; i <= segments; i++) {
      const a = (2 * Math.PI * i) / segments;
      pts.push(`${radius * Math.cos(a)},${-radius * Math.sin(a)}`);
    }
    return pts.join(" ");
  }, []);

  // Scale factor for icons (so they stay same visual size regardless of zoom)
  // Base sizes are in AU — multiply by 3 so they're visible at ~470px width
  const iconScale = (viewExtent / 2.2) * 3.0;

  if (!state) {
    return (
      <div style={styles.panel}>
        <div style={styles.title}>Solar System Map</div>
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
          Acquiring telemetry...
        </div>
      </div>
    );
  }

  return (
    <div style={styles.panel}>
      <div style={styles.titleRow}>
        <div style={styles.title}>Solar System Map</div>
        <div style={styles.controls}>
          {/* Time scale controls */}
          {onTimeScaleChange && (
            <>
              <Gauge size={10} style={{ color: "#6b7fa3" }} />
              {TIME_SCALES.map((ts) => (
                <button
                  key={ts}
                  style={{
                    ...styles.btn,
                    ...(currentTimeScale === ts ? styles.btnActive : {}),
                    padding: "2px 6px",
                    fontSize: 10,
                  }}
                  onClick={() => onTimeScaleChange(ts)}
                >
                  {TIME_SCALE_LABELS[ts]}
                </button>
              ))}
              <span style={{ width: 1, height: 16, background: "#2a3060" }} />
            </>
          )}
          {onReposition && (
            <button
              style={{
                ...styles.btn,
                ...(scenarioMode
                  ? { background: "#1a2a40", border: "1px solid #ffaa00", color: "#ffaa00" }
                  : {}),
                opacity: isRunning && !scenarioMode ? 0.3 : 1,
                cursor: isRunning && !scenarioMode ? "not-allowed" : "pointer",
              }}
              onClick={() => {
                if (isRunning) return;
                setScenarioMode((m) => !m);
                if (scenarioMode) cancelScenario();
              }}
              title={isRunning ? "Stop simulation first" : "Scenario Mode — place ship anywhere"}
            >
              <MapPin size={10} />
              {scenarioMode ? "EXIT" : "SCENARIO"}
            </button>
          )}
          <button
            style={{
              ...styles.btn,
              ...(followShip ? styles.btnActive : {}),
            }}
            onClick={() => { setFollowShip((f) => !f); setPanOffset({ x: 0, y: 0 }); }}
            title="Track spacecraft"
          >
            <Crosshair size={10} />
          </button>
          <button style={styles.btn} onClick={handleZoomOut}>
            <ZoomOut size={12} />
          </button>
          <button style={styles.btn} onClick={handleZoomIn}>
            <ZoomIn size={12} />
          </button>
        </div>
      </div>

      <div style={{ flex: 1, minHeight: 0, position: "relative" }}>
        <svg
          ref={svgRef}
          viewBox={vb}
          width="100%"
          height="100%"
          style={{
            background: "#060a14",
            borderRadius: 4,
            cursor: scenarioMode
              ? dragging ? "grabbing" : "crosshair"
              : panning ? "grabbing" : "grab",
          }}
          preserveAspectRatio="xMidYMid meet"
          onClick={scenarioMode ? handleSvgClick : undefined}
          onMouseDown={scenarioMode ? handleSvgMouseDown : handlePanStart}
          onMouseMove={scenarioMode ? handleSvgMouseMove : handlePanMove}
          onMouseUp={scenarioMode ? handleSvgMouseUp : handlePanEnd}
          onMouseLeave={scenarioMode ? handleSvgMouseUp : handlePanEnd}
        >
          {/* Grid rings (0.5 AU increments) */}
          {[0.5, 1.0, 1.5, 2.0, 2.5, 3.0].map((r) => (
            <circle
              key={r}
              cx={0}
              cy={0}
              r={r}
              fill="none"
              stroke="#1a2848"
              strokeWidth={0.006}
              strokeDasharray="0.02 0.02"
            />
          ))}

          {/* Earth orbit */}
          <polyline
            points={orbitRing(EARTH_ORBIT_AU)}
            fill="none"
            stroke="#1a4060"
            strokeWidth={0.008}
            opacity={0.7}
          />

          {/* Mars orbit */}
          <polyline
            points={orbitRing(MARS_ORBIT_AU)}
            fill="none"
            stroke="#502a20"
            strokeWidth={0.008}
            opacity={0.7}
          />

          {/* Spacecraft trail (actual path taken) */}
          {trail.length > 1 && (
            <polyline
              points={trail.map((p) => `${p.x},${-p.y}`).join(" ")}
              fill="none"
              stroke="#00ff88"
              strokeWidth={0.012}
              opacity={0.6}
            />
          )}

          {/* Projected path (dashed, extends from ship) */}
          {projectedPath.length > 0 && (
            <polyline
              points={`${shipX},${-shipY} ${projectedPath.join(" ")}`}
              fill="none"
              stroke="#00ff88"
              strokeWidth={0.008}
              strokeDasharray="0.04 0.02"
              opacity={0.35}
            />
          )}

          {/* Sun */}
          <circle cx={0} cy={0} r={0.04 * iconScale} fill="#ffcc00" />
          <circle
            cx={0}
            cy={0}
            r={0.07 * iconScale}
            fill="none"
            stroke="#ffcc00"
            strokeWidth={0.005}
            opacity={0.3}
          />
          <text
            x={0}
            y={0.09 * iconScale}
            textAnchor="middle"
            fill="#ffcc00"
            fontSize={0.04 * iconScale}
            fontFamily="monospace"
            opacity={0.7}
          >
            SUN
          </text>

          {/* Earth */}
          <circle
            cx={earth.x}
            cy={-earth.y}
            r={0.025 * iconScale}
            fill="#4488ff"
          />
          <circle
            cx={earth.x}
            cy={-earth.y}
            r={0.04 * iconScale}
            fill="none"
            stroke="#4488ff"
            strokeWidth={0.005}
            opacity={0.4}
          />
          <text
            x={earth.x}
            y={-earth.y + 0.06 * iconScale}
            textAnchor="middle"
            fill="#4488ff"
            fontSize={0.035 * iconScale}
            fontFamily="monospace"
            opacity={0.8}
          >
            EARTH
          </text>

          {/* Mars */}
          <circle
            cx={mars.x}
            cy={-mars.y}
            r={0.02 * iconScale}
            fill="#ff4422"
          />
          <circle
            cx={mars.x}
            cy={-mars.y}
            r={0.035 * iconScale}
            fill="none"
            stroke="#ff4422"
            strokeWidth={0.005}
            opacity={0.4}
          />
          <text
            x={mars.x}
            y={-mars.y + 0.055 * iconScale}
            textAnchor="middle"
            fill="#ff4422"
            fontSize={0.035 * iconScale}
            fontFamily="monospace"
            opacity={0.8}
          >
            MARS
          </text>

          {/* Hazards (asteroids) */}
          {hazards.map((h) => {
            // Use actual position if available, otherwise fall back to hash-based placement
            let hx: number, hy: number;
            if ((h as any).position_x != null && (h as any).position_y != null) {
              hx = kmToAU((h as any).position_x);
              hy = kmToAU((h as any).position_y);
            } else {
              const hAngle =
                (parseInt(h.id.slice(0, 8), 16) / 0xffffffff) * 2 * Math.PI;
              const hDist = 0.8 + (h.closest_approach_km / AU_KM) * 0.5;
              hx = hDist * Math.cos(hAngle);
              hy = hDist * Math.sin(hAngle);
            }
            return (
              <g key={h.id}>
                {/* Approach circle */}
                <circle
                  cx={hx}
                  cy={-hy}
                  r={0.03 * iconScale}
                  fill="none"
                  stroke={h.risk_score > 70 ? "#ff4444" : "#ff8800"}
                  strokeWidth={0.005}
                  strokeDasharray="0.015 0.01"
                  opacity={0.5}
                />
                {/* Hazard body */}
                <polygon
                  points={`${hx},${-hy - 0.015 * iconScale} ${hx + 0.012 * iconScale},${-hy + 0.008 * iconScale} ${hx - 0.012 * iconScale},${-hy + 0.008 * iconScale}`}
                  fill={h.risk_score > 70 ? "#ff4444" : "#ff8800"}
                  opacity={0.8}
                />
                <text
                  x={hx}
                  y={-hy + 0.05 * iconScale}
                  textAnchor="middle"
                  fill={h.risk_score > 70 ? "#ff4444" : "#ff8800"}
                  fontSize={0.025 * iconScale}
                  fontFamily="monospace"
                  opacity={0.6}
                >
                  {h.name}
                </text>
              </g>
            );
          })}

          {/* Spacecraft */}
          <g>
            {/* Glow */}
            <circle
              cx={shipX}
              cy={-shipY}
              r={0.02 * iconScale}
              fill="#00ff88"
              opacity={0.15}
            />
            {/* Ship icon (small triangle) */}
            {(() => {
              const size = 0.015 * iconScale;
              // Point in direction of velocity
              const vMag = Math.sqrt(shipVx ** 2 + shipVy ** 2) || 1;
              const nvx = shipVx / vMag;
              const nvy = shipVy / vMag;
              // Triangle vertices: tip in velocity direction
              const tipX = shipX + nvx * size * 1.5;
              const tipY = -(shipY + nvy * size * 1.5);
              const leftX = shipX + nvy * size * 0.8;
              const leftY = -(shipY - nvx * size * 0.8);
              const rightX = shipX - nvy * size * 0.8;
              const rightY = -(shipY + nvx * size * 0.8);
              return (
                <polygon
                  points={`${tipX},${tipY} ${leftX},${leftY} ${rightX},${rightY}`}
                  fill="#00ff88"
                  stroke="#00ff88"
                  strokeWidth={0.005}
                />
              );
            })()}
            {/* Velocity vector arrow */}
            {(() => {
              const vMag = Math.sqrt(shipVx ** 2 + shipVy ** 2) || 1e-10;
              const nvx = shipVx / vMag;
              const nvy = shipVy / vMag;
              const arrowLen = 0.15 * iconScale;
              const endX = shipX + nvx * arrowLen;
              const endY = -(shipY + nvy * arrowLen);
              // Arrowhead (chevron)
              const headSize = 0.02 * iconScale;
              const perpX = -nvy * headSize;
              const perpY = nvx * headSize;
              return (
                <g opacity={0.85}>
                  <line
                    x1={shipX}
                    y1={-shipY}
                    x2={endX}
                    y2={endY}
                    stroke="#00ff88"
                    strokeWidth={0.006}
                  />
                  <polygon
                    points={`${endX},${endY} ${endX - nvx * headSize * 2 + perpX},${endY + nvy * headSize * 2 - perpY} ${endX - nvx * headSize * 2 - perpX},${endY + nvy * headSize * 2 + perpY}`}
                    fill="#00ff88"
                  />
                </g>
              );
            })()}
            <text
              x={shipX}
              y={-shipY + 0.05 * iconScale}
              textAnchor="middle"
              fill="#00ff88"
              fontSize={0.035 * iconScale}
              fontFamily="monospace"
              fontWeight="bold"
            >
              ODYSSEY
            </text>
          </g>

          {/* Onboard prediction marker (cyan diamond + accuracy ring) */}
          {predictions.length > 0 && (() => {
            const latest = predictions[0];
            const predX = kmToAU(latest.predicted_position.x);
            const predY = kmToAU(latest.predicted_position.y);
            const dSize = 0.015 * iconScale;
            const assessColors: Record<string, string> = {
              on_course: "#00ffff",
              minor_deviation: "#ffaa00",
              correction_needed: "#ff4444",
            };
            const color = assessColors[latest.assessment] ?? "#00ffff";
            const hasAccuracy = latest.prediction_error_km != null;
            const errorAU = hasAccuracy ? kmToAU(latest.prediction_error_km!) : 0;
            return (
              <g>
                {/* Dashed line from ship to prediction */}
                <line
                  x1={shipX} y1={-shipY}
                  x2={predX} y2={-predY}
                  stroke={color} strokeWidth={0.004}
                  strokeDasharray="0.015 0.01" opacity={0.5}
                />
                {/* Diamond marker at predicted position */}
                <polygon
                  points={`${predX},${-predY - dSize} ${predX + dSize},${-predY} ${predX},${-predY + dSize} ${predX - dSize},${-predY}`}
                  fill={color} fillOpacity={0.3}
                  stroke={color} strokeWidth={0.003}
                />
                {/* Accuracy ring (only if backfilled) */}
                {hasAccuracy && errorAU > 0.001 && (
                  <circle
                    cx={predX} cy={-predY}
                    r={Math.min(errorAU * 2, 0.1 * iconScale)}
                    fill="none" stroke={color}
                    strokeWidth={0.002} strokeDasharray="0.008 0.005"
                    opacity={0.4}
                  />
                )}
                {/* Label */}
                <text
                  x={predX} y={-predY - dSize * 1.8}
                  textAnchor="middle" fill={color}
                  fontSize={0.025 * iconScale} fontFamily="monospace"
                  opacity={0.8}
                >
                  PRED {latest.source === "model_serving" ? "ML" : "PHY"}
                </text>
                {hasAccuracy && (
                  <text
                    x={predX} y={-predY + dSize * 2.5}
                    textAnchor="middle" fill={color}
                    fontSize={0.02 * iconScale} fontFamily="monospace"
                    opacity={0.6}
                  >
                    err: {latest.prediction_error_km! < 1000
                      ? `${latest.prediction_error_km!.toFixed(0)}km`
                      : `${(latest.prediction_error_km! / 1000).toFixed(1)}Mkm`}
                  </text>
                )}
              </g>
            );
          })()}

          {/* Line from ship to Earth (comm link) */}
          <line
            x1={shipX}
            y1={-shipY}
            x2={earth.x}
            y2={-earth.y}
            stroke="#4488ff"
            strokeWidth={0.005}
            strokeDasharray="0.03 0.02"
            opacity={0.25}
          />

          {/* Scenario Mode: ghost ship + velocity arrow */}
          {scenarioMode && scenarioPos && (() => {
            const gx = scenarioPos.x;
            const gy = scenarioPos.y;
            const distSunAU_g = Math.sqrt(gx * gx + gy * gy);
            const distEarthAU_g = Math.sqrt((gx - earth.x) ** 2 + (gy - earth.y) ** 2);
            const tooClose = distSunAU_g < 0.3;
            const tooFar = distSunAU_g > 6.0;
            const invalid = tooClose || tooFar;
            const ghostColor = invalid ? "#ff4444" : "#ffaa00";
            const size = 0.02 * iconScale;

            // Velocity arrow
            const velArrow = scenarioVel ? (() => {
              const speed = Math.sqrt(scenarioVel.x ** 2 + scenarioVel.y ** 2) || 1e-10;
              const nvx = scenarioVel.x / speed;
              const nvy = scenarioVel.y / speed;
              // Arrow length proportional to speed (30 km/s = 0.15 AU)
              const arrowLen = Math.min(speed / 30, 2) * 0.15 * iconScale;
              const endX = gx + nvx * arrowLen;
              const endY = -(gy + nvy * arrowLen);
              const headSize = 0.02 * iconScale;
              const perpX = -nvy * headSize;
              const perpY = nvx * headSize;
              return (
                <g opacity={0.7}>
                  <line x1={gx} y1={-gy} x2={endX} y2={endY}
                    stroke={ghostColor} strokeWidth={0.006} />
                  <polygon
                    points={`${endX},${endY} ${endX - nvx * headSize * 2 + perpX},${endY + nvy * headSize * 2 - perpY} ${endX - nvx * headSize * 2 - perpX},${endY + nvy * headSize * 2 + perpY}`}
                    fill={ghostColor} />
                </g>
              );
            })() : null;

            return (
              <g>
                {/* Ghost ship icon */}
                <circle cx={gx} cy={-gy} r={size * 1.5}
                  fill={ghostColor} opacity={0.15} />
                <circle cx={gx} cy={-gy} r={size}
                  fill="none" stroke={ghostColor} strokeWidth={0.005}
                  strokeDasharray="0.01 0.008" />
                <circle cx={gx} cy={-gy} r={0.008 * iconScale}
                  fill={ghostColor} opacity={0.8} />

                {velArrow}

                {/* Distance readouts */}
                <text x={gx} y={-gy - size * 2.5}
                  textAnchor="middle" fill={ghostColor}
                  fontSize={0.03 * iconScale} fontFamily="monospace"
                  fontWeight="bold" opacity={0.9}>
                  {invalid ? (tooClose ? "TOO CLOSE TO SUN" : "TOO FAR") : "DROP POINT"}
                </text>
                <text x={gx} y={-gy + size * 3}
                  textAnchor="middle" fill="#6b7fa3"
                  fontSize={0.025 * iconScale} fontFamily="monospace">
                  {distSunAU_g.toFixed(2)} AU from Sun
                </text>
                <text x={gx} y={-gy + size * 4.5}
                  textAnchor="middle" fill="#4488ff"
                  fontSize={0.025 * iconScale} fontFamily="monospace">
                  {distEarthAU_g.toFixed(2)} AU from Earth
                </text>
                {scenarioVel && (
                  <text x={gx} y={-gy + size * 6}
                    textAnchor="middle" fill={ghostColor}
                    fontSize={0.025 * iconScale} fontFamily="monospace">
                    {Math.sqrt(scenarioVel.x ** 2 + scenarioVel.y ** 2).toFixed(1)} km/s
                  </text>
                )}

                {/* Dashed line to Sun for distance reference */}
                <line x1={gx} y1={-gy} x2={0} y2={0}
                  stroke="#ffcc00" strokeWidth={0.003}
                  strokeDasharray="0.02 0.015" opacity={0.2} />
                {/* Dashed line to Earth */}
                <line x1={gx} y1={-gy} x2={earth.x} y2={-earth.y}
                  stroke="#4488ff" strokeWidth={0.003}
                  strokeDasharray="0.02 0.015" opacity={0.2} />
              </g>
            );
          })()}

          {/* Scenario Mode banner */}
          {scenarioMode && (
            <g>
              <text
                x={cx - viewExtent + 0.05} y={-(cy + viewExtent) + 0.08}
                fill="#ffaa00" fontSize={0.04 * iconScale}
                fontFamily="monospace" fontWeight="bold">
                {scenarioTool === "hazard"
                  ? "HAZARD MODE — Click anywhere to drop an asteroid"
                  : `SCENARIO MODE — Click to place ship${scenarioPos ? ", drag from ship to set velocity" : ""}`}
              </text>
            </g>
          )}
        </svg>

        {/* Scenario Mode: presets + apply/cancel panel */}
        {scenarioMode && (
          <div style={{
            position: "absolute", top: 40, right: 8,
            background: "#0d1224ee", border: "1px solid #ffaa0044",
            borderRadius: 6, padding: 10, width: 180, zIndex: 10,
          }}>
            {/* Tool toggle: Ship vs Hazard */}
            <div style={{ display: "flex", gap: 4, marginBottom: 8 }}>
              <button style={{
                ...styles.btn, flex: 1, justifyContent: "center", fontSize: 10, padding: "4px 6px",
                ...(scenarioTool === "ship" ? { background: "#1a3040", border: "1px solid #ffaa00", color: "#ffaa00" } : {}),
              }}
                onClick={() => setScenarioTool("ship")}
              >
                <MapPin size={10} />
                SHIP
              </button>
              <button style={{
                ...styles.btn, flex: 1, justifyContent: "center", fontSize: 10, padding: "4px 6px",
                ...(scenarioTool === "hazard" ? { background: "#2a1a0a", border: "1px solid #ff8800", color: "#ff8800" } : {}),
              }}
                onClick={() => setScenarioTool("hazard")}
              >
                <Flame size={10} />
                HAZARD
              </button>
            </div>

            {scenarioTool === "hazard" ? (
              <>
                <div style={{ fontSize: 10, color: "#ff8800", fontWeight: 700,
                  letterSpacing: 1, marginBottom: 6, textTransform: "uppercase" }}>
                  Drop Hazard
                </div>
                <div style={{ fontSize: 9, color: "#6b7fa3", marginBottom: 8, lineHeight: 1.4 }}>
                  Click on the map to drop an asteroid or meteoroid near the ship's path.
                </div>
                <div style={{ display: "flex", gap: 4, marginBottom: 8 }}>
                  <button style={{
                    ...styles.btn, flex: 1, justifyContent: "center", fontSize: 9, padding: "3px 4px",
                    ...(hazardType === "asteroid" ? { background: "#2a1a0a", border: "1px solid #ff8800", color: "#ff8800" } : {}),
                  }}
                    onClick={() => setHazardType("asteroid")}
                  >
                    Asteroid
                  </button>
                  <button style={{
                    ...styles.btn, flex: 1, justifyContent: "center", fontSize: 9, padding: "3px 4px",
                    ...(hazardType === "micro_meteoroid" ? { background: "#2a1a0a", border: "1px solid #ff8800", color: "#ff8800" } : {}),
                  }}
                    onClick={() => setHazardType("micro_meteoroid")}
                  >
                    Meteoroid
                  </button>
                </div>
              </>
            ) : (
              <>
                <div style={{ fontSize: 10, color: "#ffaa00", fontWeight: 700,
                  letterSpacing: 1, marginBottom: 4, textTransform: "uppercase" }}>
                  Scenarios
                </div>
                {PRESETS.filter((p) => p.category === "standard").map((p) => (
                  <button key={p.label} style={{
                    ...styles.btn, width: "100%", marginBottom: 3,
                    justifyContent: "flex-start", fontSize: 10, padding: "4px 8px",
                  }}
                    onClick={() => {
                      const params = p.fn();
                      setScenarioPos({
                        x: kmToAU(params.position_x),
                        y: kmToAU(params.position_y ?? 0),
                      });
                      setScenarioVel({
                        x: params.velocity_x,
                        y: params.velocity_y,
                      });
                      setScenarioFuel(params.fuel_remaining_kg ?? null);
                      setScenarioLabel(params.scenario_name ?? p.label);
                    }}
                    title={p.desc}
                  >
                    <span style={{ width: 6, height: 6, borderRadius: 3,
                      background: p.color, display: "inline-block", flexShrink: 0 }} />
                    {p.label}
                  </button>
                ))}
                <div style={{ fontSize: 9, color: "#ff6644", fontWeight: 700,
                  letterSpacing: 1, marginTop: 6, marginBottom: 4, textTransform: "uppercase",
                  borderTop: "1px solid #1a2040", paddingTop: 6 }}>
                  Challenges
                </div>
                {PRESETS.filter((p) => p.category === "challenge").map((p) => (
                  <button key={p.label} style={{
                    ...styles.btn, width: "100%", marginBottom: 3,
                    justifyContent: "flex-start", fontSize: 9, padding: "3px 8px",
                  }}
                    onClick={() => {
                      const params = p.fn();
                      setScenarioPos({
                        x: kmToAU(params.position_x),
                        y: kmToAU(params.position_y ?? 0),
                      });
                      setScenarioVel({
                        x: params.velocity_x,
                        y: params.velocity_y,
                      });
                      setScenarioFuel(params.fuel_remaining_kg ?? null);
                      setScenarioLabel(params.scenario_name ?? p.label);
                    }}
                    title={p.desc}
                  >
                    <span style={{ width: 6, height: 6, borderRadius: 3,
                      background: p.color, display: "inline-block", flexShrink: 0 }} />
                    {p.label}
                    <span style={{ marginLeft: "auto", fontSize: 7, color: "#ff664488", letterSpacing: 1 }}>
                      HARD
                    </span>
                  </button>
                ))}
              </>
            )}
            <div style={{ marginTop: 8, display: "flex", gap: 4 }}>
              {scenarioTool === "hazard" ? (
                <button style={{
                  ...styles.btn, flex: 1, justifyContent: "center",
                  border: "1px solid #00ff8866", color: "#00ff88",
                }}
                  onClick={cancelScenario}
                >
                  <Check size={10} />
                  DONE
                </button>
              ) : (
                <>
                  <button style={{
                    ...styles.btn, flex: 1, justifyContent: "center",
                    border: "1px solid #00ff8866", color: "#00ff88",
                    opacity: !scenarioPos || applying ? 0.4 : 1,
                    cursor: !scenarioPos || applying ? "not-allowed" : "pointer",
                  }}
                    disabled={!scenarioPos || applying}
                    onClick={() => applyScenario()}
                  >
                    <Check size={10} />
                    {applying ? "..." : "APPLY"}
                  </button>
                  <button style={{
                    ...styles.btn, flex: 1, justifyContent: "center",
                    border: "1px solid #ff444466", color: "#ff4444",
                  }}
                    onClick={cancelScenario}
                  >
                    <X size={10} />
                    CANCEL
                  </button>
                </>
              )}
            </div>
          </div>
        )}

        {/* Legend overlay */}
        <div style={styles.legend}>
          <div style={styles.legendItem}>
            <span
              style={{
                width: 16,
                height: 2,
                background: "#00ff88",
                display: "inline-block",
              }}
            />
            Actual path
          </div>
          <div style={styles.legendItem}>
            <span
              style={{
                width: 16,
                height: 2,
                background: "#00ff88",
                opacity: 0.3,
                display: "inline-block",
                borderTop: "1px dashed #00ff88",
              }}
            />
            Projected path
          </div>
          <div style={styles.legendItem}>
            <span
              style={{
                width: 0,
                height: 0,
                borderLeft: "8px solid #00ff88",
                borderTop: "3px solid transparent",
                borderBottom: "3px solid transparent",
                display: "inline-block",
                marginRight: 2,
              }}
            />
            Velocity vector
          </div>
          <div style={styles.legendItem}>
            <span
              style={{
                width: 16,
                height: 2,
                background: "#4488ff",
                opacity: 0.3,
                display: "inline-block",
                borderTop: "1px dashed #4488ff",
              }}
            />
            Comm link
          </div>
          <div style={styles.legendItem}>
            <span
              style={{
                width: 8,
                height: 8,
                background: "#ff8800",
                clipPath: "polygon(50% 0%, 100% 70%, 0% 70%)",
                display: "inline-block",
              }}
            />
            Hazard
          </div>
          <div style={styles.legendItem}>
            <span
              style={{
                width: 8,
                height: 8,
                background: "#00ffff",
                clipPath: "polygon(50% 0%, 100% 50%, 50% 100%, 0% 50%)",
                display: "inline-block",
              }}
            />
            ML Prediction
          </div>
        </div>

        {/* Info overlay */}
        <div style={styles.info}>
          <div>
            Dist to Earth:{" "}
            <span style={{ color: "#4488ff" }}>{distEarthAU.toFixed(4)} AU</span>
          </div>
          <div>
            Dist to Sun:{" "}
            <span style={{ color: "#ffcc00" }}>{distSunAU.toFixed(4)} AU</span>
          </div>
          <div>
            Speed:{" "}
            <span style={{ color: "#00ff88" }}>
              {state.velocity.magnitude.toFixed(1)} km/s
            </span>
          </div>
          <div>
            Comm delay:{" "}
            <span style={{ color: "#6b7fa3" }}>
              {state.comm_delay_seconds.toFixed(0)}s
            </span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default SolarSystemMap;
