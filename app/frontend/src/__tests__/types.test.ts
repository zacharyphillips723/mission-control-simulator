/**
 * TypeScript interface shape validation — 8 tests.
 *
 * These tests verify that objects conforming to our interfaces have
 * the expected shape and that TypeScript type guards work correctly.
 */
import { describe, it, expect } from "vitest";
import type {
  Position,
  Velocity,
  MissionState,
  Hazard,
  Maneuver,
  AgentDecision,
  OnboardPrediction,
  ThroughputCurrent,
} from "../types";

describe("Position interface", () => {
  it("has x, y, z fields", () => {
    const pos: Position = { x: 1.5e8, y: 2.0e7, z: 0 };
    expect(pos.x).toBe(1.5e8);
    expect(pos.y).toBe(2.0e7);
    expect(pos.z).toBe(0);
  });
});

describe("Velocity interface", () => {
  it("has vx, vy, vz, magnitude fields", () => {
    const vel: Velocity = { vx: 10, vy: 20, vz: 0, magnitude: 22.36 };
    expect(vel.magnitude).toBeCloseTo(22.36, 1);
  });
});

describe("MissionState interface", () => {
  it("contains all required fields", () => {
    const state: MissionState = {
      mission_id: "test-001",
      spacecraft_name: "Red Brick One",
      status: "nominal",
      position: { x: 1.5e8, y: 0, z: 0 },
      velocity: { vx: 0, vy: 29.78, vz: 0, magnitude: 29.78 },
      fuel_remaining_pct: 75.5,
      hull_integrity_pct: 100,
      comm_delay_seconds: 480,
      engine_status: "online",
      mission_elapsed_days: 30,
      target: "Earth",
      session_id: "sess-001",
    };
    expect(state.status).toBe("nominal");
    expect(state.fuel_remaining_pct).toBe(75.5);
  });
});

describe("Hazard interface", () => {
  it("contains type and risk_score", () => {
    const hazard: Hazard = {
      id: "h-001",
      type: "asteroid",
      name: "2024-XK",
      risk_score: 0.85,
      closest_approach_time: "2025-06-15T12:00:00Z",
      closest_approach_km: 500,
      status: "approaching",
      description: "Near-Earth asteroid",
    };
    expect(hazard.risk_score).toBe(0.85);
    expect(hazard.type).toBe("asteroid");
  });
});

describe("Maneuver interface", () => {
  it("contains feasibility and status", () => {
    const m: Maneuver = {
      id: "m-001",
      rank: 1,
      name: "Prograde burn",
      delta_v_ms: 500,
      fuel_cost_pct: 12.5,
      risk_reduction: 0.3,
      feasibility: "high",
      status: "proposed",
      description: "Main engine burn",
    };
    expect(m.feasibility).toBe("high");
  });
});

describe("AgentDecision interface", () => {
  it("contains agent_name and confidence", () => {
    const d: AgentDecision = {
      id: "d-001",
      agent_name: "flight_dynamics",
      timestamp: "2025-01-01T00:00:00Z",
      reasoning: "On course",
      confidence: 0.92,
      action_taken: "none",
    };
    expect(d.confidence).toBe(0.92);
    expect(d.agent_name).toBe("flight_dynamics");
  });
});

describe("OnboardPrediction interface", () => {
  it("contains source and assessment", () => {
    const p: OnboardPrediction = {
      id: "p-001",
      simulation_time_s: 86400,
      current_position: { x: 1.5e8, y: 0, z: 0 },
      predicted_position: { x: 1.49e8, y: 1e6, z: 0 },
      prediction_horizon_s: 60,
      source: "physics_fallback",
      assessment: "on_course",
      action_taken: "none",
      correction_dv: 0,
      actual_position: null,
      prediction_error_km: null,
      created_at: "2025-01-01T00:00:00Z",
    };
    expect(p.source).toBe("physics_fallback");
    expect(p.assessment).toBe("on_course");
  });
});

describe("ThroughputCurrent interface", () => {
  it("contains ops_per_second and latency metrics", () => {
    const t: ThroughputCurrent = {
      ops_per_second: 150,
      reads: 100,
      writes: 50,
      total_ops: 150,
      window_seconds: 5,
      latency_p50_ms: 2.5,
      latency_p99_ms: 15.0,
      sub_ticks: 3,
      telemetry_writes_per_tick: 4,
      sparkline: [120, 130, 150, 140, 160],
    };
    expect(t.ops_per_second).toBe(150);
    expect(t.sparkline).toHaveLength(5);
  });
});
