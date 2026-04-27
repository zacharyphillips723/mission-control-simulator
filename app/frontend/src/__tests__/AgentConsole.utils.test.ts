/**
 * Tests for AgentConsole.tsx utility functions — 4 tests.
 *
 * Functions are replicated since they're not exported.
 */
import { describe, it, expect } from "vitest";

// Replicated from AgentConsole.tsx L74-77
function confidenceColor(c: number): string {
  if (c >= 0.8) return "#00ff88";
  if (c >= 0.5) return "#ffaa00";
  return "#ff4444";
}

// Replicated from AgentConsole.tsx L108-114
function assessmentColor(key: string): string {
  switch (key) {
    case "on_course":
      return "#00ff88";
    case "minor_deviation":
      return "#ffaa00";
    case "correction_needed":
      return "#ff4444";
    default:
      return "#6b7fa3";
  }
}

describe("confidenceColor", () => {
  it("returns green for high confidence (0.9)", () => {
    expect(confidenceColor(0.9)).toBe("#00ff88");
  });

  it("returns amber for medium confidence (0.6)", () => {
    expect(confidenceColor(0.6)).toBe("#ffaa00");
  });

  it("returns red for low confidence (0.3)", () => {
    expect(confidenceColor(0.3)).toBe("#ff4444");
  });
});

describe("assessmentColor", () => {
  it('returns green for "on_course"', () => {
    expect(assessmentColor("on_course")).toBe("#00ff88");
  });

  it('returns amber for "minor_deviation"', () => {
    expect(assessmentColor("minor_deviation")).toBe("#ffaa00");
  });

  it('returns red for "correction_needed"', () => {
    expect(assessmentColor("correction_needed")).toBe("#ff4444");
  });

  it("returns default for unknown key", () => {
    expect(assessmentColor("unknown")).toBe("#6b7fa3");
  });
});
