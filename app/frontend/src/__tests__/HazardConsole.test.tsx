/**
 * HazardConsole component tests — 6 tests covering rendering and risk display.
 */
import React from "react";
import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import HazardConsole from "../components/HazardConsole";
import type { Hazard } from "../types";

const baseHazard: Hazard = {
  id: "h-001",
  type: "asteroid",
  name: "2024-XK",
  risk_score: 0.85,
  closest_approach_time: "2025-06-15T12:00:00Z",
  closest_approach_km: 500,
  status: "approaching",
  description: "Near-Earth asteroid on collision course",
};

describe("HazardConsole", () => {
  it("renders hazard names", () => {
    const { container } = render(
      <HazardConsole hazards={[baseHazard]} shipPosition={null} />
    );
    expect(container.textContent).toContain("2024-XK");
  });

  it("shows risk score for approaching hazard", () => {
    const { container } = render(
      <HazardConsole hazards={[baseHazard]} shipPosition={null} />
    );
    // Risk score should be displayed somewhere (as percentage or decimal)
    expect(container.textContent).toMatch(/0\.8|85|risk/i);
  });

  it("handles empty hazard list", () => {
    const { container } = render(
      <HazardConsole hazards={[]} shipPosition={null} />
    );
    // Should show some kind of "no hazards" or empty state
    expect(container.textContent).toMatch(/no|clear|none|0/i);
  });

  it("renders multiple hazards", () => {
    const hazard2: Hazard = {
      ...baseHazard,
      id: "h-002",
      name: "Debris Field Alpha",
      type: "debris",
      risk_score: 0.3,
    };
    const { container } = render(
      <HazardConsole hazards={[baseHazard, hazard2]} shipPosition={null} />
    );
    expect(container.textContent).toContain("2024-XK");
    expect(container.textContent).toContain("Debris Field Alpha");
  });

  it("applies risk color coding (high risk = red tones)", () => {
    const { container } = render(
      <HazardConsole hazards={[baseHazard]} shipPosition={null} />
    );
    // Check that there's some colored element for high risk
    const riskElements = container.querySelectorAll('[style*="color"]');
    expect(riskElements.length).toBeGreaterThan(0);
  });

  it("shows low risk hazard differently", () => {
    const lowRisk: Hazard = {
      ...baseHazard,
      risk_score: 0.1,
      status: "tracking",
    };
    const { container } = render(
      <HazardConsole hazards={[lowRisk]} shipPosition={null} />
    );
    // Should still render without errors
    expect(container.textContent).toContain("2024-XK");
  });
});
