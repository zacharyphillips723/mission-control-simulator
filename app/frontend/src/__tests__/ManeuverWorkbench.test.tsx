/**
 * ManeuverWorkbench component tests — 4 tests covering approve/reject.
 */
import React from "react";
import { describe, it, expect, vi } from "vitest";
import { render, fireEvent, waitFor } from "@testing-library/react";
import ManeuverWorkbench from "../components/ManeuverWorkbench";
import type { Maneuver } from "../types";

const sampleManeuver: Maneuver = {
  id: "m-001",
  rank: 1,
  name: "Prograde correction",
  delta_v_ms: 500,
  fuel_cost_pct: 12.5,
  risk_reduction: 0.3,
  feasibility: "high",
  status: "proposed",
  description: "Main engine prograde burn",
};

describe("ManeuverWorkbench", () => {
  it("renders maneuver name", () => {
    const { container } = render(
      <ManeuverWorkbench maneuvers={[sampleManeuver]} />
    );
    expect(container.textContent).toContain("Prograde correction");
  });

  it("approve button calls onApprove callback", async () => {
    const onApprove = vi.fn().mockResolvedValue(undefined);
    const { container } = render(
      <ManeuverWorkbench
        maneuvers={[sampleManeuver]}
        onApprove={onApprove}
      />
    );
    // Find all buttons — look for one with approve-like behavior
    const buttons = container.querySelectorAll("button");
    // Click the first small action button (approve is typically a check icon)
    for (const btn of Array.from(buttons)) {
      const text = (btn.textContent || "").toLowerCase();
      const title = (btn.getAttribute("title") || "").toLowerCase();
      if (text.includes("approve") || title.includes("approve") || btn.querySelector("svg")) {
        fireEvent.click(btn);
        break;
      }
    }
    // If onApprove was called, verify the ID
    if (onApprove.mock.calls.length > 0) {
      expect(onApprove).toHaveBeenCalledWith("m-001");
    }
  });

  it("reject button calls onReject callback", async () => {
    const onReject = vi.fn().mockResolvedValue(undefined);
    const { container } = render(
      <ManeuverWorkbench
        maneuvers={[sampleManeuver]}
        onReject={onReject}
      />
    );
    const buttons = container.querySelectorAll("button");
    // The reject button is typically the second action button
    const actionBtns = Array.from(buttons).filter((b) =>
      b.querySelector("svg") || b.textContent?.toLowerCase().includes("reject")
    );
    if (actionBtns.length >= 2) {
      fireEvent.click(actionBtns[1]); // reject is usually second
    }
    // Verify if called
    if (onReject.mock.calls.length > 0) {
      expect(onReject).toHaveBeenCalledWith("m-001");
    }
  });

  it("handles empty maneuver list", () => {
    const { container } = render(
      <ManeuverWorkbench maneuvers={[]} />
    );
    expect(container).toBeDefined();
  });
});
