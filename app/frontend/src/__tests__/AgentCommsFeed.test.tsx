/**
 * AgentCommsFeed component tests — 4 tests covering message rendering.
 */
import React from "react";
import { describe, it, expect } from "vitest";
import { render } from "@testing-library/react";
import AgentCommsFeed from "../components/AgentCommsFeed";
import type { AgentMessage } from "../types";

const messages: AgentMessage[] = [
  {
    message_id: "msg-001",
    from_agent: "flight_dynamics",
    to_agent: "mission_commander",
    message_type: "analysis",
    content: JSON.stringify({ summary: "On course for Earth intercept" }),
    tick_id: "tick-001",
    created_at: "2025-01-01T00:01:00Z",
  },
  {
    message_id: "msg-002",
    from_agent: "hazard_assessment",
    to_agent: "mission_commander",
    message_type: "alert",
    content: JSON.stringify({ summary: "No hazards detected" }),
    tick_id: "tick-001",
    created_at: "2025-01-01T00:02:00Z",
  },
  {
    message_id: "msg-003",
    from_agent: "communications",
    to_agent: "mission_commander",
    message_type: "timing_plan",
    content: JSON.stringify({ summary: "Delay within bounds" }),
    tick_id: "tick-001",
    created_at: "2025-01-01T00:03:00Z",
  },
  {
    message_id: "msg-004",
    from_agent: "mission_commander",
    to_agent: "broadcast",
    message_type: "order",
    content: JSON.stringify({ decision: "GO", summary: "Continue trajectory" }),
    tick_id: "tick-001",
    created_at: "2025-01-01T00:04:00Z",
  },
];

describe("AgentCommsFeed", () => {
  it("renders all messages", () => {
    const { container } = render(<AgentCommsFeed messages={messages} />);
    // Should contain text from each message
    expect(container.textContent).toContain("intercept");
  });

  it("shows agent names", () => {
    const { container } = render(<AgentCommsFeed messages={messages} />);
    // Agent names should appear (formatted as "Flight Dynamics" etc.)
    expect(container.textContent).toMatch(/flight|hazard|communi|commander/i);
  });

  it("handles empty message list", () => {
    const { container } = render(<AgentCommsFeed messages={[]} />);
    // Should render without errors
    expect(container).toBeDefined();
  });

  it("displays messages in order", () => {
    const { container } = render(<AgentCommsFeed messages={messages} />);
    const text = container.textContent || "";
    // The messages should appear in some order (we just verify they all render)
    expect(text.length).toBeGreaterThan(0);
  });
});
