/**
 * API client tests — 8 tests with mocked fetch.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { api } from "../api";

// Mock global fetch
const mockFetch = vi.fn();

beforeEach(() => {
  vi.stubGlobal("fetch", mockFetch);
});

afterEach(() => {
  vi.restoreAllMocks();
});

function mockJsonResponse(data: unknown, status = 200) {
  return Promise.resolve({
    ok: status >= 200 && status < 300,
    status,
    statusText: status === 200 ? "OK" : "Error",
    json: () => Promise.resolve(data),
  } as Response);
}

describe("getMissionState", () => {
  it("returns parsed data", async () => {
    mockFetch.mockReturnValueOnce(
      mockJsonResponse({
        status: "nominal",
        position: { x: 1.5e8, y: 0, z: 0 },
      })
    );
    const state = await api.getMissionState();
    expect(state.status).toBe("nominal");
    expect(mockFetch).toHaveBeenCalledWith("/api/mission/state");
  });
});

describe("startSimulation", () => {
  it("sends POST", async () => {
    mockFetch.mockReturnValueOnce(mockJsonResponse({ status: "started" }));
    await api.startSimulation();
    expect(mockFetch).toHaveBeenCalledWith("/api/simulation/start", {
      method: "POST",
    });
  });
});

describe("resetSimulation", () => {
  it("sends POST", async () => {
    mockFetch.mockReturnValueOnce(mockJsonResponse({ status: "reset" }));
    await api.resetSimulation();
    expect(mockFetch).toHaveBeenCalledWith("/api/simulation/reset", {
      method: "POST",
    });
  });
});

describe("injectHazard", () => {
  it("sends body with position", async () => {
    mockFetch.mockReturnValueOnce(mockJsonResponse({ ok: true }));
    await api.injectHazard({
      hazard_type: "asteroid",
      position_x: 1.5e8,
      position_y: 1000,
      velocity_x: -5,
      velocity_y: 0,
    });
    expect(mockFetch).toHaveBeenCalledWith(
      "/api/hazards/inject",
      expect.objectContaining({
        method: "POST",
        headers: { "Content-Type": "application/json" },
      })
    );
  });
});

describe("approveManeuver", () => {
  it("sends POST with maneuver ID", async () => {
    mockFetch.mockReturnValueOnce(mockJsonResponse({ status: "approved" }));
    await api.approveManeuver("m-123");
    expect(mockFetch).toHaveBeenCalledWith("/api/maneuvers/m-123/approve", {
      method: "POST",
    });
  });
});

describe("sendManualBurn", () => {
  it("sends command payload", async () => {
    mockFetch.mockReturnValueOnce(mockJsonResponse({ ok: true }));
    await api.sendManualBurn({
      direction: "prograde",
      delta_v: 0.01,
      burn_duration_s: 10,
    });
    const call = mockFetch.mock.calls[0];
    expect(call[0]).toBe("/api/commands/manual-burn");
    expect(call[1].method).toBe("POST");
    const body = JSON.parse(call[1].body);
    expect(body.direction).toBe("prograde");
  });
});

describe("error handling", () => {
  it("throws on 500 error", async () => {
    mockFetch.mockReturnValueOnce(
      mockJsonResponse(null, 500)
    );
    await expect(api.getMissionState()).rejects.toThrow("500");
  });

  it("throws on network failure", async () => {
    mockFetch.mockRejectedValueOnce(new TypeError("Failed to fetch"));
    await expect(api.getMissionState()).rejects.toThrow("Failed to fetch");
  });
});
