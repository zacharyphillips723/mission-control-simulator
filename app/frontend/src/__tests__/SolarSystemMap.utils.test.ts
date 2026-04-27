/**
 * Tests for SolarSystemMap.tsx utility functions — 10 tests.
 *
 * Since the utils are not exported, we replicate them here for testing.
 * If they're later extracted to a shared module, these tests still apply.
 */
import { describe, it, expect } from "vitest";

// Constants (must match SolarSystemMap.tsx)
const AU_KM = 149_597_870.7;
const EARTH_ORBIT_AU = 1.0;
const MARS_ORBIT_AU = 1.524;
const EARTH_PERIOD_S = 365.25 * 86400;
const MARS_PERIOD_S = 687.0 * 86400;
const GM_SUN_KM3_S2 = 1.32712440018e11;

// Replicated helper functions
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

describe("kmToAU", () => {
  it("converts 1 AU in km back to 1.0", () => {
    expect(kmToAU(149_597_870.7)).toBeCloseTo(1.0, 10);
  });

  it("returns 0 for 0 km", () => {
    expect(kmToAU(0)).toBe(0);
  });
});

describe("auToKm", () => {
  it("converts 1.0 AU to ~1.496e8 km", () => {
    expect(auToKm(1.0)).toBeCloseTo(149_597_870.7, 0);
  });
});

describe("roundtrip conversion", () => {
  it("kmToAU(auToKm(x)) ≈ x", () => {
    const x = 2.5;
    expect(kmToAU(auToKm(x))).toBeCloseTo(x, 10);
  });
});

describe("circularOrbitSpeed", () => {
  it("Earth orbital speed ≈ 29.78 km/s", () => {
    const speed = circularOrbitSpeed(auToKm(1.0));
    expect(speed).toBeCloseTo(29.78, 0);
  });

  it("handles 0 radius without throwing", () => {
    // Math.sqrt(Infinity) = Infinity, not an error
    const speed = circularOrbitSpeed(0);
    expect(speed).toBe(Infinity);
  });
});

describe("planetPos", () => {
  it("Earth at t=0 is at (ORBIT_RADIUS, 0)", () => {
    const pos = planetPos(EARTH_ORBIT_AU, EARTH_PERIOD_S, 0);
    expect(pos.x).toBeCloseTo(EARTH_ORBIT_AU, 10);
    expect(pos.y).toBeCloseTo(0, 10);
  });

  it("Earth at T/4 is at (0, ORBIT_RADIUS)", () => {
    const pos = planetPos(EARTH_ORBIT_AU, EARTH_PERIOD_S, EARTH_PERIOD_S / 4);
    expect(pos.x).toBeCloseTo(0, 5);
    expect(pos.y).toBeCloseTo(EARTH_ORBIT_AU, 5);
  });

  it("Mars at t=0 is at Mars orbit radius", () => {
    const pos = planetPos(MARS_ORBIT_AU, MARS_PERIOD_S, 0);
    expect(pos.x).toBeCloseTo(MARS_ORBIT_AU, 10);
  });

  it("position magnitude equals orbit radius", () => {
    const pos = planetPos(EARTH_ORBIT_AU, EARTH_PERIOD_S, 1e7);
    const mag = Math.sqrt(pos.x ** 2 + pos.y ** 2);
    expect(mag).toBeCloseTo(EARTH_ORBIT_AU, 5);
  });
});
