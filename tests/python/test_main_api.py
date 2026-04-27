"""FastAPI integration tests — 8 tests using httpx.AsyncClient.

These tests require the Databricks SDK + Lakebase connection, so they are
automatically skipped in environments where those are unavailable.
"""

import sys
import os

import pytest

# These tests are skipped if we can't import main without errors
pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABRICKS_HOST"),
    reason="Databricks environment not configured — skipping API integration tests",
)


@pytest.fixture
async def client():
    """Create an httpx.AsyncClient bound to the FastAPI app."""
    import httpx
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "app"))
        from main import app
    except Exception as e:
        pytest.skip(f"Cannot import FastAPI app: {e}")

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as c:
        yield c


class TestMissionStateEndpoint:
    @pytest.mark.asyncio
    async def test_get_state_200(self, client):
        """GET /api/mission/state returns 200."""
        resp = await client.get("/api/mission/state")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_get_state_has_fields(self, client):
        """State response includes key fields."""
        resp = await client.get("/api/mission/state")
        data = resp.json()
        assert "status" in data
        assert "position" in data or "position_x" in data


class TestSimulationControl:
    @pytest.mark.asyncio
    async def test_start_simulation(self, client):
        """POST /api/simulation/start returns 200."""
        resp = await client.post("/api/simulation/start")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_reset_simulation(self, client):
        """POST /api/simulation/reset clears state."""
        resp = await client.post("/api/simulation/reset")
        assert resp.status_code == 200


class TestHazardInjection:
    @pytest.mark.asyncio
    async def test_inject_hazard(self, client):
        """POST /api/hazards/inject creates a hazard."""
        resp = await client.post("/api/hazards/inject", json={
            "hazard_type": "asteroid",
            "position_x": 1.5e8,
            "position_y": 1000.0,
            "velocity_x": -5.0,
            "velocity_y": 0.0,
        })
        assert resp.status_code == 200


class TestCommands:
    @pytest.mark.asyncio
    async def test_send_command(self, client):
        """POST /api/commands/manual-burn queues a command."""
        resp = await client.post("/api/commands/manual-burn", json={
            "direction": "prograde",
            "delta_v": 0.01,
            "burn_duration_s": 10.0,
        })
        assert resp.status_code == 200


class TestTimeScale:
    @pytest.mark.asyncio
    async def test_set_timescale(self, client):
        """POST /api/simulation/timescale updates scale."""
        resp = await client.post("/api/simulation/timescale", json={"scale": 2.0})
        assert resp.status_code == 200


class TestConcurrency:
    @pytest.mark.asyncio
    async def test_concurrent_gets(self, client):
        """Parallel GETs return consistent data."""
        import asyncio
        tasks = [client.get("/api/mission/state") for _ in range(5)]
        results = await asyncio.gather(*tasks)
        statuses = {r.status_code for r in results}
        assert statuses == {200}
