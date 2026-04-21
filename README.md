# Databricks Mission Control Simulator

A NASA-style mission control system built entirely on Databricks. Simulates the DSV Odyssey spacecraft returning from Mars to Earth while 4 LLM-powered agents coordinate trajectory corrections, hazard evasion, and command timing — all with realistic communication delay.

Showcases Lakebase (sub-10ms operational state), Delta Lake (historical analytics), Mosaic AI Model Serving, Foundation Model API agents, and per-second throughput tracking — deployed as a single Databricks Asset Bundle.

## Architecture

```
                        ┌──────────────────────────────────────────────┐
                        │         Databricks App (React + FastAPI)     │
                        │  Overview │ NavMap │ Hazards │ Agents │ Cmds │
                        └────────────────────┬─────────────────────────┘
                                             │ REST API
           ┌─────────────────────────────────┼─────────────────────────────────┐
           │                                 │                                 │
    ┌──────▼──────────┐          ┌───────────▼───────────┐          ┌──────────▼──────────┐
    │    Lakebase      │          │      Delta Lake        │          │   Model Serving      │
    │   (Real-time)    │          │     (Historical)       │          │     (ML + LLM)       │
    │                  │          │                        │          │                      │
    │ mission_state    │          │ telemetry.spacecraft_  │          │ trajectory_predictor │
    │ command_queue    │          │   telemetry            │          │ fuel_estimator       │
    │ ground_state     │          │ hazards.detected_      │          │ hazard_risk_scorer   │
    │ active_hazards   │          │   hazards              │          │ maneuver_ranker      │
    │ agent_memory     │          │ commands.command_log   │          │ delay_aware_policy   │
    │ autopilot_state  │          │ agents.decision_log   │          │                      │
    │ telemetry_realtime│         │ agents.message_log    │          │ meta-llama-3.3-70b   │
    │ throughput_metrics│         │ models.inference_log  │          │  (Foundation Model)  │
    └─────────────────┘          └────────────────────────┘          └──────────────────────┘
```

### Closed-Loop Simulation (every 5 minutes)

```
  ┌─────────────────┐     ┌──────────────┐     ┌─────────────────────┐     ┌────────────────────┐
  │ Spacecraft Tick  │────▶│ Ground Tick   │────▶│ Agent Decision Loop │────▶│ Command Transmission│
  │     (04b)        │     │    (04)       │     │       (08)          │     │       (09)          │
  │                  │     │               │     │                     │     │                     │
  │ • Physics (1s)   │     │ • Delayed view│     │ • Flight Dynamics   │     │ • Validate commands │
  │ • Autopilot      │     │ • Hazard scan │     │ • Hazard Assessment │     │ • Calculate delay   │
  │ • Execute cmds   │     │ • Maneuver gen│     │ • Communications    │     │ • Transmit          │
  │ • Per-sec telem  │     │ • Ground state│     │ • Mission Commander │     │ • Log to Delta      │
  └─────────────────┘     └──────────────┘     └─────────────────────┘     └────────────────────┘
         ▲                                              │
         │              Communication Delay             │
         └──────────────── (minutes) ───────────────────┘
```

## Key Features

- **4 LLM-Powered Agents** — Flight Dynamics, Hazard Assessment, Communications, and Mission Commander agents call real Foundation Model API endpoints with tool schemas, execute physics/ML tools, and communicate via shared Lakebase state
- **Deterministic Spacecraft Autopilot** — Priority-based state machine (evasion > commands > station keeping > fuel conservation > coast) handles safety-critical decisions without LLM latency
- **Realistic Communication Delay** — Commands travel at light speed. Ground sees stale telemetry. The ship must act autonomously between command windows.
- **Per-Second Lakebase Writes** — Every simulation second writes telemetry, autopilot state, and throughput counters to Lakebase, showcasing sub-10ms operational performance
- **Full Audit Trail** — Every LLM call (input/output/latency), every agent message, every autopilot decision logged to Delta Lake for debriefing and retraining
- **11 Mission Profiles** — Training data spans early/late/optimal launch windows, fast/slow departures, low/high fuel, Jupiter gravity assist, dense asteroid belt, and solar storm scenarios
- **5 ML Models** — Trajectory prediction, fuel estimation, hazard risk scoring, maneuver ranking, and delay-aware command policy
- **Realistic Hazards** — Kirkwood gap-aware asteroid generation, meteor showers with intensity profiles, expanding debris fields, solar flare CME propagation

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Dashboard | Databricks Apps (React 18 + Vite + FastAPI) | Mission control UI with NASA dark theme |
| Operational DB | Lakebase (Postgres) | Sub-10ms state reads/writes for real-time loop |
| Analytics | Delta Lake + Unity Catalog | Telemetry history, audit trail, governance |
| ML Models | Mosaic AI Model Serving (5 endpoints) | Trajectory, fuel, risk, ranking, timing |
| AI Agents | Foundation Model API + custom ReAct framework | 4-agent decision chain with tool calling |
| Orchestration | Databricks Workflows | Setup DAG + 5-minute simulation cron |
| Deployment | Databricks Asset Bundles | Single `bundle deploy` for everything |
| Physics | Custom N-body engine (velocity Verlet) | Orbital mechanics, gravity assists |

## Project Structure

```
mission-control-simulator/
├── databricks.yml                     # DAB config — parameterized catalog (dev/staging/prod)
├── config/
│   └── mission_config.json            # Mission parameters, autopilot config, agent models
├── resources/
│   ├── jobs.yml                       # Setup job (7 tasks) + Simulation loop (4 tasks)
│   ├── app.yml                        # Databricks App deployment
│   └── model_serving.yml              # 5 Model Serving endpoints (scale-to-zero)
├── src/
│   ├── notebooks/
│   │   ├── 00_setup_catalog.py        # Create catalog + 6 schemas
│   │   ├── 01_setup_delta_tables.py   # Delta tables (telemetry, hazards, commands, agents, models)
│   │   ├── 02_setup_lakebase.py       # Lakebase tables (mission_state, command_queue, etc.)
│   │   ├── 03_generate_telemetry.py   # Generate telemetry (single or 11 multi-profile)
│   │   ├── 04_simulation_tick.py      # Ground-side tick (delayed view, hazard detection)
│   │   ├── 04b_spacecraft_tick.py     # Ship-side tick (physics, autopilot, per-sec telemetry)
│   │   ├── 05_seed_celestial_bodies.py# Reference data (Sun, Earth, Mars, Jupiter, Moon)
│   │   ├── 06_train_models.py         # Train 5 ML models with multi-profile stratification
│   │   ├── 07_deploy_agents.py        # Register agent configs + tool schemas in MLflow
│   │   ├── 08_agent_decision_loop.py  # Run 4-agent LLM chain with real tool execution
│   │   └── 09_transmit_commands.py    # Validate + transmit commands with delay tracking
│   └── python/
│       ├── physics_engine.py          # N-body gravity, velocity Verlet, gravity assists
│       ├── telemetry_generator.py     # Synthetic telemetry with sensor noise
│       ├── hazard_simulation.py       # Asteroids, meteor showers, debris fields, solar flares
│       ├── mission_profiles.py        # 11 mission profiles for diverse training data
│       ├── spacecraft_autopilot.py    # Deterministic autopilot state machine
│       ├── agent_framework.py         # MissionControlAgent + AgentOrchestrator (ReAct loop)
│       ├── agent_tools.py             # Tool functions (physics, ML, data queries)
│       ├── command_executor.py        # Command lifecycle (create → validate → transmit → execute)
│       └── inference_logger.py        # Wraps model calls, captures I/O + latency to Delta
├── app/
│   ├── app.yaml                       # uvicorn startup config
│   ├── main.py                        # FastAPI backend (~20 API endpoints)
│   ├── requirements.txt               # fastapi, uvicorn, databricks-sdk
│   └── frontend/
│       ├── package.json               # React 18, Vite, TypeScript, Recharts
│       └── src/
│           ├── App.tsx                # Main layout with tab navigation
│           ├── api.ts                 # API client with auto-refresh
│           ├── types.ts               # TypeScript interfaces
│           └── components/
│               ├── MissionOverview.tsx # Spacecraft state, fuel, hull, comm delay
│               ├── NavigationMap.tsx   # 2D trajectory plot with celestial bodies
│               ├── HazardConsole.tsx   # Active hazards ranked by risk
│               ├── ManeuverWorkbench.tsx# Candidate maneuvers with scoring
│               ├── AgentConsole.tsx    # Agent decisions + inter-agent messages
│               └── CommandLog.tsx      # Command queue + transmission status
└── README.md
```

## Schemas

| Schema | Storage | Tables | Purpose |
|--------|---------|--------|---------|
| `telemetry` | Delta | `spacecraft_telemetry` | Per-second position, velocity, fuel, hull readings |
| `navigation` | Delta | `candidate_maneuvers`, `celestial_bodies` | Trajectory options, reference orbits |
| `hazards` | Delta | `detected_hazards` | Full hazard history with classification |
| `commands` | Delta | `command_log` | Command lifecycle audit trail |
| `agents` | Delta | `decision_log`, `message_log` | Agent reasoning traces, inter-agent comms |
| `models` | Delta | `inference_log` | Every ML/LLM call with input, output, latency |
| `ops` | Lakebase | 10 tables | Real-time state for the simulation loop |

### Lakebase Tables (ops schema)

| Table | Description | Write Rate |
|-------|-------------|------------|
| `mission_state` | True spacecraft state (single row) | Every tick |
| `ground_state` | What mission control sees (delayed) | Every tick |
| `command_queue` | Command lifecycle tracking | Per event |
| `active_hazards` | Currently active hazards | Per event |
| `agent_memory` | Inter-agent shared state | Per agent per tick |
| `agent_messages_realtime` | Current tick messages | 10-20/tick |
| `spacecraft_autopilot_state` | Autopilot mode + counters | Every tick |
| `telemetry_realtime` | Rolling 600-row telemetry buffer | 1/sec |
| `simulation_clock` | Simulation time tracking | Every tick |
| `throughput_metrics` | Read/write performance counters | Every tick |

## Quick Start

### 1. Configure

Edit `databricks.yml` to set your workspace host if different from the default:

```bash
databricks bundle validate -t dev
```

### 2. Deploy

```bash
databricks bundle deploy -t dev
```

### 3. Run Setup

```bash
# Creates catalog, schemas, tables, generates telemetry, trains models, deploys agents
databricks bundle run mission_control_setup -t dev
```

This runs a 7-task DAG:
`setup_catalog` → `setup_delta_tables` + `setup_lakebase` → `seed_celestial_bodies` → `generate_telemetry` → `train_models` → `deploy_agents`

### 4. Start Simulation

```bash
# Starts the closed-loop simulation (runs every 5 minutes)
databricks bundle run mission_control_simulation_loop -t dev
```

Each cycle runs: `spacecraft_tick` → `ground_tick` → `agent_decision_loop` → `command_transmission`

### 5. View Dashboard

The Mission Control app is available at your workspace's apps URL after deployment. The FastAPI backend also exposes interactive docs at `/docs`.

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/mission/state` | GET | Current spacecraft state from Lakebase |
| `/api/mission/clock` | GET | Simulation clock state |
| `/api/mission/ground-state` | GET | What ground control sees (delayed) |
| `/api/mission/timeline` | GET | Unified event timeline |
| `/api/telemetry/recent` | GET | Recent telemetry readings |
| `/api/telemetry/trajectory` | GET | Trajectory data for visualization |
| `/api/telemetry/realtime` | GET | Per-second telemetry from Lakebase |
| `/api/hazards/active` | GET | Active hazards ranked by risk |
| `/api/hazards/history` | GET | Hazard history from Delta |
| `/api/maneuvers/candidates` | GET | Ranked candidate maneuvers |
| `/api/commands/queue` | GET | Current command queue |
| `/api/commands/log` | GET | Command execution history |
| `/api/commands/approve` | POST | Operator approves a pending command |
| `/api/agents/decisions` | GET | Agent decision log |
| `/api/agents/messages` | GET | Inter-agent messages |
| `/api/agents/messages/latest` | GET | Most recent tick messages |
| `/api/models/inference-log` | GET | Model call log with full I/O |
| `/api/spacecraft/autopilot` | GET | Autopilot state and mode |
| `/api/throughput` | GET | Live read/write performance metrics |
| `/api/stats/summary` | GET | High-level mission statistics |

## Simulation Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| Tick interval | 300s (5 min sim time) | Simulation seconds per workflow run |
| Telemetry rate | 1 Hz | Per-second Lakebase writes |
| Hazard probability | 0.05% per second | Hazard spawn chance |
| Autopilot evasion threshold | 500 km | Emergency burn trigger distance |
| Fuel safe mode | < 100 kg | Restricts to critical commands only |
| Fuel emergency only | < 50 kg | Only evasion burns allowed |
| Communication delay | Variable | Light-speed based on Earth-spacecraft distance |
| Agent LLM | meta-llama-3.3-70b-instruct | Foundation Model for all 4 agents |

## Required Packages

**Backend / Notebooks:**
- `databricks-sdk`
- `pyspark`
- `mlflow`
- `fastapi`
- `uvicorn`
- `databricks-sql-connector`
- `scikit-learn`
- `numpy`

**Frontend:**
- `react` 18
- `recharts`
- `typescript`
- `vite`
