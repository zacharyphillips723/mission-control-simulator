"""
Mission Control — Command Executor

Manages the full command lifecycle:
  1. CREATE   — Agent approves a maneuver, command is queued
  2. VALIDATE — Check command is still safe given current state
  3. TRANSMIT — Send command (enters light-speed transit)
  4. RECEIVE  — Spacecraft receives after comm delay
  5. EXECUTE  — Spacecraft applies the burn
  6. LOG      — Record full lifecycle to Delta + Lakebase

The communication delay is real: between transmission and receipt,
the spacecraft state changes. Commands must be validated against
the PREDICTED state at receipt time, not the current state.
"""

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

import lakebase_client
from physics_engine import (
    SpacecraftState,
    Vector3,
    estimate_fuel_cost,
    propagate_trajectory,
)


@dataclass
class Command:
    """A command in the mission control command queue."""
    command_id: str
    command_type: str  # burn, attitude_adjust, system_check, abort
    maneuver_id: Optional[str]
    payload: dict  # burn vector, duration, etc.
    priority: int  # 1=critical, 10=routine
    created_at: datetime
    approved_by: Optional[str] = None
    approved_at: Optional[datetime] = None
    transmit_time: Optional[datetime] = None
    estimated_receive_time: Optional[datetime] = None
    actual_receive_time: Optional[datetime] = None
    execution_time: Optional[datetime] = None
    status: str = "pending"  # pending, approved, transmitting, in_flight, received, executing, executed, failed, rejected
    result: Optional[str] = None
    rejection_reason: Optional[str] = None


@dataclass
class CommandResult:
    """Result of executing a command on the spacecraft."""
    command_id: str
    success: bool
    thrust_applied: Optional[Vector3] = None
    fuel_consumed_kg: float = 0.0
    state_before: Optional[SpacecraftState] = None
    state_after: Optional[SpacecraftState] = None
    message: str = ""


class CommandExecutor:
    """
    Manages command lifecycle from creation through execution.

    This class is used by multiple components:
    - Agent loop: creates and approves commands
    - Transmission notebook: transmits approved commands
    - Spacecraft tick: receives and executes commands
    """

    def __init__(self, catalog: str = "mission_control_dev"):
        self.catalog = catalog
        self._ops_count = 0

    @staticmethod
    def create_command(
        command_type: str,
        payload: dict,
        maneuver_id: Optional[str] = None,
        priority: int = 5,
        approved_by: str = "mission_commander",
    ) -> Command:
        """Create a new command from an agent decision."""
        now = datetime.now(timezone.utc)
        return Command(
            command_id=str(uuid.uuid4()),
            command_type=command_type,
            maneuver_id=maneuver_id,
            payload=payload,
            priority=priority,
            created_at=now,
            approved_by=approved_by,
            approved_at=now,
            status="approved",
        )

    @staticmethod
    def create_burn_command(
        burn_vector_x: float,
        burn_vector_y: float,
        burn_vector_z: float,
        burn_duration_s: float,
        maneuver_id: Optional[str] = None,
        priority: int = 5,
        approved_by: str = "mission_commander",
    ) -> Command:
        """Convenience: create a burn command from vector components."""
        payload = {
            "burn_vector_x": burn_vector_x,
            "burn_vector_y": burn_vector_y,
            "burn_vector_z": burn_vector_z,
            "burn_duration_s": burn_duration_s,
        }
        return CommandExecutor.create_command(
            command_type="burn",
            payload=payload,
            maneuver_id=maneuver_id,
            priority=priority,
            approved_by=approved_by,
        )

    @staticmethod
    def validate_command(
        command: Command,
        spacecraft_state: SpacecraftState,
        comm_delay_s: float,
    ) -> tuple[bool, str]:
        """
        Validate a command against the predicted spacecraft state at receipt time.

        Checks:
        1. Fuel sufficiency (including emergency reserve of 50 kg)
        2. Engine status (must be nominal or degraded-but-functional)
        3. No critical hazards that invalidate the maneuver

        Returns:
            (is_valid, reason)
        """
        if command.command_type != "burn":
            return True, "Non-burn commands always valid"

        payload = command.payload

        # Predict state at receipt time
        predicted_states = propagate_trajectory(
            spacecraft_state,
            duration_s=comm_delay_s + 60,  # Add 60s buffer
            dt=10.0,
        )
        state_at_receipt = predicted_states[-1]

        # Check fuel
        burn_vec = Vector3(
            payload["burn_vector_x"],
            payload["burn_vector_y"],
            payload["burn_vector_z"],
        )
        burn_duration = payload["burn_duration_s"]
        delta_v = burn_vec.magnitude() * burn_duration
        fuel_needed = estimate_fuel_cost(delta_v)
        emergency_reserve = 50.0

        if state_at_receipt.fuel_remaining_kg - fuel_needed < emergency_reserve:
            return False, (
                f"Insufficient fuel at receipt time. "
                f"Predicted fuel: {state_at_receipt.fuel_remaining_kg:.1f} kg, "
                f"needed: {fuel_needed:.1f} kg, "
                f"reserve: {emergency_reserve:.1f} kg"
            )

        # Check engine
        if state_at_receipt.engine_status == "offline":
            return False, "Engine predicted offline at receipt time"

        # Check hull
        if state_at_receipt.hull_integrity < 10:
            return False, f"Hull integrity critical: {state_at_receipt.hull_integrity:.1f}%"

        return True, "Command validated"

    @staticmethod
    def execute_burn(
        command: Command,
        spacecraft_state: SpacecraftState,
    ) -> CommandResult:
        """
        Execute a burn command on the spacecraft.

        Applies thrust for the specified duration and returns the resulting state.
        """
        if command.command_type != "burn":
            return CommandResult(
                command_id=command.command_id,
                success=True,
                message=f"Non-burn command '{command.command_type}' acknowledged",
            )

        payload = command.payload
        burn_vec = Vector3(
            payload["burn_vector_x"],
            payload["burn_vector_y"],
            payload["burn_vector_z"],
        )
        burn_duration = payload["burn_duration_s"]

        # Check fuel
        delta_v = burn_vec.magnitude() * burn_duration
        fuel_needed = estimate_fuel_cost(delta_v)

        if spacecraft_state.fuel_remaining_kg < fuel_needed:
            return CommandResult(
                command_id=command.command_id,
                success=False,
                state_before=spacecraft_state,
                state_after=spacecraft_state,
                message=f"Burn failed: insufficient fuel ({spacecraft_state.fuel_remaining_kg:.1f} < {fuel_needed:.1f} kg)",
            )

        # Execute the burn by propagating with thrust
        from physics_engine import propagate_state

        state_before = spacecraft_state
        current = spacecraft_state
        fuel_burn_rate = fuel_needed / burn_duration

        # Apply burn in 1-second steps
        steps = int(burn_duration)
        for _ in range(steps):
            current = propagate_state(
                current,
                dt=1.0,
                thrust=burn_vec,
                fuel_burn_rate_kg_per_s=fuel_burn_rate,
            )

        return CommandResult(
            command_id=command.command_id,
            success=True,
            thrust_applied=burn_vec,
            fuel_consumed_kg=state_before.fuel_remaining_kg - current.fuel_remaining_kg,
            state_before=state_before,
            state_after=current,
            message=f"Burn executed: Δv={delta_v:.4f} km/s, fuel used={state_before.fuel_remaining_kg - current.fuel_remaining_kg:.1f} kg",
        )

    def queue_command_sql(self, command: Command) -> str:
        """Generate SQL to insert a command into Lakebase command queue."""
        self._ops_count += 1
        payload_json = json.dumps(command.payload).replace("'", "''")
        return f"""
            INSERT INTO command_queue VALUES (
                '{command.command_id}',
                '{command.command_type}',
                '{payload_json}',
                {command.priority},
                NOW(),
                '{command.approved_by or ""}',
                NOW(),
                NULL,
                NULL,
                '{command.status}',
                NOW()
            )
        """

    def queue_command(self, command: Command) -> int:
        """Insert a command into Lakebase command queue via lakebase_client."""
        self._ops_count += 1
        payload_json = json.dumps(command.payload)
        return lakebase_client.execute(
            "INSERT INTO command_queue VALUES (%(command_id)s, %(command_type)s, %(payload)s, %(priority)s, NOW(), %(approved_by)s, NOW(), NULL, NULL, %(status)s, NOW())",
            {
                "command_id": command.command_id,
                "command_type": command.command_type,
                "payload": payload_json,
                "priority": command.priority,
                "approved_by": command.approved_by or "",
                "status": command.status,
            }
        )

    def transmit_command_sql(
        self,
        command_id: str,
        comm_delay_s: float,
    ) -> str:
        """Generate SQL to mark a command as transmitted and set estimated receive time."""
        self._ops_count += 1
        return f"""
            UPDATE command_queue
            SET
                status = 'in_flight',
                transmit_time = NOW(),
                estimated_receive_time = NOW() + INTERVAL {int(comm_delay_s)} SECONDS,
                updated_at = NOW()
            WHERE command_id = '{command_id}'
              AND status = 'approved'
        """

    def transmit_command(self, command_id: str, comm_delay_s: float) -> int:
        """Mark a command as transmitted via lakebase_client."""
        self._ops_count += 1
        return lakebase_client.execute(
            "UPDATE command_queue SET status = 'in_flight', transmit_time = NOW(), estimated_receive_time = NOW() + INTERVAL %(delay)s SECONDS, updated_at = NOW() WHERE command_id = %(command_id)s AND status = 'approved'",
            {"command_id": command_id, "delay": int(comm_delay_s)}
        )

    def mark_received_sql(self, command_id: str) -> str:
        """Generate SQL to mark a command as received by spacecraft."""
        self._ops_count += 1
        return f"""
            UPDATE command_queue
            SET status = 'received', updated_at = NOW()
            WHERE command_id = '{command_id}'
              AND status = 'in_flight'
        """

    def mark_received(self, command_id: str) -> int:
        """Mark a command as received via lakebase_client."""
        self._ops_count += 1
        return lakebase_client.execute(
            "UPDATE command_queue SET status = 'received', updated_at = NOW() WHERE command_id = %(command_id)s AND status = 'in_flight'",
            {"command_id": command_id}
        )

    def mark_executed_sql(self, command_id: str, result_msg: str) -> str:
        """Generate SQL to mark a command as executed."""
        self._ops_count += 1
        safe_msg = result_msg.replace("'", "''")
        return f"""
            UPDATE command_queue
            SET status = 'executed', updated_at = NOW()
            WHERE command_id = '{command_id}'
        """

    def mark_executed(self, command_id: str, result_msg: str) -> int:
        """Mark a command as executed via lakebase_client."""
        self._ops_count += 1
        return lakebase_client.execute(
            "UPDATE command_queue SET status = 'executed', updated_at = NOW() WHERE command_id = %(command_id)s",
            {"command_id": command_id}
        )

    def mark_failed_sql(self, command_id: str, reason: str) -> str:
        """Generate SQL to mark a command as failed/rejected."""
        self._ops_count += 1
        safe_reason = reason.replace("'", "''")
        return f"""
            UPDATE command_queue
            SET status = 'failed', updated_at = NOW()
            WHERE command_id = '{command_id}'
        """

    def mark_failed(self, command_id: str, reason: str) -> int:
        """Mark a command as failed via lakebase_client."""
        self._ops_count += 1
        return lakebase_client.execute(
            "UPDATE command_queue SET status = 'failed', updated_at = NOW() WHERE command_id = %(command_id)s",
            {"command_id": command_id}
        )

    def log_to_delta_sql(self, command: Command, result: Optional[CommandResult] = None) -> str:
        """Generate SQL to log a command lifecycle event to Delta."""
        self._ops_count += 1
        payload_json = json.dumps(command.payload).replace("'", "''")
        result_str = ""
        if result:
            result_str = result.message.replace("'", "''")

        return f"""
            INSERT INTO `{self.catalog}`.commands.command_log VALUES (
                '{command.command_id}',
                {f"'{command.maneuver_id}'" if command.maneuver_id else 'NULL'},
                '{command.command_type}',
                '{payload_json}',
                CURRENT_TIMESTAMP(),
                {f"'{command.approved_by}'" if command.approved_by else 'NULL'},
                {f"TIMESTAMP '{command.approved_at.strftime('%Y-%m-%d %H:%M:%S')}'" if command.approved_at else 'NULL'},
                {f"TIMESTAMP '{command.transmit_time.strftime('%Y-%m-%d %H:%M:%S')}'" if command.transmit_time else 'NULL'},
                {f"TIMESTAMP '{command.estimated_receive_time.strftime('%Y-%m-%d %H:%M:%S')}'" if command.estimated_receive_time else 'NULL'},
                {f"TIMESTAMP '{command.actual_receive_time.strftime('%Y-%m-%d %H:%M:%S')}'" if command.actual_receive_time else 'NULL'},
                {f"TIMESTAMP '{command.execution_time.strftime('%Y-%m-%d %H:%M:%S')}'" if command.execution_time else 'NULL'},
                '{command.status}',
                '{result_str}'
            )
        """

    @property
    def ops_count(self) -> int:
        return self._ops_count
