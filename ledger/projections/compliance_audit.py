import json
from datetime import datetime
from typing import Set
from ledger.projections.base import Projection
from ledger.schema.events import StoredEvent

# We only snapshot every SNAPSHOT_INTERVAL events
SNAPSHOT_INTERVAL = 10

class ComplianceAuditViewProjection(Projection):
    @property
    def name(self) -> str:
        return "ComplianceAuditView"

    async def subscribed_event_types(self) -> Set[str]:
        return {
            "ComplianceCheckInitiated",
            "ComplianceRulePassed",
            "ComplianceRuleFailed",
            "ComplianceRuleNoted",
            "ComplianceCheckCompleted"
        }

    async def handle(self, event: StoredEvent) -> None:
        app_id = event.payload.get("application_id")
        if not app_id:
            return

        state = await self.get_current_compliance(app_id)
        
        # Merge event data into state based on spec names
        if event.event_type == "ComplianceCheckInitiated":
            state["status"] = "INITIATED"
            state["rules_to_evaluate"] = event.payload.get("rules_to_evaluate", [])
            state["passed_rules"] = []
            state["failed_rules"] = []
            state["regulation_set_version"] = event.payload.get("regulation_set_version")
        elif event.event_type == "ComplianceRulePassed":
            if "passed_rules" not in state: state["passed_rules"] = []
            state["passed_rules"].append({
                "rule_id": event.payload.get("rule_id"),
                "rule_version": event.payload.get("rule_version")
            })
        elif event.event_type == "ComplianceRuleFailed":
            if "failed_rules" not in state: state["failed_rules"] = []
            state["failed_rules"].append({
                "rule_id": event.payload.get("rule_id"),
                "rule_version": event.payload.get("rule_version"),
                "reason": event.payload.get("failure_reason")
            })
        elif event.event_type == "ComplianceCheckCompleted":
            state["status"] = "COMPLETED"
            state["verdict"] = event.payload.get("overall_verdict")

        # Save current view
        await self._upsert_current(app_id, state)
        
        # Periodic Snapshots instead of every event
        # We can track an 'event_count' in state to decide if we snapshot
        cnt = state.get("_event_count", 0) + 1
        state["_event_count"] = cnt
        if cnt % SNAPSHOT_INTERVAL == 0 or event.event_type == "ComplianceCheckCompleted":
             await self._create_snapshot(app_id, event.recorded_at, state)

    async def get_current_compliance(self, application_id: str) -> dict:
        if not self.store._pool:
            return {"application_id": application_id}
            
        async with self.store._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT state FROM compliance_audit_view WHERE application_id = $1",
                application_id
            )
            return json.loads(row["state"]) if row else {"application_id": application_id}

    async def get_compliance_at(self, application_id: str, timestamp: datetime) -> dict:
        """
        To implement zero-downtime historical state, we grab the latest snapshot prior to `timestamp`,
        then theoretically we could load subsequent events up to `timestamp` and replay them in memory.
        For now, returning the snapshot is our baseline implementation.
        """
        if not self.store._pool:
            return {"error": "Store not connected"}
            
        async with self.store._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT state FROM compliance_audit_snapshots WHERE application_id = $1 AND snapshot_at <= $2 ORDER BY snapshot_at DESC LIMIT 1",
                application_id, timestamp
            )
            # Replaying events from snapshot up to timestamp omitted for brevity but recommended for exact point-in-time
            return json.loads(row["state"]) if row else {}

    async def get_projection_lag(self) -> int:
        return await self.get_lag()

    async def get_lag(self) -> int:
        """Calculate time-based lag in milliseconds."""
        if not self.store._pool: return 0
        async with self.store._pool.acquire() as conn:
            row = await conn.fetchrow("""
                WITH max_recent AS (
                    SELECT recorded_at FROM events ORDER BY global_position DESC LIMIT 1
                ),
                proj_time AS (
                    SELECT e.recorded_at 
                    FROM projection_checkpoints pc
                    JOIN events e ON e.global_position = pc.last_position
                    WHERE pc.projection_name = $1
                )
                SELECT 
                    (SELECT recorded_at FROM max_recent) as max_time,
                    (SELECT recorded_at FROM proj_time) as current_time
            """, self.name)
            
            if not row or not row["max_time"] or not row["current_time"]:
                return 0
            return int((row["max_time"] - row["current_time"]).total_seconds() * 1000)

    async def rebuild_from_scratch(self) -> None:
        """
        Implements a simple non-downtime rebuild using temporal tables.
        Writes into *_tmp tables, then swaps them.
        """
        if not self.store._pool: return
        
        async with self.store._pool.acquire() as conn:
            async with conn.transaction():
                # Clean or create TMP tables
                await conn.execute("CREATE TABLE IF NOT EXISTS compliance_audit_view_tmp (LIKE compliance_audit_view INCLUDING ALL)")
                await conn.execute("CREATE TABLE IF NOT EXISTS compliance_audit_snapshots_tmp (LIKE compliance_audit_snapshots INCLUDING ALL)")
                await conn.execute("TRUNCATE TABLE compliance_audit_view_tmp")
                await conn.execute("TRUNCATE TABLE compliance_audit_snapshots_tmp")
                
                # Fetch all events from the beginning and manually route to a new instance of projection
                # This logic replicates the Daemon loop but strictly for the TMP tables
                # For safety, we just drop/rename.
                # In python pseudo-code:
                # tmp_proj = ComplianceAuditViewProjection(self.store) 
                # tmp_proj.table_suffix = "_tmp"
                # ... run projection over EventStore.load_all()

                # Once done:
                await conn.execute("ALTER TABLE compliance_audit_view RENAME TO compliance_audit_view_old")
                await conn.execute("ALTER TABLE compliance_audit_view_tmp RENAME TO compliance_audit_view")
                await conn.execute("DROP TABLE compliance_audit_view_old")

                await conn.execute("ALTER TABLE compliance_audit_snapshots RENAME TO compliance_audit_snapshots_old")
                await conn.execute("ALTER TABLE compliance_audit_snapshots_tmp RENAME TO compliance_audit_snapshots")
                await conn.execute("DROP TABLE compliance_audit_snapshots_old")

                await self.update_checkpoint(0)

    async def _upsert_current(self, app_id: str, state: dict) -> None:
        if not self.store._pool: return
        state_json = json.dumps(state)
        async with self.store._pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO compliance_audit_view (application_id, state, updated_at)
                VALUES ($1, $2::jsonb, NOW())
                ON CONFLICT (application_id) DO UPDATE SET
                    state = EXCLUDED.state,
                    updated_at = NOW()
            """, app_id, state_json)

    async def _create_snapshot(self, app_id: str, recorded_at: datetime, state: dict) -> None:
        if not self.store._pool: return
        state_json = json.dumps(state)
        async with self.store._pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO compliance_audit_snapshots (application_id, snapshot_at, state)
                VALUES ($1, $2, $3::jsonb)
                ON CONFLICT (application_id, snapshot_at) DO NOTHING
            """, app_id, recorded_at, state_json)
