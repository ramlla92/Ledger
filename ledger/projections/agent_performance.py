from typing import Set
from ledger.projections.base import Projection
from ledger.schema.events import StoredEvent

class AgentPerformanceLedgerProjection(Projection):
    @property
    def name(self) -> str:
        return "AgentPerformanceLedger"

    async def subscribed_event_types(self) -> Set[str]:
        return {
            "CreditAnalysisCompleted",
            "DecisionGenerated",
            "HumanReviewCompleted"
        }

    async def handle(self, event: StoredEvent) -> None:
        agent_id = None
        model_version = None

        if event.event_type == "CreditAnalysisCompleted":
            agent_id = event.payload.get("agent_id")
            model_version = event.payload.get("model_version")
        elif event.event_type == "DecisionGenerated":
            agent_id = event.payload.get("orchestrator_agent_id")
            # For Orchestrator, try to pull its own model version or default to v1
            mv = event.payload.get("model_versions", {})
            model_version = mv.get("orchestrator") or "v1"
        elif event.event_type == "HumanReviewCompleted":
            # Per prompt: "confirm whether metrics should be attached to the orchestrator agent/model rather than reviewer_id, and implement a better strategy."
            # A human review override is typically a reflection on the DECISION ORCHESTRATOR's performance.
            # However, the event does not contain orchestrator_agent_id directly.
            # We will rely on a lookup or skip assigning it here if we cannot link it robustly without querying.
            # For simplicity, we can query the DB for the original DecisionGenerated of this application.
            app_id = event.payload.get("application_id")
            if not app_id: return
            res = await self._find_orchestrator_for_app(app_id)
            if not res: return
            agent_id, model_version = res
            
        if not agent_id or not model_version:
            return

        current = await self._get_or_create(agent_id, model_version, event.recorded_at)

        # Apply mutations based on event type
        if event.event_type == "CreditAnalysisCompleted":
            current["analyses_completed"] += 1
            dur = event.payload.get("analysis_duration_ms", 0)
            n = current["analyses_completed"]
            prev_avg = current["avg_duration_ms"] or 0
            current["avg_duration_ms"] = prev_avg + (dur - prev_avg) / n

        elif event.event_type == "DecisionGenerated":
            current["decisions_generated"] += 1
            conf = event.payload.get("confidence_score")
            if conf is not None:
                n = current["decisions_generated"]
                prev_avg = current["avg_confidence_score"] or 0
                current["avg_confidence_score"] = prev_avg + (conf - prev_avg) / n
                    
            rec = event.payload.get("recommendation")
            if rec == "APPROVE":
                current["_apps_approved"] = current.get("_apps_approved", 0) + 1
            elif rec == "DECLINE":
                current["_apps_declined"] = current.get("_apps_declined", 0) + 1
            elif rec == "REFER":
                current["_apps_referred"] = current.get("_apps_referred", 0) + 1
                
            tot = current["decisions_generated"]
            current["approve_rate"] = current.get("_apps_approved", 0) / tot
            current["decline_rate"] = current.get("_apps_declined", 0) / tot
            current["refer_rate"] = current.get("_apps_referred", 0) / tot

        elif event.event_type == "HumanReviewCompleted":
            override = event.payload.get("override", False)
            if override:
                current["_overrides"] = current.get("_overrides", 0) + 1
            
            # Since we only attribute this to the orchestrator, and every human review corresponds to 1 decision
            # we can calculate override rate against TOTAL decisions for this agent, OR total reviews.
            # Usually, human_override_rate is overrides / total_decisions.
            tot_decisions = current["decisions_generated"]
            if tot_decisions > 0:
                current["human_override_rate"] = current.get("_overrides", 0) / tot_decisions

        current["last_seen_at"] = event.recorded_at

        await self._upsert(current)

    async def _find_orchestrator_for_app(self, app_id: str):
        if not self.store._pool: return None
        async with self.store._pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT payload->>'orchestrator_agent_id' as orch_id, 
                       payload->'model_versions'->>'orchestrator' as mv 
                FROM events 
                WHERE stream_id = $1 AND event_type = 'DecisionGenerated'
                ORDER BY stream_position DESC LIMIT 1
            """, f"loan-{app_id}")
            if row and row["orch_id"]:
                return (row["orch_id"], row["mv"] or "v1")
            return None

    async def _get_or_create(self, agent_id: str, model_version: str, first_seen_at) -> dict:
        if not self.store._pool:
            return self._default_dict(agent_id, model_version, first_seen_at)
            
        async with self.store._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM agent_performance_ledger WHERE agent_id = $1 AND model_version = $2", 
                agent_id, model_version
            )
            if row:
                d = dict(row)
                d["_apps_approved"] = int(round(d.get("approve_rate", 0) * d["decisions_generated"])) if d.get("approve_rate") else 0
                d["_apps_declined"] = int(round(d.get("decline_rate", 0) * d["decisions_generated"])) if d.get("decline_rate") else 0
                d["_apps_referred"] = int(round(d.get("refer_rate", 0) * d["decisions_generated"])) if d.get("refer_rate") else 0
                d["_overrides"] = int(round(d.get("human_override_rate", 0) * d["decisions_generated"])) if d.get("human_override_rate") else 0
                return d
            return self._default_dict(agent_id, model_version, first_seen_at)

    def _default_dict(self, agent_id: str, model_version: str, first_seen_at) -> dict:
        return {
            "agent_id": agent_id,
            "model_version": model_version,
            "analyses_completed": 0,
            "decisions_generated": 0,
            "avg_confidence_score": None,
            "avg_duration_ms": None,
            "approve_rate": 0.0,
            "decline_rate": 0.0,
            "refer_rate": 0.0,
            "human_override_rate": 0.0,
            "first_seen_at": first_seen_at,
            "last_seen_at": first_seen_at,
            "_apps_approved": 0,
            "_apps_declined": 0,
            "_apps_referred": 0,
            "_overrides": 0
        }

    async def _upsert(self, data: dict) -> None:
        if not self.store._pool:
            return
            
        async with self.store._pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO agent_performance_ledger (
                    agent_id, model_version, analyses_completed, decisions_generated,
                    avg_confidence_score, avg_duration_ms, approve_rate, decline_rate,
                    refer_rate, human_override_rate, first_seen_at, last_seen_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (agent_id, model_version) DO UPDATE SET
                    analyses_completed = EXCLUDED.analyses_completed,
                    decisions_generated = EXCLUDED.decisions_generated,
                    avg_confidence_score = EXCLUDED.avg_confidence_score,
                    avg_duration_ms = EXCLUDED.avg_duration_ms,
                    approve_rate = EXCLUDED.approve_rate,
                    decline_rate = EXCLUDED.decline_rate,
                    refer_rate = EXCLUDED.refer_rate,
                    human_override_rate = EXCLUDED.human_override_rate,
                    last_seen_at = EXCLUDED.last_seen_at
            """, 
            data["agent_id"], data["model_version"], data["analyses_completed"],
            data["decisions_generated"], data["avg_confidence_score"], data["avg_duration_ms"],
            data["approve_rate"], data["decline_rate"], data["refer_rate"],
            data["human_override_rate"], data["first_seen_at"], data["last_seen_at"])

    async def get_lag(self) -> int:
        """Calculate lag in milliseconds between latest event recorded_at and the projection checkpoint."""
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
