import json
from typing import Set, Optional
from ledger.projections.base import Projection
from ledger.schema.events import StoredEvent

class ApplicationSummaryProjection(Projection):
    @property
    def name(self) -> str:
        return "ApplicationSummary"

    async def subscribed_event_types(self) -> Set[str]:
        return {
            "ApplicationSubmitted",
            "CreditAnalysisCompleted",
            "FraudScreeningCompleted",
            "DecisionGenerated",
            "HumanReviewCompleted",
            "ApplicationApproved",
            "ApplicationDeclined"
        }

    async def handle(self, event: StoredEvent) -> None:
        """Process event and upsert to application_summary table."""
        app_id = event.payload.get("application_id")
        if not app_id:
            return
            
        current = await self._get_or_create(app_id)

        # Apply state updates based on event type
        if event.event_type == "ApplicationSubmitted":
            current["state"] = "SUBMITTED"
            current["applicant_id"] = event.payload.get("applicant_id")
            current["requested_amount_usd"] = event.payload.get("requested_amount_usd")
        
        elif event.event_type == "CreditAnalysisCompleted":
            # Don't jump backwards if we are already in compliance or pending decision
            if current["state"] in ("SUBMITTED", "UNKNOWN", "CREDIT_ANALYSIS_REQUESTED", "DOCUMENTS_PROCESSED"):
                current["state"] = "CREDIT_ANALYSIS_COMPLETE"
            current["risk_tier"] = event.payload.get("risk_tier")
            sess = event.payload.get("session_id")
            if sess and sess not in current["agent_sessions_completed"]:
                current["agent_sessions_completed"].append(sess)

        elif event.event_type == "FraudScreeningCompleted":
            # Fraud screening doesn't cleanly overwrite the state in the primary machine path 
            # unless it's a specific step. Assuming it advances or supplements state.
            if current["state"] in ("SUBMITTED", "UNKNOWN", "CREDIT_ANALYSIS_REQUESTED", "CREDIT_ANALYSIS_COMPLETE"):
                current["state"] = "FRAUD_SCREENING_COMPLETE"
            current["fraud_score"] = event.payload.get("fraud_score")
            current["anomaly_flags"] = event.payload.get("anomaly_flags", [])
            sess = event.payload.get("session_id")
            if sess and sess not in current["agent_sessions_completed"]:
                current["agent_sessions_completed"].append(sess)

        elif event.event_type == "DecisionGenerated":
            if current["state"] not in ("PENDING_HUMAN_REVIEW", "APPROVED", "DECLINED"):
                current["state"] = "PENDING_DECISION"
            current["decision"] = event.payload.get("recommendation")
            
        elif event.event_type == "HumanReviewCompleted":
            current["state"] = "PENDING_HUMAN_REVIEW"
            current["decision"] = event.payload.get("final_decision")
            current["human_reviewer_id"] = event.payload.get("reviewer_id")
            current["final_decision_at"] = event.recorded_at

        elif event.event_type == "ApplicationApproved":
            current["state"] = "APPROVED"
            current["approved_amount_usd"] = event.payload.get("approved_amount_usd")
            current["final_decision_at"] = event.recorded_at

        elif event.event_type == "ApplicationDeclined":
            current["state"] = "DECLINED"
            current["final_decision_at"] = event.recorded_at

        current["last_event_type"] = event.event_type
        current["last_event_at"] = event.recorded_at

        await self._upsert(current)

    async def _get_or_create(self, app_id: str) -> dict:
        if getattr(self.store, "_pool", None) is None:
            if not hasattr(self, "_mem_db"): self._mem_db = {}
            if app_id in self._mem_db:
                return dict(self._mem_db[app_id])
            return {
                "application_id": app_id, 
                "state": "UNKNOWN", 
                "agent_sessions_completed": [],
                "applicant_id": None, "requested_amount_usd": None,
                "approved_amount_usd": None, "risk_tier": None,
                "fraud_score": None, "anomaly_flags": [], "compliance_status": None,
                "decision": None, "last_event_type": None,
                "last_event_at": None, "human_reviewer_id": None,
                "final_decision_at": None
            }
            
        async with self.store._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM application_summary WHERE application_id = $1", 
                app_id
            )
            if row:
                d = dict(row)
                if d["agent_sessions_completed"] is None:
                    d["agent_sessions_completed"] = []
                return d
            return {
                "application_id": app_id, 
                "state": "UNKNOWN", 
                "agent_sessions_completed": [],
                "applicant_id": None,
                "requested_amount_usd": None,
                "approved_amount_usd": None,
                "risk_tier": None,
                "fraud_score": None, "anomaly_flags": [],
                "compliance_status": None,
                "decision": None,
                "last_event_type": None,
                "last_event_at": None,
                "human_reviewer_id": None,
                "final_decision_at": None
            }

    async def _upsert(self, data: dict) -> None:
        if getattr(self.store, "_pool", None) is None:
            if not hasattr(self, "_mem_db"): self._mem_db = {}
            # Replicate coalesce behavior for in memory
            existing = self._mem_db.get(data["application_id"], {})
            for k in data:
                if existing.get(k) is not None and data[k] is None:
                    continue # COALESCE keeps old non-null
                existing[k] = data[k]
            self._mem_db[data["application_id"]] = existing
            return
            
        async with self.store._pool.acquire() as conn:
            # We use COALESCE(EXCLUDED.col, application_summary.col) so if the new event doesn't have it, we keep the old
            await conn.execute("""
                INSERT INTO application_summary (
                    application_id, state, applicant_id, requested_amount_usd,
                    approved_amount_usd, risk_tier, fraud_score, compliance_status,
                    decision, agent_sessions_completed, last_event_type, last_event_at,
                    human_reviewer_id, final_decision_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT (application_id) DO UPDATE SET
                    state = EXCLUDED.state,
                    applicant_id = COALESCE(EXCLUDED.applicant_id, application_summary.applicant_id),
                    requested_amount_usd = COALESCE(EXCLUDED.requested_amount_usd, application_summary.requested_amount_usd),
                    approved_amount_usd = COALESCE(EXCLUDED.approved_amount_usd, application_summary.approved_amount_usd),
                    risk_tier = COALESCE(EXCLUDED.risk_tier, application_summary.risk_tier),
                    fraud_score = COALESCE(EXCLUDED.fraud_score, application_summary.fraud_score),
                    compliance_status = COALESCE(EXCLUDED.compliance_status, application_summary.compliance_status),
                    decision = COALESCE(EXCLUDED.decision, application_summary.decision),
                    agent_sessions_completed = EXCLUDED.agent_sessions_completed,
                    last_event_type = EXCLUDED.last_event_type,
                    last_event_at = EXCLUDED.last_event_at,
                    human_reviewer_id = COALESCE(EXCLUDED.human_reviewer_id, application_summary.human_reviewer_id),
                    final_decision_at = COALESCE(EXCLUDED.final_decision_at, application_summary.final_decision_at)
            """, 
            data["application_id"], data["state"], data["applicant_id"], 
            data["requested_amount_usd"], data["approved_amount_usd"], 
            data["risk_tier"], data["fraud_score"], data["compliance_status"],
            data["decision"], data["agent_sessions_completed"], 
            data["last_event_type"], data["last_event_at"], 
            data["human_reviewer_id"], data["final_decision_at"])

    async def get_lag(self) -> int:
        """
        Calculate lag in milliseconds between the latest event in the DB and this projection's last processed event time.
        """
        if getattr(self.store, "_pool", None) is None:
            if not getattr(self.store, "_global", None): return 0
            max_time = self.store._global[-1]["recorded_at"]
            pos = await self.get_last_position()
            current_time = None
            for e in self.store._global:
                if e["global_position"] == pos:
                    current_time = e["recorded_at"]
                    break
            if not current_time: return 0
            
            if isinstance(max_time, str):
                from datetime import datetime
                # Python 3.11 supports fromisoformat with Z
                max_time = datetime.fromisoformat(max_time.replace("Z", "+00:00"))
            if isinstance(current_time, str):
                from datetime import datetime
                current_time = datetime.fromisoformat(current_time.replace("Z", "+00:00"))
                
            return int((max_time - current_time).total_seconds() * 1000)
            
        async with self.store._pool.acquire() as conn:
            # Single query to get both times using a subselect
            # Coalesce to current time if there are no events at all so lag is 0
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
            
            if not row or not row["max_time"]:
                return 0

            max_time = row["max_time"]
            current_time = row["current_time"]

            if not current_time:
                # Projection never processed an event; treat lag as age of latest event or 0.
                return 0

            lag_timedelta = max_time - current_time
            return int(lag_timedelta.total_seconds() * 1000)
    async def get_by_id(self, app_id: str) -> Optional[dict]:
        """Public query method for MCP resources."""
        data = await self._get_or_create(app_id)
        if data.get("state") == "UNKNOWN":
            return None
        return data
