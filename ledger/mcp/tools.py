from __future__ import annotations
import json
from uuid import UUID
from datetime import datetime
from typing import Any, Optional, Dict, List, Literal
from pydantic import BaseModel, Field, field_validator
from ledger.event_store import EventStore, OptimisticConcurrencyError

# --- Standard Error Envelope ---
class LedgerError(BaseModel):
    error_type: Literal["OptimisticConcurrencyError", "PreconditionFailed", "ValidationError", "IntegrityError", "UnexpectedError"]
    message: str
    suggested_action: Literal["reload_stream_and_retry", "start_agent_session_first", "restart_agent_session_with_correct_model_version", "fix_request_and_retry", "check_ledger_logs"]
    details: Optional[Dict[str, Any]] = None

class ToolResult(BaseModel):
    ok: bool
    result: Optional[Dict[str, Any]] = None
    error: Optional[LedgerError] = None

    def to_json(self) -> str:
        return self.model_dump_json()

# --- Tool Request Models ---

class SubmitApplicationRequest(BaseModel):
    application_id: str
    applicant_id: str
    loan_amount: float
    purpose: str

class StartSessionRequest(BaseModel):
    agent_id: str
    session_id: str
    model_version: str

class RecordAnalysisRequest(BaseModel):
    application_id: str
    agent_id: str
    session_id: str
    model_version: str
    confidence_score: float = Field(ge=0.0, le=1.0)
    risktier: str
    recommended_limit_usd: float
    analysis_duration_ms: int
    input_data_hash: str
    expected_version: int

class RecordFraudRequest(BaseModel):
    application_id: str
    agent_id: str
    session_id: str
    fraud_score: float = Field(ge=0.0, le=1.0, description="Fraud probability score from 0.0 to 1.0")
    anomaly_flags: List[str]
    screening_model_version: str
    expected_version: int

class RecordComplianceRequest(BaseModel):
    application_id: str
    rule_id: str
    passed: bool
    failure_reason: Optional[str] = None
    regulation_set_version: str
    expected_version: int

    @field_validator('failure_reason')
    @classmethod
    def check_failure_reason(cls, v: Optional[str], info: Any) -> Optional[str]:
        if info.data.get('passed') is False and not v:
            raise ValueError('failure_reason is required when passed is False')
        return v

class GenerateDecisionRequest(BaseModel):
    application_id: str
    orchestrator_agent_id: str
    recommendation: Literal["APPROVE", "DECLINE", "REFER"]
    confidence_score: float = Field(ge=0.0, le=1.0)
    contributing_agent_sessions: List[str]
    decision_basis_summary: str
    expected_version: int

class RecordHumanReviewRequest(BaseModel):
    application_id: str
    reviewer_id: str
    final_decision: Literal["APPROVED", "DECLINED"]
    notes: str
    approved_amount_usd: Optional[float] = 0.0
    conditions: Optional[List[str]] = None
    expected_version: int

class IntegrityCheckRequest(BaseModel):
    entity_type: str
    entity_id: str

# --- Tool Implementations ---

async def submit_application(store: EventStore, req: SubmitApplicationRequest) -> ToolResult:
    """Submits a new loan application. PRECONDITION: application_id must be unique."""
    stream_id = f"loan-{req.application_id}"
    event = {
        "event_type": "ApplicationSubmitted",
        "event_version": 1,
        "payload": req.model_dump()
    }
    try:
        await store.append(stream_id, [event], -1)
        return ToolResult(ok=True, result={"stream_id": stream_id, "version": 0})
    except OptimisticConcurrencyError as e:
        return ToolResult(ok=False, error=LedgerError(
            error_type="OptimisticConcurrencyError",
            message=f"Application {req.application_id} already exists.",
            suggested_action="fix_request_and_retry",
            details={"stream_id": e.stream_id}
        ))

async def start_agent_session(store: EventStore, req: StartSessionRequest) -> ToolResult:
    """Initializes an AI agent session. PRECONDITION: session_id should be unique for the agent."""
    stream_id = f"agent-{req.agent_id}-{req.session_id}"
    events = [
        {
            "event_type": "AgentSessionStarted",
            "event_version": 1,
            "payload": {"session_id": req.session_id, "agent_id": req.agent_id, "started_at": datetime.utcnow().isoformat()}
        },
        {
            "event_type": "AgentContextLoaded",
            "event_version": 1,
            "payload": {"model_version": req.model_version, "loaded_at": datetime.utcnow().isoformat()}
        }
    ]
    try:
        await store.append(stream_id, events, -1)
        return ToolResult(ok=True, result={"stream_id": stream_id})
    except Exception as e:
        return ToolResult(ok=False, error=LedgerError(
            error_type="UnexpectedError",
            message=str(e),
            suggested_action="check_ledger_logs"
        ))

async def record_credit_analysis(store: EventStore, req: RecordAnalysisRequest) -> ToolResult:
    """
    Records v2 credit analysis results. 
    PRECONDITION: Requires AgentSession where first event is AgentContextLoaded 
    and model_version matches.
    """
    session_stream = f"agent-{req.agent_id}-{req.session_id}"
    events = await store.load_stream(session_stream)
    
    if not events:
        return ToolResult(ok=False, error=LedgerError(
            error_type="PreconditionFailed",
            message=f"No active session found for {req.agent_id}/{req.session_id}",
            suggested_action="start_agent_session_first"
        ))
    
    ctx_event = next((e for e in events if e.event_type == "AgentContextLoaded"), None)
    if not ctx_event:
        return ToolResult(ok=False, error=LedgerError(
            error_type="PreconditionFailed",
            message="Session stream corruption: AgentContextLoaded missing.",
            suggested_action="check_ledger_logs"
        ))
        
    context_model = ctx_event.payload.get("model_version")
    if context_model != req.model_version:
        return ToolResult(ok=False, error=LedgerError(
            error_type="PreconditionFailed",
            message=f"Model version mismatch. Session: {context_model}, Request: {req.model_version}",
            suggested_action="restart_agent_session_with_correct_model_version",
            details={"session_model": context_model, "request_model": req.model_version}
        ))

    stream_id = f"loan-{req.application_id}"
    # Flattened v2 payload as per spec
    event = {
        "event_type": "CreditAnalysisCompleted",
        "event_version": 2,
        "payload": {
            "application_id": req.application_id,
            "agent_id": req.agent_id,
            "session_id": req.session_id,
            "model_version": req.model_version,
            "confidence_score": req.confidence_score,
            "risk_tier": req.risktier,
            "recommended_limit_usd": req.recommended_limit_usd,
            "analysis_duration_ms": req.analysis_duration_ms,
            "input_data_hash": req.input_data_hash,
            "expected_version_used": req.expected_version,
            "completed_at": datetime.utcnow().isoformat()
        }
    }
    try:
        await store.append(stream_id, [event], req.expected_version)
        return ToolResult(ok=True, result={"new_version": req.expected_version + 1})
    except OptimisticConcurrencyError as e:
        return ToolResult(ok=False, error=LedgerError(
            error_type="OptimisticConcurrencyError",
            message="Stream version mismatch.",
            suggested_action="reload_stream_and_retry",
            details={"expected": e.expected, "actual": e.actual, "stream_id": e.stream_id}
        ))

async def record_fraud_screening(store: EventStore, req: RecordFraudRequest) -> ToolResult:
    """Records fraud screening results with fractional scoring."""
    session_stream = f"agent-{req.agent_id}-{req.session_id}"
    events = await store.load_stream(session_stream)
    if not events:
        return ToolResult(ok=False, error=LedgerError(error_type="PreconditionFailed", message="AgentSession not found."))

    stream_id = f"loan-{req.application_id}"
    event = {
        "event_type": "FraudScreeningCompleted",
        "event_version": 1,
        "payload": {
            "application_id": req.application_id,
            "agent_id": req.agent_id,
            "session_id": req.session_id,
            "fraud_score": req.fraud_score,
            "anomaly_flags": req.anomaly_flags,
            "screening_model_version": req.screening_model_version,
            "expected_version_used": req.expected_version,
            "completed_at": datetime.utcnow().isoformat()
        }
    }
    try:
        await store.append(stream_id, [event], req.expected_version)
        return ToolResult(ok=True, result={"new_version": req.expected_version + 1})
    except OptimisticConcurrencyError as e:
        return ToolResult(ok=False, error=LedgerError(
            error_type="OptimisticConcurrencyError",
            message="Stream version mismatch.",
            suggested_action="reload_stream_and_retry",
            details={"expected": e.expected, "actual": e.actual, "stream_id": e.stream_id}
        ))

async def generate_decision(store: EventStore, req: GenerateDecisionRequest) -> ToolResult:
    """Generates an orchestral decision. PRECONDITION: Requires analysis sessions."""
    stream_id = f"loan-{req.application_id}"
    event = {
        "event_type": "DecisionGenerated",
        "event_version": 2,
        "payload": {
            "application_id": req.application_id,
            "orchestrator_agent_id": req.orchestrator_agent_id,
            "recommendation": req.recommendation,
            "confidence_score": req.confidence_score,
            "contributing_agent_sessions": req.contributing_agent_sessions,
            "decision_basis_summary": req.decision_basis_summary,
            "completed_at": datetime.utcnow().isoformat()
        }
    }
    try:
        await store.append(stream_id, [event], req.expected_version)
        return ToolResult(ok=True, result={"new_version": req.expected_version + 1})
    except OptimisticConcurrencyError as e:
        return ToolResult(ok=False, error=LedgerError(
            error_type="OptimisticConcurrencyError",
            message="Stream version mismatch.",
            suggested_action="reload_stream_and_retry",
            details={"expected": e.expected_version, "actual": e.actual_version, "stream_id": e.stream_id}
        ))

async def record_human_review(store: EventStore, req: RecordHumanReviewRequest) -> ToolResult:
    """Records final human reviewer decision and advances lifecycle state."""
    stream_id = f"loan-{req.application_id}"
    
    # HumanReviewCompleted captures the full request context
    review_event = {
        "event_type": "HumanReviewCompleted",
        "event_version": 1,
        "payload": req.model_dump()
    }
    
    # Terminal lifecycle event for projections
    terminal_type = "ApplicationApproved" if req.final_decision == "APPROVED" else "ApplicationDeclined"
    terminal_event = {
        "event_type": terminal_type,
        "event_version": 1,
        "payload": {
            "application_id": req.application_id,
            "reviewer_id": req.reviewer_id,
            "final_decision": req.final_decision,
            "approved_amount_usd": req.approved_amount_usd,
            "conditions": req.conditions,
            "notes": req.notes
        }
    }
    
    try:
        await store.append(stream_id, [review_event, terminal_event], req.expected_version)
        return ToolResult(ok=True, result={"new_version": req.expected_version + 2})
    except OptimisticConcurrencyError as e:
        return ToolResult(ok=False, error=LedgerError(
            error_type="OptimisticConcurrencyError",
            message="Stream version mismatch.",
            suggested_action="reload_stream_and_retry",
            details={"expected": e.expected_version, "actual": e.actual_version, "stream_id": e.stream_id}
        ))

async def run_integrity_check(store: EventStore, req: IntegrityCheckRequest) -> ToolResult:
    """Triggers a cryptographic integrity check on an entity's stream."""
    from ledger.audit.integrity import run_integrity_check as run_check
    try:
        result = await run_check(store, req.entity_type, req.entity_id)
        audit_stream = f"audit-{req.entity_type}-{req.entity_id}"
        return ToolResult(ok=True, result={
            "audit_stream": audit_stream, 
            "checksum": result.integrity_hash,
            "chain_valid": result.chain_valid,
            "tamper_detected": result.tamper_detected,
            "events_verified_count": result.events_verified_count
        })
    except Exception as e:
        return ToolResult(ok=False, error=LedgerError(
            error_type="IntegrityError",
            message=str(e),
            suggested_action="check_ledger_logs"
        ))
