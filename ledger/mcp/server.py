from __future__ import annotations
import os
import asyncio
from typing import Optional, Any, List
from mcp.server.fastmcp import FastMCP
from ledger.event_store import EventStore, InMemoryEventStore, UpcasterRegistry
from ledger.upcasters import register_upcasters
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.projections.compliance_audit import ComplianceAuditViewProjection
from ledger.projections.daemon import ProjectionDaemon
from ledger.mcp import tools, resources

class LedgerContext:
    def __init__(self, store: EventStore, daemon: ProjectionDaemon, projections: dict):
        self.store = store
        self.daemon = daemon
        self.projections = projections

# Create the FastMCP server
mcp = FastMCP("Ledger")

# Storage for the runtime context
_ctx: Optional[LedgerContext] = None

def build_ledger_context(store: Optional[EventStore] = None) -> LedgerContext:
    """Helper to assemble the ledger engine and projections."""
    registry = UpcasterRegistry()
    register_upcasters(registry)
    
    if store is None:
        store = InMemoryEventStore(upcaster_registry=registry)
    
    app_summary = ApplicationSummaryProjection(store)
    compliance_audit = ComplianceAuditViewProjection(store)
    
    projections = {
        "application_summary": app_summary,
        "compliance_audit": compliance_audit
    }
    
    daemon = ProjectionDaemon(store, [app_summary, compliance_audit])
    return LedgerContext(store, daemon, projections)

async def init_ledger():
    global _ctx
    if _ctx: 
        # print(f"DEBUG: init_ledger skipping, _ctx already exists with store type: {type(_ctx.store)}")
        return
    
    # print("DEBUG: init_ledger initializing default context (InMemory)")
    _ctx = build_ledger_context()
    asyncio.create_task(_ctx.daemon.run_forever())

# --- Tool Registration ---

@mcp.tool()
async def submit_application(applicant_id: str, loan_amount: float, purpose: str, application_id: str) -> str:
    """
    Submits a new loan application. 
    INVARIANT: application_id must be unique. 
    RESULT: stream-0 for the loan.
    """
    await init_ledger()
    req = tools.SubmitApplicationRequest(
        application_id=application_id, 
        applicant_id=applicant_id, 
        loan_amount=loan_amount, 
        purpose=purpose
    )
    res = await tools.submit_application(_ctx.store, req)
    return res.model_dump_json()

@mcp.tool()
async def start_agent_session(agent_id: str, session_id: str, model_version: str) -> str:
    """
    Starts an AI agent session for tracking telemetry. 
    PRECONDITION: None. 
    INVARIANT: session_id unique per agent.
    """
    await init_ledger()
    req = tools.StartSessionRequest(agent_id=agent_id, session_id=session_id, model_version=model_version)
    res = await tools.start_agent_session(_ctx.store, req)
    return res.model_dump_json()

@mcp.tool()
async def record_fraud_screening(
    application_id: str, agent_id: str, session_id: str, 
    fraud_score: float, anomaly_flags: List[str], screening_model_version: str, 
    expected_version: int
) -> str:
    """
    Records fraud screening results. 
    PRECONDITION: Must call start_agent_session first. 
    FRAUD_SCORE: float 0.0-1.0.
    """
    await init_ledger()
    req = tools.RecordFraudRequest(
        application_id=application_id, agent_id=agent_id, 
        session_id=session_id, fraud_score=fraud_score, 
        anomaly_flags=anomaly_flags, screening_model_version=screening_model_version,
        expected_version=expected_version
    )
    res = await tools.record_fraud_screening(_ctx.store, req)
    return res.model_dump_json()

@mcp.tool()
async def record_credit_analysis(
    application_id: str, agent_id: str, session_id: str, expected_version: int, 
    model_version: str, confidence_score: float, risktier: str, 
    recommended_limit_usd: float, analysis_duration_ms: int, input_data_hash: str
) -> str:
    """
    Records v2 credit analysis results. 
    PRECONDITION: Must call start_agent_session with matching model_version first.
    EXPECTED_VERSION: Current stream version for optimistic concurrency.
    """
    await init_ledger()
    req = tools.RecordAnalysisRequest(
        application_id=application_id, agent_id=agent_id, session_id=session_id,
        model_version=model_version, confidence_score=confidence_score,
        risktier=risktier, recommended_limit_usd=recommended_limit_usd,
        analysis_duration_ms=analysis_duration_ms, input_data_hash=input_data_hash,
        expected_version=expected_version
    )
    res = await tools.record_credit_analysis(_ctx.store, req)
    return res.model_dump_json()

@mcp.tool()
async def generate_decision(
    application_id: str, orchestrator_agent_id: str, 
    recommendation: str, confidence_score: float, 
    contributing_agent_sessions: List[str], decision_basis_summary: str, 
    expected_version: int
) -> str:
    """
    Generates an orchestral decision. 
    PRECONDITION: Requires analysis/fraud sessions to be referenced.
    RECOMMENDATION: Literal["APPROVE", "DECLINE", "REFER"].
    """
    await init_ledger()
    req = tools.GenerateDecisionRequest(
        application_id=application_id, orchestrator_agent_id=orchestrator_agent_id,
        recommendation=recommendation, confidence_score=confidence_score,
        contributing_agent_sessions=contributing_agent_sessions,
        decision_basis_summary=decision_basis_summary, expected_version=expected_version
    )
    res = await tools.generate_decision(_ctx.store, req)
    return res.model_dump_json()

@mcp.tool()
async def record_human_review(
    application_id: str, reviewer_id: str, 
    final_decision: str, notes: str, expected_version: int,
    approved_amount_usd: float = 0.0, conditions: Optional[List[str]] = None
) -> str:
    """
    Records human reviewer decision and triggers terminal state. 
    PRECONDITION: Orchestral decision should be generated first.
    """
    await init_ledger()
    req = tools.RecordHumanReviewRequest(
        application_id=application_id, reviewer_id=reviewer_id,
        final_decision=final_decision, notes=notes, 
        approved_amount_usd=approved_amount_usd, conditions=conditions,
        expected_version=expected_version
    )
    res = await tools.record_human_review(_ctx.store, req)
    return res.model_dump_json()

@mcp.tool()
async def run_integrity_check(entity_type: str, entity_id: str) -> str:
    """
    Runs a cryptographic integrity check on an entity stream (e.g. loan, 123) 
    and appends result to its audit trail.
    """
    await init_ledger()
    req = tools.IntegrityCheckRequest(entity_type=entity_type, entity_id=entity_id)
    res = await tools.run_integrity_check(_ctx.store, req)
    return res.model_dump_json()

# --- Resource Registration ---

@mcp.resource("ledger://applications/{application_id}")
async def get_application(application_id: str) -> str:
    """Returns the current ApplicationSummary projection."""
    await init_ledger()
    return await resources.get_application_summary(application_id, _ctx.projections)

@mcp.resource("ledger://applications/{application_id}/compliance")
async def get_compliance(application_id: str) -> str:
    """Returns the current ComplianceAuditView projection."""
    await init_ledger()
    return await resources.get_compliance_view(application_id, _ctx.projections)

@mcp.resource("ledger://applications/{application_id}/audit-trail")
async def get_audit_trail(application_id: str) -> str:
    """Returns full audit stream with correlation IDs and segment metadata."""
    await init_ledger()
    return await resources.get_audit_trail(application_id, _ctx.store)

@mcp.resource("ledger://applications/{application_id}/integrity-trail")
async def get_integrity_trail(application_id: str) -> str:
    """Returns the cryptographic integrity check records for an application."""
    await init_ledger()
    # Direct access to the audit stream for verification
    stream_id = f"audit-loan-{application_id}"
    events = await _ctx.store.load_stream(stream_id)
    log = [
        {
            "event_type": e.event_type,
            "recorded_at": e.recorded_at.isoformat(),
            "payload": e.payload,
            "stream_position": e.stream_position
        }
        for e in events
    ]
    return resources.wrap_result(log)

@mcp.resource("ledger://ledger/health")
async def get_health() -> str:
    """Returns projection engine lags."""
    await init_ledger()
    return await resources.get_ledger_health(_ctx.daemon)

if __name__ == "__main__":
    mcp.run()
