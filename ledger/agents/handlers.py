from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, List
from ledger.event_store import EventStore
from ledger.domain.loan_application import LoanApplicationAggregate, ApplicationState
from ledger.domain.agent_session import AgentSessionAggregate
from ledger.schema.events import (
    ApplicationSubmitted,
    CreditAnalysisRequested,
    CreditAnalysisCompleted,
    DecisionGenerated,
    HumanReviewCompleted,
    ApplicationApproved,
    ApplicationDeclined,
    FraudScreeningCompleted,
    LoanPurpose,
    DomainError,
    CreditDecision
)

@dataclass
class SubmitApplicationCommand:
    application_id: str
    applicant_id: str
    amount: Decimal
    purpose: LoanPurpose
    term_months: int

@dataclass
class CreditAnalysisCompletedCommand:
    application_id: str
    agent_id: str
    session_id: str
    model_version: str
    decision: CreditDecision

@dataclass
class GenerateDecisionCommand:
    application_id: str
    orchestrator_agent_id: str
    orchestrator_session_id: str
    recommendation: str
    confidence_score: float
    contributing_sessions: List[str]
    executive_summary: str

@dataclass
class HumanReviewCompletedCommand:
    application_id: str
    reviewer_id: str
    override: bool
    original_recommendation: str
    final_decision: str
    override_reason: str | None = None

@dataclass
class ApplicationApprovedCommand:
    application_id: str
    approved_amount_usd: Decimal
    interest_rate_pct: float
    term_months: int
    approved_by: str
    conditions: list[str] | None = None
    effective_date: str = "2024-01-01"

@dataclass
class FraudScreeningCompletedCommand:
    application_id: str
    session_id: str
    fraud_score: float
    risk_level: str
    anomalies_found: int
    recommendation: str
    screening_model_version: str
    input_data_hash: str

async def handle_submit_application(cmd: SubmitApplicationCommand, store: EventStore) -> None:
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    
    submit_event = ApplicationSubmitted(
        application_id=cmd.application_id,
        applicant_id=cmd.applicant_id,
        requested_amount_usd=cmd.amount,
        loan_purpose=cmd.purpose,
        loan_term_months=cmd.term_months,
        submission_channel="DIRECT",
        contact_email="test@example.com",
        contact_name="Test User",
        submitted_at=datetime.utcnow(),
        application_reference=f"REF-{cmd.application_id}"
    )
    
    req_event = CreditAnalysisRequested(
        application_id=cmd.application_id,
        requested_at=datetime.utcnow(),
        requested_by="system-orchestrator",
        priority="NORMAL"
    )
    
    # Batch append both events using the current aggregate version as expected_version
    await store.append(
        f"loan-{cmd.application_id}",
        [submit_event.to_store_dict(), req_event.to_store_dict()],
        expected_version=app.version
    )

async def handle_credit_analysis_completed(cmd: CreditAnalysisCompletedCommand, store: EventStore) -> None:
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    agent = await AgentSessionAggregate.load(store, cmd.agent_id, cmd.session_id)

    # Validations
    agent.assert_context_loaded()
    agent.assert_model_version_current(cmd.model_version)
    agent.assert_can_append_decision_for_application(cmd.application_id, app)
    app.assert_awaiting_credit_analysis()

    analysis_event = CreditAnalysisCompleted(
        application_id=cmd.application_id,
        agent_id=cmd.agent_id,
        session_id=cmd.session_id,
        decision=cmd.decision,
        model_version=cmd.model_version,
        model_deployment_id="dep-001",
        input_data_hash="hash-abc",
        analysis_duration_ms=1200,
        completed_at=datetime.utcnow()
    )

    # 1. Append to Agent Stream
    await store.append(
        f"agent-{cmd.agent_id}-{cmd.session_id}", 
        [analysis_event.to_store_dict()], 
        expected_version=agent.version
    )

    # 2. Append same event to Loan Stream
    await store.append(
        f"loan-{cmd.application_id}",
        [analysis_event.to_store_dict()],
        expected_version=app.version
    )

async def handle_generate_decision(cmd: GenerateDecisionCommand, store: EventStore) -> None:
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    
    # State-based guard
    if app.state not in (
        ApplicationState.CREDIT_ANALYSIS_COMPLETE,
        ApplicationState.COMPLIANCE_CHECK_COMPLETE,
        ApplicationState.PENDING_DECISION,
    ):
        raise DomainError(f"Cannot generate decision from state {app.state}")

    # Causal chain verification
    await app.assert_contributing_sessions_valid(cmd.contributing_sessions, store)
    
    # Confidence floor
    final_recommendation = app.enforce_confidence_floor(cmd.recommendation, cmd.confidence_score)
    
    decision_event = DecisionGenerated(
        application_id=cmd.application_id,
        orchestrator_agent_id=cmd.orchestrator_agent_id,
        orchestrator_session_id=cmd.orchestrator_session_id,
        recommendation=final_recommendation,
        confidence_score=cmd.confidence_score,
        executive_summary=cmd.executive_summary,
        contributing_sessions=cmd.contributing_sessions,
        generated_at=datetime.utcnow()
    )
    
    await store.append(
        f"loan-{cmd.application_id}",
        [decision_event.to_store_dict()],
        expected_version=app.version
    )

async def handle_application_approved(cmd: ApplicationApprovedCommand, store: EventStore) -> None:
    app = await LoanApplicationAggregate.load(store, cmd.application_id)

    # Enforce compliance dependency
    await app.assert_compliance_satisfied(store)

    approved_event = ApplicationApproved(
        application_id=cmd.application_id,
        approved_amount_usd=cmd.approved_amount_usd,
        interest_rate_pct=cmd.interest_rate_pct,
        term_months=cmd.term_months,
        conditions=cmd.conditions or [],
        approved_by=cmd.approved_by,
        effective_date=cmd.effective_date,
        approved_at=datetime.utcnow(),
    )

    await store.append(
        f"loan-{cmd.application_id}",
        [approved_event.to_store_dict()],
        expected_version=app.version,
    )

async def handle_human_review_completed(cmd: HumanReviewCompletedCommand, store: EventStore) -> None:
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    
    review_event = HumanReviewCompleted(
        application_id=cmd.application_id,
        reviewer_id=cmd.reviewer_id,
        override=cmd.override,
        original_recommendation=cmd.original_recommendation,
        final_decision=cmd.final_decision,
        override_reason=cmd.override_reason,
        reviewed_at=datetime.utcnow()
    )
    
    await store.append(
        f"loan-{cmd.application_id}",
        [review_event.to_store_dict()],
        expected_version=app.version
    )

async def handle_fraud_screening_completed(cmd: FraudScreeningCompletedCommand, store: EventStore) -> None:
    # Later: load a FraudAggregate here to enforce rules.
    # For now, blindly append to the fraud stream.
    fraud_event = FraudScreeningCompleted(
        application_id=cmd.application_id,
        session_id=cmd.session_id,
        fraud_score=cmd.fraud_score,
        risk_level=cmd.risk_level,
        anomalies_found=cmd.anomalies_found,
        recommendation=cmd.recommendation,
        screening_model_version=cmd.screening_model_version,
        input_data_hash=cmd.input_data_hash,
        completed_at=datetime.utcnow()
    )
    
    await store.append(
        f"fraud-{cmd.application_id}",
        [fraud_event.to_store_dict()],
        expected_version=-1   # Assuming new stream, or optimistic append
    )
