from __future__ import annotations
from typing import Any, Set
from decimal import Decimal
from ledger.event_store import EventStore
from ledger.schema.events import (
    ApplicationState,
    StoredEvent,
    DomainError,
    ApplicationSubmitted,
    CreditAnalysisRequested,
    DecisionGenerated,
    HumanReviewCompleted,
    ApplicationApproved,
    ApplicationDeclined,
    ComplianceCheckRequested,
    ComplianceRulePassed
)

# ApplicationState mapping to canonical flow:
# Submitted -> ApplicationState.SUBMITTED
# AwaitingAnalysis -> ApplicationState.CREDIT_ANALYSIS_REQUESTED
# AnalysisComplete -> ApplicationState.CREDIT_ANALYSIS_COMPLETE
# ComplianceReview -> ApplicationState.COMPLIANCE_CHECK_REQUESTED
# PendingDecision -> ApplicationState.PENDING_DECISION
# ApprovedPendingHuman -> ApplicationState.PENDING_HUMAN_REVIEW (with human decision pending)
# FinalApproved -> ApplicationState.APPROVED

class LoanApplicationAggregate:
    def __init__(self, application_id: str):
        self.application_id = application_id
        self.state: ApplicationState | None = None
        self.version: int = -1
        self.applicant_id: str | None = None
        self.requested_amount_usd: Decimal | None = None
        self.approved_amount_usd: Decimal | None = None
        self.compliance_required_checks: Set[str] = set()
        self.compliance_passed_checks: Set[str] = set()
        self.first_credit_analysis_completed: bool = False
        self.human_review_override_applied: bool = False
        self.valid_decision_sessions: Set[str] = set()
        self.compliance_stream_id: str = f"compliance-{application_id}"

    @classmethod
    async def load(cls, store: EventStore, application_id: str) -> "LoanApplicationAggregate":
        stream_id = f"loan-{application_id}"
        events = await store.load_stream(stream_id)
        agg = cls(application_id=application_id)
        for event in events:
            agg._apply(event)
        return agg

    def _apply(self, event: StoredEvent) -> None:
        handler_name = f"_on_{event['event_type']}"
        handler = getattr(self, handler_name, None)
        if handler:
            handler(event["payload"])
        self.version = event["stream_position"]

    # --- Handlers ---

    def _on_ApplicationSubmitted(self, payload: dict[str, Any]) -> None:
        self.assert_valid_transition(ApplicationState.SUBMITTED)
        self.state = ApplicationState.SUBMITTED
        self.applicant_id = payload["applicant_id"]
        # Use Decimal for financial accuracy
        val = payload.get("requested_amount_usd")
        self.requested_amount_usd = Decimal(str(val)) if val is not None else None

    def _on_CreditAnalysisRequested(self, payload: dict[str, Any]) -> None:
        self.assert_valid_transition(ApplicationState.CREDIT_ANALYSIS_REQUESTED)
        self.state = ApplicationState.CREDIT_ANALYSIS_REQUESTED

    def _on_DecisionGenerated(self, payload: dict[str, Any]) -> None:
        self.state = ApplicationState.PENDING_DECISION
        # Record session for causal chain
        self.valid_decision_sessions.add(payload["orchestrator_session_id"])

    def _on_CreditAnalysisCompleted(self, payload: dict[str, Any]) -> None:
        # This event now exists on BOTH the agent and loan streams
        self.assert_valid_transition(ApplicationState.CREDIT_ANALYSIS_COMPLETE)
        self.state = ApplicationState.CREDIT_ANALYSIS_COMPLETE

    def _on_HumanReviewCompleted(self, payload: dict[str, Any]) -> None:
        self.human_review_override_applied = payload["override"]
        final_decision = payload["final_decision"]
        if final_decision == "APPROVED":
            self.state = ApplicationState.APPROVED # or temporary internal state
        elif final_decision == "DECLINED":
            self.state = ApplicationState.DECLINED

    def _on_ApplicationApproved(self, payload: dict[str, Any]) -> None:
        self.assert_valid_transition(ApplicationState.APPROVED)
        self.state = ApplicationState.APPROVED
        self.approved_amount_usd = Decimal(str(payload["approved_amount_usd"]))

    def _on_ApplicationDeclined(self, payload: dict[str, Any]) -> None:
        self.assert_valid_transition(ApplicationState.DECLINED)
        self.state = ApplicationState.DECLINED

    # --- Business Rules ---

    def assert_valid_transition(self, target_state: ApplicationState) -> None:
        """
        Enforces allowed transitions:
        None -> SUBMITTED
        SUBMITTED -> CREDIT_ANALYSIS_REQUESTED
        CREDIT_ANALYSIS_REQUESTED -> CREDIT_ANALYSIS_COMPLETE
        CREDIT_ANALYSIS_COMPLETE -> COMPLIANCE_CHECK_REQUESTED (implied)
        COMPLIANCE_CHECK_REQUESTED -> COMPLIANCE_CHECK_COMPLETE (implied)
        ... -> PENDING_DECISION
        PENDING_DECISION -> APPROVED/DECLINED
        """
        # Simplification of the user's specific requested path:
        # SUBMITTED -> AWAITING_ANALYSIS -> ANALYSIS_COMPLETE -> COMPLIANCE_REVIEW -> PENDING_DECISION -> ...
        
        # Mapping ApplicationState enums to the user's semantic steps
        allowed = {
            None: [ApplicationState.SUBMITTED],
            ApplicationState.SUBMITTED: [ApplicationState.CREDIT_ANALYSIS_REQUESTED],
            ApplicationState.CREDIT_ANALYSIS_REQUESTED: [ApplicationState.CREDIT_ANALYSIS_COMPLETE],
            ApplicationState.CREDIT_ANALYSIS_COMPLETE: [ApplicationState.COMPLIANCE_CHECK_REQUESTED, ApplicationState.PENDING_DECISION],
            ApplicationState.COMPLIANCE_CHECK_REQUESTED: [ApplicationState.COMPLIANCE_CHECK_COMPLETE, ApplicationState.PENDING_DECISION],
            ApplicationState.COMPLIANCE_CHECK_COMPLETE: [ApplicationState.PENDING_DECISION],
            ApplicationState.PENDING_DECISION: [ApplicationState.APPROVED, ApplicationState.DECLINED, ApplicationState.PENDING_HUMAN_REVIEW],
            ApplicationState.PENDING_HUMAN_REVIEW: [ApplicationState.APPROVED, ApplicationState.DECLINED],
        }
        
        if target_state not in allowed.get(self.state, []):
            raise DomainError(f"Invalid state transition from {self.state} to {target_state}")

    def assert_awaiting_credit_analysis(self) -> None:
        if self.state != ApplicationState.CREDIT_ANALYSIS_REQUESTED:
            raise DomainError(f"Cannot complete analysis for application in state {self.state}")

    async def assert_compliance_satisfied(self, store: EventStore) -> None:
        """Verify all required compliance checks passed."""
        events = await store.load_stream(self.compliance_stream_id)
        required = set()
        passed = set()
        for e in events:
            if e["event_type"] == "ComplianceCheckRequested":
                required.update(e["payload"]["rules_to_evaluate"])
            elif e["event_type"] == "ComplianceRulePassed":
                passed.add(e["payload"]["rule_id"])
        
        missing = required - passed
        if missing:
            raise DomainError(f"Compliance checks not satisfied. Missing: {missing}")

    async def assert_contributing_sessions_valid(self, contributing_sessions: list[str], store: EventStore) -> None:
        """Enforce causal chain verification."""
        for session_stream_id in contributing_sessions:
            # session_stream_id is like "agent-credit_analysis-123"
            events = await store.load_stream(session_stream_id)
            found = False
            for e in events:
                if e["event_type"] in ("CreditAnalysisCompleted", "DecisionGenerated"):
                    if e["payload"].get("application_id") == self.application_id:
                        found = True
                        break
            if not found:
                raise DomainError(f"Causal chain violation: Session {session_stream_id} never processed application {self.application_id}")

    def enforce_confidence_floor(self, recommendation: str, confidence_score: float) -> str:
        if confidence_score < 0.6:
            return "REFER"
        return recommendation
