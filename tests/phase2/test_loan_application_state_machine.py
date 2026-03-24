import pytest
from ledger.event_store import InMemoryEventStore
from ledger.domain.loan_application import LoanApplicationAggregate, ApplicationState
from ledger.agents.handlers import handle_submit_application, SubmitApplicationCommand
from ledger.schema.events import DomainError, LoanPurpose
from decimal import Decimal
from datetime import datetime

@pytest.mark.asyncio
async def test_valid_loan_application_flow_atomic_submission():
    store = InMemoryEventStore()
    app_id = "test-atomic-001"
    
    # 1. Use the handler for submission (atomic append of Submitted + Requested)
    cmd = SubmitApplicationCommand(
        application_id=app_id,
        applicant_id="user-123",
        amount=Decimal("10000"),
        purpose=LoanPurpose.EXPANSION,
        term_months=24
    )
    await handle_submit_application(cmd, store)
    
    # 2. Verify state advanced to CREDIT_ANALYSIS_REQUESTED due to atomic append
    app = await LoanApplicationAggregate.load(store, app_id)
    assert app.state == ApplicationState.CREDIT_ANALYSIS_REQUESTED
    assert app.version == 1 # 0 and 1
    
    # 3. Transition to Analysis Complete (event on loan stream)
    await store.append(f"loan-{app_id}", [{
        "event_type": "CreditAnalysisCompleted",
        "payload": {
            "application_id": app_id,
            "agent_id": "credit_analysis",
            "session_id": "sess-001",
            "decision": {"risk_tier": "LOW", "recommended_limit_usd": 5000, "confidence": 0.9, "rationale": "OK"},
            "model_version": "v1",
            "model_deployment_id": "dep-1",
            "input_data_hash": "hash-1",
            "analysis_duration_ms": 100,
            "completed_at": datetime.utcnow().isoformat()
        }
    }], expected_version=app.version)

    app = await LoanApplicationAggregate.load(store, app_id)
    assert app.state == ApplicationState.CREDIT_ANALYSIS_COMPLETE

@pytest.mark.asyncio
async def test_invalid_transition_via_direct_append_raises_on_load():
    store = InMemoryEventStore()
    app_id = "test-invalid-001"
    
    # Direct append of Approved BEFORE Submitted
    await store.append(f"loan-{app_id}", [{
        "event_type": "ApplicationApproved",
        "payload": {
            "application_id": app_id,
            "approved_amount_usd": 5000,
            "interest_rate_pct": 0.05,
            "term_months": 12,
            "conditions": [],
            "approved_by": "admin",
            "effective_date": "2024-01-01",
            "approved_at": datetime.utcnow().isoformat()
        }
    }], expected_version=-1)
    
    # Load should fail because _on_ApplicationApproved calls assert_valid_transition
    with pytest.raises(DomainError, match="Invalid state transition"):
        await LoanApplicationAggregate.load(store, app_id)
