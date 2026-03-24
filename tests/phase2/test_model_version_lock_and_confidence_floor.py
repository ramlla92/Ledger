import pytest
from ledger.event_store import InMemoryEventStore
from ledger.domain.loan_application import LoanApplicationAggregate, ApplicationState
from ledger.domain.agent_session import AgentSessionAggregate
from ledger.schema.events import DomainError, LoanPurpose, CreditDecision, RiskTier
from ledger.agents.handlers import (
    handle_generate_decision, 
    handle_human_review_completed,
    handle_submit_application,
    GenerateDecisionCommand,
    HumanReviewCompletedCommand,
    SubmitApplicationCommand
)
from decimal import Decimal
from datetime import datetime

@pytest.mark.asyncio
async def test_confidence_floor_enforcement_logic():
    app = LoanApplicationAggregate("test-app")
    # Low confidence -> Force REFER
    assert app.enforce_confidence_floor("APPROVE", 0.5) == "REFER"
    # High confidence -> Keep
    assert app.enforce_confidence_floor("APPROVE", 0.7) == "APPROVE"

@pytest.mark.asyncio
async def test_analysis_churn_protection_full_payloads():
    store = InMemoryEventStore()
    agent_id = "credit"
    session_id = "sess-1"
    app_id = "loan-1"
    
    # 1. Setup session with context
    await store.append(f"agent-{agent_id}-{session_id}", [{
        "event_type": "AgentContextLoaded",
        "payload": {
            "session_id": session_id,
            "agent_id": agent_id,
            "model_version": "v1.0.0",
            "loaded_at": datetime.utcnow().isoformat()
        }
    }], expected_version=-1)
    
    # 2. First analysis
    await store.append(f"agent-{agent_id}-{session_id}", [{
        "event_type": "CreditAnalysisCompleted",
        "payload": {
            "application_id": app_id,
            "agent_id": agent_id,
            "session_id": session_id,
            "decision": {"risk_tier": "LOW", "recommended_limit_usd": 5000, "confidence": 0.9, "rationale": "OK"},
            "model_version": "v1.0.0",
            "model_deployment_id": "dep-1",
            "input_data_hash": "hash-1",
            "analysis_duration_ms": 100,
            "completed_at": datetime.utcnow().isoformat()
        }
    }], expected_version=0)
    
    agent = await AgentSessionAggregate.load(store, agent_id, session_id)
    app = await LoanApplicationAggregate.load(store, app_id)
    
    # 3. Second analysis for same app should fail (Churn Blocked)
    with pytest.raises(DomainError, match="Churn blocked"):
        agent.assert_can_append_decision_for_application(app_id, app)
    
    # 4. If human override applied, churn is allowed
    app.human_review_override_applied = True
    agent.assert_can_append_decision_for_application(app_id, app) # Should not raise

@pytest.mark.asyncio
async def test_handle_generate_decision_causal_chain_full():
    store = InMemoryEventStore()
    app_id = "loan-3"
    agent_id = "credit"
    session_id = "sess-3"
    
    # 1. Setup application state correctly (Must be CREDIT_ANALYSIS_COMPLETE)
    cmd_submit = SubmitApplicationCommand(app_id, "u1", Decimal("1000"), LoanPurpose.EXPANSION, 12)
    await handle_submit_application(cmd_submit, store)
    
    # Manually append analysis to move state to COMPLETE (handler would do this too, but we are testing decision logic)
    await store.append(f"loan-{app_id}", [{
        "event_type": "CreditAnalysisCompleted",
        "payload": {
            "application_id": app_id, "agent_id": agent_id, "session_id": session_id,
            "decision": {"risk_tier": "LOW", "recommended_limit_usd": 1000, "confidence": 0.9, "rationale": "OK"},
            "model_version": "v1", "model_deployment_id": "d1", "input_data_hash": "h1", "analysis_duration_ms": 1,
            "completed_at": datetime.utcnow().isoformat()
        }
    }], expected_version=1) # 0 and 1 from handle_submit_application
    
    # 2. Setup session with analysis
    await store.append(f"agent-{agent_id}-{session_id}", [{
        "event_type": "CreditAnalysisCompleted",
        "payload": {
            "application_id": app_id, "agent_id": agent_id, "session_id": session_id,
            "decision": {"risk_tier": "LOW", "recommended_limit_usd": 1000, "confidence": 0.9, "rationale": "OK"},
            "model_version": "v1", "model_deployment_id": "d1", "input_data_hash": "h1", "analysis_duration_ms": 1,
            "completed_at": datetime.utcnow().isoformat()
        }
    }], expected_version=-1)
    
    # 3. Trigger decision with VALID causal chain
    cmd_valid = GenerateDecisionCommand(
        application_id=app_id,
        orchestrator_agent_id="orch-1",
        orchestrator_session_id="orch-sess-1",
        recommendation="APPROVE",
        confidence_score=0.9,
        contributing_sessions=[f"agent-{agent_id}-{session_id}"],
        executive_summary="All good."
    )
    await handle_generate_decision(cmd_valid, store) # Should pass
    
    # 4. Trigger decision with INVALID causal chain (ghost session)
    # Use a NEW app to avoid state guard conflict with the previous success
    app_id_invalid = "loan-3-invalid"
    cmd_submit_invalid = SubmitApplicationCommand(app_id_invalid, "u1", Decimal("1000"), LoanPurpose.EXPANSION, 12)
    await handle_submit_application(cmd_submit_invalid, store)
    
    await store.append(f"loan-{app_id_invalid}", [{
        "event_type": "CreditAnalysisCompleted",
        "payload": {
            "application_id": app_id_invalid, "agent_id": agent_id, "session_id": session_id,
            "decision": {"risk_tier": "LOW", "recommended_limit_usd": 1000, "confidence": 0.9, "rationale": "OK"},
            "model_version": "v1", "model_deployment_id": "d1", "input_data_hash": "h1", "analysis_duration_ms": 1,
            "completed_at": datetime.utcnow().isoformat()
        }
    }], expected_version=1)

    cmd_invalid = GenerateDecisionCommand(
        application_id=app_id_invalid,
        orchestrator_agent_id="orch-1",
        orchestrator_session_id="orch-sess-2",
        recommendation="APPROVE",
        confidence_score=0.9,
        contributing_sessions=["agent-credit-ghost-999"],
        executive_summary="Lies."
    )
    try:
        await handle_generate_decision(cmd_invalid, store)
        pytest.fail("Should have raised DomainError")
    except DomainError as e:
        assert "Causal chain violation" in str(e), f"Expected 'Causal chain violation' in '{str(e)}'"

@pytest.mark.asyncio
async def test_human_review_clears_churn_lock_full():
    store = InMemoryEventStore()
    app_id = "loan-4"
    agent_id = "credit"
    session_id = "sess-4"
    
    # 1. Setup session and app
    await store.append(f"agent-{agent_id}-{session_id}", [{
        "event_type": "CreditAnalysisCompleted",
        "payload": {
            "application_id": app_id, "agent_id": agent_id, "session_id": session_id,
            "decision": {"risk_tier": "LOW", "recommended_limit_usd": 1000, "confidence": 0.9, "rationale": "OK"},
            "model_version": "v1", "model_deployment_id": "d1", "input_data_hash": "h1", "analysis_duration_ms": 1,
            "completed_at": datetime.utcnow().isoformat()
        }
    }], expected_version=-1)
    
    agent = await AgentSessionAggregate.load(store, agent_id, session_id)
    app = await LoanApplicationAggregate.load(store, app_id)
    
    # Check lock exists
    with pytest.raises(DomainError, match="Churn blocked"):
        agent.assert_can_append_decision_for_application(app_id, app)
        
    # 2. Complete human review with override=True
    cmd_review = HumanReviewCompletedCommand(
        application_id=app_id,
        reviewer_id="human-reviewer-1",
        override=True,
        original_recommendation="APPROVE",
        final_decision="DECLINE"
    )
    await handle_human_review_completed(cmd_review, store)
    
    # 3. Verify lock is cleared
    app = await LoanApplicationAggregate.load(store, app_id)
    assert app.human_review_override_applied is True
    agent.assert_can_append_decision_for_application(app_id, app) # Should not raise
