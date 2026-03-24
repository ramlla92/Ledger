import pytest
from ledger.event_store import InMemoryEventStore
from ledger.agents.handlers import handle_generate_decision, GenerateDecisionCommand, SubmitApplicationCommand, handle_submit_application
from ledger.schema.events import DomainError, LoanPurpose
from datetime import datetime

@pytest.mark.asyncio
async def test_causal_chain_enforcement_in_decision_orchestrator():
    store = InMemoryEventStore()
    app_id = "loan-causal-1"
    
    # 1. Setup application (moves to CREDIT_ANALYSIS_REQUESTED)
    cmd_submit = SubmitApplicationCommand(
        application_id=app_id, applicant_id="u1", amount=5000, 
        purpose=LoanPurpose.WORKING_CAPITAL, term_months=12
    )
    await handle_submit_application(cmd_submit, store)
    
    # 2. Advance state to CREDIT_ANALYSIS_COMPLETE (required for handle_generate_decision)
    await store.append(f"loan-{app_id}", [{
        "event_type": "CreditAnalysisCompleted",
        "payload": {
            "application_id": app_id, 
            "agent_id": "credit-a", 
            "session_id": "sess-a",
            "decision": {"risk_tier": "LOW", "recommended_limit_usd": 1000, "confidence": 0.9, "rationale": "OK"},
            "model_version": "v1", "model_deployment_id": "d1", "input_data_hash": "h1", "analysis_duration_ms": 1,
            "completed_at": datetime.utcnow().isoformat()
        }
    }], expected_version=1)
    
    # 3. Fake sessions
    # Agent session A: Valid (has processed the app)
    agent_a = "credit-a"
    sess_a = "sess-a"
    await store.append(f"agent-{agent_a}-{sess_a}", [{
        "event_type": "CreditAnalysisCompleted",
        "payload": {"application_id": app_id, "agent_id": agent_a, "session_id": sess_a}
    }], expected_version=-1)
    
    # Agent session B: Invalid (processed OTHER app)
    agent_b = "credit-b"
    sess_b = "sess-b"
    await store.append(f"agent-{agent_b}-{sess_b}", [{
        "event_type": "CreditAnalysisCompleted",
        "payload": {"application_id": "other-app", "agent_id": agent_b, "session_id": sess_b}
    }], expected_version=-1)

    # 4. Decision including session B -> Should raise DomainError (Causal Chain violation)
    cmd_decision = GenerateDecisionCommand(
        application_id=app_id,
        orchestrator_agent_id="orch-1",
        orchestrator_session_id="orch-sess-1",
        recommendation="APPROVE",
        confidence_score=0.9,
        contributing_sessions=[f"agent-{agent_a}-{sess_a}", f"agent-{agent_b}-{sess_b}"],
        executive_summary="Trust me."
    )
    
    with pytest.raises(DomainError, match="Causal chain violation"):
        await handle_generate_decision(cmd_decision, store)
