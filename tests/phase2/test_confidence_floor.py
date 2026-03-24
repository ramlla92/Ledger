import pytest
from ledger.event_store import InMemoryEventStore
from ledger.agents.handlers import handle_submit_application, handle_generate_decision, SubmitApplicationCommand, GenerateDecisionCommand
from ledger.schema.events import LoanPurpose, DecisionGenerated
from decimal import Decimal
from datetime import datetime

@pytest.mark.asyncio
async def test_confidence_floor_implementation_in_decision_handler():
    store = InMemoryEventStore()
    app_id = "loan-conf-test"
    
    # 1. Submit application (triggers APPLICATION_SUBMITTED and CREDIT_ANALYSIS_REQUESTED)
    cmd_submit = SubmitApplicationCommand(
        application_id=app_id,
        applicant_id="user-1",
        amount=5000,
        purpose=LoanPurpose.WORKING_CAPITAL,
        term_months=12
    )
    await handle_submit_application(cmd_submit, store)
    
    # 2. ADVANCE STATE to CREDIT_ANALYSIS_COMPLETE
    await store.append(f"loan-{app_id}", [{
        "event_type": "CreditAnalysisCompleted",
        "payload": {
            "application_id": app_id, 
            "agent_id": "credit", 
            "session_id": "sess-1",
            "decision": {"risk_tier": "LOW", "recommended_limit_usd": 5000, "confidence": 0.9, "rationale": "OK"},
            "model_version": "v1",
            "model_deployment_id": "d1",
            "input_data_hash": "h1",
            "analysis_duration_ms": 1,
            "completed_at": datetime.utcnow().isoformat()
        }
    }], expected_version=1) # -1, 0, 1
    
    # 3. Fake a credit analysis session actually processing this app
    agent_id = "credit"
    session_id = "sess-1"
    await store.append(f"agent-{agent_id}-{session_id}", [{
        "event_type": "CreditAnalysisCompleted",
        "payload": {
            "application_id": app_id, "agent_id": agent_id, "session_id": session_id,
            "decision": {"risk_tier": "LOW", "recommended_limit_usd": 5000, "confidence": 0.9, "rationale": "OK"},
            "model_version": "v1", "model_deployment_id": "d1", "input_data_hash": "h1", "analysis_duration_ms": 1,
            "completed_at": datetime.utcnow().isoformat()
        }
    }], expected_version=-1)

    # 4. Decision with confidence 0.5 (below 0.6)
    cmd_decision = GenerateDecisionCommand(
        application_id=app_id,
        orchestrator_agent_id="orch-1",
        orchestrator_session_id="orch-sess-1",
        recommendation="APPROVE",
        confidence_score=0.5,
        contributing_sessions=[f"agent-{agent_id}-{session_id}"],
        executive_summary="Risky."
    )
    await handle_generate_decision(cmd_decision, store)
    
    # 5. Verify the stored event has recommendation == "REFER"
    events = await store.load_stream(f"loan-{app_id}")
    decision_event = next(e for e in events if e["event_type"] == "DecisionGenerated")
    assert decision_event["payload"]["recommendation"] == "REFER"
    assert decision_event["payload"]["confidence_score"] == 0.5
