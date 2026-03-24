import pytest
from ledger.event_store import InMemoryEventStore
from ledger.agents.handlers import handle_credit_analysis_completed, CreditAnalysisCompletedCommand, SubmitApplicationCommand, handle_submit_application
from ledger.schema.events import DomainError, CreditDecision, RiskTier, LoanPurpose
from ledger.domain.agent_session import AgentSessionAggregate
from decimal import Decimal

@pytest.mark.asyncio
async def test_agent_session_gas_town_scenario_1_no_context():
    store = InMemoryEventStore()
    app_id = "loan-gas-1"
    agent_id = "credit"
    session_id = "sess-gas-1"
    
    # 1. Submit application (to move it to CREDIT_ANALYSIS_REQUESTED)
    cmd_submit = SubmitApplicationCommand(
        application_id=app_id, applicant_id="u1", amount=5000, 
        purpose=LoanPurpose.WORKING_CAPITAL, term_months=12
    )
    await handle_submit_application(cmd_submit, store)
    
    # 2. Agent session exists BUT NO context loaded event
    await store.append(f"agent-{agent_id}-{session_id}", [{
        "event_type": "AgentSessionStarted",
        "payload": {"session_id": session_id, "agent_id": agent_id}
    }], expected_version=-1)

    # Replay assertion: context should not be loaded yet
    agent_agg = await AgentSessionAggregate.load(store, agent_id, session_id)
    assert agent_agg.context_loaded is False
    assert agent_agg.model_version is None

    # 3. Try to complete analysis -> Should raise DomainError (Gas Town)
    cmd_analysis = CreditAnalysisCompletedCommand(
        application_id=app_id, agent_id=agent_id, session_id=session_id,
        model_version="v1",
        decision=CreditDecision(risk_tier=RiskTier.LOW, recommended_limit_usd=Decimal("5000"), confidence=0.9, rationale="OK")
    )
    
    with pytest.raises(DomainError, match="not loaded context"):
        await handle_credit_analysis_completed(cmd_analysis, store)

@pytest.mark.asyncio
async def test_agent_session_scenario_2_churn_lock():
    store = InMemoryEventStore()
    app_id = "loan-churn-1"
    agent_id = "credit"
    session_id = "sess-churn-1"
    
    # 1. Setup session with context
    await store.append(f"agent-{agent_id}-{session_id}", [{
        "event_type": "AgentContextLoaded",
        "payload": {"session_id": session_id, "agent_id": agent_id, "model_version": "v1"}
    }], expected_version=-1)
    
    # 2. Setup application
    cmd_submit = SubmitApplicationCommand(
        application_id=app_id, applicant_id="u1", amount=5000, 
        purpose=LoanPurpose.WORKING_CAPITAL, term_months=12
    )
    await handle_submit_application(cmd_submit, store)
    
    # 3. First analysis -> OK
    cmd_analysis = CreditAnalysisCompletedCommand(
        application_id=app_id, agent_id=agent_id, session_id=session_id,
        model_version="v1",
        decision=CreditDecision(
            risk_tier=RiskTier.LOW, 
            recommended_limit_usd=Decimal("5000"), 
            confidence=0.9, 
            rationale="OK"
        )
    )
    await handle_credit_analysis_completed(cmd_analysis, store)
    
    # 4. Second analysis for same app -> Should fail (Churn)
    with pytest.raises(DomainError, match="Churn blocked"):
        await handle_credit_analysis_completed(cmd_analysis, store)
