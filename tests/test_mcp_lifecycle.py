import json
import pytest
import asyncio
from ledger.mcp import server

@pytest.mark.asyncio
async def test_mcp_causal_lifecycle():
    # 1. Setup
    app_id = "loan-debug-final-v3"
    agent_id = "analyst-007"
    sess_id = "sess-omega"
    
    async def call(fn, **kwargs):
        res_json = await fn(**kwargs)
        res = json.loads(res_json)
        if not res.get("ok"):
            print(f"DEBUG: Tool {fn.__name__} FAILED: {res.get('error')}")
        return res

    # 2. SUBMIT APPLICATION
    res = await call(server.submit_application,
        application_id=app_id,
        applicant_id="cust-202",
        loan_amount=50000.0,
        purpose="Debug Flow"
    )
    assert res["ok"]
    
    # 3. START AGENT SESSION
    res = await call(server.start_agent_session,
        agent_id=agent_id,
        session_id=sess_id,
        model_version="gpt-4o"
    )
    assert res["ok"]
    
    # 4. RECORD FRAUD SCREENING (expected_version=0)
    res = await call(server.record_fraud_screening,
        application_id=app_id,
        agent_id=agent_id,
        session_id=sess_id,
        fraud_score=0.08, 
        anomaly_flags=["low_velocity"],
        screening_model_version="fraud-detect-v1",
        expected_version=0
    )
    assert res["ok"]
    
    # 5. RECORD CREDIT ANALYSIS (expected_version=1)
    res = await call(server.record_credit_analysis,
        application_id=app_id,
        agent_id=agent_id,
        session_id=sess_id,
        expected_version=1,
        model_version="gpt-4o", # Matches session
        confidence_score=0.95,
        risktier="B+",
        recommended_limit_usd=55000.0,
        analysis_duration_ms=620,
        input_data_hash="sha256:7e8f9b...",
    )
    assert res["ok"]
    
    # 6. GENERATE DECISION (expected_version=2)
    res = await call(server.generate_decision,
        application_id=app_id,
        orchestrator_agent_id="orch-1",
        recommendation="APPROVE",
        confidence_score=0.98,
        contributing_agent_sessions=[f"agent-{agent_id}-{sess_id}"],
        decision_basis_summary="All checks passed.",
        expected_version=2
    )
    assert res["ok"]
    
    # 7. RECORD HUMAN REVIEW (expected_version=3)
    res = await call(server.record_human_review,
        application_id=app_id,
        reviewer_id="human-99",
        final_decision="APPROVED",
        notes="Verified.",
        approved_amount_usd=50000.0,
        expected_version=3
    )
    assert res["ok"]
    
    # 8. WAIT FOR PROJECTIONS
    await asyncio.sleep(2.0)
    
    # 9. VERIFY APPLICATION SUMMARY
    res_json = await server.get_application(id=app_id)
    res = json.loads(res_json)
    summary = res["result"]
    
    if summary["state"] != "APPROVED":
        audit_json = await server.get_audit_trail(id=app_id)
        audit = json.loads(audit_json)["result"]
        print(f"DEBUG: Summary State = {summary['state']}")
        print(f"DEBUG: Audit Trail Types = {[e['event_type'] for e in audit]}")
        print(f"DEBUG: Complete Summary = {summary}")
        # Explicitly fail with better message
        assert summary["state"] == "APPROVED"
    
    print("✅ MCP Causal Lifecycle Integration Test: PASSED")

if __name__ == "__main__":
    asyncio.run(test_mcp_causal_lifecycle())
