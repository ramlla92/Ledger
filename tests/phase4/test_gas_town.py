import asyncio
import pytest
from datetime import datetime
from ledger.event_store import InMemoryEventStore
from ledger.agents.context import reconstruct_agent_context

@pytest.mark.asyncio
async def test_gas_town_crash_recovery():
    store = InMemoryEventStore()
    agent_id = "credit_analyst"
    session_id = "sess-crash"
    stream_id = f"agent-{agent_id}-{session_id}"
    
    # 1. Start session
    await store.append(stream_id, [{"event_type": "AgentSessionStarted", "payload": {"model": "gpt-4"}}], -1)
    
    # 2. Start node but don't finish
    await store.append(stream_id, [{"event_type": "AgentNodeStarted", "payload": {"node_name": "FetchFinancials"}}], 0)
    
    # 3. Simulate failure
    await store.append(stream_id, [{"event_type": "AgentSessionFailed", "payload": {"error": "Timeout"}}], 1)
    
    # 4. Reconstruct context
    context = await reconstruct_agent_context(store, agent_id, session_id)
    
    assert context.session_health_status == "NEEDS_RECONCILIATION"
    assert context.last_event_position == 2, "Should identify the last recorded position correctly"
    
    # Verify pending work has reason and correct types
    reasons = [pw["reason"] for pw in context.pending_work]
    types = [pw["type"] for pw in context.pending_work]
    
    assert "node_not_finished" in reasons
    assert "session_failed" in reasons
    assert "node" in types, "Should have a pending node task"
    assert "session" in types, "Should have a session-level failure task"
    
    # Verify summarization includes CRITICAL
    assert "CRITICAL" in context.context_text
    assert "Timeout" in context.context_text
    print("✅ test_gas_town_crash_recovery: PASSED")

if __name__ == "__main__":
    asyncio.run(test_gas_town_crash_recovery())
