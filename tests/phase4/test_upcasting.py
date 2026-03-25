import asyncio
import pytest
from ledger.event_store import InMemoryEventStore, UpcasterRegistry, StoredEvent
from ledger.upcasters import register_upcasters

@pytest.mark.asyncio
async def test_mandated_immutability():
    registry = UpcasterRegistry()
    register_upcasters(registry)
    store = InMemoryEventStore(upcaster_registry=registry)
    
    stream_id = "loan-abc"
    payload = {"application_id": "abc", "agent_id": "agent-1"}
    
    # 1. Append v1
    await store.append(stream_id, [{"event_type": "CreditAnalysisCompleted", "event_version": 1, "payload": payload}], -1)
    
    # 2. Verify v2 on load
    events = await store.load_stream(stream_id)
    assert events[0].event_version == 2
    assert events[0].payload["model_version"] == "legacy-pre-2026"
    
    # 3. Verify raw row unchanged
    # In InMemoryEventStore, it's easy to check the internal dict
    raw = store._streams[stream_id][0]
    assert raw["event_version"] == 1
    assert "model_version" not in raw["payload"]
    assert "regulatory_basis" not in raw["payload"], "Raw V1 payload must not have v2 fields"
    print("✅ test_mandated_immutability: PASSED")

@pytest.mark.asyncio
async def test_mixed_versions_chaining():
    registry = UpcasterRegistry()
    
    # Define chained upcasters
    @registry.register("TestEvent", from_version=1)
    def v1_v2(payload, store=None, registry=None):
        payload["v2"] = True
        return payload
        
    @registry.register("TestEvent", from_version=2)
    def v2_v3(payload, store=None, registry=None):
        payload["v3"] = True
        return payload

    store = InMemoryEventStore(upcaster_registry=registry)
    stream_id = "test-1"
    
    # Append v1 and v2
    await store.append(stream_id, [
        {"event_type": "TestEvent", "event_version": 1, "payload": {"id": 1}},
        {"event_type": "TestEvent", "event_version": 2, "payload": {"id": 2}}
    ], -1)
    
    events = await store.load_stream(stream_id)
    
    # Both should be v3
    assert events[0].event_version == 3
    assert events[0].payload["v2"] is True
    assert events[0].payload["v3"] is True
    
    assert events[1].event_version == 3
    assert "v2" not in events[1].payload # v1 event upcaster logic is specific
    assert events[1].payload["v3"] is True
    print("✅ test_mixed_versions_chaining: PASSED")

if __name__ == "__main__":
    asyncio.run(test_mandated_immutability())
    asyncio.run(test_mixed_versions_chaining())
