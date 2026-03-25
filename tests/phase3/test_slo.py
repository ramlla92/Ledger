import pytest
import asyncio
from uuid import uuid4
import asyncpg
import os
from datetime import datetime, timedelta

from ledger.event_store import EventStore
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.projections.compliance_audit import ComplianceAuditViewProjection
from ledger.schema.events import StoredEvent

# Assuming tests run against test DB
TEST_DB_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:apex@localhost:5433/apex_ledger")

@pytest.fixture
async def event_store():
    store = EventStore(TEST_DB_URL)
    await store.connect()
    try:
        # Wipe DB for test isolation
        async with store._pool.acquire() as conn:
            await conn.execute("TRUNCATE TABLE events, event_streams, projection_checkpoints, outbox CASCADE;")
        yield store
    finally:
        await store.close()

@pytest.fixture
async def projections_schema(event_store):
    with open("ledger/projections/schema.sql") as f:
        sql = f.read()
    async with event_store._pool.acquire() as conn:
        await conn.execute(sql)
        await conn.execute("TRUNCATE TABLE application_summary, agent_performance_ledger, compliance_audit_view, compliance_audit_snapshots CASCADE;")

@pytest.mark.asyncio
async def test_application_summary_lag_stays_under_slo(event_store, projections_schema):
    """
    Test the lag metric calculation for ApplicationSummary with DB generation.
    """
    proj = ApplicationSummaryProjection(event_store)
    
    # Generate 100 events
    events = []
    for i in range(1, 101):
        events.append({
            "event_type": "ApplicationSubmitted",
            "event_version": 1,
            "payload": {"application_id": f"app-{i}", "requested_amount_usd": 1000},
        })
    
    # Insert events to real DB
    await event_store.append("test-stream", events, -1)
    
    # At this point, lag is max
    initial_lag = await proj.get_lag()
    assert initial_lag > 0

    # Act like daemon - Process 100 events
    async for event in event_store.load_all(0, 500):
        await proj.handle(event)
        await proj.update_checkpoint(event.global_position)

    # After processing all, lag should be 0
    final_lag = await proj.get_lag()
    assert final_lag == 0

@pytest.mark.asyncio
async def test_compliance_audit_snapshot_strategy(event_store, projections_schema):
    proj = ComplianceAuditViewProjection(event_store)
    
    app_id = "app-comp-1"
    
    # We append enough events to trigger a snapshot (10 is the interval)
    events = []
    for i in range(1, 15):
        events.append({
            "event_type": "ComplianceRulePassed" if i < 14 else "ComplianceCheckCompleted",
            "event_version": 1,
            "payload": {
                "application_id": app_id, 
                "rule_id": f"rule-{i}",
                "overall_verdict": "PASS" if i == 14 else None
            },
        })
    
    # Append
    await event_store.append(f"comp-{app_id}", events, -1)

    # Manual process
    ts_mid = None
    async for event in event_store.load_all(0, 500):
        await proj.handle(event)
        await proj.update_checkpoint(event.global_position)
        if event.global_position == 10:
            ts_mid = event.recorded_at

    # Get current vs past
    current = await proj.get_current_compliance(app_id)
    assert current["status"] == "COMPLETED"
    
    # In a real environment, T10's snapshot recorded_at might perfectly hit or miss a fractional second.
    # So we pad it safely and verify the historical state.
    past = await proj.get_compliance_at(app_id, ts_mid + timedelta(seconds=1))
    
    assert past["application_id"] == app_id
    assert "status" not in past or past.get("status") != "COMPLETED" # hasn't completed yet
    assert len(past.get("passed_rules", [])) >= 10
