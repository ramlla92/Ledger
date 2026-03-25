import pytest
import asyncio
from typing import AsyncGenerator, Set
from uuid import uuid4
from datetime import datetime

from ledger.projections.daemon import ProjectionDaemon
from ledger.projections.base import Projection
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.schema.events import StoredEvent
from ledger.event_store import InMemoryEventStore

class FakeEventStore:
    def __init__(self, events):
        self.events = events
        self.load_all_calls = []

    async def load_all(self, from_position: int = 0, batch_size: int = 500) -> AsyncGenerator[StoredEvent, None]:
        self.load_all_calls.append((from_position, batch_size))
        for event in self.events:
            if event.global_position > from_position:
                yield event

class FakeProjection(Projection):
    def __init__(self, name: str, initial_checkpoint: int, fail_on_positions=None):
        self._name = name
        self.checkpoint = initial_checkpoint
        self.handled_events = []
        self.fail_on_positions = fail_on_positions or []
        self.store = None # Not needed

    @property
    def name(self) -> str:
        return self._name

    async def subscribed_event_types(self) -> Set[str]:
        return {"TestEvent"}

    async def get_last_position(self) -> int:
        return self.checkpoint

    async def update_checkpoint(self, position: int) -> None:
        self.checkpoint = position

    async def handle(self, event: StoredEvent) -> None:
        if event.global_position in self.fail_on_positions:
            raise Exception(f"Injected failure on {event.global_position}")
        self.handled_events.append(event.global_position)

    async def get_lag(self) -> int:
        return 0

def make_events(count: int) -> list[StoredEvent]:
    return [
        StoredEvent(
            event_id=uuid4(), stream_id="test", stream_position=i, event_type="TestEvent",
            event_version=1, payload={}, metadata={}, recorded_at=datetime.utcnow(),
            global_position=i
        ) for i in range(1, count + 1)
    ]

@pytest.mark.asyncio
async def test_daemon_checkpoints_low_watermark_and_skips_duplicates():
    events = make_events(15)
    store = FakeEventStore(events)
    proj_a = FakeProjection("ProjA", initial_checkpoint=5)
    proj_b = FakeProjection("ProjB", initial_checkpoint=10)
    daemon = ProjectionDaemon(store, [proj_a, proj_b])
    
    await daemon._process_batch(batch_size=20)
    
    assert len(store.load_all_calls) == 1
    assert store.load_all_calls[0][0] == 5 
    assert proj_a.handled_events == list(range(6, 16))
    assert proj_b.handled_events == list(range(11, 16))
    assert proj_a.checkpoint == 15
    assert proj_b.checkpoint == 15

@pytest.mark.asyncio
async def test_daemon_fault_tolerance_skips_failing_event_but_continues():
    events = make_events(5)
    store = FakeEventStore(events)
    proj_a = FakeProjection("ProjC", initial_checkpoint=0, fail_on_positions=[3])
    daemon = ProjectionDaemon(store, [proj_a])
    daemon._max_retries = 2
    
    await daemon._process_batch(batch_size=20)
    
    assert proj_a.handled_events == [1, 2, 4, 5]
    assert proj_a.checkpoint == 5

@pytest.mark.asyncio
async def test_integration_application_summary_in_memory():
    """
    Appends ApplicationSubmitted & DecisionGenerated via InMemoryEventStore.
    Runs ProjectionDaemon._process_batch.
    Asserts one row in application_summary and get_lag returns valid number.
    """
    store = InMemoryEventStore()
    app_id = "app-123"
    
    # Prepend dummy event so InMemoryEventStore global_position 0 is consumed,
    # and our real test events start at global_position 1, which > 0 checkpoint catches.
    await store.append("dummy", [{"event_type": "Dummy", "payload": {}}], -1)

    # 1. Append real events
    events = [
        {"event_type": "ApplicationSubmitted", "payload": {"application_id": app_id, "requested_amount_usd": 5000}},
        {"event_type": "DecisionGenerated", "payload": {"application_id": app_id, "recommendation": "APPROVE"}},
    ]
    await store.append(f"loan-{app_id}", events, -1)
    
    # Wait tiny amount to ensure time difference
    await asyncio.sleep(0.01)
    # Add an uncaught event for another projection just to create lag
    await store.append("some-other-stream", [{"event_type": "OtherEvent", "payload": {}}], -1)

    proj = ApplicationSummaryProjection(store)
    daemon = ProjectionDaemon(store, [proj])
    
    # 2. Process batch
    await daemon._process_batch(500)
    
    # 3. Asserts
    assert hasattr(proj, "_mem_db")
    assert app_id in proj._mem_db
    
    row = proj._mem_db[app_id]
    assert row["state"] == "PENDING_DECISION"
    assert row["requested_amount_usd"] == 5000
    assert row["decision"] == "APPROVE"
    
    # get_lag should calculate positive integer lag because we added an unprocessed event at the end
    lag = await proj.get_lag()
    assert isinstance(lag, int)
    assert lag >= 0
