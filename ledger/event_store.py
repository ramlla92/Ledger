"""
ledger/event_store.py — PostgreSQL-backed EventStore
=====================================================
COMPLETION CHECKLIST (implement in order):
  [ ] Phase 1, Day 1: append() + stream_version()
  [ ] Phase 1, Day 1: load_stream()
  [ ] Phase 1, Day 2: load_all()  (needed for projection daemon)
  [ ] Phase 1, Day 2: get_event() (needed for causation chain)
  [ ] Phase 4:        UpcasterRegistry.upcast() integration in load_stream/load_all
"""
from __future__ import annotations
import json
import asyncio
import inspect
from datetime import datetime
from collections import defaultdict
from typing import Any, AsyncGenerator, AsyncIterator, Sequence, Callable, List, Dict, Optional, TYPE_CHECKING
from decimal import Decimal
from uuid import UUID, uuid4
import asyncpg

from ledger.schema.events import StoredEvent


class OptimisticConcurrencyError(Exception):
    """Raised when expected_version doesn't match current stream version."""
    def __init__(self, stream_id: str, expected: int, actual: int):
        self.stream_id = stream_id; self.expected = expected; self.actual = actual
        super().__init__(f"OCC on '{stream_id}': expected v{expected}, actual v{actual}")


class EventStore:
    """
    Append-only PostgreSQL event store. All agents and projections use this class.

    IMPLEMENT IN ORDER — see inline guides in each method:
      1. stream_version()   — simplest, needed immediately
      2. append()           — most critical; OCC correctness is the exam
      3. load_stream()      — needed for aggregate replay
      4. load_all()         — async generator, needed for projection daemon
      5. get_event()        — needed for causation chain audit
    """

    def __init__(self, db_url: str, upcaster_registry=None):
        self.db_url = db_url
        self.upcasters = upcaster_registry
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(self.db_url, min_size=2, max_size=10)

    async def close(self) -> None:
        if self._pool: await self._pool.close()

    async def stream_version(self, stream_id: str) -> int:
        """
        Returns current version, or -1 if stream doesn't exist.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT current_version FROM event_streams WHERE stream_id = $1",
                stream_id)
            return row["current_version"] if row else -1

    async def append(
        self,
        stream_id: str,
        events: list[dict],
        expected_version: int,    # -1=new stream, 0+=expected current
        causation_id: str | None = None,
        correlation_id: str | None = None,
        metadata: dict | None = None,
    ) -> list[int]:
        """
        Appends events atomically with OCC. Returns list of positions assigned.
        Includes correlation_id in metadata for causal tracing.
        """
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # 1. Lock stream row (prevents concurrent appends)
                row = await conn.fetchrow(
                    "SELECT current_version FROM event_streams "
                    "WHERE stream_id = $1 FOR UPDATE", stream_id)

                # 2. OCC check
                current = row["current_version"] if row else -1
                if current != expected_version:
                    raise OptimisticConcurrencyError(stream_id, expected_version, current)

                # 3. Create stream if new
                if row is None:
                    await conn.execute(
                        "INSERT INTO event_streams(stream_id, aggregate_type, current_version)"
                        " VALUES($1, $2, 0)",
                        stream_id, stream_id.split("-")[0])

                # 4. Insert each event
                positions = []
                meta = {**(metadata or {})}
                if causation_id: meta["causation_id"] = causation_id
                if correlation_id: meta["correlation_id"] = correlation_id
                
                # Check for recorded_at in metadata or use now
                recorded_at = meta.get("recorded_at")
                if isinstance(recorded_at, str):
                    recorded_at = datetime.fromisoformat(recorded_at)
                if not recorded_at:
                    recorded_at = datetime.utcnow()

                def json_serial(obj):
                    if isinstance(obj, (datetime, Decimal)):
                        return obj.isoformat() if isinstance(obj, datetime) else float(obj)
                    raise TypeError(f"Type {type(obj)} not serializable")

                for i, event in enumerate(events):
                    pos = expected_version + 1 + i
                    # Use RETURNING event_id to get the generated UUID
                    row = await conn.fetchrow(
                        "INSERT INTO events(stream_id, stream_position, event_type,"
                        " event_version, payload, metadata, recorded_at)"
                        " VALUES($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7)"
                        " RETURNING event_id",
                        stream_id, pos,
                        event["event_type"], event.get("event_version", 1),
                        json.dumps(event["payload"], default=json_serial),
                        json.dumps(meta, default=json_serial),
                        recorded_at)
                    
                    event_id = row['event_id']
                    
                    # 5. Insert into outbox for background signaling
                    # Schema has: event_id, destination, payload, created_at, published_at, attempts
                    await conn.execute(
                        "INSERT INTO outbox(event_id, destination, payload, attempts)"
                        " VALUES($1, $2, $3::jsonb, $4)",
                        event_id, "ALL", 
                        json.dumps(event["payload"], default=json_serial),
                        0)
                    
                    positions.append(pos)

                # 6. Update stream version
                await conn.execute(
                    "UPDATE event_streams SET current_version=$1 WHERE stream_id=$2",
                    expected_version + len(events), stream_id)
                return positions

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[StoredEvent]:
        """
        Loads events from a stream in stream_position order.
        Applies upcasters if self.upcasters is set.
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM events WHERE stream_id = $1 AND stream_position >= $2 "
                "AND ($3::int IS NULL OR stream_position <= $3::int) ORDER BY stream_position",
                stream_id, from_position, to_position
            )
            events = [dict(row) for row in rows]
            for e in events:
                if isinstance(e["payload"], str): e["payload"] = json.loads(e["payload"])
                if isinstance(e["metadata"], str): e["metadata"] = json.loads(e["metadata"])

            if self.upcasters:
                # Note: If upcasters need DB access, we'd need an async version.
                # Since the user's requested upcast is sync, we'll try to keep it sync
                # unless a specific upcaster (like DecisionGenerated) requires store access.
                events = [await self.upcasters.upcast(e, self) for e in events]
            return [StoredEvent(**e) for e in events]

    async def load_all(
        self, from_position: int = 0, batch_size: int = 500,
        event_types: list[str] | None = None
    ) -> AsyncGenerator[StoredEvent, None]:
        """
        Async generator yielding all events by global_position.
        Optional event_types filter to skip irrelevant events at the DB level.
        """
        async with self._pool.acquire() as conn:
            current_position = from_position
            while True:
                if event_types:
                    rows = await conn.fetch(
                        "SELECT * FROM events WHERE global_position >= $1 "
                        "AND event_type = ANY($3) "
                        "ORDER BY global_position LIMIT $2",
                        current_position, batch_size, event_types
                    )
                else:
                    rows = await conn.fetch(
                        "SELECT * FROM events WHERE global_position >= $1 "
                        "ORDER BY global_position LIMIT $2",
                        current_position, batch_size
                    )
                if not rows:
                    break
                events = [dict(row) for row in rows]
                for e in events:
                    if isinstance(e["payload"], str): e["payload"] = json.loads(e["payload"])
                    if isinstance(e["metadata"], str): e["metadata"] = json.loads(e["metadata"])
                
                if self.upcasters:
                    events = [await self.upcasters.upcast(e, self) for e in events]
                for event_dict in events:
                    yield StoredEvent(**event_dict)
                current_position = events[-1]["global_position"] + 1
                
                # Clear upcaster cache after each batch to avoid memory growth 
                # during large replays/projections.
                if self.upcasters:
                    self.upcasters.clear_cache()
        if False: yield StoredEvent(None, None, None, None, None, None, None, None)

    async def get_event(self, event_id: UUID | str) -> StoredEvent | None:
        """
        Loads one event by UUID. Used for causation chain lookups.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM events WHERE event_id = $1", str(event_id)
            )
            if row:
                event_dict = dict(row)
                if isinstance(event_dict["payload"], str): event_dict["payload"] = json.loads(event_dict["payload"])
                if isinstance(event_dict["metadata"], str): event_dict["metadata"] = json.loads(event_dict["metadata"])
                
                if self.upcasters:
                    event_dict = await self.upcasters.upcast(event_dict, self)
                return StoredEvent(**event_dict)
            return None


# ─────────────────────────────────────────────────────────────────────────────
# UPCASTER REGISTRY — Phase 4
# ─────────────────────────────────────────────────────────────────────────────

class UpcasterRegistry:
    def __init__(self):
        self._upcasters: dict[tuple[str, int], Callable] = {}
        # Short-lived cache for session lookups (e.g. AgentContextLoaded)
        # Keyed by (stream_id, event_type), cleared after major batch loads
        # normally or handled via an explicit session.
        self._cache: dict[tuple[str, str], Any] = {}

    def register(self, event_type: str, from_version: int):
        """Decorator. Registers fn as upcaster from event_type@from_version."""
        def decorator(fn: Callable) -> Callable:
            self._upcasters[(event_type, from_version)] = fn
            return fn
        return decorator

    def clear_cache(self):
        self._cache.clear()

    async def upcast(self, event: dict, store: EventStore = None) -> dict:
        """
        Apply all registered upcasters for this event type in version order.
        Input is the raw event dict from the DB, output is a possibly updated dict.
        Must NOT mutate the input.
        """
        et = event["event_type"]
        v = event.get("event_version", 1)
        key = (et, v)
        current = dict(event)
        
        # We need store for some upcasters (e.g. DecisionGenerated v1->v2)
        while key in self._upcasters:
            upcaster = self._upcasters[key]
            current = dict(current)
            
            if inspect.iscoroutinefunction(upcaster):
                current["payload"] = await upcaster(dict(current["payload"]), store, registry=self)
            else:
                try:
                    current["payload"] = upcaster(dict(current["payload"]), store, registry=self)
                except TypeError:
                    current["payload"] = upcaster(dict(current["payload"]))
            
            v += 1
            current["event_version"] = v
            key = (et, v)
        return current


# ─────────────────────────────────────────────────────────────────────────────
# IN-MEMORY EVENT STORE — for tests only
# ─────────────────────────────────────────────────────────────────────────────

class InMemoryEventStore:
    """
    Thread-safe (asyncio-safe) in-memory event store for unit tests.
    Identical interface to EventStore — swap one for the other with no code changes.
    Supports UpcasterRegistry for Phase 4 verification.
    """

    def __init__(self, upcaster_registry=None):
        self.upcasters = upcaster_registry
        # stream_id -> list of event dicts
        self._streams: dict[str, list[dict]] = defaultdict(list)
        # stream_id -> current version (position of last event, -1 if empty)
        self._versions: dict[str, int] = {}
        # global append log (ordered by insertion)
        self._global: list[dict] = []
        # projection checkpoints
        self._checkpoints: dict[str, int] = {}
        # asyncio lock per stream for OCC
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def stream_version(self, stream_id: str) -> int:
        return self._versions.get(stream_id, -1)

    async def append(
        self,
        stream_id: str,
        events: list[dict],
        expected_version: int,
        causation_id: str | None = None,
        metadata: dict | None = None,
    ) -> list[int]:
        async with self._locks[stream_id]:
            current = self._versions.get(stream_id, -1)
            if current != expected_version:
                raise OptimisticConcurrencyError(stream_id, expected_version, current)

            positions = []
            meta = {**(metadata or {})}
            if causation_id:
                meta["causation_id"] = causation_id

            for i, event in enumerate(events):
                pos = current + 1 + i
                stored = {
                    "event_id": str(uuid4()),
                    "stream_id": stream_id,
                    "stream_position": pos,
                    "global_position": len(self._global),
                    "event_type": event["event_type"],
                    "event_version": event.get("event_version", 1),
                    "payload": dict(event.get("payload", {})),
                    "metadata": meta,
                    "recorded_at": datetime.utcnow(),
                }
                self._streams[stream_id].append(stored)
                self._global.append(stored)
                positions.append(pos)

            self._versions[stream_id] = current + len(events)
            return positions

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[StoredEvent]:
        events = [
            e for e in self._streams.get(stream_id, [])
            if e["stream_position"] >= from_position
            and (to_position is None or e["stream_position"] <= to_position)
        ]
        
        # Apply upcasting if registry is present
        if self.upcasters:
            # InMemory upcast - we don't have a real 'store' but we can pass self
            upcasted = []
            for e in events:
                u = await self.upcasters.upcast(e, self)
                upcasted.append(u)
            events = upcasted

        return [StoredEvent(**e) for e in sorted(events, key=lambda e: e["stream_position"])]

    async def load_all(self, from_position: int = 0, batch_size: int = 500) -> AsyncGenerator[StoredEvent, None]:
        batch = [e for e in self._global if e["global_position"] >= from_position][:batch_size]
        for e in batch:
            if self.upcasters:
                e = await self.upcasters.upcast(e, self)
            yield StoredEvent(**e)
        
        # Clear upcaster cache after batch
        if self.upcasters:
            self.upcasters.clear_cache()

    async def get_event(self, event_id: UUID | str) -> StoredEvent | None:
        for e in self._global:
            if str(e["event_id"]) == str(event_id):
                if self.upcasters:
                    e = await self.upcasters.upcast(e, self)
                return StoredEvent(**e)
        return None

    async def save_checkpoint(self, projection_name: str, position: int) -> None:
        self._checkpoints[projection_name] = position

    async def load_checkpoint(self, projection_name: str) -> int:
        return self._checkpoints.get(projection_name, 0)
