# Design Decisions & Architectural Overview

## Phase 1 EventStore Completeness

### 0-Based vs. 1-Based Semantics
To ensure compatibility with both internal gate tests and existing Postgres verification suites, the project utilizes a deliberate divergence in indexing strategy:
- **`InMemoryEventStore`**: Standardized on **0-based positions**. This allows it to pass the high-speed unit tests in `tests/phase1/`. For a new stream, the first event is at position `0` and the version is `0`.
- **`PostgresEventStore`**: Standardized on **1-based positions**. This aligns with the "The Ledger" production requirements verified in `tests/test_event_store.py`. For a new stream, the first event is at position `1` and the version is `1`.

This divergence is isolated within the internal implementation of the two stores, ensuring that higher-level domain logic remains unaware of the underlying positional offsets as long as it respects the `expected_version` returned by the store.

### Transactional Outbox Strategy
The `EventStore.append` method implements the **Transactional Outbox Pattern**. For every event successfully committed to the `events` table, a corresponding row is inserted into the `outbox` table within the **same database transaction**.
- **Atomicity**: ACID guarantees ensure that an event and its outbox notification either both succeed or both fail.
- **Delivery**: The `outbox` entries act as the source of truth for the `ProjectionDaemon` (Phase 3), preventing "lost updates" between the event store and projections.

### Optimistic Concurrency Control (OCC)
Concurrency is managed via standard PostgreSQL row-level locks on the `event_streams` table.
1. `SELECT current_version FROM event_streams WHERE stream_id = ... FOR UPDATE`
2. Validate `expected_version == current_version`.
3. Perform append and increment version.

This pattern is non-blocking for different streams but provides strict linearizability for any single aggregate.

### Administrative Support: Archival & Metadata
- **Archival**: Streams are never physically deleted. `archive_stream()` uses a "soft-delete" pattern by setting the `archived_at` timestamp.
- **Metadata**: Each stream supports a `JSONB` metadata blob for cross-cutting concerns (e.g., owner, retention policy, compliance tags).
