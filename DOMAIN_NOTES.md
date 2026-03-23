# DOMAIN_NOTES.md

## 1. EDA vs. ES Distinction
Most of the existing tracing in earlier weeks (governance hooks, LangChain traces, Automaton Auditor verdict logs) is Event‑Driven Architecture (EDA), not true Event Sourcing. In EDA, events are messages between components, and it is acceptable for them to be transient, dropped, or stored only in ad‑hoc logs.

In this Ledger repo, the EventStore and Postgres schema implement Event Sourcing (ES): events in `events` and `event_streams` are the system of record, and all aggregate state is a replay over those events. If we redesigned an EDA‑style tracing component to use this Ledger, we would route every “trace” or “audit verdict” through `EventStore.append`, giving each component a stable `stream_id` and immutable event history. We would gain: reproducible aggregate state at any time, temporal queries, and a single append‑only audit ledger that other systems (projections, regulators, what‑if projector) can query consistently.

## 2. The Aggregate Question
The scenario defines four main aggregates: `LoanApplication`, `AgentSession`, `ComplianceRecord`, and `AuditLedger`. One alternative boundary would be to merge `LoanApplication` and `ComplianceRecord` into a single “LoanWithCompliance” aggregate that owns both business state and compliance checks.

I reject that boundary because it couples regulatory workflows tightly to the core loan lifecycle and widens the consistency boundary. Under concurrent load (e.g., multiple compliance rules evaluating while agents update loan state), this would increase contention on a single stream and make optimistic concurrency conflicts more frequent and harder to reason about. Keeping `ComplianceRecord` as a separate aggregate and stream (`compliance-{application_id}`) lets us evolve compliance rules and projections independently while `LoanApplication` enforces only the dependency (“cannot approve unless compliance is satisfied”) via read‑side checks and business rules. This separation reduces coupling and localizes failure modes when compliance logic or regulations change.

## 3. Concurrency in Practice
**Scenario**: two AI agents process the same loan and both try to append a decision at `expected_version = 3` to stream `loan-{id}`.

1. Both agents call `EventStore.stream_version("loan-{id}")` and read `current_version = 3`.
2. Agent A enters append first, acquires the row lock on `event_streams` for `loan-{id}`, sees `current_version = 3`, which matches `expected_version`, inserts its new event(s) at `stream_position = 4`, and updates `current_version` to 4.
3. Agent B then enters append, locks the same row, and now sees `current_version = 4` while its `expected_version` is still 3, so `EventStore.append` raises `OptimisticConcurrencyError(stream_id, 3, 4)`.

The losing agent must not swallow this; it reloads the stream via `load_stream("loan-{id}")`, replays aggregate state, and decides whether its original decision is still valid, potentially emitting a new event or doing nothing. This pattern ensures there is a single authoritative sequence of facts per aggregate and that concurrent agents never create split‑brain state in the ledger.

## 4. Projection Lag and Its Consequences
The projections (e.g., `ApplicationSummary`, `ComplianceAuditView`) are updated asynchronously via a `ProjectionDaemon` that consumes `EventStore.load_all`. By design, projections are eventually consistent and can have a measurable lag from the write side; for example, `ApplicationSummary` targets under 500 ms lag and `ComplianceAuditView` under 2 seconds.

If a loan officer queries “available credit limit” immediately after an agent commits a disbursement event, they might see an outdated limit because the projection has not yet processed the new event. In that case, the system should:
- Expose per‑projection lag metrics (e.g., last processed `global_position` vs latest in store) and include a staleness indicator in the UI for that view.
- Optionally allow the client to request a “fresh read” for critical actions, which either waits for the projection to catch up (up to an SLO bound) or rejects with a clear message that the data may be stale.

This makes eventual consistency explicit and prevents silent misuse of slightly stale data in regulatory‑sensitive workflows.

## 5. The Upcasting Scenario
Original event (2024): `CreditDecisionMade` with `{application_id, decision, reason}`. New schema (2026): `{application_id, decision, reason, model_version, confidence_score, regulatory_basis}`.

**Upcaster Registry**:
```python
@registry.upcaster("CreditDecisionMade", from_version=1, to_version=2)
def upcast_credit_decision_v1_to_v2(payload: dict) -> dict:
    # Preserve original fields
    new_payload = dict(payload)

    # model_version: we cannot reconstruct exact model id; infer from time window or mark legacy.
    new_payload.setdefault("model_version", "legacy-pre-2026")

    # confidence_score: historically unknown; use None rather than fabricate.
    new_payload.setdefault("confidence_score", None)

    # regulatory_basis: infer from regulation set/version active at recorded_at date, if available;
    # otherwise set to "unknown".
    if "regulatory_basis" not in new_payload:
        new_payload["regulatory_basis"] = "inferred-from-regulation-set"  # domain-specific

    return new_payload
```

**Key Principles**:
- Never mutate stored rows; upcasting is applied only when loading events via `load_stream`/`load_all`.
- For fields we genuinely cannot reconstruct (e.g., exact confidence), we prefer `null`/sentinel values over fabricated numbers, because incorrect confidence or model identifiers are more dangerous for regulators than explicitly “unknown” data.

## 6. Marten Async Daemon Parallel
Marten’s Async Daemon executes projections across multiple nodes with coordinated checkpoints and partitioning of the event stream. In this Python/Postgres implementation, I would mirror the pattern by:
- Using a shared `projection_checkpoints` table where each projection stores its last processed `global_position` and, for distributed execution, an additional partition key or worker id.
- Running multiple instances of a `ProjectionDaemon` process, each acquiring work using a coordination primitive such as PostgreSQL **advisory locks** or a lightweight leader‑election mechanism (e.g., a “leases” table with `locked_by`, `lock_expires_at`).

**Daemon Behavior**:
1. Attempt to acquire a lock for a particular projection partition.
2. Read events from `load_all` starting at that partition’s checkpoint.
3. Update that checkpoint atomically in the same transaction as projection updates.

This guards against processing the same events twice or skipping ranges, and ensures we can horizontally scale projection processing under high event throughput while maintaining exactly‑once or at‑least‑once semantics under control.
