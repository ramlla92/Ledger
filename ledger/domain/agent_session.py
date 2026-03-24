from __future__ import annotations
from typing import Set, Any, TYPE_CHECKING
from ledger.event_store import EventStore
from ledger.schema.events import (
    StoredEvent,
    DomainError
)

if TYPE_CHECKING:
    from ledger.domain.loan_application import LoanApplicationAggregate

class AgentSessionAggregate:
    def __init__(self, agent_id: str, session_id: str):
        self.agent_id = agent_id
        self.session_id = session_id
        self.version: int = -1
        self.context_loaded: bool = False
        self.model_version: str | None = None
        self.processed_applications: Set[str] = set()
        self.decision_applications: Set[str] = set()

    @classmethod
    async def load(cls, store: EventStore, agent_id: str, session_id: str) -> "AgentSessionAggregate":
        # Stream ID depends on the type, but let's assume we use a general pattern
        # or the caller provides the full stream ID. User said agent-{agent_id}-{session_id}.
        stream_id = f"agent-{agent_id}-{session_id}"
        events = await store.load_stream(stream_id)
        agg = cls(agent_id=agent_id, session_id=session_id)
        for event in events:
            agg._apply(event)
        return agg

    def _apply(self, event: StoredEvent) -> None:
        handler_name = f"_on_{event['event_type']}"
        handler = getattr(self, handler_name, None)
        if handler:
            handler(event["payload"])
        self.version = event["stream_position"]

    # --- Handlers ---

    def _on_AgentContextLoaded(self, payload: dict[str, Any]) -> None:
        self.context_loaded = True
        self.model_version = payload["model_version"]

    def _on_CreditAnalysisCompleted(self, payload: dict[str, Any]) -> None:
        app_id = payload["application_id"]
        self.processed_applications.add(app_id)
        self.decision_applications.add(app_id)

    def _on_FraudScreeningCompleted(self, payload: dict[str, Any]) -> None:
        self.processed_applications.add(payload["application_id"])

    # --- Business Rules ---

    def assert_context_loaded(self) -> None:
        """Enforce Gas Town rule: context must be loaded before decisions."""
        if not self.context_loaded:
            raise DomainError(f"Agent session {self.session_id} has not loaded context. Action forbidden (Gas Town).")

    def assert_model_version_current(self, model_version: str) -> None:
        """Ensure consistency with the session's locked model version."""
        if model_version != self.model_version:
            raise DomainError(f"Model version mismatch. Session locked to {self.model_version}, but command uses {model_version}.")

    def assert_can_append_decision_for_application(self, application_id: str, app: LoanApplicationAggregate) -> None:
        """Prevent analysis churn unless override is applied."""
        if application_id in self.decision_applications and not app.human_review_override_applied:
            raise DomainError(f"Decision already exists for application {application_id} in this session. Churn blocked.")
