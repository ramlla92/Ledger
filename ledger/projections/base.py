from abc import ABC, abstractmethod
from typing import Set
from ledger.event_store import EventStore
from ledger.schema.events import StoredEvent

class Projection(ABC):
    def __init__(self, store: EventStore) -> None:
        self.store = store

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the projection, used for retrieving the checkpoint."""
        pass

    @abstractmethod
    async def subscribed_event_types(self) -> Set[str]:
        """
        Return the set of event types this projection is interested in.
        Return an empty set to subscribe to all events.
        """
        pass

    @abstractmethod
    async def handle(self, event: StoredEvent) -> None:
        """Handle a single event."""
        pass

    async def get_last_position(self) -> int:
        """
        Retrieves the last processed global position from projection_checkpoints.
        """
        if getattr(self.store, "_pool", None) is None:
            return getattr(self, "_mem_checkpoint", 0)
            
        async with self.store._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT last_position FROM projection_checkpoints WHERE projection_name = $1",
                self.name
            )
            return row["last_position"] if row else 0

    async def update_checkpoint(self, position: int) -> None:
        """
        Updates the global position in projection_checkpoints.
        """
        if getattr(self.store, "_pool", None) is None:
            self._mem_checkpoint = position
            return
            
        async with self.store._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO projection_checkpoints (projection_name, last_position, updated_at) 
                VALUES ($1, $2, NOW())
                ON CONFLICT (projection_name) DO UPDATE 
                SET last_position = EXCLUDED.last_position, updated_at = NOW()
                """,
                self.name, position
            )

    @abstractmethod
    async def get_lag(self) -> int:
        """
        Return the lag (difference between highest global_position in events table
        and this projection's checkpoint). Could be in millis or positional count.
        """
        pass
