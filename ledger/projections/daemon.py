import asyncio
import logging
from typing import List, Dict
from ledger.event_store import EventStore
from ledger.projections.base import Projection
from ledger.schema.events import StoredEvent

logger = logging.getLogger(__name__)

class ProjectionDaemon:
    def __init__(self, store: EventStore, projections: List[Projection]) -> None:
        self.store = store
        self.projections = projections
        self._running = False
        self._task: asyncio.Task | None = None
        self._max_retries = 3

    async def run_forever(self, poll_interval_ms: int = 100) -> None:
        """Loop indefinitely and process batches from EventStore."""
        self._running = True
        logger.info("ProjectionDaemon started.")
        while self._running:
            try:
                await self._process_batch(batch_size=500)
            except Exception as e:
                logger.error(f"Error in EventStore polling loop: {e}")
            
            await asyncio.sleep(poll_interval_ms / 1000.0)

    async def stop(self) -> None:
        """Stop the daemon loop."""
        self._running = False
        if self._task:
            self._task.cancel()
        logger.info("ProjectionDaemon stopped.")

    async def _process_batch(self, batch_size: int = 500) -> None:
        """Load the next batch of events and fan out."""
        # 1. Compute get_last_position() for each projection ONCE per batch
        checkpoints = {}
        subscriptions = {}
        for p in self.projections:
            checkpoints[p.name] = await p.get_last_position()
            subscriptions[p.name] = await p.subscribed_event_types()
            
        lowest = min(checkpoints.values()) if checkpoints else 0
        from_position = lowest

        async for event in self.store.load_all(from_position=from_position, batch_size=batch_size):
            gpos = event.global_position
            
            for proj in self.projections:
                p_name = proj.name
                
                # Check if this projection already processed it
                if gpos <= checkpoints[p_name]:
                    continue
                    
                subs = subscriptions[p_name]
                if not subs or event.event_type in subs:
                    success = await self._handle_with_retry(proj, event)
                    if success:
                        checkpoints[p_name] = gpos # Update our local cache
                    else:
                        checkpoints[p_name] = gpos

    async def _handle_with_retry(self, proj: Projection, event: StoredEvent) -> bool:
        """
        Handle a single event for a projection with retries. Update checkpoint on success.
        Returns True if successful, False if skipped after retries.
        """
        for attempt in range(1, self._max_retries + 1):
            try:
                await proj.handle(event)
                await proj.update_checkpoint(event.global_position)
                return True
            except Exception as e:
                if attempt == self._max_retries:
                    logger.error(f"Projection {proj.name} failed to handle event "
                                 f"{event.event_type} at global_position {event.global_position} "
                                 f"after {self._max_retries} attempts. Skipping. Error: {e}")
                    # Need to update checkpoint anyway to skip it
                    await proj.update_checkpoint(event.global_position)
                    return False
                else:
                    logger.warning(f"Projection {proj.name} attempt {attempt} failed on event "
                                   f"{event.event_type} at pos {event.global_position}: {e}. Retrying...")
                    await asyncio.sleep(0.5 * attempt)
        return False

    async def get_lags(self) -> Dict[str, int]:
        """Retrieve lags in milliseconds from all registered projections."""
        lags = {}
        for p in self.projections:
            lags[p.name] = await p.get_lag()
        return lags
