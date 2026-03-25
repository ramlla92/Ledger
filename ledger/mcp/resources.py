from __future__ import annotations
import json
from typing import Any, Dict, Optional
from ledger.event_store import EventStore
from datetime import datetime
from decimal import Decimal

class LedgerJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

def to_json(data: Any) -> str:
    return json.dumps(data, cls=LedgerJSONEncoder)

def wrap_result(data: Any) -> str:
    # Ensure data is a dict if it's a pydantic model
    if hasattr(data, "model_dump"):
        data = data.model_dump()
    return to_json({"ok": True, "result": data})

def wrap_error(message: str, error_type: str = "NotFoundError", suggested_action: str = "check_projection_status", details: Optional[Dict] = None) -> str:
    return to_json({
        "ok": False, 
        "error": {
            "error_type": error_type, 
            "message": message,
            "suggested_action": suggested_action,
            "details": details
        }
    })

# --- Resource Implementations ---

async def get_application_summary(id: str, projections: Dict[str, Any]) -> str:
    """Returns the ApplicationSummary projection for a given application ID."""
    repo = projections.get("application_summary")
    if not repo: return wrap_error("Projection not found", "InternalError")
    
    summary = await repo.get_by_id(id)
    if not summary: return wrap_error(f"Application {id} not found", suggested_action="ensure_application_exists")
    
    return wrap_result(summary)

async def get_compliance_view(application_id: str, projections: Dict[str, Any]) -> str:
    """Returns the current ComplianceAuditView projection."""
    repo = projections.get("compliance_audit")
    if not repo: return wrap_error("Projection not found", "InternalError")
    
    view = await repo.get_by_id(application_id)
    if not view: return wrap_error(f"No compliance data found for {application_id}", suggested_action="ensure_application_exists")
    
    return wrap_result(view)

async def get_audit_trail(id: str, store: EventStore) -> str:
    """Returns the primary application lifecycle log with sequence numbers and IDs."""
    stream_id = f"loan-{id}"
    events = await store.load_stream(stream_id)
    
    trail = [
        {
            "event_id": str(e.event_id),
            "event_type": e.event_type,
            "recorded_at": e.recorded_at.isoformat(),
            "stream_position": e.stream_position,
            "event_version": e.event_version,
            "correlation_id": e.metadata.get("correlation_id"),
            "payload": e.payload,
            "metadata": e.metadata
        }
        for e in events
    ]
    return wrap_result(trail)

async def get_agent_session(agent_id: str, session_id: str, store: EventStore) -> str:
    """Returns the raw event log for an agent session with IDs and metadata."""
    stream_id = f"agent-{agent_id}-{session_id}"
    events = await store.load_stream(stream_id)
    
    log = [
        {
            "event_id": str(e.event_id),
            "event_type": e.event_type,
            "recorded_at": e.recorded_at.isoformat(),
            "stream_position": e.stream_position,
            "event_version": e.event_version,
            "correlation_id": e.metadata.get("correlation_id"),
            "payload": e.payload
        }
        for e in events
    ]
    return wrap_result(log)

async def get_ledger_health(daemon: Any) -> str:
    """Returns system health and projection lags."""
    if not hasattr(daemon, "get_lags"): 
        return wrap_error("Daemon interface mismatch", "InternalError")
    
    lags = await daemon.get_lags()
    return wrap_result({"status": "OK", "projection_lags": lags})
