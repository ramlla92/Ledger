from __future__ import annotations
from pydantic import BaseModel
from typing import Any
from ledger.event_store import EventStore

class AgentContext(BaseModel):
    context_text: str
    last_event_position: int
    pending_work: list[dict]
    session_health_status: str  # e.g. "OK", "NEEDS_RECONCILIATION"

async def reconstruct_agent_context(
    store: EventStore,
    agent_id: str,
    session_id: str,
    token_budget: int = 8000,
) -> AgentContext:
    """
    Reconstructs an AI agent's context from its event stream (Gas Town pattern).
    Refinements:
    - Prioritized Summarization: Errors/Decisions/Compliance first.
    - Improved Health Detection: Partial decisions and long-lived nodes.
    - Structured Pending Work: Added 'reason' field.
    """
    stream_id = f"agent-{agent_id}-{session_id}"
    events = await store.load_stream(stream_id)
    
    if not events:
        return AgentContext(
            context_text="No history found for this session.",
            last_event_position=-1,
            pending_work=[],
            session_health_status="OK"
        )
        
    last_event = events[-1]
    last_pos = last_event.stream_position
    
    # 1. State/Health Analysis
    session_health_status = "OK"
    pending_work = []
    
    # Identify started nodes without completion
    started_nodes = {}
    for e in events:
        if e.event_type == "AgentNodeStarted":
            started_nodes[e.payload["node_name"]] = e
        elif e.event_type == "AgentNodeExecuted":
            started_nodes.pop(e.payload["node_name"], None)
            
    for node_name, e in started_nodes.items():
        pending_work.append({
            "type": "node", 
            "name": node_name, 
            "started_at": e.recorded_at.isoformat(),
            "reason": "node_not_finished"
        })

    # Check for partial decision / failure
    if last_event.event_type == "AgentSessionFailed":
        session_health_status = "NEEDS_RECONCILIATION"
        pending_work.append({"type": "session", "reason": "session_failed", "error": last_event.payload.get("error")})
    
    # Partial decision pattern: e.g. CreditAnalysisCompleted exists but DecisionGenerated doesn't
    # (Assuming orchestrator flow)
    has_analysis = any(e.event_type == "CreditAnalysisCompleted" for e in events)
    has_decision = any(e.event_type == "DecisionGenerated" for e in events)
    if has_analysis and not has_decision:
        session_health_status = "NEEDS_RECONCILIATION"
        pending_work.append({"type": "decision", "reason": "partial_decision_detected"})

    # 2. Prioritized Summarization
    # Category 1: Critical (Errors/Failures)
    critical = [e for e in events if "Failed" in e.event_type or "Error" in e.event_type]
    # Category 2: Key Milestones (Decisions/Compliance)
    milestones = [e for e in events if e.event_type in ["CreditAnalysisCompleted", "DecisionGenerated", "ComplianceCheckCompleted"]]
    # Category 3: Recent (Last 3)
    recent = events[-3:]
    # Category 4: General History
    history = [e for e in events if e not in critical and e not in milestones and e not in recent]

    def fmt_event(e, verbose=False):
        base = f"- {e.recorded_at.strftime('%H:%M:%S')}: {e.event_type}"
        if verbose:
            return f"{base}\n  Payload: {json.dumps(e.payload)}"
        return base

    lines = []
    # Add Critical first
    for e in critical: lines.append(f"CRITICAL: {fmt_event(e, True)}")
    # Add Milestones
    for e in milestones: lines.append(f"MILESTONE: {fmt_event(e, True)}")
    # Add Recent
    for e in recent: lines.append(f"RECENT: {fmt_event(e, True)}")
    
    # Backfill with history until budget
    max_chars = token_budget * 4
    current_chars = sum(len(l) for l in lines)
    
    history_lines = []
    for e in reversed(history): # Newer history first
        line = fmt_event(e)
        if current_chars + len(line) + 1 < max_chars:
            history_lines.insert(0, line)
            current_chars += len(line) + 1
        else:
            break
            
    context_text = "\n".join(lines + history_lines)
    
    return AgentContext(
        context_text=context_text,
        last_event_position=last_pos,
        pending_work=pending_work,
        session_health_status=session_health_status
    )

import json # Required for json.dumps in fmt_event
