from __future__ import annotations
from datetime import datetime
from typing import TYPE_CHECKING, Any
from ledger.event_store import EventStore

if TYPE_CHECKING:
    from ledger.event_store import EventStore, UpcasterRegistry

def register_upcasters(registry: UpcasterRegistry):
    
    @registry.register("CreditAnalysisCompleted", from_version=1)
    def upcast_credit_v1_v2(payload: dict, store: EventStore = None, registry: UpcasterRegistry = None) -> dict:
        """
        v1 -> v2: Add model_version, confidence_score, regulatory_basis.
        """
        new_payload = dict(payload)
        # DESIGN NOTE: "legacy-pre-2026" marks data from the previous monolithic system.
        # These records lack granular model telemetry and should be excluded from 
        # comparative model drift analysis.
        new_payload.setdefault("model_version", "legacy-pre-2026")
        new_payload.setdefault("confidence_score", None) # Regulatory integrity: unknown
        new_payload.setdefault("regulatory_basis", ["legacy-policy"])
        return new_payload

    @registry.register("DecisionGenerated", from_version=1)
    async def upcast_decision_v1_v2(payload: dict, store: EventStore = None, registry: UpcasterRegistry = None) -> dict:
        """
        v1 -> v2: Add model_versions dict. Uses cache for session lookups.
        """
        new_payload = dict(payload)
        contributing = new_payload.get("contributing_sessions", [])
        model_versions = {}
        
        if store and contributing:
            for session_stream_id in contributing:
                parts = session_stream_id.split("-")
                agent_type = parts[1] if len(parts) > 1 else "unknown"
                
                # Check Registry Cache
                cache_key = (session_stream_id, "AgentContextLoaded")
                if registry and cache_key in registry._cache:
                    model_versions[agent_type] = registry._cache[cache_key]
                    continue
                
                try:
                    # Gas Town pattern: AgentContextLoaded is typically within the first few events
                    events = await store.load_stream(session_stream_id, to_position=5)
                    found = False
                    for e in events:
                        if e.event_type == "AgentContextLoaded":
                            version = e.payload.get("model_version", "unknown")
                            model_versions[agent_type] = version
                            if registry: registry._cache[cache_key] = version
                            found = True
                            break
                    
                    if not found:
                        # Guard: Surface data quality issues instead of silent omission
                        model_versions[agent_type] = "missing-context"
                        if registry: registry._cache[cache_key] = "missing-context"
                        
                except Exception:
                    model_versions[agent_type] = "unknown"
                    if registry: registry._cache[cache_key] = "unknown"
        
        if "orchestrator" not in model_versions:
            model_versions["orchestrator"] = "legacy-pre-2026"
            
        new_payload["model_versions"] = model_versions
        return new_payload
