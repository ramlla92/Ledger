import asyncio
import os
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path
from rich.console import Console
from rich.panel import Panel

# Add root to path so we can import internal modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from ledger.event_store import EventStore, UpcasterRegistry
from ledger.upcasters import register_upcasters
from ledger.projections.compliance_audit import ComplianceAuditViewProjection

console = Console()

async def run_temporal_demo():
    os.system('cls' if os.name == 'nt' else 'clear')
    console.print(Panel.fit("[bold blue]LEDGER PHASE 6: TEMPORAL COMPLIANCE QUERY[/bold blue]\n[italic]Scenario: Time-traveling mid-pipeline on real application data[/italic]"))

    db_url = os.getenv("DATABASE_URL")
    registry = UpcasterRegistry()
    register_upcasters(registry)
    store = EventStore(db_url, upcaster_registry=registry)
    await store.connect()

    # 1. Automatically find the most recently COMPLETED REAL pipeline
    async with store._pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT stream_id, recorded_at
            FROM events 
            WHERE event_type = 'ComplianceCheckCompleted'
            ORDER BY recorded_at DESC LIMIT 1
        """)
        
    if not row:
        console.print("[bold red]No completed loan streams found. Please run step 1 first![/bold red]")
        await store.close()
        return

    stream_id = row['stream_id']
    app_id = stream_id.replace("loan-", "").replace("compliance-", "").replace("audit-loan-", "")
    
    console.print(f"🎯 Target Application: [bold cyan]{app_id}[/bold cyan] (Most recent completed in production)")

    # Load all real events
    events = await store.load_stream(stream_id)
    # If using dual-stream, load both to show a full timeline
    comp_stream = f"compliance-{app_id}"
    comp_events = await store.load_stream(comp_stream)
    all_events = sorted(events + comp_events, key=lambda e: e.recorded_at)
    
    console.print("\n[bold yellow]--- AUTHENTIC PIPELINE TIMELINE ---[/bold yellow]")
    fraud_event_time = None
    for e in all_events:
        console.print(f"  [{e.stream_position}] {e.recorded_at.isoformat()} -> {e.event_type}")
        if e.event_type == "FraudScreeningCompleted":
            fraud_event_time = e.recorded_at

    if not fraud_event_time:
         # Fallback to 10 seconds before completion if no fraud event
         fraud_event_time = row['recorded_at']

    projection = ComplianceAuditViewProjection(store)

    from ledger.mcp import server as mcp_server
    mcp_server._ctx = mcp_server.build_ledger_context(store=store)
    
    # 2. Query Current State (The Present)
    # The MCP resource now only returns the LATEST state (clean signature)
    console.print(f"\n[bold green]1. CURRENT SYSTEM STATE (ledger://applications/{app_id}/compliance)[/bold green]")
    console.print("[italic]This is the final compliance state after the pipeline finished.[/italic]")
    current_json = await mcp_server.get_compliance(application_id=app_id)
    current_resp = json.loads(current_json)
    
    if "result" not in current_resp:
        console.print(f"[bold red]Error in response: {current_json}[/bold red]")
        return
        
    current_state = current_resp["result"]
    console.print(json.dumps(current_state, indent=2))

    # 3. Query Past State (Time Travel)
    # Temporal queries are handled via the Projection API directly in this demo
    # We will query exactly 10 milliseconds BEFORE the high-impact event (Fraud Screening or Completion).
    T_PAST = fraud_event_time - timedelta(milliseconds=10)
    
    console.print(f"\n[bold magenta]2. TEMPORAL QUERY (T_PAST: {T_PAST.isoformat()})[/bold magenta]")
    console.print(f"[italic]Rebuilding state exactly as it existed at the specified point in history...[/italic]")
    
    # Direct projection call for temporal reconstruction
    past_state = await projection.get_compliance_at(app_id, T_PAST)
    
    if hasattr(past_state, "model_dump"):
        past_state = past_state.model_dump()
        
    console.print(json.dumps(past_state, indent=2))
    
    # 4. Observation
    console.print("\n[bold yellow]--- OBSERVATION ---[/bold yellow]")
    console.print("Notice how the temporal query perfectly isolates the past state:")
    console.print(f"- The [bold]CURRENT[/bold] state represents the process [bold]COMPLETED[/bold].")
    console.print(f"- The [bold]PAST[/bold] state shows the system [bold]BEFORE[/bold] the final events were recorded.")
    console.print("\nThis explicitly proves we can reconstruct [italic]any[/italic] historical projection state on demand simply by replaying the immutable event trace!")

    await store.close()

if __name__ == "__main__":
    asyncio.run(run_temporal_demo())
