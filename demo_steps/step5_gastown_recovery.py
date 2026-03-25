import asyncio
import os
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# Add root to path so we can import internal modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from ledger.event_store import EventStore, UpcasterRegistry
from ledger.upcasters import register_upcasters
from ledger.agents.context import reconstruct_agent_context

console = Console()

async def run_gastown_demo():
    console.print(Panel.fit(
        "[bold magenta]LEDGER PHASE 5: GAS TOWN RECOVERY[/bold magenta]\n"
        "Goal: Reconstruct an AI agent's memory after a total process crash.\n"
        "Mechanism: Replay the agent's immutable session stream.",
        border_style="magenta"
    ))

    db_url = os.getenv("DATABASE_URL")
    registry = UpcasterRegistry()
    register_upcasters(registry)
    store = EventStore(db_url, upcaster_registry=registry)
    await store.connect()

    # 1. State: The Agent is Running
    agent_id = "analyst-orch-01"
    session_id = f"recovery-{datetime.now().strftime('%H%M%S')}"
    stream_id = f"agent-{agent_id}-{session_id}"
    
    console.print(f"\n[bold yellow]1. AGENT SESSION ACTIVE[/bold yellow] (ID: {session_id})")
    console.print("[italic]Agent is performing complex reasoning steps...[/italic]")
    
    events = [
        {
            "event_type": "AgentContextLoaded",
            "payload": {"model_version": "gpt-4o", "temperature": 0.7}
        },
        {
            "event_type": "AgentNodeStarted",
            "payload": {"node_name": "DataExtraction"}
        },
        {
            "event_type": "AgentNodeExecuted",
            "payload": {"node_name": "DataExtraction", "result": "Success"}
        },
        {
            "event_type": "InteractionRecorded",
            "payload": {"role": "user", "content": "Analyze the revenue growth."}
        },
        {
            "event_type": "AgentNodeStarted",
            "payload": {"node_name": "CreditRiskAssessment"} # This node will be 'interrupted' by the crash
        }
    ]
    
    await store.append(stream_id, events, expected_version=-1)
    console.print(f"✅ Appended {len(events)} events to [cyan]{stream_id}[/cyan]")

    # 2. The Disaster: Process Crash
    console.print("\n[bold red]💥 DISASTER: AGENT PROCESS CRASHED 💥[/bold red]")
    console.print("[dim italic]All in-memory state, pending tasks, and context buffers have been lost...[/dim italic]")
    
    # 3. The Recovery: Gas Town Pattern
    console.print("\n[bold green]2. EXECUTING GAS TOWN RECOVERY[/bold green]")
    console.print("[italic]Replaying immutable event trace to rebuild the agent's neural state...[/italic]")
    
    # Trigger reconstruction
    context = await reconstruct_agent_context(store, agent_id, session_id)
    
    # 4. Action: Resumption & Fulfillment (The Proof)
    console.print("\n[bold yellow]3. RESUMPTION & FULFILLMENT (The Critical Proof)[/bold yellow]")
    console.print("[italic]Now, the recovered agent 'continues the story' on the ledger...[/italic]")
    
    # We identify the pending task
    pending_task = context.pending_work[0]
    node_name = pending_task["name"]
    
    # 5. Record resumption and completion events to the ledger
    # This proves the recovered agent has enough 'memory' to finish its work
    resumption_events = [
        {
            "event_type": "AgentSessionResumed",
            "payload": {
                "session_id": session_id,
                "recovered_at": datetime.now().isoformat(),
                "resuming_from_node": node_name,
                "history_length": len(context.context_text.split("\n"))
            }
        },
        {
            "event_type": "AgentNodeExecuted",
            "payload": {
                "session_id": session_id,
                "node_name": node_name,
                "result": "Success (Post-Recovery Execution)",
                "executed_at": datetime.now().isoformat()
            }
        },
        {
            "event_type": "AgentSessionCompleted",
            "payload": {
                "session_id": session_id,
                "status": "COMPLETED",
                "completed_at": datetime.now().isoformat()
            }
        }
    ]
    
    # In a real system, the agent would perform the work. Here we record the proof of that work.
    new_version = await store.stream_version(stream_id)
    await store.append(stream_id, resumption_events, expected_version=new_version)
    console.print(f"✅ Recovery finalized. Recorded Resumption + Completion for [bold]{node_name}[/bold].")

    # 6. Final Verification: The Complete Unified Timeline
    console.print("\n[bold yellow]4. FINAL AUDIT LOG: A UNIFIED STORY[/bold yellow]")
    console.print("[italic]Proof that the crash is now just a blip in an otherwise continuous audit trail...[/italic]")
    
    final_events = await store.load_stream(stream_id)
    
    table = Table(title=f"Complete Session History (Recovery Proof)")
    table.add_column("Pos", style="dim")
    table.add_column("Phase", style="cyan")
    table.add_column("Event Type", style="white")
    
    for i, e in enumerate(final_events):
        phase = "[red]PRE-CRASH[/red]" if i < 5 else "[green]POST-RECOVERY[/green]"
        style = "bold green" if i >= 5 else "white"
        table.add_row(str(i), phase, e.event_type, style=style)
        
    console.print(table)
    
    # Final Executive Summary
    console.print("\n[bold green]✨ EXECUTIVE SUMMARY:[/bold green]")
    console.print("1. [bold]CRASH[/bold]: The agent process died mid-task.")
    console.print("2. [bold]RECOVERY[/bold]: Gas Town logic rebuilt the mind from the immutable ledger.")
    console.print("3. [bold]FULFILLMENT[/bold]: The agent resumed and reached its goal with zero data loss.")
    console.print("\nThis concludes the Infrastructure Demo. The Ledger is now the 'Absolute Memory' of your AI Workforce.")

    await store.close()

if __name__ == "__main__":
    asyncio.run(run_gastown_demo())
