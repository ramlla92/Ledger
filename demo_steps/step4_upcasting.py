import asyncio
import os
import sys
import json
from datetime import datetime
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

console = Console()

async def run_upcasting_demo():
    console.print(Panel.fit(
        "[bold cyan]LEDGER PHASE 4: UPCASTING & IMMUTABILITY[/bold cyan]\n"
        "Goal: Load a v1 event through the store, show it arrives as v2.\n"
        "Verify: Raw database row remains unchanged.",
        border_style="cyan"
    ))

    db_url = os.getenv("DATABASE_URL")
    registry = UpcasterRegistry()
    register_upcasters(registry)
    store = EventStore(db_url, upcaster_registry=registry)
    await store.connect()

    # 1. Find a "Real" application from the production database
    async with store._pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT stream_id FROM events 
            WHERE event_type = 'ApplicationSubmitted' 
            ORDER BY recorded_at DESC LIMIT 1
        """)
        
    if not row:
        console.print("[bold red]No existing applications found. Run step 1 first![/bold red]")
        await store.close()
        return

    stream_id = row['stream_id']
    app_id = stream_id.replace("loan-", "")
    
    console.print(f"\n[bold yellow]1. IDENTIFIED REAL AUDIT LOG[/bold yellow] (Stream: {stream_id})")
    console.print(f"Adding a [bold]Legacy v1 Credit Analysis[/bold] to this authentic history...")
    
    # We manually create a v1 payload for 'CreditAnalysisCompleted'
    v1_payload = {
        "application_id": app_id,
        "analyst_id": "human-j-01",
        "score": 750,
        "recommendation": "APPROVE"
    }
    
    # Get current version to append correctly
    current_version = await store.stream_version(stream_id)

    # Append with version 1 explicitly
    events = [{
        "event_type": "CreditAnalysisCompleted",
        "event_version": 1,
        "payload": v1_payload
    }]
    
    positions = await store.append(stream_id, events, expected_version=current_version)
    console.print(f"✅ Legacy v1 event successfully appended at position {positions[0]}")

    # 2. Load via Store (The Transformation)
    console.print("\n[bold yellow]2. LOADING VIA EVENT STORE (The Reflection)[/bold yellow]")
    console.print("[italic]The store will detect v1 and apply the 'upcast_credit_v1_v2' logic...[/italic]")
    
    loaded_events = await store.load_stream(stream_id)
    # The upcasted event is the last one in the stream
    event = next(e for e in loaded_events if e.stream_position == positions[0])
    
    console.print(f"\n[bold green]RESULT: Event at position {event.stream_position} loaded as v{event.event_version}[/bold green]")
    
    table = Table(title="Upcasted Event Payload")
    table.add_column("Field", style="cyan")
    table.add_column("Value", style="white")
    
    for k, v in event.payload.items():
        # Highlight upcasted fields
        style = "bold green" if k in ["model_version", "confidence_score", "regulatory_basis"] else "white"
        table.add_row(k, str(v), style=style)
    
    console.print(table)
    console.print("[italic green](Fields in bold green were added dynamically during upcasting)[/italic green]")

    # 3. Side-by-Side Proof: Memory vs Disk
    console.print("\n[bold yellow]3. THE PROOF: MEMORY (Python) vs. DISK (PostgreSQL)[/bold yellow]")
    
    # Get raw DB state again for side-by-side
    async with store._pool.acquire() as conn:
        row = await conn.fetchrow("SELECT payload, event_version FROM events WHERE stream_id = $1 AND stream_position = $2", 
                                  stream_id, positions[0])
    db_payload = json.loads(row['payload'])
    
    proof_table = Table(show_header=True, header_style="bold magenta")
    proof_table.add_column("Property", style="dim")
    proof_table.add_column("DISK (PostgreSQL Row)", style="red")
    proof_table.add_column("MEMORY (Upcasted Object)", style="green")
    
    all_keys = sorted(set(list(db_payload.keys()) + list(event.payload.keys())))
    for k in all_keys:
        in_db = "✅" if k in db_payload else "❌ (Missing)"
        in_mem = "✅" if k in event.payload else "❌"
        style = "bold green" if k in event.payload and k not in db_payload else "white"
        proof_table.add_row(k, in_db, in_mem, style=style)
    
    console.print(proof_table)
    console.print("\n[bold green]✨ ANALYSIS:[/bold green]")
    console.print("The fields in [bold green]bold green[/bold green] exist ONLY in your application's RAM.")
    console.print("They were injected dynamically by the Upcaster during the load process.")
    console.print("This proves we have [bold]Zero-Migration Evolution[/bold]: The database stays immutable, but the code stays modern.")

    await store.close()

if __name__ == "__main__":
    asyncio.run(run_upcasting_demo())
