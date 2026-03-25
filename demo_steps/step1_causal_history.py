import asyncio
import json
import os
import sys
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from ledger.mcp.server import init_ledger, _ctx

# Ensure we can find the project modules
sys.path.append(os.getcwd())

console = Console()

async def run_demo():
    from dotenv import load_dotenv
    load_dotenv()
    
    # 1. Setup & Clear
    os.system('cls' if os.name == 'nt' else 'clear')
    console.print(Panel.fit("[bold blue]LEDGER PHASE 6: THE WEEK STANDARD DEMO[/bold blue]\n[italic]Scenario: Full Autonomous Decision History Trace[/italic]"))
    
    app_id = f"demo-loan-{datetime.now().strftime('%M%S')}"
    company_id = "COMP-080"
    
    # 2. Execute Pipeline
    console.print(f"\n[bold green]STEP 1: Executing Autonomous Pipeline for {company_id}...[/bold green]")
    import subprocess
    cmd = [sys.executable, "scripts/run_pipeline.py", "--company", company_id, "--application", app_id]
    
    import time
    start_time = time.time()
    process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    with console.status("[bold yellow]LLM Agents are working (Fraud, Credit, Compliance)...[/bold yellow]"):
        process.wait()
    
    duration = time.time() - start_time
    console.print(f"✅ Pipeline Completed in [bold cyan]{duration:.1f}s[/bold cyan] (Target: < 60s)")
    
    from ledger.mcp import resources as mcp_resources
    from ledger.event_store import EventStore
    from ledger.mcp.server import build_ledger_context
    import ledger.mcp.server as mcp_server
    
    # 3. Retrieve Governance Trace via PostgreSQL
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        console.print("[bold red]ERROR: DATABASE_URL not found in environment. Check your .env file.[/bold red]")
        return
        
    store = EventStore(db_url)
    await store.connect()
    
    # Build context with PostgreSQL store
    mcp_server._ctx = build_ledger_context(store)
    # Start projections
    asyncio.create_task(mcp_server._ctx.daemon.run_forever())
    
    # Wait for projections to catch up
    with console.status("[bold yellow]Projections are catching up...[/bold yellow]"):
        await asyncio.sleep(2)
    
    _ctx = mcp_server._ctx
    
    # Fetch data directly from projections and store for the "Reveal"
    summary_json = await mcp_resources.get_application_summary(app_id, _ctx.projections)
    summary = json.loads(summary_json)["result"]
    
    audit_json = await mcp_resources.get_audit_trail(app_id, _ctx.store)
    events = json.loads(audit_json)["result"]
    
    # Direct access to the audit stream for integrity check record
    stream_id = f"audit-loan-{app_id}"
    integrity_events = await _ctx.store.load_stream(stream_id)
    integrity_log = [{"payload": e.payload} for e in integrity_events]

    # 4. Display the Decision History Table
    table = Table(title=f"Complete Decision History (App: {app_id})")
    table.add_column("Seq", style="dim")
    table.add_column("Event Type", style="bold")
    table.add_column("Agent/Entity", style="cyan")
    table.add_column("Result/State", style="magenta")
    
    for e in events:
        payload = e["payload"]
        agent = payload.get("agent_id") or payload.get("reviewer_id") or "SYSTEM"
        result = payload.get("final_decision") or payload.get("recommendation") or payload.get("state") or e["event_type"]
        table.add_row(str(e["stream_position"]), e["event_type"], str(agent), str(result))
    
    console.print("\n")
    console.print(table)
    
    # 5. The "Wait, there's more" - Integrity & Outbox
    console.print(f"\n[bold green]STEP 2: Cryptographic Integrity Verification[/bold green]")
    if integrity_log:
        chain_valid = integrity_log[0]["payload"]["chain_valid"]
        status = "[bold green]VALID[/bold green]" if chain_valid else "[bold red]TAMPERED[/bold red]"
        console.print(f"🔒 Hash Chain Status: {status}")
        console.print(f"🔑 Final Integrity Hash: [dim]{integrity_log[0]['payload']['integrity_hash'][:32]}...[/dim]")
    else:
        console.print("⚠️ No integrity check found yet.")
    
    console.print(f"\n[bold green]STEP 3: Transactional Outbox (Reliability)[/bold green]")
    # Quick check for outbox count for this application
    async with _ctx.store._pool.acquire() as conn:
        count = await conn.fetchval("SELECT count(*) FROM outbox o JOIN events e ON o.event_id = e.event_id WHERE e.stream_id LIKE $1", f"%{app_id}%")
    console.print(f"📨 Reliable Outbox Signals Created: [bold cyan]{count}[/bold cyan]")
    
    console.print("\n[bold blue]--- END OF DEMO SESSION ---[/bold blue]")

if __name__ == "__main__":
    asyncio.run(run_demo())
