import asyncio
import os
import sys
import json
from datetime import datetime
from pathlib import Path
from rich.console import Console

# Add root to path so we can import internal modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from ledger.mcp import server
from ledger.event_store import EventStore, UpcasterRegistry
from ledger.upcasters import register_upcasters

console = Console()

async def dump_raw_table(store, stream_id):
    """Outputs the raw events table exactly as it looks in the database."""
    console.print(f"\n[bold magenta]--- RAW DATABASE TABLE: events ---[/bold magenta]")
    
    async with store._pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM events WHERE stream_id = $1 ORDER BY stream_position", stream_id)
        
        # Print headers
        header = f"{'stream_position':<15} | {'event_type':<25} | {'payload'}"
        console.print(f"[bold]{header}[/bold]")
        console.print("-" * 120)
        
        for r in rows:
            pos = str(r['stream_position']).ljust(15)
            evt = r['event_type'].ljust(25)
            payload_str = str(r['payload'])
            console.print(f"{pos} | {evt} | {payload_str}")
    console.print("[bold magenta]----------------------------------[/bold magenta]\n")


async def run_real_concurrency_demo():
    company_id = "COMP-001"
    app_id = f"loan-CONC-{datetime.now().strftime('%M%S')}"
    
    # Initialize Store & Server
    registry = UpcasterRegistry()
    register_upcasters(registry)
    db_url = os.getenv("DATABASE_URL")
    store = EventStore(db_url, upcaster_registry=registry)
    await store.connect()
    server._ctx = server.build_ledger_context(store=store)
    daemon_task = asyncio.create_task(server._ctx.daemon.run_forever())

    # Submit Application
    res_json = await server.submit_application(
        application_id=app_id, applicant_id=f"cust-{company_id}", loan_amount=500000.0, purpose="Equipment upgrade"
    )
    res = json.loads(res_json)
    if not res["ok"]: return
        
    stream_id = res["result"]["stream_id"]
    current_version = res["result"]["version"]
    
    # Start Agent Session
    agent_id = "ai-analyst-gen2"
    sess_id = f"sess-{datetime.now().strftime('%M%S')}"
    await server.start_agent_session(agent_id=agent_id, session_id=sess_id, model_version="gemini-2.0-flash")

    # Concurrent Agent Actions (Fraud vs Credit) colliding at current_version
    async def fraud_task(expected_v):
        return await server.record_fraud_screening(
            application_id=app_id, agent_id=agent_id, session_id=sess_id,
            fraud_score=0.15, anomaly_flags=["None"], screening_model_version="gemini-2.0-flash",
            expected_version=expected_v
        )
            
    async def credit_task(expected_v):
        return await server.record_credit_analysis(
            application_id=app_id, agent_id=agent_id, session_id=sess_id,
            risktier="B", confidence_score=0.9, analysis_duration_ms=1200, recommended_limit_usd=400000.0,
            model_version="gemini-2.0-flash", input_data_hash="fakehash",
            expected_version=expected_v
        )

    # Run simultaneously to force Optimistic Concurrency collision
    f_res_json, c_res_json = await asyncio.gather(
        fraud_task(current_version),
        credit_task(current_version)
    )
    
    f_res = json.loads(f_res_json)
    c_res = json.loads(c_res_json)

    # Automatic Retry for the one that failed the collision
    if not f_res["ok"]:
        console.print(f"\n[bold yellow]>>> COLLISION DETECTED: Fraud Agent blocked by OCC. Auto-retrying...[/bold yellow]")
        new_version = await store.stream_version(stream_id)
        f_res = json.loads(await fraud_task(new_version))
        current_version = f_res["result"]["new_version"]
    elif not c_res["ok"]:
        console.print(f"\n[bold yellow]>>> COLLISION DETECTED: Credit Agent blocked by OCC. Auto-retrying...[/bold yellow]")
        new_version = await store.stream_version(stream_id)
        c_res = json.loads(await credit_task(new_version))
        current_version = c_res["result"]["new_version"]

    # Continue pipeline
    res_json = await server.generate_decision(
        application_id=app_id, orchestrator_agent_id="orch-master",
        recommendation="APPROVE", confidence_score=0.9, contributing_agent_sessions=[sess_id],
        decision_basis_summary="Fraud and Credit checks passed.", expected_version=current_version
    )
    current_version = json.loads(res_json)["result"]["new_version"]
    
    res_json = await server.record_human_review(
        application_id=app_id, reviewer_id="human-underwriter-01",
        final_decision="APPROVED", notes="Looks good.", approved_amount_usd=400000.0,
        expected_version=current_version
    )
    
    await server.run_integrity_check(entity_type="loan", entity_id=app_id)
    
    # Final dump
    await dump_raw_table(store, stream_id)

    daemon_task.cancel()
    await store.close()

if __name__ == "__main__":
    asyncio.run(run_real_concurrency_demo())
