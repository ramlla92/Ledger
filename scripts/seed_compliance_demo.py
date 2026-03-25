import asyncio
import os
import json
from datetime import datetime, timedelta
from ledger.event_store import EventStore

async def seed():
    db_url = os.getenv("DATABASE_URL")
    store = EventStore(db_url)
    await store.connect()

    # We'll seed for an application ID used in demo Step 3
    app_id = "demo-loan-1625"
    loan_stream = f"loan-{app_id}"
    comp_stream = f"compliance-{app_id}"

    base_time = datetime.now() - timedelta(days=1)

    # 1. Start with some loan events
    loan_events = [
        {
            "event_type": "ApplicationSubmitted",
            "payload": {"application_id": app_id, "applicant_id": "COMP-080", "amount": 50000},
            "recorded_at": (base_time).isoformat()
        },
        {
            "event_type": "CreditAnalysisCompleted",
            "payload": {"application_id": app_id, "score": 750, "recommendation": "APPROVE"},
            "recorded_at": (base_time + timedelta(minutes=10)).isoformat()
        }
    ]
    await store.append(loan_stream, loan_events, expected_version=-1)

    # 2. Seed Compliance events (The primary focus of Step 3)
    comp_events = [
        {
            "event_type": "ComplianceCheckInitiated",
            "payload": {"application_id": app_id, "checks": ["AML", "KYC", "SANCTIONS"]},
            "recorded_at": (base_time + timedelta(minutes=20)).isoformat()
        },
        {
            "event_type": "ComplianceRulePassed",
            "payload": {"application_id": app_id, "rule_id": "AML-001", "rule_name": "Anti-Money Laundering"},
            "recorded_at": (base_time + timedelta(minutes=25)).isoformat()
        },
        {
            "event_type": "ComplianceRulePassed",
            "payload": {"application_id": app_id, "rule_id": "KYC-001", "rule_name": "Know Your Customer"},
            "recorded_at": (base_time + timedelta(minutes=30)).isoformat()
        },
        {
            "event_type": "ComplianceRuleFailed", # This creates interest for 'Time Travel'
            "payload": {"application_id": app_id, "rule_id": "SANCTIONS-001", "rule_name": "Sanctions Review", "reason": "Potential match found"},
            "recorded_at": (base_time + timedelta(minutes=35)).isoformat()
        },
        {
            "event_type": "ComplianceCheckCompleted",
            "payload": {"application_id": app_id, "verdict": "FLAGGED", "failed_rules": ["SANCTIONS-001"]},
            "recorded_at": (base_time + timedelta(minutes=40)).isoformat()
        }
    ]
    await store.append(comp_stream, comp_events, expected_version=-1)

    print(f"✅ Successfully seeded history for [bold]{app_id}[/bold].")
    print(f" - Loan Stream: {loan_stream}")
    print(f" - Compliance Stream: {comp_stream}")
    
    await store.close()

if __name__ == "__main__":
    asyncio.run(seed())
