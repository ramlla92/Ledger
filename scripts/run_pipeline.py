"""
scripts/run_pipeline.py — Process one application through all agents using LLM orchestration.
Usage: python scripts/run_pipeline.py --company COMP-001 --application APEX-2026-001
"""
import argparse
import asyncio
import os
import json
import logging
import requests
import sys
from datetime import datetime
from pathlib import Path

# Add root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from ledger.mcp import server
from ledger.event_store import EventStore, UpcasterRegistry
from ledger.upcasters import register_upcasters

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Pipeline")

class OpenRouterClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.url = "https://openrouter.ai/api/v1/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "HTTP-Referer": "https://github.com/10Academy/Ledger",
            "X-Title": "Ledger Pipeline",
            "Content-Type": "application/json"
        }

    def chat(self, prompt: str, model: str = "google/gemini-2.0-flash-001") -> str:
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "response_format": {"type": "json_object"}
        }
        res = requests.post(self.url, headers=self.headers, json=payload, timeout=30)
        res.raise_for_status()
        return res.json()["choices"][0]["message"]["content"]

async def load_documents(company_id: str) -> str:
    doc_path = Path(f"documents/{company_id}/financial_summary.csv")
    if not doc_path.exists():
        raise FileNotFoundError(f"Documents for {company_id} not found at {doc_path}")
    return doc_path.read_text()

async def run_pipeline(company_id: str, application_id: str):
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key or "YOUR_KEY" in api_key:
        logger.error("Missing OPENROUTER_API_KEY in .env")
        return

    client = OpenRouterClient(api_key)
    
    # 0. Load Data
    logger.info(f"--- Loading data for {company_id} ---")
    doc_content = await load_documents(company_id)
    
    # Initialize Ledger Server Context (uses DB from .env)
    # We need to ensure the DB-backed store is used
    registry = UpcasterRegistry()
    register_upcasters(registry)
    db_url = os.getenv("DATABASE_URL")
    store = EventStore(db_url, upcaster_registry=registry)
    await store.connect()
    
    # Patch the server context to use our DB store
    server._ctx = server.build_ledger_context(store=store)
    # Start projection daemon in background
    daemon_task = asyncio.create_task(server._ctx.daemon.run_forever())
    
    try:
        # 1. Extraction & Submission
        logger.info("Step 1: Extracting Application Details...")
        extract_prompt = f"""
        Analyze this financial CSV for company {company_id}:
        {doc_content}
        
        Extract the following as JSON:
        - loan_amount: requested amount (guess based on 5% of revenue if not specified)
        - purpose: derived from industry or logic
        - applicant_id: use format 'cust-{company_id}'
        """
        raw_extraction = client.chat(extract_prompt)
        ext = json.loads(raw_extraction)
        
        logger.info(f"Submitting Application: {application_id} for {ext['applicant_id']}")
        res_json = await server.submit_application(
            application_id=application_id,
            applicant_id=ext["applicant_id"],
            loan_amount=float(ext["loan_amount"]),
            purpose=ext["purpose"]
        )
        res = json.loads(res_json)
        if not res["ok"]: raise Exception(f"Submit Failed: {res['error']}")
        version = 0

        # 2. Agent Sessions
        logger.info("Step 2: Starting Agent Sessions...")
        agent_id = "ai-analyst-gen2"
        sess_id = f"sess-{datetime.now().strftime('%M%S')}"
        await server.start_agent_session(agent_id=agent_id, session_id=sess_id, model_version="gemini-2.0-flash")

        # 3. Fraud Screening
        logger.info("Step 3: Fraud Screening...")
        fraud_prompt = f"""
        As a Fraud Agent, analyze this data: {doc_content}
        Provide JSON:
        - fraud_score: 0.0 to 1.0 (float)
        - anomaly_flags: list of strings
        """
        fraud_raw = client.chat(fraud_prompt)
        fraud_data = json.loads(fraud_raw)
        
        res_json = await server.record_fraud_screening(
            application_id=application_id, agent_id=agent_id, session_id=sess_id,
            fraud_score=fraud_data["fraud_score"], 
            anomaly_flags=fraud_data["anomaly_flags"],
            screening_model_version="gemini-2.0-flash",
            expected_version=version
        )
        res = json.loads(res_json)
        if not res["ok"]: raise Exception(f"Fraud Failed: {res['error']}")
        version += 1

        # 4. Credit Analysis
        logger.info("Step 4: Credit Analysis...")
        credit_prompt = f"""
        As a Credit Agent, analyze this data: {doc_content}
        Provide JSON:
        - risktier: A to F (string)
        - recommended_limit_usd: number
        - confidence_score: 0.0 to 1.0
        - rationale: string
        """
        credit_raw = client.chat(credit_prompt)
        credit_data = json.loads(credit_raw)
        
        res_json = await server.record_credit_analysis(
            application_id=application_id, agent_id=agent_id, session_id=sess_id,
            expected_version=version,
            model_version="gemini-2.0-flash",
            confidence_score=credit_data["confidence_score"],
            risktier=credit_data["risktier"],
            recommended_limit_usd=credit_data["recommended_limit_usd"],
            analysis_duration_ms=450,
            input_data_hash="sha256:dynamic"
        )
        res = json.loads(res_json)
        if not res["ok"]: raise Exception(f"Credit Failed: {res['error']}")
        version += 1

        # 5. Final Orchestration
        logger.info("Step 5: Orchestrating Final Decision...")
        decision_prompt = f"""
        Final review for {application_id}.
        Fraud Score: {fraud_data['fraud_score']}
        Risk Tier: {credit_data['risktier']}
        
        Return JSON:
        - recommendation: "APPROVE", "DECLINE", or "REFER"
        - confidence: 0.0 to 1.0
        - summary: brief explanation
        """
        dec_raw = client.chat(decision_prompt)
        dec_data = json.loads(dec_raw)
        
        res_json = await server.generate_decision(
            application_id=application_id, orchestrator_agent_id="orch-master",
            recommendation=dec_data["recommendation"],
            confidence_score=dec_data["confidence"],
            contributing_agent_sessions=[f"agent-{agent_id}-{sess_id}"],
            decision_basis_summary=dec_data["summary"],
            expected_version=version
        )
        res = json.loads(res_json)
        if not res["ok"]: raise Exception(f"Decision Failed: {res['error']}")
        version += 1

        # 6. Human Review (Simulated)
        logger.info("Step 6: Recording Human Review...")
        recommendation = dec_data.get("recommendation", "REFER")
        final_decision = "APPROVED" if recommendation in ["APPROVE", "REFER"] else "DECLINED"
        
        res_json = await server.record_human_review(
            application_id=application_id, 
            reviewer_id="human-underwriter-01",
            final_decision=final_decision,
            notes=f"Simulated review of {recommendation} recommendation.",
            approved_amount_usd=float(ext["loan_amount"]) if final_decision == "APPROVED" else 0.0,
            expected_version=version
        )
        res = json.loads(res_json)
        if not res["ok"]: raise Exception(f"Human Review Failed: {res['error']}")
        # Increment version by 2 because tool appends 2 events
        version += 2

        # 7. Integrity Check
        logger.info("Step 7: Executing Causal Integrity Audit...")
        await server.run_integrity_check(entity_type="loan", entity_id=application_id)
        
        # 8. Verification Output
        logger.info("--- Pipeline Complete. Fetching Governance Trace ---")
        await asyncio.sleep(1) # Wait for projections
        
        summary_json = await server.get_application(id=application_id)
        summary = json.loads(summary_json)["result"]
        
        logger.info(f"FINAL STATE: {summary.get('state', 'UNKNOWN')}")
        logger.info(f"AUDIT LOG: {application_id}")
        
    finally:
        daemon_task.cancel()
        if store: await store.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--company", default="COMP-001")
    parser.add_argument("--application", default=f"loan-{datetime.now().strftime('%Y%j%H%M')}")
    args = parser.parse_args()
    
    asyncio.run(run_pipeline(args.company, args.application))
