# The Ledger — Enterprise Agentic Event Store

A production-grade, dual-stream Event Store and Agentic Orchestration infrastructure designed for high-compliance financial workflows.

## 🚀 Quick Start & Setup

### 1. Environment & Dependencies
```bash
# Install dependencies
pip install -r requirements.txt

# Start PostgreSQL (Required for all phases)
docker run -d -e POSTGRES_PASSWORD=apex -e POSTGRES_DB=apex_ledger -p 5432:5432 postgres:16

# Set environment variables
cp .env.example .env
# Edit .env — add your ANTHROPIC_API_KEY and DATABASE_URL
```

### 2. Generate Initial Data (Phase 0)
Generate companies, documents, and 1,200+ seed events to populate your database:
```bash
python datagen/generate_all.py --db-url postgresql://postgres:postgres@localhost:5432/ledger
```

### 3. Run the Infrastructure Demo (Phase 5)
Follow these guided steps to verify the core architectural pillars:
```powershell
# Set PYTHONPATH for the demo scripts
$env:PYTHONPATH="."

# Step 0: Seed historical compliance data
python scripts/seed_compliance_demo.py

# Step 1: Causal History & Dual-Stream Coordination
python demo_steps/step1_causal_history.py

# Step 2: Concurrency & OCC (Optimistic Concurrency Control)
python demo_steps/step2_concurrency_real.py

# Step 3: Temporal Compliance Query ("Time Travel")
python demo_steps/step3_temporal_query.py

# Step 4: Schema Upcasting & Immutability (v1 -> v2)
python demo_steps/step4_upcasting.py

# Step 5: Gas Town Recovery & Agentic Resumption
python demo_steps/step5_gastown_recovery.py
```

---

## 🛠️ Core Architectural Pillars
- **Dual-Stream Sourcing**: Merges independent `loan-*` and `compliance-*` streams for unified temporal auditing.
- **Absolute Immutability**: Historical records are never modified. Schema evolution is handled via on-the-fly **Upcasting**.
- **Agentic Memory**: Uses the **Gas Town** pattern to reconstruct agent reasoning states and identify "Pending Work" after failures.
- **Transactional Integrity**: Implements the **Outbox Pattern** to ensure external signals stay synced with the internal ledger.

## ✅ Gate Tests by Phase
Verify individual component integrity across all development phases:
```powershell
pytest tests/test_schema_and_generator.py -v  # Phase 0: Schema Validation
pytest tests/test_event_store.py -v           # Phase 1: Persistence
pytest tests/test_domain.py -v               # Phase 2: Aggregates
pytest tests/test_narratives.py -v           # Phase 3: Autonomous Orchestration
pytest tests/test_projections.py -v          # Phase 4: Projections & Upcasting
pytest tests/test_mcp_lifecycle.py -v        # Phase 5: MCP & Audit Resources
```

---
**Note**: This repository contains the reference implementation for Weeks 9-10 of the Apex Financial AI Engineering course. The system provides end-to-end auditability, temporal fidelity, and agentic resilience.
