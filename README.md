# Snowflake ETL Control Plane

Enterprise-style ETL demo project that highlights:
- **Pydantic v2 data contracts** (strict validation + typed parsing)
- **Batch error reporting** (row + field + reason)
- **ETL shape** (extract → validate → transform → load-ready output)
- **Snowflake-ready patterns** (upsert via MERGE SQL generation + tests)

This repo is intentionally structured like a production-style codebase: `src/` layout, config via `.env`, clean git hygiene, and a test suite to lock in behavior.

---

## How it works 

### Step 1 — Extract
We read raw records (CSV now; API/DB later) into dictionaries.

### Step 2 — Validate (Pydantic v2 Contracts)
Each row must match a strict schema (types, required fields, constraints).
If a file has issues, we produce a **batch error report** that identifies:
- row index
- field
- reason/type

### Step 3 — Transform
Normalize values (trim strings, standardize IDs, dedupe keys, derive columns).  
(Currently minimal; designed for expansion.)


### Step 4 — Load (Snowflake-ready)
Convert validated rows into load-ready structures and (later) apply:
- COPY INTO staging table
- MERGE into final table (idempotent upsert)

---

## Quickstart (Local Demo)

### 1) Create venv + install
```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -U pip
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"


python -m sfetl.cli validate data/sample_bond_prices.csv

```powershell
.\.venv\Scripts\python.exe -m sfetl.cli validate data\sample_bond_prices.csv


