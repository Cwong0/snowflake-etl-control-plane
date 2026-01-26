# Snowflake ETL Control Plane

Enterprise-style ETL project that demonstrates:
- **Pydantic v2 data contracts** (strict validation + batch error reporting)
- **ETL shape** (extract → validate → transform → load-ready output)
- Snowflake-ready patterns (MERGE/upsert planned)

This repo is intentionally designed to look like a real “production” codebase: `src/` layout, config via `.env`, clean git hygiene, and test-ready structure.

---

## How it works (high level)

### Step 1 — Extract
We read raw records (CSV now; API/DB later) into dictionaries.

### Step 2 — Validate (Pydantic v2 Contracts)
Each row must match a strict schema (types, required fields, constraints).
If a file has issues, we produce a **batch error report** that identifies:
- row index
- field
- reason

### Step 3 — Transform
Normalize values (trim strings, standardize IDs, dedupe keys, derive columns).

### Step 4 — Load (Snowflake-ready)
Convert validated rows into load-ready structures and (later) apply:
- COPY INTO staging table
- MERGE into final table (idempotent upsert)

---

## Quickstart (Local Demo)

### 1 Create venv + install
```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -U pip
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
