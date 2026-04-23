# CFPB Consumer Complaints: Database & Analytics Pipeline

End-to-end data engineering project that transforms the CFPB's 14M+ consumer complaint records into a normalized PostgreSQL star schema, then runs a 10-query analytics pack covering JOINs, window functions, CASE logic, and correlated subqueries.

## Tech Stack

| Layer | Tool |
|-------|------|
| Processing | Apache Spark (PySpark) |
| Database | PostgreSQL 17 |
| Languages | Python 3, SQL |
| Notebooks | Jupyter (Phases 3–4) |
| ERD | StarUML (.mdj) + HTML viewer |

## Schema Design

Star schema with **1 fact table** and **3 dimension tables**:

```
            ┌────────────┐
            │  products   │
            └─────┬──────┘
                  │
┌───────────┐     │     ┌────────────┐
│  issues    ├────►│◄────┤  companies  │
└───────────┘     │     └────────────┘
                  │
           ┌──────┴──────┐
           │  complaints  │
           │   (fact)     │
           └─────────────┘
```

See `docs/screenshots/er-diagram.png` for the full ERD.

## Project Structure

```
├── scripts/                    # Python pipeline scripts
│   ├── Step0_VerifyCompanyNames.py   # Detect name inconsistencies in raw CSV
│   ├── Step1_SplitData.py            # PySpark: normalize CSV → 4 tables
│   └── Step3_LoadComplaints.py       # Bulk-load complaints into PostgreSQL
│
├── sql/                        # SQL scripts (run in order)
│   ├── Step2_CreateTables.sql        # DDL: create star schema
│   ├── Step3_LoadData.sql            # COPY commands for dimension tables
│   ├── Step4_Verification.sql        # Row counts + data quality checks
│   ├── Step5_AnalyticsPack.sql       # 10 analytical queries
│   ├── CFPB_Phase2_Complete.sql      # Combined DDL + verification script
│   └── CFPB_Phase2_Analytics.sql     # Standalone analytics (polished)
│
├── notebooks/                  # Jupyter notebooks for Phases 3–4
│   ├── Phase3_Local.ipynb
│   └── Phase4Script.ipynb
│
├── diagrams/                   # Data model diagrams
│   ├── CFPB_ERD.mdj                 # StarUML project file
│   └── Step6_ERD.html                # Interactive HTML ERD viewer
│
├── docs/screenshots/           # Verification evidence
│   ├── er-diagram.png
│   ├── total-records.png
│   ├── verification-future-dates.png
│   ├── verification-illogical-dates.png
│   └── verification-orphan-complaints.png
│
├── .env.example                # Template for database credentials
├── .gitignore
└── README.md
```

## Pipeline Steps

1. **Step 0** — Scan the raw CSV for company name inconsistencies (case, punctuation, legal suffixes).
2. **Step 1** — PySpark job splits the flat CSV into 4 normalized CSVs (products, issues, companies, complaints).
3. **Step 2** — Create the star schema in PostgreSQL (DDL + foreign keys + indexes).
4. **Step 3** — Load dimension tables via `COPY`, then bulk-load complaints via Python (`psycopg2`).
5. **Step 4** — Verify row counts and run data quality checks (future dates, illogical dates, orphan records).
6. **Step 5** — Run 10 analytical queries demonstrating JOINs, GROUP BY, HAVING, CASE, window functions, and correlated subqueries.
7. **Step 6** — Generate and review the ERD.

## Setup

### Prerequisites

- Python 3.10+
- PostgreSQL 17
- Apache Spark (for Step 1)
- pip packages: `psycopg2`, `pyspark`

### Configuration

```bash
cp .env.example .env
# Edit .env with your PostgreSQL credentials
```

The pipeline reads database credentials from environment variables. See `.env.example` for the full list.

### Running the Pipeline

```bash
# Step 0 — Verify company names
python scripts/Step0_VerifyCompanyNames.py

# Step 1: Split and normalize
python scripts/Step1_SplitData.py

# Step 2: Create schema (run in pgAdmin or psql)
psql -d cfpb -f sql/Step2_CreateTables.sql

# Step 3: Load dimension tables, then complaints
psql -d cfpb -f sql/Step3_LoadData.sql
python scripts/Step3_LoadComplaints.py

# Step 4: Verify
psql -d cfpb -f sql/Step4_Verification.sql

# Step 5: Analytics
psql -d cfpb -f sql/Step5_AnalyticsPack.sql
```

## Data Source

[CFPB Consumer Complaint Database](https://www.consumerfinance.gov/data-research/consumer-complaints/) : public dataset maintained by the Consumer Financial Protection Bureau.
