"""
STEP 0 — Verify company name inconsistencies in the raw CSV
Run:  python Step0_VerifyCompanyNames.py

Checks for:
  1. Case variations (e.g., "ABC Bank" vs "ABC BANK")
  2. Punctuation variations (e.g., "V.I.P. MORTGAGE, INC." vs "VIP Mortgage Inc.")
  3. Legal suffix variations (e.g., "Flagstar Bank, N.A." vs "Flagstar Bank, National Association")

Estimated runtime: ~5-8 minutes for the full 13M+ row dataset.
No data is modified or exported.
"""

import re
from collections import defaultdict
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── Point this to your raw CSV ──────────────────────────────────────────────
RAW_CSV = r"C:\Users\H¨P\Desktop\Applications\Aptiv\complaints.csv"

# ── Spark Session ────────────────────────────────────────────────────────────
spark = (SparkSession.builder
         .appName("CFPB_VerifyCompanyNames")
         .master("local[*]")
         .config("spark.driver.memory", "8g")
         .getOrCreate())

print("Reading raw CSV (with multiLine to handle embedded newlines) …")
raw = (spark.read
       .option("header", "true")
       .option("multiLine", "true")       # handles newlines inside quoted fields
       .option("escape", '"')             # handles escaped quotes inside fields
       .option("inferSchema", "false")
       .csv(RAW_CSV))

total_rows = raw.count()
print(f"Total rows in dataset: {total_rows:,}")

# ── Count complaints per company name in ONE pass ────────────────────────────
print("Counting complaints per company name (single pass) …")

# Filter out NULL and any "company name" longer than 300 chars
# (real companies are short; long strings = misread narrative text)
company_counts = (raw
                  .filter(F.col("Company").isNotNull())
                  .filter(F.length(F.col("Company")) <= 300)
                  .groupBy("Company")
                  .count()
                  .collect())

# Build a dict: company_name -> complaint_count
name_to_count = {row["Company"]: row["count"] for row in company_counts if row["Company"]}
company_names = list(name_to_count.keys())
print(f"Unique company names (as-is): {len(company_names):,}")


# ─── CHECK 1: Case / whitespace variations ────────────────────────────────
print("\n" + "=" * 70)
print("CHECK 1: CASE / WHITESPACE VARIATIONS")
print("Same company stored with different capitalization or extra spaces")
print("=" * 70)

case_groups = defaultdict(list)
for name in company_names:
    case_groups[name.strip().upper()].append(name)

case_dupes = {k: v for k, v in case_groups.items() if len(v) > 1}
print(f"Found {len(case_dupes)} companies with case/spacing variations:\n")

for key, variants in sorted(case_dupes.items()):
    print(f"  Normalized: '{key}'")
    for v in sorted(variants):
        print(f"    -> '{v}'  ({name_to_count[v]:,} complaints)")
    print()


# ─── CHECK 2: Punctuation variations ──────────────────────────────────────
print("=" * 70)
print("CHECK 2: PUNCTUATION VARIATIONS")
print("Same company but different punctuation (commas, periods, etc.)")
print("=" * 70)

punct_groups = defaultdict(list)
for name in company_names:
    key = re.sub(r'[^A-Za-z0-9 ]', '', name.upper()).strip()
    key = re.sub(r'\s+', ' ', key)
    punct_groups[key].append(name)

punct_dupes = {k: v for k, v in punct_groups.items() if len(v) > 1}
print(f"Found {len(punct_dupes)} groups with punctuation differences:\n")

shown = 0
for key, variants in sorted(punct_dupes.items()):
    print(f"  Stripped: '{key}'")
    for v in sorted(variants):
        print(f"    -> '{v}'  ({name_to_count[v]:,} complaints)")
    print()
    shown += 1
    if shown >= 25:
        remaining = len(punct_dupes) - 25
        if remaining > 0:
            print(f"  ... and {remaining} more groups\n")
        break


# ─── CHECK 3: Legal suffix variations ─────────────────────────────────────
print("=" * 70)
print("CHECK 3: LEGAL SUFFIX VARIATIONS")
print("Same entity but different legal suffix (LLC vs Inc vs Corp, etc.)")
print("=" * 70)

suffix_pattern = r'\b(LLC|L\.?L\.?C\.?|INC\.?|INCORPORATED|CORP\.?|CORPORATION|CO\.?|COMPANY|LTD\.?|LIMITED|N\.?A\.?|NATIONAL ASSOCIATION)\b'
base_groups = defaultdict(list)
for name in company_names:
    base = re.sub(suffix_pattern, '', name.upper())
    base = re.sub(r'[^A-Z0-9 ]', '', base).strip()
    base = re.sub(r'\s+', ' ', base)
    if base:
        base_groups[base].append(name)

suffix_dupes = {k: v for k, v in base_groups.items() if len(v) > 1}
print(f"Found {len(suffix_dupes)} groups with different legal suffixes:\n")

shown = 0
for key, variants in sorted(suffix_dupes.items()):
    print(f"  Base name: '{key}'")
    for v in sorted(variants):
        print(f"    -> '{v}'  ({name_to_count[v]:,} complaints)")
    print()
    shown += 1
    if shown >= 25:
        remaining = len(suffix_dupes) - 25
        if remaining > 0:
            print(f"  ... and {remaining} more groups\n")
        break


# ─── SUMMARY ──────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
print(f"Total rows:                          {total_rows:,}")
print(f"Unique company names (raw):          {len(company_names):,}")
print(f"Case/whitespace duplicates:          {len(case_dupes)} groups")
print(f"Punctuation variation groups:        {len(punct_dupes)} groups")
print(f"Legal suffix variation groups:       {len(suffix_dupes)} groups")
print(f"\nAfter uppercasing + trimming, unique companies would be: "
      f"{len(case_groups):,}")
print(f"That merges {len(company_names) - len(case_groups):,} duplicate entries")
print("=" * 70)

spark.stop()
