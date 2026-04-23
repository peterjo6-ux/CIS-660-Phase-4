"""
STEP 1 — Split raw CFPB CSV into 4 normalized tables
"""

import os, shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

RAW_CSV    = r"C:\Users\H¨P\Desktop\complaints.csv"
OUTPUT_DIR = r"C:\Users\H¨P\Desktop\phase2_csv_output"

spark = (SparkSession.builder
         .appName("CFPB_Phase2_Split")
         .master("local[*]")
         .config("spark.driver.memory", "8g")
         .getOrCreate())

print("Reading raw CSV (with multiLine to handle embedded newlines) …")
raw = (spark.read
       .option("header", "true")
       .option("multiLine", "true")
       .option("escape", '"')
       .option("inferSchema", "true")
       .csv(RAW_CSV))
total = raw.count()
print(f"Total rows: {total:,}")

raw = (raw
       .withColumnRenamed("Complaint ID",                "complaint_id")
       .withColumnRenamed("Date received",               "date_received")
       .withColumnRenamed("Product",                     "product_name")
       .withColumnRenamed("Sub-product",                 "sub_product_name")
       .withColumnRenamed("Issue",                       "issue_name")
       .withColumnRenamed("Sub-issue",                   "sub_issue_name")
       .withColumnRenamed("Company",                     "company_name")
       .withColumnRenamed("Company public response",     "company_response")
       .withColumnRenamed("State",                       "state")
       .withColumnRenamed("ZIP code",                    "zip_code")
       .withColumnRenamed("Submitted via",               "submitted_via")
       .withColumnRenamed("Date sent to company",        "date_sent_to_company")
       .withColumnRenamed("Consumer consent provided?",  "consumer_consent")
       .withColumnRenamed("Timely response?",            "timely_response")
       .withColumnRenamed("Consumer disputed?",          "consumer_disputed")
       .withColumnRenamed("Tags",                        "tags")
       .withColumnRenamed("Consumer complaint narrative","complaint_narrative")
       )

# ── DIMENSION 1: products ────────────────────────────────────────────────────
products = (raw
            .select("product_name", "sub_product_name")
            .dropDuplicates()
            .withColumn("product_id",
                        F.row_number().over(
                            Window.orderBy("product_name", "sub_product_name")))
            )
print(f"products dimension: {products.count()} rows")

# ── DIMENSION 2: issues ──────────────────────────────────────────────────────
issues = (raw
          .select("issue_name", "sub_issue_name")
          .dropDuplicates()
          .withColumn("issue_id",
                      F.row_number().over(
                          Window.orderBy("issue_name", "sub_issue_name")))
          )
print(f"issues dimension: {issues.count()} rows")

# ── DIMENSION 3: companies ───────────────────────────────────────────────────
# Standardize: uppercase + trim to merge case variations
raw = raw.withColumn("company_name_clean",
                     F.upper(F.trim(F.col("company_name"))))

companies = (raw
             .select("company_name_clean")
             .dropDuplicates()
             .withColumnRenamed("company_name_clean", "company_name")
             .withColumn("company_id",
                         F.row_number().over(
                             Window.orderBy("company_name")))
             )
print(f"companies dimension: {companies.count()} rows")

# ── FACT TABLE: complaints ───────────────────────────────────────────────────
complaints = (raw
              .join(products,  ["product_name", "sub_product_name"], "left")
              .join(issues,    ["issue_name",   "sub_issue_name"],   "left")
              .join(companies, raw["company_name_clean"] == companies["company_name"], "left")
              .select(
                  F.col("complaint_id").cast("int"),
                  "product_id",
                  "issue_id",
                  "company_id",
                  F.to_date("date_received", "MM/dd/yyyy").alias("date_received"),
                  F.to_date("date_sent_to_company", "MM/dd/yyyy").alias("date_sent_to_company"),
                  "state",
                  "zip_code",
                  "submitted_via",
                  "consumer_consent",
                  F.when(F.col("timely_response") == "Yes", True)
                   .otherwise(False).alias("timely_response"),
                  F.when(F.col("consumer_disputed") == "Yes", True)
                   .otherwise(False).alias("consumer_disputed"),
                  "tags",
                  "company_response",
                  "complaint_narrative"
              ))
print(f"complaints fact: {complaints.count():,} rows")

# ── Export to CSV ─────────────────────────────────────────────────────────────
os.makedirs(OUTPUT_DIR, exist_ok=True)

def save_single_csv(df, name):
    tmp = os.path.join(OUTPUT_DIR, f"_{name}_tmp")
    df.coalesce(1).write.csv(tmp, header=True, mode="overwrite")
    for f in os.listdir(tmp):
        if f.startswith("part-") and f.endswith(".csv"):
            os.rename(os.path.join(tmp, f),
                      os.path.join(OUTPUT_DIR, f"{name}.csv"))
    shutil.rmtree(tmp, ignore_errors=True)
    print(f"  saved {name}.csv")

print("\nExporting CSVs …")
save_single_csv(products.select("product_id", "product_name", "sub_product_name"),
                "products")
save_single_csv(issues.select("issue_id", "issue_name", "sub_issue_name"),
                "issues")
save_single_csv(companies.select("company_id", "company_name"),
                "companies")
save_single_csv(complaints, "complaints")

print("\n" + "=" * 60)
print("DONE — 4 CSVs saved to:", OUTPUT_DIR)
print("=" * 60)

spark.stop()