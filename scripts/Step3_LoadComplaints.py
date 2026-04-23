"""
Step 3b — Load complaints.csv into PostgreSQL using Python
Handles multiline/unquoted complaint_narrative fields
"""
import csv
import os
import psycopg2

DB_CONFIG = {
    "dbname":   os.environ.get("CFPB_DB_NAME", "cfpb"),
    "user":     os.environ.get("CFPB_DB_USER", "postgres"),
    "password": os.environ["CFPB_DB_PASSWORD"],
    "host":     os.environ.get("CFPB_DB_HOST", "127.0.0.1"),
    "port":     int(os.environ.get("CFPB_DB_PORT", 5432)),
}

CSV_PATH = os.environ.get("CFPB_CSV_PATH", r"C:\csvdata\complaints.csv")
BATCH_SIZE = 10000

INSERT_SQL = """
INSERT INTO complaints (
    complaint_id, product_id, issue_id, company_id,
    date_received, date_sent_to_company, state, zip_code,
    submitted_via, consumer_consent, timely_response,
    consumer_disputed, tags, company_response, complaint_narrative
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

def to_none(val):
    return None if val == "" else val

def to_bool(val):
    if val == "":
        return None
    return val.lower() == "true"

def to_int(val):
    if val == "":
        return None
    return int(val)

def is_valid_id(val):
    """Check if a value looks like a valid integer complaint_id."""
    if val == "":
        return False
    try:
        int(val)
        return True
    except ValueError:
        return False

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

loaded = 0
skipped = 0
batch = []

with open(CSV_PATH, "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    header = next(reader)  # skip header

    for row in reader:
        # If first field isn't a valid integer, it's a broken narrative row — skip
        if not row or not is_valid_id(row[0]):
            skipped += 1
            continue

        # If row has more than 15 fields, the narrative had unquoted commas
        # Rejoin everything from index 14 onwards as the narrative
        if len(row) > 15:
            narrative = ",".join(row[14:])
            row = row[:14] + [narrative]
        elif len(row) < 15:
            skipped += 1
            continue

        try:
            record = (
                to_int(row[0]),      # complaint_id
                to_int(row[1]),      # product_id
                to_int(row[2]),      # issue_id
                to_int(row[3]),      # company_id
                to_none(row[4]),     # date_received
                to_none(row[5]),     # date_sent_to_company
                to_none(row[6]),     # state
                to_none(row[7]),     # zip_code
                to_none(row[8]),     # submitted_via
                to_none(row[9]),     # consumer_consent
                to_bool(row[10]),    # timely_response
                to_bool(row[11]),    # consumer_disputed
                to_none(row[12]),    # tags
                to_none(row[13]),    # company_response
                to_none(row[14]),    # complaint_narrative
            )
            batch.append(record)
        except (ValueError, IndexError):
            skipped += 1
            continue

        if len(batch) >= BATCH_SIZE:
            cur.executemany(INSERT_SQL, batch)
            conn.commit()
            loaded += len(batch)
            print(f"  Loaded {loaded:,} rows ...", flush=True)
            batch = []

    # final batch
    if batch:
        cur.executemany(INSERT_SQL, batch)
        conn.commit()
        loaded += len(batch)

cur.close()
conn.close()
print(f"\nDONE — {loaded:,} rows loaded, {skipped:,} skipped (malformed).")
