-- =============================================================================
-- STEP 3 — Bulk-load the 4 CSVs into PostgreSQL
-- Run this in pgAdmin → Query Tool AFTER Step 2
--
-- File paths point to C:\csvdata\ (copied there to avoid special-char issues)
-- =============================================================================

-- Load dimensions FIRST (order matters because of foreign keys)

COPY products(product_id, product_name, sub_product_name)
  FROM 'C:\csvdata\products.csv'
  WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY issues(issue_id, issue_name, sub_issue_name)
  FROM 'C:\csvdata\issues.csv'
  WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY companies(company_id, company_name)
  FROM 'C:\csvdata\companies.csv'
  WITH (FORMAT csv, HEADER true, DELIMITER ',');

-- Load fact table LAST
COPY complaints(complaint_id, product_id, issue_id, company_id,
                date_received, date_sent_to_company, state, zip_code,
                submitted_via, consumer_consent, timely_response,
                consumer_disputed, tags, company_response,
                complaint_narrative)
  FROM 'C:\csvdata\complaints.csv'
  WITH (FORMAT csv, HEADER true, DELIMITER ',');
