-- =============================================================================
-- CFPB Consumer Complaints — Phase 2: Complete SQL Script
-- Database: cfpb  |  Engine: PostgreSQL 17
-- Star Schema: 1 Fact Table (complaints) + 3 Dimension Tables
-- =============================================================================


-- =============================================================================
-- SECTION 1: SCHEMA CREATION (DDL)
-- =============================================================================

-- DIMENSION: products
-- Stores unique product and sub-product combinations from CFPB taxonomy
CREATE TABLE products (
    product_id       SERIAL       PRIMARY KEY,
    product_name     VARCHAR(100),
    sub_product_name VARCHAR(150)
);

-- DIMENSION: issues
-- Stores unique issue and sub-issue combinations reported by consumers
CREATE TABLE issues (
    issue_id       SERIAL       PRIMARY KEY,
    issue_name     VARCHAR(200),
    sub_issue_name VARCHAR(250)
);

-- DIMENSION: companies
-- Stores unique company names (standardized: uppercased + trimmed)
CREATE TABLE companies (
    company_id   SERIAL       PRIMARY KEY,
    company_name VARCHAR(200) NOT NULL UNIQUE
);

-- FACT: complaints
-- Each row = one consumer complaint filed with the CFPB
CREATE TABLE complaints (
    complaint_id         INT          PRIMARY KEY,
    product_id           INT,
    issue_id             INT,
    company_id           INT,
    date_received        DATE,
    date_sent_to_company DATE,
    state                VARCHAR(50),
    zip_code             VARCHAR(20),
    submitted_via        TEXT,
    consumer_consent     TEXT,
    timely_response      BOOLEAN,
    consumer_disputed    BOOLEAN,
    tags                 TEXT,
    company_response     TEXT,
    complaint_narrative  TEXT
);

-- Foreign key constraints (linking fact to dimensions)
ALTER TABLE complaints ADD CONSTRAINT fk_product
    FOREIGN KEY (product_id) REFERENCES products (product_id);

ALTER TABLE complaints ADD CONSTRAINT fk_issue
    FOREIGN KEY (issue_id) REFERENCES issues (issue_id);

ALTER TABLE complaints ADD CONSTRAINT fk_company
    FOREIGN KEY (company_id) REFERENCES companies (company_id);

-- Indexes for faster joins and analytics
CREATE INDEX idx_complaints_product  ON complaints (product_id);
CREATE INDEX idx_complaints_issue    ON complaints (issue_id);
CREATE INDEX idx_complaints_company  ON complaints (company_id);
CREATE INDEX idx_complaints_date     ON complaints (date_received);
CREATE INDEX idx_complaints_state    ON complaints (state);


-- =============================================================================
-- SECTION 2: DATA LOADING (via COPY : dimensions first, then fact)
-- =============================================================================
-- NOTE: Dimension CSVs were loaded via PostgreSQL COPY.
--       The complaints fact table was loaded via a Python script
--
-- COPY products(product_id, product_name, sub_product_name)
--   FROM 'C:\csvdata\products.csv'
--   WITH (FORMAT csv, HEADER true, DELIMITER ',');
--
-- COPY issues(issue_id, issue_name, sub_issue_name)
--   FROM 'C:\csvdata\issues.csv'
--   WITH (FORMAT csv, HEADER true, DELIMITER ',');
--
-- COPY companies(company_id, company_name)
--   FROM 'C:\csvdata\companies.csv'
--   WITH (FORMAT csv, HEADER true, DELIMITER ',');
--
-- Complaints: loaded 14,142,060 rows


-- =============================================================================
-- SECTION 3: VERIFICATION & DATA QUALITY AUDIT
-- =============================================================================

-- Row counts for every table (single result set)
SELECT 'products'   AS table_name, COUNT(*) AS row_count FROM products
UNION ALL
SELECT 'issues',     COUNT(*) FROM issues
UNION ALL
SELECT 'companies',  COUNT(*) FROM companies
UNION ALL
SELECT 'complaints', COUNT(*) FROM complaints
ORDER BY table_name;


-- Data Quality Audit : finding "impossible" or suspicious data

-- Check 1: Complaints with a date_received in the future
SELECT COUNT(*) AS future_dates
FROM   complaints
WHERE  date_received > CURRENT_DATE;

-- Check 2: date_sent_to_company BEFORE date_received (illogical)
SELECT COUNT(*) AS illogical_dates
FROM   complaints
WHERE  date_sent_to_company < date_received;


-- Check 3: Orphan check : complaints referencing a product_id not in products
SELECT COUNT(*) AS orphan_complaints
FROM   complaints c
LEFT JOIN products p ON c.product_id = p.product_id
WHERE  p.product_id IS NULL;