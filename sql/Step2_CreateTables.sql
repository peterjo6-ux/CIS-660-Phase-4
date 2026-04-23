-- =============================================================================
-- STEP 2 — Create the 4 tables in PostgreSQL
-- Run this in pgAdmin → Query Tool BEFORE loading any data
-- =============================================================================

DROP TABLE IF EXISTS complaints  CASCADE;
DROP TABLE IF EXISTS products    CASCADE;
DROP TABLE IF EXISTS issues      CASCADE;
DROP TABLE IF EXISTS companies   CASCADE;

-- DIMENSION: products
CREATE TABLE products (
    product_id       SERIAL       PRIMARY KEY,
    product_name     VARCHAR(100),
    sub_product_name VARCHAR(150)
);

-- DIMENSION: issues
CREATE TABLE issues (
    issue_id       SERIAL       PRIMARY KEY,
    issue_name     VARCHAR(200),
    sub_issue_name VARCHAR(250)
);

-- DIMENSION: companies
CREATE TABLE companies (
    company_id   SERIAL       PRIMARY KEY,
    company_name VARCHAR(200) NOT NULL UNIQUE
);

-- FACT: complaints
CREATE TABLE complaints (
    complaint_id         INT          PRIMARY KEY,
    product_id           INT,
    issue_id             INT,
    company_id           INT,
    date_received        DATE,
    date_sent_to_company DATE,
    state                VARCHAR(5),
    zip_code             VARCHAR(10),
    submitted_via        VARCHAR(30),
    consumer_consent     VARCHAR(50),
    timely_response      BOOLEAN,
    consumer_disputed    BOOLEAN,
    tags                 VARCHAR(50),
    company_response     VARCHAR(100),
    complaint_narrative  TEXT,

    CONSTRAINT fk_product  FOREIGN KEY (product_id)  REFERENCES products  (product_id),
    CONSTRAINT fk_issue    FOREIGN KEY (issue_id)    REFERENCES issues    (issue_id),
    CONSTRAINT fk_company  FOREIGN KEY (company_id)  REFERENCES companies (company_id)
);

-- Indexes for faster joins and analytics
CREATE INDEX idx_complaints_product  ON complaints (product_id);
CREATE INDEX idx_complaints_issue    ON complaints (issue_id);
CREATE INDEX idx_complaints_company  ON complaints (company_id);
CREATE INDEX idx_complaints_date     ON complaints (date_received);
CREATE INDEX idx_complaints_state    ON complaints (state);
