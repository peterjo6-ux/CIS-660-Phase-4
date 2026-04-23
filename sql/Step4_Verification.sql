-- =============================================================================
-- STEP 4 — Verification & Data Quality Audit
-- Run this AFTER Step 3 to confirm data loaded correctly
-- SCREENSHOT THE ROW COUNT RESULTS for your submission
-- =============================================================================

-- 4a. Row counts for every table (single output)
SELECT 'products'   AS table_name, COUNT(*) AS row_count FROM products
UNION ALL
SELECT 'issues',     COUNT(*) FROM issues
UNION ALL
SELECT 'companies',  COUNT(*) FROM companies
UNION ALL
SELECT 'complaints', COUNT(*) FROM complaints
ORDER BY table_name;


-- 4b. Data Quality Audit — find "impossible" or suspicious data

-- Check 1: Complaints with a date_received in the future
SELECT complaint_id, date_received
FROM   complaints
WHERE  date_received > CURRENT_DATE;

-- Check 2: NULL or blank product names (should never be empty)
SELECT product_id, product_name
FROM   products
WHERE  product_name IS NULL OR TRIM(product_name) = '';



-- Check 5: Orphan check — complaints referencing a product_id that doesn't exist
SELECT c.complaint_id, c.product_id
FROM   complaints c
LEFT JOIN products p ON c.product_id = p.product_id
WHERE  p.product_id IS NULL;
