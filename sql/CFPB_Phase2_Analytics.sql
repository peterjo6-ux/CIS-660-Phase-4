-- =============================================================================
-- CFPB Consumer Complaints — Phase 2: Analytical SQL Queries
-- Database: cfpb  |  Engine: PostgreSQL 17
-- 10 queries demonstrating: JOINs, GROUP BY, HAVING, subqueries, CASE,
-- window functions (RANK, SUM OVER), correlated subqueries
-- =============================================================================


-- QUERY 1 : Multi-table JOIN + GROUP BY + ORDER BY
-- Top 10 companies by total complaint volume
SELECT co.company_name,
       COUNT(c.complaint_id) AS total_complaints
FROM   complaints c
JOIN   companies co ON c.company_id = co.company_id
GROUP  BY co.company_name
ORDER  BY total_complaints DESC
LIMIT  10;


-- QUERY 2 : HAVING clause
-- Products that received more than 500 complaints
SELECT p.product_name,
       COUNT(c.complaint_id) AS complaint_count
FROM   complaints c
JOIN   products p ON c.product_id = p.product_id
GROUP  BY p.product_name
HAVING COUNT(c.complaint_id) > 500
ORDER  BY complaint_count DESC;


-- QUERY 3 : Subquery
-- Companies whose complaint count exceeds the overall average
SELECT co.company_name,
       COUNT(c.complaint_id) AS complaint_count
FROM   complaints c
JOIN   companies co ON c.company_id = co.company_id
GROUP  BY co.company_name
HAVING COUNT(c.complaint_id) > (
           SELECT AVG(cnt)
           FROM  (SELECT COUNT(*) AS cnt
                  FROM   complaints
                  GROUP  BY company_id) sub
       )
ORDER  BY complaint_count DESC;


-- QUERY 4 : CASE logic
-- Classify companies into complaint volume tiers
SELECT co.company_name,
       COUNT(c.complaint_id) AS total,
       CASE
           WHEN COUNT(c.complaint_id) >= 10000 THEN 'Critical'
           WHEN COUNT(c.complaint_id) >= 1000  THEN 'High'
           WHEN COUNT(c.complaint_id) >= 100   THEN 'Medium'
           ELSE 'Low'
       END AS volume_tier
FROM   complaints c
JOIN   companies co ON c.company_id = co.company_id
GROUP  BY co.company_name
ORDER  BY total DESC
LIMIT  20;


-- QUERY 5 : Ordering / Ranking (Window Function)
-- Top 3 issues within each product category
SELECT *
FROM (
    SELECT p.product_name,
           i.issue_name,
           COUNT(c.complaint_id) AS issue_count,
           RANK() OVER (PARTITION BY p.product_name
                        ORDER BY COUNT(c.complaint_id) DESC) AS rnk
    FROM   complaints c
    JOIN   products p ON c.product_id = p.product_id
    JOIN   issues   i ON c.issue_id   = i.issue_id
    GROUP  BY p.product_name, i.issue_name
) ranked
WHERE rnk <= 3
ORDER BY product_name, rnk;


-- QUERY 6 : CASE inside aggregate
-- How timely each submission channel is at getting responses.
SELECT c.submitted_via,
       COUNT(*)                                                    AS total_complaints,
       SUM(CASE WHEN c.timely_response = TRUE THEN 1 ELSE 0 END)  AS timely_count,
       ROUND(
           100.0 * SUM(CASE WHEN c.timely_response = TRUE THEN 1 ELSE 0 END)
                 / NULLIF(COUNT(*), 0), 2
       ) AS timely_pct
FROM   complaints c
GROUP  BY c.submitted_via
ORDER  BY timely_pct ASC;


-- QUERY 7 : Year-over-Year trend with window function
-- Monthly complaint volume with running year-to-date total
SELECT month_start,
       monthly_count,
       SUM(monthly_count) OVER (PARTITION BY EXTRACT(YEAR FROM month_start)
                                ORDER BY month_start) AS ytd_running_total
FROM (
    SELECT DATE_TRUNC('month', date_received) AS month_start,
           COUNT(*)                            AS monthly_count
    FROM   complaints
    GROUP  BY DATE_TRUNC('month', date_received)
) monthly
ORDER BY month_start;


-- QUERY 8 : GROUP BY + HAVING with compound condition
-- States with > 1000 complaints where dispute rate exceeds 20%
SELECT c.state,
       COUNT(c.complaint_id) AS total_complaints,
       SUM(CASE WHEN c.consumer_disputed = TRUE THEN 1 ELSE 0 END) AS disputed,
       ROUND(
           100.0 * SUM(CASE WHEN c.consumer_disputed = TRUE THEN 1 ELSE 0 END)
                 / NULLIF(COUNT(*), 0), 2
       ) AS dispute_rate_pct
FROM   complaints c
GROUP  BY c.state
HAVING COUNT(c.complaint_id) > 1000
   AND 100.0 * SUM(CASE WHEN c.consumer_disputed = TRUE THEN 1 ELSE 0 END)
             / NULLIF(COUNT(*), 0) > 20
ORDER  BY dispute_rate_pct DESC;


-- QUERY 9 : Correlated subquery
-- For each product, the company with the most complaints
SELECT product_name, company_name, complaints
FROM (
    SELECT p.product_name,
           co.company_name,
           COUNT(c.complaint_id) AS complaints,
           RANK() OVER (PARTITION BY p.product_name
                        ORDER BY COUNT(c.complaint_id) DESC) AS rnk
    FROM   complaints c
    JOIN   products  p  ON c.product_id = p.product_id
    JOIN   companies co ON c.company_id = co.company_id
    GROUP  BY p.product_name, co.company_name
) ranked
WHERE rnk = 1
ORDER BY complaints DESC;


-- QUERY 10 : 3-table JOIN full picture
-- Complaint resolution snapshot per product-issue pair
SELECT p.product_name,
       i.issue_name,
       COUNT(c.complaint_id)                                        AS total,
       SUM(CASE WHEN c.timely_response = TRUE  THEN 1 ELSE 0 END)  AS timely,
       SUM(CASE WHEN c.consumer_disputed = TRUE THEN 1 ELSE 0 END) AS disputed,
       ROUND(100.0 * SUM(CASE WHEN c.timely_response = TRUE THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0), 1)                       AS timely_pct
FROM   complaints c
JOIN   products p ON c.product_id = p.product_id
JOIN   issues   i ON c.issue_id   = i.issue_id
GROUP  BY p.product_name, i.issue_name
ORDER  BY total DESC
LIMIT  25;