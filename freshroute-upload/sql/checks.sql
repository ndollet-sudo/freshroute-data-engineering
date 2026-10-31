-- Evidence for the defence: replay, reconciliation, lineage, access control.
USE WAREHOUSE FRESHROUTE_WH;

-- 1. Where did every bronze row go? bronze = silver + quarantine + duplicates removed
SELECT
  (SELECT COUNT(*) FROM FRESHROUTE.BRONZE.DELIVERIES)              AS bronze_rows,
  (SELECT COUNT(*) FROM FRESHROUTE.SILVER.SILVER_DELIVERIES)       AS silver_rows,
  (SELECT COUNT(*) FROM FRESHROUTE.SILVER.DELIVERIES_QUARANTINE)   AS quarantined_rows,
  bronze_rows - silver_rows - quarantined_rows                     AS duplicates_or_old_versions_removed;

-- 2. Why were rows quarantined?
SELECT f.value::STRING AS problem, COUNT(*) AS n
FROM FRESHROUTE.SILVER.DELIVERIES_QUARANTINE q, LATERAL FLATTEN(q.problems) f
GROUP BY 1 ORDER BY 2 DESC;

-- 3. Replay: run the pipeline twice for the same batch, these must not move
SELECT _batch, COUNT(*) FROM FRESHROUTE.BRONZE.DELIVERIES GROUP BY 1 ORDER BY 1;
SELECT SUM(delivery_fee_gbp) FROM FRESHROUTE.GOLD.GOLD_REVENUE_WEEKLY;

-- 4. Reconciliation silver -> gold (UC3): both totals must be equal
SELECT (SELECT SUM(delivery_fee_gbp) FROM FRESHROUTE.SILVER.SILVER_DELIVERIES
          WHERE outcome = 'delivered')                                    AS silver_gbp,
       (SELECT SUM(delivery_fee_gbp) FROM FRESHROUTE.GOLD.GOLD_REVENUE_WEEKLY) AS gold_gbp;

-- 5. Lineage: which file / run produced the rows?
SELECT _source_file, COUNT(*) FROM FRESHROUTE.BRONZE.ROUTES GROUP BY 1;
SELECT _run_id, _batch, COUNT(*) FROM FRESHROUTE.BRONZE.DELIVERIES GROUP BY 1, 2;

-- 6. Access control: with the analyst role, bronze is DENIED, gold is ALLOWED
USE ROLE FRESHROUTE_ANALYST;
SELECT * FROM FRESHROUTE.BRONZE.DELIVERIES LIMIT 1;                -- expected: error (insufficient privileges)
SELECT * FROM FRESHROUTE.GOLD.GOLD_DELIVERY_PERFORMANCE_DAILY LIMIT 10;  -- expected: OK
