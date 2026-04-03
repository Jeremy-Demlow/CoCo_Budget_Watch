-- =============================================================================
-- CoCo Budgets: Useful Queries
-- Standalone SQL queries for Cortex Code credit monitoring.
-- Run these in a Snowsight worksheet — no app deployment required.
-- =============================================================================

-- -------------------------------------------------------------------------
-- PREREQUISITES
-- These queries use SNOWFLAKE.ACCOUNT_USAGE views, which require the
-- ACCOUNTADMIN role (or IMPORTED PRIVILEGES on SNOWFLAKE database).
-- Data in ACCOUNT_USAGE views can lag up to ~1 hour.
-- -------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;

-- =====================================================================
-- PRICING CTE (reused across queries)
-- Adjust rates below if Snowflake publishes new Cortex Code pricing.
-- Rates are per 1M tokens (USD).
-- =====================================================================

-- =====================================================================
-- 1. TOTAL CREDITS & REQUESTS (current month)
-- Quick KPI summary for the current billing period.
-- =====================================================================

WITH requests AS (
    SELECT 'CLI' AS SOURCE, REQUEST_ID, USER_ID, USAGE_TIME, TOKENS_GRANULAR,
           ROW_NUMBER() OVER (PARTITION BY REQUEST_ID ORDER BY USAGE_TIME DESC) AS _RN
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY
    WHERE TOKENS_GRANULAR IS NOT NULL
    UNION ALL
    SELECT 'SNOWSIGHT', REQUEST_ID, USER_ID, USAGE_TIME, TOKENS_GRANULAR,
           ROW_NUMBER() OVER (PARTITION BY REQUEST_ID ORDER BY USAGE_TIME DESC) AS _RN
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY
    WHERE TOKENS_GRANULAR IS NOT NULL
),
deduped AS (
    SELECT * FROM requests WHERE _RN = 1
),
pricing AS (
    SELECT 'claude-4-sonnet'   AS MODEL_NAME, 1.50 AS INPUT_RATE, 7.50 AS OUTPUT_RATE, 1.88 AS CACHE_WRITE_RATE, 0.15 AS CACHE_READ_RATE
    UNION ALL SELECT 'claude-opus-4-5',   2.75, 13.75, 3.44, 0.28
    UNION ALL SELECT 'claude-opus-4-6',   2.75, 13.75, 3.44, 0.28
    UNION ALL SELECT 'claude-sonnet-4-5', 1.65,  8.25, 2.06, 0.17
    UNION ALL SELECT 'claude-sonnet-4-6', 1.65,  8.25, 2.07, 0.17
    UNION ALL SELECT 'openai-gpt-5.2',    0.97,  7.70, 0.00, 0.10
),
flattened AS (
    SELECT r.REQUEST_ID, r.USER_ID, r.USAGE_TIME, r.SOURCE,
        tk.key AS MODEL_NAME,
        COALESCE(tk.value:input::FLOAT, 0)              AS INPUT_TOKENS,
        COALESCE(tk.value:output::FLOAT, 0)             AS OUTPUT_TOKENS,
        COALESCE(tk.value:cache_write_input::FLOAT, 0)  AS CACHE_WRITE_TOKENS,
        COALESCE(tk.value:cache_read_input::FLOAT, 0)   AS CACHE_READ_TOKENS
    FROM deduped r,
        LATERAL FLATTEN(input => r.TOKENS_GRANULAR) tk
    WHERE r.USAGE_TIME >= DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
      AND r.USAGE_TIME <  DATEADD('MONTH', 1, DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()))
)
SELECT
    SUM(
        f.INPUT_TOKENS        * COALESCE(p.INPUT_RATE, 0) / 1e6
      + f.OUTPUT_TOKENS       * COALESCE(p.OUTPUT_RATE, 0) / 1e6
      + f.CACHE_WRITE_TOKENS  * COALESCE(p.CACHE_WRITE_RATE, 0) / 1e6
      + f.CACHE_READ_TOKENS   * COALESCE(p.CACHE_READ_RATE, 0) / 1e6
    ) AS TOTAL_CREDITS,
    TOTAL_CREDITS * 2.00 AS EST_COST_USD,  -- adjust credit rate as needed
    SUM(f.INPUT_TOKENS + f.OUTPUT_TOKENS + f.CACHE_WRITE_TOKENS + f.CACHE_READ_TOKENS) AS TOTAL_TOKENS,
    COUNT(DISTINCT f.REQUEST_ID) AS TOTAL_REQUESTS,
    COUNT(DISTINCT f.USER_ID) AS ACTIVE_USERS
FROM flattened f
LEFT JOIN pricing p ON f.MODEL_NAME = p.MODEL_NAME;


-- =====================================================================
-- 2. CREDITS BY USER (current month)
-- Who is consuming the most Cortex Code credits?
-- =====================================================================

WITH requests AS (
    SELECT 'CLI' AS SOURCE, REQUEST_ID, USER_ID, USAGE_TIME, TOKENS_GRANULAR,
           ROW_NUMBER() OVER (PARTITION BY REQUEST_ID ORDER BY USAGE_TIME DESC) AS _RN
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY
    WHERE TOKENS_GRANULAR IS NOT NULL
    UNION ALL
    SELECT 'SNOWSIGHT', REQUEST_ID, USER_ID, USAGE_TIME, TOKENS_GRANULAR,
           ROW_NUMBER() OVER (PARTITION BY REQUEST_ID ORDER BY USAGE_TIME DESC) AS _RN
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY
    WHERE TOKENS_GRANULAR IS NOT NULL
),
deduped AS (SELECT * FROM requests WHERE _RN = 1),
pricing AS (
    SELECT 'claude-4-sonnet'   AS MODEL_NAME, 1.50 AS INPUT_RATE, 7.50 AS OUTPUT_RATE, 1.88 AS CACHE_WRITE_RATE, 0.15 AS CACHE_READ_RATE
    UNION ALL SELECT 'claude-opus-4-5',   2.75, 13.75, 3.44, 0.28
    UNION ALL SELECT 'claude-opus-4-6',   2.75, 13.75, 3.44, 0.28
    UNION ALL SELECT 'claude-sonnet-4-5', 1.65,  8.25, 2.06, 0.17
    UNION ALL SELECT 'claude-sonnet-4-6', 1.65,  8.25, 2.07, 0.17
    UNION ALL SELECT 'openai-gpt-5.2',    0.97,  7.70, 0.00, 0.10
),
flattened AS (
    SELECT r.REQUEST_ID, r.USER_ID, r.USAGE_TIME, r.SOURCE,
        tk.key AS MODEL_NAME,
        COALESCE(tk.value:input::FLOAT, 0) AS INPUT_TOKENS,
        COALESCE(tk.value:output::FLOAT, 0) AS OUTPUT_TOKENS,
        COALESCE(tk.value:cache_write_input::FLOAT, 0) AS CACHE_WRITE_TOKENS,
        COALESCE(tk.value:cache_read_input::FLOAT, 0) AS CACHE_READ_TOKENS
    FROM deduped r,
        LATERAL FLATTEN(input => r.TOKENS_GRANULAR) tk
    WHERE r.USAGE_TIME >= DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
      AND r.USAGE_TIME <  DATEADD('MONTH', 1, DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()))
)
SELECT
    u.NAME AS USER_NAME,
    f.SOURCE,
    SUM(
        f.INPUT_TOKENS        * COALESCE(p.INPUT_RATE, 0) / 1e6
      + f.OUTPUT_TOKENS       * COALESCE(p.OUTPUT_RATE, 0) / 1e6
      + f.CACHE_WRITE_TOKENS  * COALESCE(p.CACHE_WRITE_RATE, 0) / 1e6
      + f.CACHE_READ_TOKENS   * COALESCE(p.CACHE_READ_RATE, 0) / 1e6
    ) AS CREDITS,
    COUNT(DISTINCT f.REQUEST_ID) AS REQUESTS
FROM flattened f
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON f.USER_ID = u.USER_ID
LEFT JOIN pricing p ON f.MODEL_NAME = p.MODEL_NAME
GROUP BY u.NAME, f.SOURCE
ORDER BY CREDITS DESC;


-- =====================================================================
-- 3. CREDITS BY MODEL (current month)
-- Which LLM models are driving the most cost?
-- =====================================================================

WITH requests AS (
    SELECT 'CLI' AS SOURCE, REQUEST_ID, USER_ID, USAGE_TIME, TOKENS_GRANULAR,
           ROW_NUMBER() OVER (PARTITION BY REQUEST_ID ORDER BY USAGE_TIME DESC) AS _RN
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY
    WHERE TOKENS_GRANULAR IS NOT NULL
    UNION ALL
    SELECT 'SNOWSIGHT', REQUEST_ID, USER_ID, USAGE_TIME, TOKENS_GRANULAR,
           ROW_NUMBER() OVER (PARTITION BY REQUEST_ID ORDER BY USAGE_TIME DESC) AS _RN
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY
    WHERE TOKENS_GRANULAR IS NOT NULL
),
deduped AS (SELECT * FROM requests WHERE _RN = 1),
pricing AS (
    SELECT 'claude-4-sonnet'   AS MODEL_NAME, 1.50 AS INPUT_RATE, 7.50 AS OUTPUT_RATE, 1.88 AS CACHE_WRITE_RATE, 0.15 AS CACHE_READ_RATE
    UNION ALL SELECT 'claude-opus-4-5',   2.75, 13.75, 3.44, 0.28
    UNION ALL SELECT 'claude-opus-4-6',   2.75, 13.75, 3.44, 0.28
    UNION ALL SELECT 'claude-sonnet-4-5', 1.65,  8.25, 2.06, 0.17
    UNION ALL SELECT 'claude-sonnet-4-6', 1.65,  8.25, 2.07, 0.17
    UNION ALL SELECT 'openai-gpt-5.2',    0.97,  7.70, 0.00, 0.10
),
flattened AS (
    SELECT r.REQUEST_ID, r.USER_ID, r.USAGE_TIME, r.SOURCE,
        tk.key AS MODEL_NAME,
        COALESCE(tk.value:input::FLOAT, 0) AS INPUT_TOKENS,
        COALESCE(tk.value:output::FLOAT, 0) AS OUTPUT_TOKENS,
        COALESCE(tk.value:cache_write_input::FLOAT, 0) AS CACHE_WRITE_TOKENS,
        COALESCE(tk.value:cache_read_input::FLOAT, 0) AS CACHE_READ_TOKENS
    FROM deduped r,
        LATERAL FLATTEN(input => r.TOKENS_GRANULAR) tk
    WHERE r.USAGE_TIME >= DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
      AND r.USAGE_TIME <  DATEADD('MONTH', 1, DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()))
)
SELECT
    f.MODEL_NAME AS MODEL,
    u.NAME AS USER_NAME,
    COUNT(DISTINCT f.REQUEST_ID) AS REQUESTS,
    SUM(
        f.INPUT_TOKENS        * COALESCE(p.INPUT_RATE, 0) / 1e6
      + f.OUTPUT_TOKENS       * COALESCE(p.OUTPUT_RATE, 0) / 1e6
      + f.CACHE_WRITE_TOKENS  * COALESCE(p.CACHE_WRITE_RATE, 0) / 1e6
      + f.CACHE_READ_TOKENS   * COALESCE(p.CACHE_READ_RATE, 0) / 1e6
    ) AS CREDITS,
    SUM(f.INPUT_TOKENS + f.OUTPUT_TOKENS + f.CACHE_WRITE_TOKENS + f.CACHE_READ_TOKENS) AS TOTAL_TOKENS
FROM flattened f
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON f.USER_ID = u.USER_ID
LEFT JOIN pricing p ON f.MODEL_NAME = p.MODEL_NAME
GROUP BY f.MODEL_NAME, u.NAME
ORDER BY CREDITS DESC;


-- =====================================================================
-- 4. DAILY CREDIT TREND (last 30 days)
-- Spot usage spikes and trending patterns.
-- =====================================================================

WITH requests AS (
    SELECT 'CLI' AS SOURCE, REQUEST_ID, USER_ID, USAGE_TIME, TOKENS_GRANULAR,
           ROW_NUMBER() OVER (PARTITION BY REQUEST_ID ORDER BY USAGE_TIME DESC) AS _RN
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY
    WHERE TOKENS_GRANULAR IS NOT NULL
    UNION ALL
    SELECT 'SNOWSIGHT', REQUEST_ID, USER_ID, USAGE_TIME, TOKENS_GRANULAR,
           ROW_NUMBER() OVER (PARTITION BY REQUEST_ID ORDER BY USAGE_TIME DESC) AS _RN
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY
    WHERE TOKENS_GRANULAR IS NOT NULL
),
deduped AS (SELECT * FROM requests WHERE _RN = 1),
pricing AS (
    SELECT 'claude-4-sonnet'   AS MODEL_NAME, 1.50 AS INPUT_RATE, 7.50 AS OUTPUT_RATE, 1.88 AS CACHE_WRITE_RATE, 0.15 AS CACHE_READ_RATE
    UNION ALL SELECT 'claude-opus-4-5',   2.75, 13.75, 3.44, 0.28
    UNION ALL SELECT 'claude-opus-4-6',   2.75, 13.75, 3.44, 0.28
    UNION ALL SELECT 'claude-sonnet-4-5', 1.65,  8.25, 2.06, 0.17
    UNION ALL SELECT 'claude-sonnet-4-6', 1.65,  8.25, 2.07, 0.17
    UNION ALL SELECT 'openai-gpt-5.2',    0.97,  7.70, 0.00, 0.10
),
flattened AS (
    SELECT r.REQUEST_ID, r.USER_ID, r.USAGE_TIME, r.SOURCE,
        tk.key AS MODEL_NAME,
        COALESCE(tk.value:input::FLOAT, 0) AS INPUT_TOKENS,
        COALESCE(tk.value:output::FLOAT, 0) AS OUTPUT_TOKENS,
        COALESCE(tk.value:cache_write_input::FLOAT, 0) AS CACHE_WRITE_TOKENS,
        COALESCE(tk.value:cache_read_input::FLOAT, 0) AS CACHE_READ_TOKENS
    FROM deduped r,
        LATERAL FLATTEN(input => r.TOKENS_GRANULAR) tk
    WHERE r.USAGE_TIME >= DATEADD('DAY', -30, CURRENT_TIMESTAMP())
)
SELECT
    DATE(f.USAGE_TIME) AS USAGE_DATE,
    u.NAME AS USER_NAME,
    f.SOURCE,
    SUM(
        f.INPUT_TOKENS        * COALESCE(p.INPUT_RATE, 0) / 1e6
      + f.OUTPUT_TOKENS       * COALESCE(p.OUTPUT_RATE, 0) / 1e6
      + f.CACHE_WRITE_TOKENS  * COALESCE(p.CACHE_WRITE_RATE, 0) / 1e6
      + f.CACHE_READ_TOKENS   * COALESCE(p.CACHE_READ_RATE, 0) / 1e6
    ) AS DAILY_CREDITS,
    COUNT(DISTINCT f.REQUEST_ID) AS REQUESTS
FROM flattened f
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON f.USER_ID = u.USER_ID
LEFT JOIN pricing p ON f.MODEL_NAME = p.MODEL_NAME
GROUP BY USAGE_DATE, u.NAME, f.SOURCE
ORDER BY USAGE_DATE;


-- =====================================================================
-- 5. CUMULATIVE SPEND (current month, for burn-down charts)
-- =====================================================================

WITH requests AS (
    SELECT 'CLI' AS SOURCE, REQUEST_ID, USER_ID, USAGE_TIME, TOKENS_GRANULAR,
           ROW_NUMBER() OVER (PARTITION BY REQUEST_ID ORDER BY USAGE_TIME DESC) AS _RN
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY
    WHERE TOKENS_GRANULAR IS NOT NULL
    UNION ALL
    SELECT 'SNOWSIGHT', REQUEST_ID, USER_ID, USAGE_TIME, TOKENS_GRANULAR,
           ROW_NUMBER() OVER (PARTITION BY REQUEST_ID ORDER BY USAGE_TIME DESC) AS _RN
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY
    WHERE TOKENS_GRANULAR IS NOT NULL
),
deduped AS (SELECT * FROM requests WHERE _RN = 1),
pricing AS (
    SELECT 'claude-4-sonnet'   AS MODEL_NAME, 1.50 AS INPUT_RATE, 7.50 AS OUTPUT_RATE, 1.88 AS CACHE_WRITE_RATE, 0.15 AS CACHE_READ_RATE
    UNION ALL SELECT 'claude-opus-4-5',   2.75, 13.75, 3.44, 0.28
    UNION ALL SELECT 'claude-opus-4-6',   2.75, 13.75, 3.44, 0.28
    UNION ALL SELECT 'claude-sonnet-4-5', 1.65,  8.25, 2.06, 0.17
    UNION ALL SELECT 'claude-sonnet-4-6', 1.65,  8.25, 2.07, 0.17
    UNION ALL SELECT 'openai-gpt-5.2',    0.97,  7.70, 0.00, 0.10
),
flattened AS (
    SELECT r.REQUEST_ID, r.USER_ID, r.USAGE_TIME,
        tk.key AS MODEL_NAME,
        COALESCE(tk.value:input::FLOAT, 0) AS INPUT_TOKENS,
        COALESCE(tk.value:output::FLOAT, 0) AS OUTPUT_TOKENS,
        COALESCE(tk.value:cache_write_input::FLOAT, 0) AS CACHE_WRITE_TOKENS,
        COALESCE(tk.value:cache_read_input::FLOAT, 0) AS CACHE_READ_TOKENS
    FROM deduped r,
        LATERAL FLATTEN(input => r.TOKENS_GRANULAR) tk
    WHERE r.USAGE_TIME >= DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
      AND r.USAGE_TIME <  DATEADD('MONTH', 1, DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()))
),
daily AS (
    SELECT
        DATE(f.USAGE_TIME) AS USAGE_DATE,
        SUM(
            f.INPUT_TOKENS        * COALESCE(p.INPUT_RATE, 0) / 1e6
          + f.OUTPUT_TOKENS       * COALESCE(p.OUTPUT_RATE, 0) / 1e6
          + f.CACHE_WRITE_TOKENS  * COALESCE(p.CACHE_WRITE_RATE, 0) / 1e6
          + f.CACHE_READ_TOKENS   * COALESCE(p.CACHE_READ_RATE, 0) / 1e6
        ) AS DAILY_CREDITS
    FROM flattened f
    LEFT JOIN pricing p ON f.MODEL_NAME = p.MODEL_NAME
    GROUP BY USAGE_DATE
)
SELECT
    USAGE_DATE,
    DAILY_CREDITS,
    SUM(DAILY_CREDITS) OVER (ORDER BY USAGE_DATE) AS CUMULATIVE_CREDITS
FROM daily
ORDER BY USAGE_DATE;


-- =====================================================================
-- 6. ALL USERS WITH BUDGET STATUS (current month)
-- Shows each user's spend vs. budget, including top-ups.
-- Requires the CoCo Budgets backend tables to be deployed.
-- =====================================================================

WITH requests AS (
    SELECT 'CLI' AS SOURCE, REQUEST_ID, USER_ID, USAGE_TIME, TOKENS_GRANULAR,
           ROW_NUMBER() OVER (PARTITION BY REQUEST_ID ORDER BY USAGE_TIME DESC) AS _RN
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY
    WHERE TOKENS_GRANULAR IS NOT NULL
    UNION ALL
    SELECT 'SNOWSIGHT', REQUEST_ID, USER_ID, USAGE_TIME, TOKENS_GRANULAR,
           ROW_NUMBER() OVER (PARTITION BY REQUEST_ID ORDER BY USAGE_TIME DESC) AS _RN
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY
    WHERE TOKENS_GRANULAR IS NOT NULL
),
deduped AS (SELECT * FROM requests WHERE _RN = 1),
pricing AS (
    SELECT 'claude-4-sonnet'   AS MODEL_NAME, 1.50 AS INPUT_RATE, 7.50 AS OUTPUT_RATE, 1.88 AS CACHE_WRITE_RATE, 0.15 AS CACHE_READ_RATE
    UNION ALL SELECT 'claude-opus-4-5',   2.75, 13.75, 3.44, 0.28
    UNION ALL SELECT 'claude-opus-4-6',   2.75, 13.75, 3.44, 0.28
    UNION ALL SELECT 'claude-sonnet-4-5', 1.65,  8.25, 2.06, 0.17
    UNION ALL SELECT 'claude-sonnet-4-6', 1.65,  8.25, 2.07, 0.17
    UNION ALL SELECT 'openai-gpt-5.2',    0.97,  7.70, 0.00, 0.10
),
flattened AS (
    SELECT r.REQUEST_ID, r.USER_ID, r.USAGE_TIME,
        tk.key AS MODEL_NAME,
        COALESCE(tk.value:input::FLOAT, 0) AS INPUT_TOKENS,
        COALESCE(tk.value:output::FLOAT, 0) AS OUTPUT_TOKENS,
        COALESCE(tk.value:cache_write_input::FLOAT, 0) AS CACHE_WRITE_TOKENS,
        COALESCE(tk.value:cache_read_input::FLOAT, 0) AS CACHE_READ_TOKENS
    FROM deduped r,
        LATERAL FLATTEN(input => r.TOKENS_GRANULAR) tk
    WHERE r.USAGE_TIME >= DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
      AND r.USAGE_TIME <  DATEADD('MONTH', 1, DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()))
),
usage_agg AS (
    SELECT f.USER_ID,
           SUM(
               f.INPUT_TOKENS        * COALESCE(p.INPUT_RATE, 0) / 1e6
             + f.OUTPUT_TOKENS       * COALESCE(p.OUTPUT_RATE, 0) / 1e6
             + f.CACHE_WRITE_TOKENS  * COALESCE(p.CACHE_WRITE_RATE, 0) / 1e6
             + f.CACHE_READ_TOKENS   * COALESCE(p.CACHE_READ_RATE, 0) / 1e6
           ) AS TOTAL_USED,
           COUNT(DISTINCT f.REQUEST_ID) AS REQUESTS,
           MAX(f.USAGE_TIME) AS LAST_ACTIVITY
    FROM flattened f
    LEFT JOIN pricing p ON f.MODEL_NAME = p.MODEL_NAME
    GROUP BY f.USER_ID
),
topup_agg AS (
    SELECT t.USER_ID, SUM(t.CREDITS) AS TOPUP_CREDITS
    FROM COCO_BUDGETS_DB.BUDGETS.BUDGET_TOPUPS t
    WHERE t.TARGET_TYPE = 'USER'
      AND t.EFFECTIVE_START < DATEADD('MONTH', 1, DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()))
      AND t.EFFECTIVE_END   > DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
    GROUP BY t.USER_ID
)
SELECT
    u.NAME AS USER_NAME,
    u.EMAIL,
    COALESCE(ua.TOTAL_USED, 0) AS CREDITS_USED,
    COALESCE(ua.REQUESTS, 0) AS REQUESTS,
    b.BASE_PERIOD_CREDITS AS BUDGET,
    COALESCE(tp.TOPUP_CREDITS, 0) AS TOPUPS,
    b.BASE_PERIOD_CREDITS + COALESCE(tp.TOPUP_CREDITS, 0) AS EFFECTIVE_BUDGET,
    b.BASE_PERIOD_CREDITS + COALESCE(tp.TOPUP_CREDITS, 0) - COALESCE(ua.TOTAL_USED, 0) AS REMAINING,
    CASE
        WHEN b.BASE_PERIOD_CREDITS IS NULL THEN 'NO BUDGET'
        WHEN COALESCE(ua.TOTAL_USED, 0) >= b.BASE_PERIOD_CREDITS + COALESCE(tp.TOPUP_CREDITS, 0) THEN 'OVER'
        WHEN b.BASE_PERIOD_CREDITS + COALESCE(tp.TOPUP_CREDITS, 0) > 0
             AND COALESCE(ua.TOTAL_USED, 0) >= (b.BASE_PERIOD_CREDITS + COALESCE(tp.TOPUP_CREDITS, 0)) * b.WARNING_THRESHOLD_PCT / 100.0
        THEN 'WARNING'
        ELSE 'OK'
    END AS STATUS
FROM SNOWFLAKE.ACCOUNT_USAGE.USERS u
LEFT JOIN usage_agg ua ON u.USER_ID = ua.USER_ID
LEFT JOIN COCO_BUDGETS_DB.BUDGETS.USER_BUDGETS b ON u.USER_ID = b.USER_ID AND b.IS_ACTIVE = TRUE
LEFT JOIN topup_agg tp ON u.USER_ID = tp.USER_ID
WHERE u.DELETED_ON IS NULL
  AND u.LOGIN_NAME NOT LIKE 'SF$SERVICE%'
  AND u.NAME != 'SNOWFLAKE'
ORDER BY COALESCE(ua.TOTAL_USED, 0) DESC;


-- =====================================================================
-- 7. CORTEX_USER ROLE GRANTS (who has Cortex AI access?)
-- =====================================================================

SHOW GRANTS OF ROLE CORTEX_USER_ROLE;


-- =====================================================================
-- 8. CHECK IF PUBLIC HAS SNOWFLAKE.CORTEX_USER
-- If this returns rows, enforcement is ineffective because all users
-- inherit Cortex access through PUBLIC.
-- =====================================================================

SHOW GRANTS OF DATABASE ROLE SNOWFLAKE.CORTEX_USER;
-- Look for: granted_to = 'ROLE', grantee_name = 'PUBLIC'


-- =====================================================================
-- 9. REVOKE PUBLIC ACCESS (to enable real enforcement)
-- Run this ONCE if you want per-user enforcement to work.
-- =====================================================================

-- REVOKE DATABASE ROLE SNOWFLAKE.CORTEX_USER FROM ROLE PUBLIC;


-- =====================================================================
-- 10. RAW REQUEST HISTORY (last 100 requests, useful for debugging)
-- =====================================================================

SELECT 'CLI' AS SOURCE, REQUEST_ID, USER_ID, USAGE_TIME,
       TOKENS_GRANULAR
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY
WHERE TOKENS_GRANULAR IS NOT NULL
  AND USAGE_TIME >= DATEADD('DAY', -7, CURRENT_TIMESTAMP())

UNION ALL

SELECT 'SNOWSIGHT', REQUEST_ID, USER_ID, USAGE_TIME,
       TOKENS_GRANULAR
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY
WHERE TOKENS_GRANULAR IS NOT NULL
  AND USAGE_TIME >= DATEADD('DAY', -7, CURRENT_TIMESTAMP())

ORDER BY USAGE_TIME DESC
LIMIT 100;
