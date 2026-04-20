-- =============================================================================
-- CoCo Budgets: AI Budgets Quick-Start Setup
-- Optional — the Streamlit app can perform all of these steps through the UI.
-- Run this script for a faster "one-shot" setup.
--
-- Prerequisites:
--   1. deploy/rbac.sql has been run as ACCOUNTADMIN
--   2. The Streamlit app has been started at least once (bootstrap DDL complete)
--      OR deploy/backend.sql has been run
--   3. Run this script as COCO_BUDGETS_OWNER (or ACCOUNTADMIN)
--
-- What this creates:
--   - COST_CENTER tag at COCO_BUDGETS_DB.BUDGETS.COST_CENTER
--   - Four example cost center values: ENGINEERING, FINANCE, SALES, DATA
--   - One native Budget: TEAM_AI_BUDGET (1000 credits)
--   - Shared resources: CORTEX CODE + AI FUNCTION
--   - Example scope: users tagged ENGINEERING are included in TEAM_AI_BUDGET
-- =============================================================================

USE ROLE COCO_BUDGETS_OWNER;
USE DATABASE COCO_BUDGETS_DB;
USE SCHEMA BUDGETS;

-- ---------------------------------------------------------------------------
-- 1) Create the COST_CENTER tag
-- ---------------------------------------------------------------------------
CREATE TAG IF NOT EXISTS COCO_BUDGETS_DB.BUDGETS.COST_CENTER
    COMMENT = 'Cost center for native AI budget scoping (managed by CoCo Budgets)';

-- ---------------------------------------------------------------------------
-- 2) Register example cost center values in the app registry
-- ---------------------------------------------------------------------------
INSERT INTO COCO_BUDGETS_DB.BUDGETS.COST_CENTER_TAGS
    (TAG_DB, TAG_SCHEMA, TAG_NAME, TAG_VALUE, DESCRIPTION)
SELECT v.* FROM (VALUES
    ('COCO_BUDGETS_DB', 'BUDGETS', 'COST_CENTER', 'ENGINEERING',  'Engineering team'),
    ('COCO_BUDGETS_DB', 'BUDGETS', 'COST_CENTER', 'FINANCE',      'Finance team'),
    ('COCO_BUDGETS_DB', 'BUDGETS', 'COST_CENTER', 'SALES',        'Sales team'),
    ('COCO_BUDGETS_DB', 'BUDGETS', 'COST_CENTER', 'DATA',         'Data & Analytics team')
) v(TAG_DB, TAG_SCHEMA, TAG_NAME, TAG_VALUE, DESCRIPTION)
WHERE NOT EXISTS (
    SELECT 1 FROM COCO_BUDGETS_DB.BUDGETS.COST_CENTER_TAGS t
    WHERE t.TAG_DB = v.TAG_DB AND t.TAG_SCHEMA = v.TAG_SCHEMA
      AND t.TAG_NAME = v.TAG_NAME AND t.TAG_VALUE = v.TAG_VALUE
);

-- ---------------------------------------------------------------------------
-- 3) Create the native Budget object (1000 credits default)
--    NOTE: Requires the native Budgets feature to be enabled on your account.
--    Contact your Snowflake account team if SHOW SNOWFLAKE.CORE.BUDGET fails.
-- ---------------------------------------------------------------------------
CREATE SNOWFLAKE.CORE.BUDGET IF NOT EXISTS COCO_BUDGETS_DB.BUDGETS.TEAM_AI_BUDGET ();
CALL COCO_BUDGETS_DB.BUDGETS.TEAM_AI_BUDGET!SET_SPENDING_LIMIT(1000);

-- Register it in the app registry
INSERT INTO COCO_BUDGETS_DB.BUDGETS.SNOWFLAKE_BUDGET_REGISTRY
    (BUDGET_DB, BUDGET_SCHEMA, BUDGET_NAME, CREDIT_QUOTA, DESCRIPTION)
SELECT 'COCO_BUDGETS_DB', 'BUDGETS', 'TEAM_AI_BUDGET', 1000,
       'Engineering team AI budget — tracks Cortex Code + AI Functions'
WHERE NOT EXISTS (
    SELECT 1 FROM COCO_BUDGETS_DB.BUDGETS.SNOWFLAKE_BUDGET_REGISTRY
    WHERE BUDGET_DB = 'COCO_BUDGETS_DB'
      AND BUDGET_SCHEMA = 'BUDGETS'
      AND BUDGET_NAME = 'TEAM_AI_BUDGET'
);

-- ---------------------------------------------------------------------------
-- 4) Add shared resources: CORTEX CODE and AI FUNCTION
--    Pass domain name as a plain string (not SYSTEM$REFERENCE).
-- ---------------------------------------------------------------------------
CALL COCO_BUDGETS_DB.BUDGETS.TEAM_AI_BUDGET!ADD_SHARED_RESOURCE('CORTEX CODE');
CALL COCO_BUDGETS_DB.BUDGETS.TEAM_AI_BUDGET!ADD_SHARED_RESOURCE('AI FUNCTION');

-- ---------------------------------------------------------------------------
-- 5) Scope budget to users tagged ENGINEERING
-- ---------------------------------------------------------------------------
CALL COCO_BUDGETS_DB.BUDGETS.TEAM_AI_BUDGET!SET_USER_TAGS(
    [[(SELECT SYSTEM$REFERENCE('TAG', 'COCO_BUDGETS_DB.BUDGETS.COST_CENTER', 'SESSION', 'APPLYBUDGET')), 'ENGINEERING']],
    'UNION'
);

-- ---------------------------------------------------------------------------
-- 6) Verify
-- ---------------------------------------------------------------------------
SHOW SNOWFLAKE.CORE.BUDGET IN SCHEMA COCO_BUDGETS_DB.BUDGETS;

CALL COCO_BUDGETS_DB.BUDGETS.TEAM_AI_BUDGET!GET_BUDGET_SCOPE();

-- ---------------------------------------------------------------------------
-- Optional: tag a specific user to ENGINEERING cost center
-- (replace <USERNAME> with the actual Snowflake user name)
-- ---------------------------------------------------------------------------
-- ALTER USER "<USERNAME>" SET TAG COCO_BUDGETS_DB.BUDGETS.COST_CENTER = 'ENGINEERING';

-- ---------------------------------------------------------------------------
-- Optional: enable native budgets feature flag in app config
-- ---------------------------------------------------------------------------
-- UPDATE COCO_BUDGETS_DB.BUDGETS.BUDGET_CONFIG
--     SET CONFIG_VALUE = 'true', UPDATED_AT = CURRENT_TIMESTAMP()
--     WHERE CONFIG_KEY = 'NATIVE_BUDGETS_ENABLED';
