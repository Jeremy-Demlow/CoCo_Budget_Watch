-- =============================================================================
-- CoCo Budgets: RBAC Setup (Least-Privilege, Full)
-- Run ONCE as ACCOUNTADMIN or SECURITYADMIN before deploying the app.
-- After this one-time setup, COCO_BUDGETS_OWNER is fully self-sufficient
-- and does not require ACCOUNTADMIN for any day-to-day app operations.
--
-- Exception: ALTER ACCOUNT SET CORTEX_MODELS_ALLOWLIST (Model Allowlist tab
-- in Enforcement page) always requires ACCOUNTADMIN — this is a Snowflake
-- platform constraint and cannot be delegated.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- ---------------------------------------------------------------------------
-- 1) Create roles
-- ---------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS COCO_BUDGETS_OWNER
    COMMENT = 'Full app maintainer: owns DB objects, tags users, manages native AI budgets';

CREATE ROLE IF NOT EXISTS COCO_BUDGETS_APP_USER
    COMMENT = 'Budget manager: can edit budgets and run enforcement cycles';

CREATE ROLE IF NOT EXISTS COCO_BUDGETS_READER
    COMMENT = 'Read-only: dashboards and audit logs only';

-- ---------------------------------------------------------------------------
-- 2) Role hierarchy: READER -> APP_USER -> OWNER -> SYSADMIN
-- ---------------------------------------------------------------------------
GRANT ROLE COCO_BUDGETS_READER   TO ROLE COCO_BUDGETS_APP_USER;
GRANT ROLE COCO_BUDGETS_APP_USER TO ROLE COCO_BUDGETS_OWNER;
GRANT ROLE COCO_BUDGETS_OWNER    TO ROLE SYSADMIN;

-- ---------------------------------------------------------------------------
-- 3) ACCOUNT_USAGE read access (for credit usage views)
-- ---------------------------------------------------------------------------
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE COCO_BUDGETS_OWNER;

-- ---------------------------------------------------------------------------
-- 4) Database creation (for self-bootstrapping on first app start)
-- ---------------------------------------------------------------------------
GRANT CREATE DATABASE ON ACCOUNT TO ROLE COCO_BUDGETS_OWNER;

-- ---------------------------------------------------------------------------
-- 5) Warehouse access
-- ---------------------------------------------------------------------------
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE COCO_BUDGETS_OWNER;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE COCO_BUDGETS_APP_USER;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE COCO_BUDGETS_READER;

-- ---------------------------------------------------------------------------
-- 6) [FIXED] User management — required for enforcement + tag application
--
--    Previously missing from this file. Without MANAGE USER, the Enforcement
--    page's "Block User" action (ALTER USER SET CORTEX_CODE_*_DAILY_EST_CREDIT
--    _LIMIT_PER_USER = 0) silently fails for non-ACCOUNTADMIN sessions.
--    Also required for ALTER USER SET TAG (cost center assignment).
-- ---------------------------------------------------------------------------
GRANT MANAGE USER TO ROLE COCO_BUDGETS_OWNER;

-- ---------------------------------------------------------------------------
-- 7) Tag management — apply cost center tags to users
--    Required for: ALTER USER "X" SET TAG db.schema.cost_center = 'VALUE'
-- ---------------------------------------------------------------------------
GRANT APPLY TAG ON ACCOUNT TO ROLE COCO_BUDGETS_OWNER;

-- ---------------------------------------------------------------------------
-- 8) Post-bootstrap grants
--    Run this block AFTER the app's first successful bootstrap
--    (i.e., after COCO_BUDGETS_DB and COCO_BUDGETS_DB.BUDGETS are created).
--    The app's bootstrap DDL runs automatically on first startup.
-- ---------------------------------------------------------------------------

-- GRANT USAGE  ON DATABASE COCO_BUDGETS_DB TO ROLE COCO_BUDGETS_APP_USER;
-- GRANT USAGE  ON DATABASE COCO_BUDGETS_DB TO ROLE COCO_BUDGETS_READER;
-- GRANT USAGE  ON SCHEMA COCO_BUDGETS_DB.BUDGETS TO ROLE COCO_BUDGETS_APP_USER;
-- GRANT USAGE  ON SCHEMA COCO_BUDGETS_DB.BUDGETS TO ROLE COCO_BUDGETS_READER;

-- Native tag creation (COCO_BUDGETS_OWNER creates the COST_CENTER tag via the app UI)
-- GRANT CREATE TAG    ON SCHEMA COCO_BUDGETS_DB.BUDGETS TO ROLE COCO_BUDGETS_OWNER;

-- Native budget creation (COCO_BUDGETS_OWNER creates Budget objects via the app UI)
-- GRANT CREATE BUDGET ON SCHEMA COCO_BUDGETS_DB.BUDGETS TO ROLE COCO_BUDGETS_OWNER;

-- DML access for APP_USER
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA COCO_BUDGETS_DB.BUDGETS
--     TO ROLE COCO_BUDGETS_APP_USER;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA COCO_BUDGETS_DB.BUDGETS
--     TO ROLE COCO_BUDGETS_APP_USER;

-- Read-only access for READER
-- GRANT SELECT ON ALL TABLES IN SCHEMA COCO_BUDGETS_DB.BUDGETS
--     TO ROLE COCO_BUDGETS_READER;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA COCO_BUDGETS_DB.BUDGETS
--     TO ROLE COCO_BUDGETS_READER;

-- ---------------------------------------------------------------------------
-- 9) Post-tag-creation grant
--    Run AFTER the COST_CENTER tag is created (via app UI or ai_budgets_setup.sql).
--    Required for SYSTEM$REFERENCE('TAG', ..., 'APPLYBUDGET') in SET_USER_TAGS.
--    If COCO_BUDGETS_OWNER created the tag, they already have OWNERSHIP (which
--    implies APPLYBUDGET). This grant is only needed if a different role created
--    the tag object.
-- ---------------------------------------------------------------------------
-- GRANT APPLYBUDGET ON TAG COCO_BUDGETS_DB.BUDGETS.COST_CENTER
--     TO ROLE COCO_BUDGETS_OWNER;

-- ---------------------------------------------------------------------------
-- 10) Assign users to roles
--     Replace placeholders with actual usernames.
-- ---------------------------------------------------------------------------
-- GRANT ROLE COCO_BUDGETS_OWNER    TO USER <admin_user>;
-- GRANT ROLE COCO_BUDGETS_APP_USER TO USER <manager_user>;
-- GRANT ROLE COCO_BUDGETS_READER   TO USER <viewer_user>;

-- ---------------------------------------------------------------------------
-- Privilege Summary for COCO_BUDGETS_OWNER
-- ---------------------------------------------------------------------------
-- IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE  → read ACCOUNT_USAGE views
-- CREATE DATABASE ON ACCOUNT                 → self-bootstrap on first run
-- USAGE ON WAREHOUSE COMPUTE_WH              → execute queries
-- MANAGE USER                                → ALTER USER SET/UNSET params+tags
-- APPLY TAG ON ACCOUNT                       → ALTER USER SET TAG (cost centers)
-- CREATE TAG ON SCHEMA COCO_BUDGETS_DB.BUDGETS    → create COST_CENTER tag
-- CREATE BUDGET ON SCHEMA COCO_BUDGETS_DB.BUDGETS → create native Budget objects
-- APPLYBUDGET ON TAG ...COST_CENTER          → SYSTEM$REFERENCE in SET_USER_TAGS
--
-- NOT delegatable (requires ACCOUNTADMIN):
-- ALTER ACCOUNT SET CORTEX_MODELS_ALLOWLIST  → Model Allowlist on Enforcement page
-- ---------------------------------------------------------------------------
