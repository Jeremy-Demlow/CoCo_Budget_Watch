-- =============================================================================
-- CoCo Budgets: RBAC Setup (Least-Privilege)
-- Run this as ACCOUNTADMIN or SECURITYADMIN before deploying the app.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- ---------------------------------------------------------------------------
-- 1) Create roles
-- ---------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS COCO_BUDGETS_OWNER
    COMMENT = 'Owns CoCo Budgets DB objects; can run DDL and read ACCOUNT_USAGE';

CREATE ROLE IF NOT EXISTS COCO_BUDGETS_APP_USER
    COMMENT = 'Can run the CoCo Budgets app and edit budgets (DML only)';

CREATE ROLE IF NOT EXISTS COCO_BUDGETS_READER
    COMMENT = 'Read-only access to CoCo Budgets dashboards';

-- ---------------------------------------------------------------------------
-- 2) Role hierarchy: READER -> APP_USER -> OWNER -> SYSADMIN
-- ---------------------------------------------------------------------------
GRANT ROLE COCO_BUDGETS_READER   TO ROLE COCO_BUDGETS_APP_USER;
GRANT ROLE COCO_BUDGETS_APP_USER TO ROLE COCO_BUDGETS_OWNER;
GRANT ROLE COCO_BUDGETS_OWNER    TO ROLE SYSADMIN;

-- ---------------------------------------------------------------------------
-- 3) ACCOUNT_USAGE access (imported privileges on SNOWFLAKE database)
-- ---------------------------------------------------------------------------
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE COCO_BUDGETS_OWNER;

-- ---------------------------------------------------------------------------
-- 4) Database + schema creation privileges
-- ---------------------------------------------------------------------------
GRANT CREATE DATABASE ON ACCOUNT TO ROLE COCO_BUDGETS_OWNER;

-- ---------------------------------------------------------------------------
-- 5) Warehouse access
-- ---------------------------------------------------------------------------
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE COCO_BUDGETS_OWNER;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE COCO_BUDGETS_APP_USER;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE COCO_BUDGETS_READER;

-- ---------------------------------------------------------------------------
-- 6) After backend.sql creates the DB/schema, run these grants:
--    (Uncomment and run after deploy/backend.sql)
-- ---------------------------------------------------------------------------
-- GRANT USAGE ON DATABASE COCO_BUDGETS_DB TO ROLE COCO_BUDGETS_APP_USER;
-- GRANT USAGE ON DATABASE COCO_BUDGETS_DB TO ROLE COCO_BUDGETS_READER;
-- GRANT USAGE ON SCHEMA  COCO_BUDGETS_DB.BUDGETS TO ROLE COCO_BUDGETS_APP_USER;
-- GRANT USAGE ON SCHEMA  COCO_BUDGETS_DB.BUDGETS TO ROLE COCO_BUDGETS_READER;
--
-- -- APP_USER: DML on budget tables
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA COCO_BUDGETS_DB.BUDGETS
--     TO ROLE COCO_BUDGETS_APP_USER;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA COCO_BUDGETS_DB.BUDGETS
--     TO ROLE COCO_BUDGETS_APP_USER;
--
-- -- READER: SELECT only
-- GRANT SELECT ON ALL TABLES IN SCHEMA COCO_BUDGETS_DB.BUDGETS
--     TO ROLE COCO_BUDGETS_READER;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA COCO_BUDGETS_DB.BUDGETS
--     TO ROLE COCO_BUDGETS_READER;
--
-- -- Grant users to roles
-- GRANT ROLE COCO_BUDGETS_OWNER TO USER <your_admin_user>;
-- GRANT ROLE COCO_BUDGETS_APP_USER TO USER <budget_manager_user>;
-- GRANT ROLE COCO_BUDGETS_READER TO USER <dashboard_viewer>;
