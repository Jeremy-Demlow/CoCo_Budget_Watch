-- =============================================================================
-- CoCo Budgets: SIS Deployment Prerequisites
-- Run BEFORE `snow streamlit deploy` from the app/ directory.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE COCO_BUDGETS_DB;
USE SCHEMA BUDGETS;

CREATE STAGE IF NOT EXISTS COCO_BUDGETS_STAGE
    COMMENT = 'Stage for CoCo Budgets Streamlit app files';
