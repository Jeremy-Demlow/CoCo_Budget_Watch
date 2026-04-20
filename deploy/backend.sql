-- =============================================================================
-- CoCo Budgets: Backend DDL
-- Creates database, schema, tables, and seeds default config.
-- Run via SnowSQL/SnowCLI or in a Snowsight worksheet.
-- Idempotent: safe to re-run at any time.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Database + Schema
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS COCO_BUDGETS_DB;
CREATE SCHEMA IF NOT EXISTS COCO_BUDGETS_DB.BUDGETS;

USE DATABASE COCO_BUDGETS_DB;
USE SCHEMA BUDGETS;

-- ---------------------------------------------------------------------------
-- Advisory Budget Tables (existing)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS USER_BUDGETS (
    USER_ID                 NUMBER          NOT NULL,
    IS_ACTIVE               BOOLEAN         DEFAULT TRUE,
    BASE_PERIOD_CREDITS     NUMBER(20,6)    NOT NULL,
    PERIOD_TYPE             VARCHAR         NOT NULL DEFAULT 'MONTHLY',
    PERIOD_START_DAY        NUMBER          DEFAULT 1,
    WARNING_THRESHOLD_PCT   NUMBER          DEFAULT 80,
    CREATED_AT              TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT              TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY              VARCHAR         DEFAULT CURRENT_USER(),
    PRIMARY KEY (USER_ID)
);

CREATE TABLE IF NOT EXISTS ACCOUNT_BUDGET (
    ACCOUNT_BUDGET_ID       NUMBER          AUTOINCREMENT,
    IS_ACTIVE               BOOLEAN         NOT NULL DEFAULT TRUE,
    BASE_PERIOD_CREDITS     NUMBER(20,6)    NOT NULL,
    PERIOD_TYPE             VARCHAR         NOT NULL DEFAULT 'MONTHLY',
    PERIOD_START_DAY        NUMBER          DEFAULT 1,
    WARNING_THRESHOLD_PCT   NUMBER          DEFAULT 80,
    EFFECTIVE_START         TIMESTAMP_TZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    EFFECTIVE_END           TIMESTAMP_TZ,
    CREATED_AT              TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY              VARCHAR         DEFAULT CURRENT_USER(),
    PRIMARY KEY (ACCOUNT_BUDGET_ID)
);

CREATE TABLE IF NOT EXISTS BUDGET_TOPUPS (
    TOPUP_ID                NUMBER          AUTOINCREMENT,
    TARGET_TYPE             VARCHAR         NOT NULL,
    USER_ID                 NUMBER,
    CREDITS                 NUMBER(20,6)    NOT NULL,
    EFFECTIVE_START         TIMESTAMP_TZ    NOT NULL,
    EFFECTIVE_END           TIMESTAMP_TZ    NOT NULL,
    NOTES                   VARCHAR,
    CREATED_AT              TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY              VARCHAR         DEFAULT CURRENT_USER(),
    PRIMARY KEY (TOPUP_ID)
);

CREATE TABLE IF NOT EXISTS BUDGET_AUDIT_LOG (
    LOG_ID                  NUMBER          AUTOINCREMENT,
    ACTION                  VARCHAR         NOT NULL,
    TARGET_TYPE             VARCHAR         NOT NULL,
    TARGET_USER_ID          NUMBER,
    OLD_VALUE               NUMBER(20,6),
    NEW_VALUE               NUMBER(20,6),
    NOTES                   VARCHAR,
    PERFORMED_BY            VARCHAR         DEFAULT CURRENT_USER(),
    PERFORMED_AT            TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS BUDGET_CONFIG (
    CONFIG_KEY              VARCHAR         NOT NULL,
    CONFIG_VALUE            VARCHAR,
    UPDATED_AT              TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (CONFIG_KEY)
);

-- ---------------------------------------------------------------------------
-- Enforcement & Alert Tables (existing)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ENFORCEMENT_LOG (
    LOG_ID              NUMBER          AUTOINCREMENT,
    ACTION              VARCHAR         NOT NULL,
    USER_ID             NUMBER,
    USER_NAME           VARCHAR,
    REASON              VARCHAR,
    PERFORMED_AT        TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS ALERT_STATE (
    ALERT_ID            NUMBER          AUTOINCREMENT,
    USER_ID             NUMBER          NOT NULL,
    ALERT_TYPE          VARCHAR         NOT NULL,
    PERIOD_KEY          VARCHAR         NOT NULL,
    SENT_AT             TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT UQ_ALERT UNIQUE (USER_ID, ALERT_TYPE, PERIOD_KEY)
);

-- ---------------------------------------------------------------------------
-- Native AI Budget Tables (new)
-- ---------------------------------------------------------------------------

-- Tracks cost center tag values defined for native budget scoping
CREATE TABLE IF NOT EXISTS COST_CENTER_TAGS (
    TAG_ID              NUMBER          AUTOINCREMENT,
    TAG_DB              VARCHAR         NOT NULL DEFAULT 'COCO_BUDGETS_DB',
    TAG_SCHEMA          VARCHAR         NOT NULL DEFAULT 'BUDGETS',
    TAG_NAME            VARCHAR         NOT NULL DEFAULT 'COST_CENTER',
    TAG_VALUE           VARCHAR         NOT NULL,
    DESCRIPTION         VARCHAR,
    CREATED_AT          TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY          VARCHAR         DEFAULT CURRENT_USER(),
    PRIMARY KEY (TAG_ID),
    UNIQUE (TAG_DB, TAG_SCHEMA, TAG_NAME, TAG_VALUE)
);

-- Audit trail for every ALTER USER ... SET/UNSET TAG operation
CREATE TABLE IF NOT EXISTS USER_TAG_ASSIGNMENTS (
    ASSIGNMENT_ID       NUMBER          AUTOINCREMENT,
    USER_ID             NUMBER          NOT NULL,
    USER_NAME           VARCHAR         NOT NULL,
    TAG_DB              VARCHAR         NOT NULL,
    TAG_SCHEMA          VARCHAR         NOT NULL,
    TAG_NAME            VARCHAR         NOT NULL,
    TAG_VALUE           VARCHAR,
    ACTION              VARCHAR         NOT NULL DEFAULT 'SET',
    ASSIGNED_AT         TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    ASSIGNED_BY         VARCHAR         DEFAULT CURRENT_USER(),
    PRIMARY KEY (ASSIGNMENT_ID)
);

-- Registry of native Snowflake Budget objects created via the app
CREATE TABLE IF NOT EXISTS SNOWFLAKE_BUDGET_REGISTRY (
    BUDGET_ID           NUMBER          AUTOINCREMENT,
    BUDGET_DB           VARCHAR         NOT NULL DEFAULT 'COCO_BUDGETS_DB',
    BUDGET_SCHEMA       VARCHAR         NOT NULL DEFAULT 'BUDGETS',
    BUDGET_NAME         VARCHAR         NOT NULL,
    CREDIT_QUOTA        NUMBER(20,6)    NOT NULL,
    DESCRIPTION         VARCHAR,
    CREATED_AT          TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY          VARCHAR         DEFAULT CURRENT_USER(),
    PRIMARY KEY (BUDGET_ID),
    UNIQUE (BUDGET_DB, BUDGET_SCHEMA, BUDGET_NAME)
);

-- ---------------------------------------------------------------------------
-- Seed default config (WHEN NOT MATCHED = never overwrites existing values)
-- ---------------------------------------------------------------------------
MERGE INTO BUDGET_CONFIG tgt
USING (
    SELECT * FROM VALUES
        ('BUDGET_TIMEZONE',                   'UTC'),
        ('DEFAULT_PERIOD_TYPE',               'MONTHLY'),
        ('DEFAULT_WARNING_THRESHOLD_PCT',     '80'),
        ('DEFAULT_USER_BASE_PERIOD_CREDITS',  '100'),
        ('ENABLE_PERSISTED_ROLLUPS',          'false'),
        ('ENABLE_MODEL_DRILLDOWN',            'false'),
        ('ENFORCEMENT_ENABLED',               'false'),
        ('ENFORCEMENT_ROLE',                  'CORTEX_USER_ROLE'),
        ('EMAIL_INTEGRATION',                'MY_EMAIL_INT'),
        ('ALERT_RECIPIENTS',                 ''),
        ('ALERT_ON_WARNING',                 'true'),
        ('ALERT_ON_OVER',                    'true'),
        ('CREDIT_RATE_USD',                  '2.00'),
        ('SLACK_ENABLED',                    'false'),
        ('SLACK_WEBHOOK_URL',                ''),
        ('NATIVE_BUDGETS_ENABLED',           'false'),
        ('BUDGET_TAG_DB',                    'COCO_BUDGETS_DB'),
        ('BUDGET_TAG_SCHEMA',                'BUDGETS'),
        ('BUDGET_TAG_NAME',                  'COST_CENTER'),
        ('DEFAULT_NATIVE_BUDGET_QUOTA',      '1000')
) AS src(CONFIG_KEY, CONFIG_VALUE)
ON tgt.CONFIG_KEY = src.CONFIG_KEY
WHEN NOT MATCHED THEN
    INSERT (CONFIG_KEY, CONFIG_VALUE, UPDATED_AT)
    VALUES (src.CONFIG_KEY, src.CONFIG_VALUE, CURRENT_TIMESTAMP());
