import streamlit as st
from lib.connection import run_query, run_ddl, get_current_role, FQN, DB, SCHEMA

st.title("Setup")
st.caption("Check backend health and fix missing objects")

REQUIRED_TABLES = [
    "USER_BUDGETS",
    "ACCOUNT_BUDGET",
    "BUDGET_TOPUPS",
    "BUDGET_AUDIT_LOG",
    "BUDGET_CONFIG",
    "ENFORCEMENT_LOG",
    "ALERT_STATE",
    "COST_CENTER_TAGS",
    "USER_TAG_ASSIGNMENTS",
    "SNOWFLAKE_BUDGET_REGISTRY",
]


def _obj_exists(sql: str) -> bool:
    df, err = run_query(sql)
    return not err and not df.empty


def check_database() -> bool:
    return _obj_exists(f"SHOW DATABASES LIKE '{DB}'")


def check_schema() -> bool:
    return _obj_exists(
        f"SHOW SCHEMAS LIKE '{SCHEMA}' IN DATABASE {DB}"
    )


def check_table(name: str) -> bool:
    return _obj_exists(
        f"SHOW TABLES LIKE '{name}' IN {FQN}"
    )


def check_config_seeded() -> bool:
    df, err = run_query(
        f"SELECT COUNT(*) AS CNT FROM {FQN}.BUDGET_CONFIG"
    )
    if err or df.empty:
        return False
    return int(df.iloc[0]["CNT"]) > 0


def check_privilege(priv_fragment: str) -> bool:
    role = get_current_role()
    if role == "UNKNOWN":
        return False
    df, err = run_query(f"SHOW GRANTS TO ROLE {role}")
    if err or df.empty:
        return False
    priv_col = "privilege" if "privilege" in df.columns else "PRIVILEGE"
    if priv_col not in df.columns:
        return False
    text = " ".join(df[priv_col].astype(str).tolist()).upper()
    return priv_fragment.upper() in text


TABLE_DDLS = {
    "USER_BUDGETS": f"""CREATE TABLE IF NOT EXISTS {FQN}.USER_BUDGETS (
    USER_ID NUMBER NOT NULL,
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    BASE_PERIOD_CREDITS NUMBER(20,6) NOT NULL,
    PERIOD_TYPE VARCHAR NOT NULL DEFAULT 'MONTHLY',
    PERIOD_START_DAY NUMBER DEFAULT 1,
    WARNING_THRESHOLD_PCT NUMBER DEFAULT 80,
    CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR DEFAULT CURRENT_USER(),
    PRIMARY KEY (USER_ID)
);""",
    "ACCOUNT_BUDGET": f"""CREATE TABLE IF NOT EXISTS {FQN}.ACCOUNT_BUDGET (
    ACCOUNT_BUDGET_ID NUMBER AUTOINCREMENT,
    IS_ACTIVE BOOLEAN NOT NULL DEFAULT TRUE,
    BASE_PERIOD_CREDITS NUMBER(20,6) NOT NULL,
    PERIOD_TYPE VARCHAR NOT NULL DEFAULT 'MONTHLY',
    PERIOD_START_DAY NUMBER DEFAULT 1,
    WARNING_THRESHOLD_PCT NUMBER DEFAULT 80,
    EFFECTIVE_START TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    EFFECTIVE_END TIMESTAMP_TZ,
    CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR DEFAULT CURRENT_USER(),
    PRIMARY KEY (ACCOUNT_BUDGET_ID)
);""",
    "BUDGET_TOPUPS": f"""CREATE TABLE IF NOT EXISTS {FQN}.BUDGET_TOPUPS (
    TOPUP_ID NUMBER AUTOINCREMENT,
    TARGET_TYPE VARCHAR NOT NULL,
    USER_ID NUMBER,
    CREDITS NUMBER(20,6) NOT NULL,
    EFFECTIVE_START TIMESTAMP_TZ NOT NULL,
    EFFECTIVE_END TIMESTAMP_TZ NOT NULL,
    NOTES VARCHAR,
    CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR DEFAULT CURRENT_USER(),
    PRIMARY KEY (TOPUP_ID)
);""",
    "BUDGET_AUDIT_LOG": f"""CREATE TABLE IF NOT EXISTS {FQN}.BUDGET_AUDIT_LOG (
    LOG_ID NUMBER AUTOINCREMENT,
    ACTION VARCHAR NOT NULL,
    TARGET_TYPE VARCHAR NOT NULL,
    TARGET_USER_ID NUMBER,
    OLD_VALUE NUMBER(20,6),
    NEW_VALUE NUMBER(20,6),
    NOTES VARCHAR,
    PERFORMED_BY VARCHAR DEFAULT CURRENT_USER(),
    PERFORMED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);""",
    "BUDGET_CONFIG": f"""CREATE TABLE IF NOT EXISTS {FQN}.BUDGET_CONFIG (
    CONFIG_KEY VARCHAR NOT NULL,
    CONFIG_VALUE VARCHAR,
    UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (CONFIG_KEY)
);""",
    "ENFORCEMENT_LOG": f"""CREATE TABLE IF NOT EXISTS {FQN}.ENFORCEMENT_LOG (
    LOG_ID NUMBER AUTOINCREMENT,
    ACTION VARCHAR NOT NULL,
    USER_ID NUMBER,
    USER_NAME VARCHAR,
    REASON VARCHAR,
    PERFORMED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);""",
    "ALERT_STATE": f"""CREATE TABLE IF NOT EXISTS {FQN}.ALERT_STATE (
    ALERT_ID NUMBER AUTOINCREMENT,
    USER_ID NUMBER NOT NULL,
    ALERT_TYPE VARCHAR NOT NULL,
    PERIOD_KEY VARCHAR NOT NULL,
    SENT_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT UQ_ALERT UNIQUE (USER_ID, ALERT_TYPE, PERIOD_KEY)
);""",
    "COST_CENTER_TAGS": f"""CREATE TABLE IF NOT EXISTS {FQN}.COST_CENTER_TAGS (
    TAG_ID NUMBER AUTOINCREMENT,
    TAG_DB VARCHAR NOT NULL DEFAULT 'COCO_BUDGETS_DB',
    TAG_SCHEMA VARCHAR NOT NULL DEFAULT 'BUDGETS',
    TAG_NAME VARCHAR NOT NULL DEFAULT 'COST_CENTER',
    TAG_VALUE VARCHAR NOT NULL,
    DESCRIPTION VARCHAR,
    CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR DEFAULT CURRENT_USER(),
    PRIMARY KEY (TAG_ID),
    UNIQUE (TAG_DB, TAG_SCHEMA, TAG_NAME, TAG_VALUE)
);""",
    "USER_TAG_ASSIGNMENTS": f"""CREATE TABLE IF NOT EXISTS {FQN}.USER_TAG_ASSIGNMENTS (
    ASSIGNMENT_ID NUMBER AUTOINCREMENT,
    USER_ID NUMBER NOT NULL,
    USER_NAME VARCHAR NOT NULL,
    TAG_DB VARCHAR NOT NULL,
    TAG_SCHEMA VARCHAR NOT NULL,
    TAG_NAME VARCHAR NOT NULL,
    TAG_VALUE VARCHAR,
    ACTION VARCHAR NOT NULL DEFAULT 'SET',
    ASSIGNED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    ASSIGNED_BY VARCHAR DEFAULT CURRENT_USER(),
    PRIMARY KEY (ASSIGNMENT_ID)
);""",
    "SNOWFLAKE_BUDGET_REGISTRY": f"""CREATE TABLE IF NOT EXISTS {FQN}.SNOWFLAKE_BUDGET_REGISTRY (
    BUDGET_ID NUMBER AUTOINCREMENT,
    BUDGET_DB VARCHAR NOT NULL DEFAULT 'COCO_BUDGETS_DB',
    BUDGET_SCHEMA VARCHAR NOT NULL DEFAULT 'BUDGETS',
    BUDGET_NAME VARCHAR NOT NULL,
    CREDIT_QUOTA NUMBER(20,6) NOT NULL,
    DESCRIPTION VARCHAR,
    CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR DEFAULT CURRENT_USER(),
    PRIMARY KEY (BUDGET_ID),
    UNIQUE (BUDGET_DB, BUDGET_SCHEMA, BUDGET_NAME)
);""",
}

CONFIG_SEED_SQL = f"""MERGE INTO {FQN}.BUDGET_CONFIG tgt
USING (SELECT * FROM VALUES
    ('BUDGET_TIMEZONE','UTC'),('DEFAULT_PERIOD_TYPE','MONTHLY'),
    ('DEFAULT_WARNING_THRESHOLD_PCT','80'),('DEFAULT_USER_BASE_PERIOD_CREDITS','100'),
    ('ENABLE_PERSISTED_ROLLUPS','false'),('ENABLE_MODEL_DRILLDOWN','false'),
    ('ENFORCEMENT_ENABLED','false'),('ENFORCEMENT_ROLE','CORTEX_USER_ROLE'),
    ('DEFAULT_DAILY_CLI_LIMIT','-1'),('DEFAULT_DAILY_SNOWSIGHT_LIMIT','-1'),
    ('EMAIL_INTEGRATION','MY_EMAIL_INT'),('ALERT_RECIPIENTS',''),
    ('ALERT_ON_WARNING','true'),('ALERT_ON_OVER','true'),
    ('CREDIT_RATE_USD','2.00'),
    ('SLACK_ENABLED','false'),('SLACK_WEBHOOK_URL',''),
    ('DEFAULT_NATIVE_BUDGET_QUOTA','1000')
) AS src(CONFIG_KEY, CONFIG_VALUE)
ON tgt.CONFIG_KEY = src.CONFIG_KEY
WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE, UPDATED_AT)
VALUES (src.CONFIG_KEY, src.CONFIG_VALUE, CURRENT_TIMESTAMP());"""

RBAC_SQL = """USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS COCO_BUDGETS_OWNER
    COMMENT = 'Full app maintainer: owns DB objects, tags users, manages native AI budgets';
CREATE ROLE IF NOT EXISTS COCO_BUDGETS_APP_USER
    COMMENT = 'Budget manager: can edit budgets and run enforcement cycles';
CREATE ROLE IF NOT EXISTS COCO_BUDGETS_READER
    COMMENT = 'Read-only: dashboards and audit logs only';

GRANT ROLE COCO_BUDGETS_READER   TO ROLE COCO_BUDGETS_APP_USER;
GRANT ROLE COCO_BUDGETS_APP_USER TO ROLE COCO_BUDGETS_OWNER;
GRANT ROLE COCO_BUDGETS_OWNER    TO ROLE SYSADMIN;

GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE COCO_BUDGETS_OWNER;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE COCO_BUDGETS_OWNER;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE COCO_BUDGETS_OWNER;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE COCO_BUDGETS_APP_USER;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE COCO_BUDGETS_READER;
GRANT MANAGE USER TO ROLE COCO_BUDGETS_OWNER;
GRANT APPLY TAG ON ACCOUNT TO ROLE COCO_BUDGETS_OWNER;"""


current_role = get_current_role()
st.info(f"Current role: **{current_role}**")

st.subheader("Diagnostic Checks")

db_ok = check_database()
schema_ok = db_ok and check_schema()

table_status = {}
config_ok = False
if schema_ok:
    for t in REQUIRED_TABLES:
        table_status[t] = check_table(t)
    if table_status.get("BUDGET_CONFIG"):
        config_ok = check_config_seeded()

all_tables_ok = schema_ok and all(table_status.values())
all_ok = all_tables_ok and config_ok

if all_ok:
    st.success("All backend objects are healthy. No action needed.")

col1, col2 = st.columns([3, 1])

with col1:
    st.markdown("**Step 1: Database**")
with col2:
    st.markdown(":green[PASS]" if db_ok else ":red[MISSING]")

with col1:
    st.markdown("**Step 2: Schema**")
with col2:
    st.markdown(
        ":green[PASS]" if schema_ok
        else (":orange[SKIP]" if not db_ok else ":red[MISSING]")
    )

for t in REQUIRED_TABLES:
    with col1:
        st.markdown(f"**Table: {t}**")
    with col2:
        if not schema_ok:
            st.markdown(":orange[SKIP]")
        elif table_status.get(t):
            st.markdown(":green[PASS]")
        else:
            st.markdown(":red[MISSING]")

with col1:
    st.markdown("**Config Seed**")
with col2:
    if not table_status.get("BUDGET_CONFIG"):
        st.markdown(":orange[SKIP]")
    elif config_ok:
        st.markdown(":green[PASS]")
    else:
        st.markdown(":red[EMPTY]")

if all_ok:
    st.divider()
    st.subheader("RBAC Setup (Optional)")
    st.markdown(
        "For production deployments, create least-privilege roles so "
        "day-to-day users don't need ACCOUNTADMIN."
    )
    with st.expander("View RBAC SQL"):
        st.code(RBAC_SQL, language="sql")
    st.stop()

st.divider()
st.subheader("Fix Missing Objects")

st.markdown(
    "The steps below will create any missing backend objects. "
    "You can either run them directly (if your role has sufficient privileges) "
    "or copy the SQL and ask an admin to run it in a Snowsight worksheet."
)

if not db_ok:
    with st.expander("Step 1: Create Database", expanded=True):
        ddl = f"CREATE DATABASE IF NOT EXISTS {DB};"
        st.code(ddl, language="sql")
        if st.button("Run Step 1", key="run_db"):
            err = run_ddl(ddl.rstrip(";"))
            if err:
                st.error(f"Failed: {err}")
            else:
                st.success("Database created.")
                st.rerun()

if db_ok and not schema_ok:
    with st.expander("Step 2: Create Schema", expanded=True):
        ddl = f"CREATE SCHEMA IF NOT EXISTS {FQN};"
        st.code(ddl, language="sql")
        if st.button("Run Step 2", key="run_schema"):
            err = run_ddl(ddl.rstrip(";"))
            if err:
                st.error(f"Failed: {err}")
            else:
                st.success("Schema created.")
                st.rerun()

if schema_ok:
    missing_tables = [t for t in REQUIRED_TABLES if not table_status.get(t)]
    if missing_tables:
        with st.expander(
            f"Step 3: Create Tables ({len(missing_tables)} missing)",
            expanded=True,
        ):
            combined = "\n".join(TABLE_DDLS[t] for t in missing_tables)
            st.code(combined, language="sql")
            if st.button("Run Step 3 — Create All Missing Tables", key="run_tables"):
                errors = []
                for t in missing_tables:
                    err = run_ddl(TABLE_DDLS[t].rstrip(";"))
                    if err:
                        errors.append(f"{t}: {err}")
                if errors:
                    for e in errors:
                        st.error(e)
                else:
                    st.success(f"Created {len(missing_tables)} table(s).")
                    st.rerun()

    if all_tables_ok and not config_ok:
        with st.expander("Step 4: Seed Default Config", expanded=True):
            st.code(CONFIG_SEED_SQL, language="sql")
            if st.button("Run Step 4 — Seed Config", key="run_config"):
                err = run_ddl(CONFIG_SEED_SQL)
                if err:
                    st.error(f"Failed: {err}")
                else:
                    st.success("Config seeded with defaults.")
                    st.rerun()

st.divider()
st.subheader("Copy Full Setup SQL")
st.markdown(
    "If you cannot run the steps above directly, copy the complete SQL below "
    "and run it in a **Snowsight worksheet** as **ACCOUNTADMIN** (or a role with "
    "CREATE DATABASE privilege)."
)

full_sql_parts = [
    f"-- CoCo Budgets: Full Backend Setup",
    f"-- Run as ACCOUNTADMIN or COCO_BUDGETS_OWNER",
    f"",
    f"CREATE DATABASE IF NOT EXISTS {DB};",
    f"CREATE SCHEMA IF NOT EXISTS {FQN};",
    f"",
]
for t in REQUIRED_TABLES:
    full_sql_parts.append(TABLE_DDLS[t])
    full_sql_parts.append("")
full_sql_parts.append(CONFIG_SEED_SQL)

full_sql = "\n".join(full_sql_parts)

with st.expander("View Complete SQL"):
    st.code(full_sql, language="sql")

st.divider()
st.subheader("RBAC Setup (Optional)")
st.markdown(
    "For production deployments, create least-privilege roles. "
    "Run the SQL below as **ACCOUNTADMIN**."
)

with st.expander("View RBAC SQL"):
    st.code(RBAC_SQL, language="sql")
    st.markdown(
        "| Role | Purpose |\n"
        "|------|--------|\n"
        "| `COCO_BUDGETS_OWNER` | Full app maintainer — owns DB, manages users/tags |\n"
        "| `COCO_BUDGETS_APP_USER` | Budget manager — DML on all budget tables |\n"
        "| `COCO_BUDGETS_READER` | Read-only dashboard access |"
    )
