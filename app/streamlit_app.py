import streamlit as st
import sys, os

sys.path.insert(0, os.path.dirname(__file__))

from lib.db import get_session, run_ddl, FQN, LATENCY_BANNER

st.set_page_config(
    page_title="CoCo Budgets",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded",
)

BOOTSTRAP_DDLS = [
    f"CREATE DATABASE IF NOT EXISTS COCO_BUDGETS_DB",
    f"CREATE SCHEMA IF NOT EXISTS {FQN}",
    f"""CREATE TABLE IF NOT EXISTS {FQN}.USER_BUDGETS (
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
    )""",
    f"""CREATE TABLE IF NOT EXISTS {FQN}.ACCOUNT_BUDGET (
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
    )""",
    f"""CREATE TABLE IF NOT EXISTS {FQN}.BUDGET_TOPUPS (
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
    )""",
    f"""CREATE TABLE IF NOT EXISTS {FQN}.BUDGET_AUDIT_LOG (
        LOG_ID NUMBER AUTOINCREMENT,
        ACTION VARCHAR NOT NULL,
        TARGET_TYPE VARCHAR NOT NULL,
        TARGET_USER_ID NUMBER,
        OLD_VALUE NUMBER(20,6),
        NEW_VALUE NUMBER(20,6),
        NOTES VARCHAR,
        PERFORMED_BY VARCHAR DEFAULT CURRENT_USER(),
        PERFORMED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
    )""",
    f"""CREATE TABLE IF NOT EXISTS {FQN}.BUDGET_CONFIG (
        CONFIG_KEY VARCHAR NOT NULL,
        CONFIG_VALUE VARCHAR,
        UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (CONFIG_KEY)
    )""",
    f"""CREATE TABLE IF NOT EXISTS {FQN}.ENFORCEMENT_LOG (
        LOG_ID NUMBER AUTOINCREMENT,
        ACTION VARCHAR NOT NULL,
        USER_ID NUMBER,
        USER_NAME VARCHAR,
        REASON VARCHAR,
        PERFORMED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
    )""",
    f"""CREATE TABLE IF NOT EXISTS {FQN}.ALERT_STATE (
        ALERT_ID NUMBER AUTOINCREMENT,
        USER_ID NUMBER NOT NULL,
        ALERT_TYPE VARCHAR NOT NULL,
        PERIOD_KEY VARCHAR NOT NULL,
        SENT_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
        CONSTRAINT UQ_ALERT UNIQUE (USER_ID, ALERT_TYPE, PERIOD_KEY)
    )""",
    f"""MERGE INTO {FQN}.BUDGET_CONFIG tgt
    USING (SELECT * FROM VALUES
        ('BUDGET_TIMEZONE','UTC'),('DEFAULT_PERIOD_TYPE','MONTHLY'),
        ('DEFAULT_WARNING_THRESHOLD_PCT','80'),('DEFAULT_USER_BASE_PERIOD_CREDITS','100'),
        ('ENABLE_PERSISTED_ROLLUPS','false'),('ENABLE_MODEL_DRILLDOWN','false'),
        ('ENFORCEMENT_ENABLED','false'),('ENFORCEMENT_ROLE','CORTEX_USER_ROLE'),
        ('EMAIL_INTEGRATION','MY_EMAIL_INT'),('ALERT_RECIPIENTS',''),
        ('ALERT_ON_WARNING','true'),('ALERT_ON_OVER','true'),
        ('CREDIT_RATE_USD','2.00'),
        ('SLACK_ENABLED','false'),('SLACK_WEBHOOK_URL','')
    ) AS src(CONFIG_KEY, CONFIG_VALUE)
    ON tgt.CONFIG_KEY = src.CONFIG_KEY
    WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE, UPDATED_AT)
    VALUES (src.CONFIG_KEY, src.CONFIG_VALUE, CURRENT_TIMESTAMP())""",
]


def bootstrap():
    if st.session_state.get("_bootstrapped"):
        return
    get_session()
    for ddl in BOOTSTRAP_DDLS:
        err = run_ddl(ddl)
        if err:
            st.error(f"Bootstrap error: {err}")
            st.stop()
    st.session_state["_bootstrapped"] = True


bootstrap()

dashboard = st.Page("pages/1_Dashboard.py", title="Dashboard", icon="📊", default=True)
user_budgets = st.Page("pages/2_User_Budgets.py", title="User Budgets", icon="👤")
account_budget = st.Page("pages/3_Account_Budget.py", title="Account Budget", icon="🏢")
enforcement = st.Page("pages/5_Enforcement.py", title="Enforcement & Controls", icon="🛡️")
settings = st.Page("pages/4_Settings.py", title="Settings", icon="⚙️")

pg = st.navigation([dashboard, user_budgets, account_budget, enforcement, settings])

with st.sidebar:
    st.title("CoCo Budgets")
    st.caption("Cortex Code Credit Budget Manager")
    st.divider()
    if st.button("🔄 Refresh Data"):
        from lib.db import clear_caches
        clear_caches()
        st.rerun()

    with st.expander("About This App", expanded=False):
        st.markdown("""
**CoCo Budgets** helps you monitor and control
[Cortex Code](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-code)
credit spending across your Snowflake account.

**Key Concepts:**
- **Credits** — Cortex Code charges credits based on
  token usage (input, output, cache) at model-specific rates.
- **User Budgets** — Set per-user monthly credit limits
  with warning thresholds.
- **Account Budget** — Set an overall account-level cap.
- **Enforcement** — Automatically revoke Cortex AI access
  when a user exceeds their budget.
- **Model Allowlist** — Restrict which AI models are
  available to control costs.
- **Email Alerts** — Get notified when users approach
  or exceed budgets.

**Data Source:** `SNOWFLAKE.ACCOUNT_USAGE` views
(up to ~1 hour lag).
""")

    with st.expander("Quick Start Guide", expanded=False):
        st.markdown("""
1. **Dashboard** — See current spend across all users
2. **User Budgets** — Add budgets for each user
   (or bulk-onboard everyone)
3. **Account Budget** — Set an account-wide credit limit
4. **Enforcement** — Enable automatic access revocation
   for over-budget users
5. **Settings** — Configure timezone, defaults, and
   view audit logs
""")

pg.run()
