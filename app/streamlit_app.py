import streamlit as st
import sys, os
from streamlit.components.v1 import html as st_html

sys.path.insert(0, os.path.dirname(__file__))

from lib.connection import (
    get_session, run_ddl, FQN, is_local_mode, list_connections,
    get_active_connection_name, switch_connection,
    get_current_role, get_available_roles, use_role,
    get_connection_error,
)
from lib.config import LATENCY_BANNER

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
    f"""CREATE TABLE IF NOT EXISTS {FQN}.COST_CENTER_TAGS (
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
    )""",
    f"""CREATE TABLE IF NOT EXISTS {FQN}.USER_TAG_ASSIGNMENTS (
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
    )""",
    f"""CREATE TABLE IF NOT EXISTS {FQN}.SNOWFLAKE_BUDGET_REGISTRY (
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
    )""",
    f"""MERGE INTO {FQN}.BUDGET_CONFIG tgt
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
    VALUES (src.CONFIG_KEY, src.CONFIG_VALUE, CURRENT_TIMESTAMP())""",
]


def bootstrap():
    if st.session_state.get("_bootstrapped"):
        return True
    session = get_session()
    if session is None:
        return False
    errors = []
    for ddl in BOOTSTRAP_DDLS:
        err = run_ddl(ddl)
        if err:
            errors.append(err)
    if errors:
        st.session_state["_bootstrap_errors"] = errors
        return False
    st.session_state["_bootstrapped"] = True
    st.session_state.pop("_bootstrap_errors", None)
    return True


boot_ok = bootstrap()

dashboard = st.Page("pages/1_Dashboard.py", title="Dashboard", icon="📊", default=True)
user_budgets = st.Page("pages/2_User_Budgets.py", title="User Budgets", icon="👤")
account_budget = st.Page("pages/3_Account_Budget.py", title="Account Budget", icon="🏢")
enforcement = st.Page("pages/5_Enforcement.py", title="Enforcement & Controls", icon="🛡️")
ai_budgets = st.Page("pages/6_AI_Budgets.py", title="AI Budgets (Native)", icon="🏷️")
settings = st.Page("pages/4_Settings.py", title="Settings", icon="⚙️")

pg = st.navigation([dashboard, user_budgets, account_budget, enforcement, ai_budgets, settings])

with st.sidebar:
    st.title("CoCo Budgets")
    st.caption("Cortex Code Credit Budget Manager")

    conn_err = get_connection_error()

    if is_local_mode():
        connections = list_connections()
        if len(connections) >= 1:
            conn_names = list(connections.keys())
            current = get_active_connection_name()
            idx = conn_names.index(current) if current in conn_names else 0
            selected = st.selectbox(
                "Account Connection",
                conn_names,
                index=idx,
                format_func=lambda c: f"{c}  ({connections[c]})",
                help="Switch between Snowflake accounts defined in connections.toml",
            )
            if selected != current:
                switch_connection(selected)
                st.session_state["_bootstrapped"] = False
                st.session_state.pop("_available_roles", None)
                st.rerun()

    if conn_err:
        st.error(f"Connection failed: {conn_err}")
        st.info("Switch to a different connection above.")
    else:
        current_role = get_current_role()
        if current_role == "UNKNOWN":
            st.session_state.pop("_bootstrapped", None)
            st.session_state.pop("_available_roles", None)
            st.warning("Could not detect current role — connection may have dropped. Click Refresh Data to reconnect.")
        else:
            if "_available_roles" not in st.session_state:
                st.session_state["_available_roles"] = get_available_roles()
            roles = st.session_state["_available_roles"]
            if roles:
                idx = roles.index(current_role) if current_role in roles else 0
                selected_role = st.selectbox(
                    "Role",
                    roles,
                    index=idx,
                    help="Switch your active Snowflake role. COCO_BUDGETS_OWNER or ACCOUNTADMIN recommended.",
                )
                if selected_role != current_role:
                    err = use_role(selected_role)
                    if err:
                        st.error(f"Cannot switch role: {err}")
                    else:
                        st.session_state["_bootstrapped"] = False
                        st.session_state.pop("_available_roles", None)
                        st.rerun()
            if not boot_ok:
                boot_errors = st.session_state.get("_bootstrap_errors", [])
                with st.expander(f"⚠️ Bootstrap incomplete — role {current_role}", expanded=True):
                    st.warning(
                        "Some setup steps failed. Switch to **COCO_BUDGETS_OWNER** or **ACCOUNTADMIN** "
                        "to create the budget database and tables, then click **Refresh Data**."
                    )
                    if boot_errors:
                        for e in boot_errors[:3]:
                            st.caption(f"• {e}")

    st.divider()
    if st.button("🔄 Refresh Data"):
        from lib.config import clear_caches
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
- **Enforcement** — Automatically block Cortex Code access
  when a user exceeds their budget using Snowflake's native
  daily credit limit parameters.
- **AI Budgets (Native)** — Create Snowflake-native Budget
  objects scoped to teams via cost center tags. Tracks spend
  across AI Functions, Cortex Code, Cortex Agents, and
  Snowflake Intelligence.
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
4. **Enforcement** — Enable automatic blocking for
   over-budget users via native daily credit limits
5. **AI Budgets (Native)** — Tag users by cost center and
   create native Snowflake Budget objects for team-level
   AI spend tracking
6. **Settings** — Configure timezone, defaults, and
   view audit logs
""")

st_html(
    """
    <script>
    (function() {
        const INTERVAL_MS = 30000;
        if (window._stKeepAlive) clearInterval(window._stKeepAlive);
        window._stKeepAlive = setInterval(function() {
            fetch("/_stcore/health").catch(function(){});
        }, INTERVAL_MS);
    })();
    </script>
    """,
    height=0,
)

pg.run()
