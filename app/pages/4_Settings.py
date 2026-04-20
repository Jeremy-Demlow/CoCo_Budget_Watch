import streamlit as st
import pandas as pd

from lib.connection import run_query, FQN
from lib.config import get_config, clear_caches, LATENCY_BANNER
from lib.budget_service import save_config_batch

st.header("Settings")

tab_config, tab_audit, tab_freshness = st.tabs(["Configuration", "Audit Log", "Data Freshness"])

cfg = get_config()

with tab_config:
    st.subheader("Budget Configuration")
    st.caption("Global defaults applied when creating new budgets and throughout the app.")

    tz = st.text_input(
        "Budget Timezone", value=cfg.get("BUDGET_TIMEZONE", "UTC"), key="cfg_tz",
        help="Timezone used to calculate period boundaries (e.g., UTC, US/Eastern, US/Pacific)."
    )
    default_period = st.selectbox(
        "Default Period Type", ["MONTHLY", "WEEKLY", "QUARTERLY"],
        index=["MONTHLY","WEEKLY","QUARTERLY"].index(cfg.get("DEFAULT_PERIOD_TYPE", "MONTHLY")),
        key="cfg_period",
        help="How often budgets reset. MONTHLY is recommended for most teams."
    )
    default_threshold = st.number_input(
        "Default Warning Threshold %",
        value=int(cfg.get("DEFAULT_WARNING_THRESHOLD_PCT", "80")),
        min_value=0, max_value=100, key="cfg_thresh",
        help="Default percentage at which users are flagged as 'WARNING'. Applied when creating new budgets."
    )
    default_credits = st.number_input(
        "Default User Budget (credits)",
        value=float(cfg.get("DEFAULT_USER_BASE_PERIOD_CREDITS", "100")),
        min_value=0.0, step=10.0, key="cfg_credits",
        help="Default credit limit pre-filled when adding a new user budget."
    )
    enable_rollups = st.checkbox(
        "Enable Persisted Rollups",
        value=cfg.get("ENABLE_PERSISTED_ROLLUPS", "false").lower() == "true",
        key="cfg_rollups",
        help="Store periodic usage snapshots for historical analysis beyond the 365-day ACCOUNT_USAGE retention."
    )
    enable_drilldown = st.checkbox(
        "Enable Model Drilldown",
        value=cfg.get("ENABLE_MODEL_DRILLDOWN", "false").lower() == "true",
        key="cfg_drilldown",
        help="Show per-model token breakdown in detailed views. May increase query time."
    )

    st.divider()
    st.subheader("Credit-to-USD Rate")
    st.caption(
        "With **cross-region** enabled: **$2.00/credit**. "
        "Without cross-region: **$2.20/credit**."
    )
    credit_rate = st.number_input(
        "Credit Rate (USD per credit)",
        value=float(cfg.get("CREDIT_RATE_USD", "2.00")),
        min_value=0.01, step=0.10, format="%.2f", key="cfg_credit_rate",
        help="Set to 2.00 if cross-region is enabled, or 2.20 if not."
    )

    if st.button("Save Configuration", type="primary", key="save_cfg"):
        updates = {
            "BUDGET_TIMEZONE": tz,
            "DEFAULT_PERIOD_TYPE": default_period,
            "DEFAULT_WARNING_THRESHOLD_PCT": str(default_threshold),
            "DEFAULT_USER_BASE_PERIOD_CREDITS": str(default_credits),
            "ENABLE_PERSISTED_ROLLUPS": str(enable_rollups).lower(),
            "ENABLE_MODEL_DRILLDOWN": str(enable_drilldown).lower(),
            "CREDIT_RATE_USD": f"{credit_rate:.2f}",
        }
        errors = save_config_batch(updates)

        if errors:
            st.error("Some config updates failed:\n" + "\n".join(errors))
        else:
            st.success("Configuration saved.")
            st.rerun()

with tab_audit:
    st.subheader("Audit Log")
    st.caption("Every budget change, top-up, and enforcement action is logged here for accountability.")

    col_action, col_type, col_limit = st.columns(3)
    action_filter = col_action.text_input("Filter by action", key="audit_action")
    type_filter = col_type.selectbox("Target type", ["ALL", "USER", "ACCOUNT"], key="audit_type")
    row_limit = col_limit.number_input("Rows", value=100, min_value=10, max_value=1000, key="audit_limit")

    where_clauses = []
    if action_filter:
        safe_action = action_filter.replace("'", "''")
        where_clauses.append(f"ACTION ILIKE '%{safe_action}%'")
    if type_filter != "ALL":
        where_clauses.append(f"TARGET_TYPE = '{type_filter}'")

    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    audit_sql = (
        f"SELECT LOG_ID, ACTION, TARGET_TYPE, TARGET_USER_ID, "
        f"OLD_VALUE, NEW_VALUE, NOTES, PERFORMED_BY, PERFORMED_AT "
        f"FROM {FQN}.BUDGET_AUDIT_LOG {where_sql} "
        f"ORDER BY PERFORMED_AT DESC LIMIT {row_limit}"
    )
    audit_df, err = run_query(audit_sql)
    if err:
        st.error(f"Audit query failed: {err}")
    elif audit_df.empty:
        st.info("No audit log entries yet. Actions will appear here as you create budgets and make changes.")
    else:
        st.dataframe(audit_df, use_container_width=True, hide_index=True)

with tab_freshness:
    st.subheader("Data Freshness")

    st.markdown("""
This app reads from `SNOWFLAKE.ACCOUNT_USAGE` views which have a built-in
latency of **up to ~1 hour**. This is a Snowflake platform behavior and cannot be reduced.

| What this means | Impact |
|:---|:---|
| A request made 30 minutes ago | May not appear in the data yet |
| Budget enforcement checks | Act on data that could be up to 1 hour old |
| Real-time blocking is not possible | Use **Model Allowlist** for instant cost control |

**Retention:** Usage views keep the last **365 days** of data.
Enable **Persisted Rollups** in Configuration to archive beyond that.
""")

    freshness_sql = f"""
    SELECT
        'CLI' AS SOURCE,
        MAX(USAGE_TIME) AS LATEST_RECORD,
        TIMESTAMPDIFF('MINUTE', MAX(USAGE_TIME), CURRENT_TIMESTAMP()) AS MINUTES_AGO,
        COUNT(*) AS TOTAL_RECORDS
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY
    UNION ALL
    SELECT
        'SNOWSIGHT' AS SOURCE,
        MAX(USAGE_TIME) AS LATEST_RECORD,
        TIMESTAMPDIFF('MINUTE', MAX(USAGE_TIME), CURRENT_TIMESTAMP()) AS MINUTES_AGO,
        COUNT(*) AS TOTAL_RECORDS
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY
    """
    fresh_df, fresh_err = run_query(freshness_sql)
    if fresh_err:
        st.error(f"Freshness query failed: {fresh_err}")
    elif not fresh_df.empty:
        st.dataframe(
            fresh_df, use_container_width=True, hide_index=True,
            column_config={
                "SOURCE": st.column_config.TextColumn("Source"),
                "LATEST_RECORD": st.column_config.DatetimeColumn("Latest Record"),
                "MINUTES_AGO": st.column_config.NumberColumn("Minutes Ago", format="%d",
                    help="How many minutes since the most recent record in this source."),
                "TOTAL_RECORDS": st.column_config.NumberColumn("Total Records", format="%d"),
            }
        )
    else:
        st.info("No usage data found.")
