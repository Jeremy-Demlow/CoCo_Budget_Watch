import streamlit as st
import altair as alt

from lib.db import (
    run_query, run_ddl, get_account_budget, get_config,
    get_model_breakdown, log_audit, clear_caches, FQN, LATENCY_BANNER,
    get_account_usage_credits, get_account_user_breakdown,
)
from lib.time import get_period_bounds, format_period

st.header("Account Budget")

st.markdown(
    "Set an **account-wide credit cap** for all Cortex Code usage. "
    "This is separate from per-user budgets — think of it as a global spending guardrail. "
    "The progress bar below shows how close the entire account is to the limit."
)

cfg = get_config()
tz = cfg.get("BUDGET_TIMEZONE", "UTC")
period_type = cfg.get("DEFAULT_PERIOD_TYPE", "MONTHLY")
credit_rate = float(cfg.get("CREDIT_RATE_USD", "2.00"))
p_start, p_end = get_period_bounds(period_type, tz)
ps = p_start.strftime("%Y-%m-%d %H:%M:%S")
pe = p_end.strftime("%Y-%m-%d %H:%M:%S")
st.caption(f"Current period: **{format_period(p_start, p_end)}** ({tz})")

acct_df = get_account_budget()

col_set, col_status = st.columns([1, 2])

with col_set:
    st.subheader("Set Account Budget")
    st.caption("Define the maximum total credits for the whole account this period.")

    current_budget = float(acct_df.iloc[0]["BASE_PERIOD_CREDITS"]) if not acct_df.empty else 0.0
    current_threshold = int(acct_df.iloc[0]["WARNING_THRESHOLD_PCT"]) if not acct_df.empty else 80

    new_budget = st.number_input(
        "Account Period Credit Budget",
        value=current_budget if current_budget > 0 else 1000.0,
        min_value=0.0, step=100.0, key="acct_budget",
        help="Total credit limit for all users combined this period."
    )
    new_threshold = st.slider(
        "Warning Threshold %", 0, 100, current_threshold, key="acct_thresh",
        help="Show a warning when account usage reaches this percentage of the budget."
    )
    acct_period = st.selectbox("Period", ["MONTHLY", "WEEKLY", "QUARTERLY"],
                               index=["MONTHLY","WEEKLY","QUARTERLY"].index(period_type),
                               key="acct_period")

    if st.button("Save Account Budget", type="primary", key="save_acct"):
        run_ddl(
            f"UPDATE {FQN}.ACCOUNT_BUDGET SET "
            f"IS_ACTIVE = FALSE, EFFECTIVE_END = CURRENT_TIMESTAMP() "
            f"WHERE IS_ACTIVE = TRUE"
        )
        safe_period = acct_period.replace("'", "''")
        err = run_ddl(
            f"INSERT INTO {FQN}.ACCOUNT_BUDGET "
            f"(IS_ACTIVE, BASE_PERIOD_CREDITS, PERIOD_TYPE, WARNING_THRESHOLD_PCT) "
            f"VALUES (TRUE, {float(new_budget)}, '{safe_period}', {int(new_threshold)})"
        )
        if err:
            st.error(f"Failed: {err}")
        else:
            log_audit("UPDATE", "ACCOUNT", old_value=current_budget, new_value=new_budget)
            clear_caches()
            st.success("Account budget updated.")
            st.rerun()

    st.divider()
    st.subheader("Account Top-up")
    st.caption("Add extra credits to the account budget for this period without changing the base limit.")
    tu_credits = st.number_input("Additional Credits", value=500.0, min_value=0.01,
                                 step=100.0, key="acct_topup_credits")
    tu_notes = st.text_input("Notes", key="acct_topup_notes")

    if st.button("Grant Account Top-up", key="acct_topup_btn"):
        notes_val = tu_notes.replace("'", "''") if tu_notes else ""
        err = run_ddl(
            f"INSERT INTO {FQN}.BUDGET_TOPUPS "
            f"(TARGET_TYPE, CREDITS, EFFECTIVE_START, EFFECTIVE_END, NOTES) "
            f"VALUES ('ACCOUNT', {tu_credits}, '{ps}', '{pe}', '{notes_val}')"
        )
        if err:
            st.error(f"Failed: {err}")
        else:
            log_audit("TOPUP", "ACCOUNT", new_value=tu_credits, notes=tu_notes)
            clear_caches()
            st.success(f"Granted {tu_credits} account credits.")
            st.rerun()

with col_status:
    st.subheader("Account Usage This Period")

    metrics_placeholder = st.empty()
    with metrics_placeholder.container():
        mc = st.columns(4)
        for i, label in enumerate(["Effective Budget", "Used", "Remaining", "Est. Cost (USD)"]):
            mc[i].metric(label, "---")

    with st.spinner("Loading account usage..."):
        total_used, total_reqs = get_account_usage_credits(ps, pe)

        acct_topup_sql = f"""
        SELECT COALESCE(SUM(CREDITS), 0) AS TOPUP_CREDITS
        FROM {FQN}.BUDGET_TOPUPS
        WHERE TARGET_TYPE = 'ACCOUNT'
          AND EFFECTIVE_START < '{pe}' AND EFFECTIVE_END > '{ps}'
        """
        topup_df, _ = run_query(acct_topup_sql)
        topup_credits = float(topup_df.iloc[0]["TOPUP_CREDITS"]) if not topup_df.empty else 0.0

    effective = new_budget + topup_credits
    pct = (total_used / effective * 100) if effective > 0 else 0

    with metrics_placeholder.container():
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Effective Budget", f"{effective:,.2f}",
                  help="Base budget + any top-ups for this period.")
        m2.metric("Used", f"{total_used:,.2f}",
                  help="Total credits consumed by all users this period.")
        m3.metric("Remaining", f"{effective - total_used:,.2f}",
                  help="Credits left before hitting the account limit.")
        m4.metric("Est. Cost (USD)", f"${total_used * credit_rate:,.2f}",
                  help=f"Estimated dollar cost at ${credit_rate:.2f} per credit. Change in Settings.")

    m5, m6 = st.columns(2)
    m5.metric("Requests", f"{total_reqs:,}",
              help="Total Cortex Code requests across all users.")
    m6.metric("Top-ups", f"{topup_credits:,.2f}",
              help="Extra credits added this period via top-ups.")

    progress_val = min(pct / 100, 1.0)
    st.progress(progress_val, text=f"{pct:.1f}% of account budget used")

    if pct >= 100:
        st.error("Account is **OVER** budget! Consider enabling Enforcement to restrict access.")
    elif pct >= new_threshold:
        st.warning("Account is approaching the budget limit.")
    elif pct > 0:
        st.success("Account spending is within budget.")

    st.divider()

    tab_by_user, tab_by_model = st.tabs(["Spend by User", "Spend by Model"])

    with tab_by_user:
        st.caption("Which users are consuming the most account-level credits?")
        with st.spinner("Loading user breakdown..."):
            bd_df = get_account_user_breakdown(ps, pe)
        if not bd_df.empty:
            bd_df["CREDITS"] = bd_df["CREDITS"].astype(float)
            chart = (
                alt.Chart(bd_df)
                .mark_bar()
                .encode(
                    x=alt.X("USER_NAME:N", sort="-y", title="User"),
                    y=alt.Y("CREDITS:Q", title="Credits"),
                    tooltip=["USER_NAME",
                              alt.Tooltip("CREDITS:Q", format=",.4f"),
                              alt.Tooltip("REQUESTS:Q", format=",")],
                )
                .properties(height=300)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No usage data for breakdown.")

    with tab_by_model:
        st.caption("Which AI models are driving account-level costs?")
        with st.spinner("Loading model breakdown..."):
            model_df = get_model_breakdown(ps, pe)
        if not model_df.empty:
            model_agg = (
                model_df.groupby("MODEL")
                .agg(CREDITS=("CREDITS", "sum"), REQUESTS=("REQUESTS", "sum"))
                .reset_index()
                .sort_values("CREDITS", ascending=False)
            )
            pie = (
                alt.Chart(model_agg)
                .mark_arc(innerRadius=50)
                .encode(
                    theta=alt.Theta("CREDITS:Q"),
                    color=alt.Color("MODEL:N"),
                    tooltip=["MODEL",
                              alt.Tooltip("CREDITS:Q", format=",.4f"),
                              alt.Tooltip("REQUESTS:Q", format=",")],
                )
                .properties(height=300)
            )
            st.altair_chart(pie, use_container_width=True)
            st.dataframe(
                model_agg.style.format({"CREDITS": "{:,.4f}", "REQUESTS": "{:,}"}),
                use_container_width=True, hide_index=True,
            )
        else:
            st.info("No model-level data available.")
