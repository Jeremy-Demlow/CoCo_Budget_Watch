import streamlit as st
import altair as alt
import pandas as pd
from datetime import datetime, timedelta

from lib.connection import run_query, FQN
from lib.config import get_config, LATENCY_BANNER
from lib.usage_queries import (
    get_coco_active_users, get_usage_by_user, get_daily_trend,
    get_model_breakdown, get_all_users_spend, get_daily_cumulative_spend,
    get_account_budget, get_cache_efficiency, get_output_ratio,
    get_ai_services_context, get_model_token_type_breakdown,
    get_rolling_24h_spend, get_new_user_onboarding,
)
from lib.time import get_period_bounds, format_period

st.header("Dashboard")

cfg = get_config()
tz = cfg.get("BUDGET_TIMEZONE", "UTC")
period_type = cfg.get("DEFAULT_PERIOD_TYPE", "MONTHLY")
credit_rate = float(cfg.get("CREDIT_RATE_USD", "2.00"))

p_start_default, p_end_default = get_period_bounds(period_type, tz)

range_col1, range_col2 = st.columns([1, 3])
with range_col1:
    date_mode = st.selectbox(
        "Period",
        ["Current Period", "Last 7 Days", "Last 30 Days", "Last 90 Days", "Custom Range"],
        key="dash_date_mode",
        help="Current Period uses your configured budget period. Custom Range lets you pick any dates."
    )
with range_col2:
    if date_mode == "Custom Range":
        today = datetime.now().date()
        dr = st.date_input(
            "Date Range",
            value=(today - timedelta(days=30), today + timedelta(days=1)),
            max_value=today + timedelta(days=1),
            key="dash_custom_range",
        )
        if isinstance(dr, (list, tuple)) and len(dr) == 2:
            p_start = datetime.combine(dr[0], datetime.min.time())
            p_end = datetime.combine(dr[1], datetime.min.time())
        else:
            p_start, p_end = p_start_default, p_end_default
    elif date_mode == "Last 7 Days":
        p_end = datetime.now()
        p_start = p_end - timedelta(days=7)
        st.caption(f"{format_period(p_start, p_end)}")
    elif date_mode == "Last 30 Days":
        p_end = datetime.now()
        p_start = p_end - timedelta(days=30)
        st.caption(f"{format_period(p_start, p_end)}")
    elif date_mode == "Last 90 Days":
        p_end = datetime.now()
        p_start = p_end - timedelta(days=90)
        st.caption(f"{format_period(p_start, p_end)}")
    else:
        p_start, p_end = p_start_default, p_end_default
        st.caption(f"Current period: **{format_period(p_start, p_end)}** ({tz})")

ps = p_start.strftime("%Y-%m-%d %H:%M:%S")
pe = p_end.strftime("%Y-%m-%d %H:%M:%S")

is_current_period = (date_mode == "Current Period")

with st.spinner("Loading usage data..."):
    if is_current_period:
        budget_df = get_all_users_spend(ps, pe)
        budget_df = budget_df[budget_df["STATUS"] != "NO BUDGET"] if not budget_df.empty else budget_df
    else:
        budget_df = pd.DataFrame()
    active_users_df = get_coco_active_users(ps, pe)

has_budgets = not budget_df.empty
has_usage = not active_users_df.empty

if not has_budgets and not has_usage:
    if is_current_period:
        st.info(
            "**Welcome to CoCo Budgets!** This app monitors and controls Cortex Code "
            "credit spending across your Snowflake account.\n\n"
            "**To get started:**\n"
            "1. Go to **User Budgets** to assign credit limits to users\n"
            "2. Go to **Account Budget** to set an account-wide cap\n"
            "3. Once budgets are set, this dashboard will show spending vs. limits\n\n"
            "Data comes from `SNOWFLAKE.ACCOUNT_USAGE` views (up to ~1 hour lag)."
        )
    else:
        st.info("No usage data found for the selected date range.")
    st.stop()

if not has_budgets and has_usage and is_current_period:
    st.warning(
        "**Users are spending credits but no budgets are configured.** "
        "Go to **User Budgets** to set limits, or **Account Budget** "
        "for an account-wide cap."
    )
if not is_current_period:
    st.info("Viewing historical data. Budget status columns reflect the current period only.")

filter_col1, filter_col2 = st.columns([3, 2])
with filter_col1:
    user_options = {}
    if has_usage:
        for _, r in active_users_df.iterrows():
            label = f"{r['USER_NAME']} — {r['TOTAL_CREDITS']:.2f} credits"
            user_options[label] = r["USER_ID"]
    selected_labels = st.multiselect(
        "Filter by user",
        options=list(user_options.keys()),
        placeholder="All users",
    )
with filter_col2:
    source_filter = st.multiselect(
        "Source", ["CLI", "SNOWSIGHT"], default=["CLI", "SNOWSIGHT"],
        help="CLI = Cortex Code desktop/terminal, SNOWSIGHT = browser-based Cortex Code"
    )
    if not source_filter:
        source_filter = ["CLI", "SNOWSIGHT"]

selected_user_ids = tuple(user_options[l] for l in selected_labels) if selected_labels else None

if has_usage:
    if selected_user_ids:
        filtered = active_users_df[active_users_df["USER_ID"].isin(selected_user_ids)]
    else:
        filtered = active_users_df
    total_credits = filtered["TOTAL_CREDITS"].sum()
    total_requests = int(filtered["REQUEST_COUNT"].sum())
    total_tokens = int(filtered["TOTAL_TOKENS"].sum()) if "TOTAL_TOKENS" in filtered.columns else 0
    active_count = len(filtered)
    avg_per_req = total_credits / total_requests if total_requests > 0 else 0
else:
    total_credits = 0
    total_requests = 0
    total_tokens = 0
    active_count = 0
    avg_per_req = 0

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Credits", f"{total_credits:,.2f}",
          help="Sum of all Cortex Code credits consumed this period, calculated from token-level pricing.")
k2.metric("Est. Cost (USD)", f"${total_credits * credit_rate:,.2f}",
          help=f"Estimated dollar cost at ${credit_rate:.2f} per credit. Change in Settings.")
k3.metric("Total Tokens", f"{total_tokens:,}",
          help="Total tokens processed (input + output + cache). Reflects raw LLM volume.")
k4.metric("Requests", f"{total_requests:,}",
          help="Number of Cortex Code requests (prompts) sent this period.")

k5, k6, k7, k8 = st.columns(4)
k5.metric("Active Users", active_count,
          help="Users who have made at least one Cortex Code request this period.")
k6.metric("Avg Credits/Req", f"{avg_per_req:.4f}",
          help="Average credit cost per request. Higher values may indicate expensive model usage.")

if has_budgets:
    over_count = int((budget_df["STATUS"] == "OVER").sum())
    k7.metric("Over Budget", over_count, delta=None,
              delta_color="inverse" if over_count > 0 else "off",
              help="Users whose spending exceeds their budget limit.")
else:
    k7.metric("Over Budget", "—" if is_current_period else "N/A",
              help="No budgets configured." if is_current_period else "Budget status only available for current period.")

if is_current_period and total_credits > 0:
    now = datetime.now(p_start.tzinfo) if p_start.tzinfo else datetime.now()
    days_elapsed = max((now - p_start).days, 1)
    days_in_month = (p_end - p_start).days or 30
    projected = total_credits / days_elapsed * days_in_month
    k8.metric("Projected Monthly", f"{projected:,.2f} cr",
              help=f"Projected spend: {total_credits:.2f} credits over {days_elapsed} days → {days_in_month}-day projection.")
else:
    k8.metric("Projected Monthly", "—",
              help="Available for current period only.")

if is_current_period:
    try:
        ai_ctx = get_ai_services_context()
        if ai_ctx["total"] > 0:
            coco_metered = ai_ctx["coco"]
            coco_pct = (coco_metered / ai_ctx["total"] * 100)
            ai1, ai2, ai3, _ = st.columns(4)
            ai1.metric("Total AI Credits (MTD)", f"{ai_ctx['total']:,.2f} cr",
                       help="All AI-related service credits (AI_SERVICES + Cortex Code + Cortex Agents) from METERING_DAILY_HISTORY.")
            ai2.metric("CoCo % of AI Spend", f"{coco_pct:.1f}%",
                       help="Cortex Code (CLI + Snowsight) as a percentage of all AI service credits. Both from METERING_DAILY_HISTORY.")
            ai3.metric("CoCo Credits (Metered)", f"{coco_metered:,.2f} cr",
                       help="Cortex Code credits from METERING_DAILY_HISTORY. May differ slightly from token-level calculation above.")
    except Exception:
        pass

if has_budgets:
    over_count = int((budget_df["STATUS"] == "OVER").sum())
    if over_count > 0:
        over_names = budget_df[budget_df["STATUS"] == "OVER"]["USER_NAME"].tolist()
        st.error(
            f"**{over_count} user(s) over budget:** {', '.join(over_names)}. "
            f"Go to **Enforcement & Controls** to revoke access, or see **All Users & Spend** for details."
        )
    warn_count = int((budget_df["STATUS"] == "WARNING").sum())
    if warn_count > 0:
        warn_names = budget_df[budget_df["STATUS"] == "WARNING"]["USER_NAME"].tolist()
        st.warning(
            f"**{warn_count} user(s) approaching budget:** {', '.join(warn_names)}"
        )

st.divider()

tabs_list = ["By User", "By Model", "Trends", "Efficiency", "Rolling 24h"]
if is_current_period:
    tabs_list += ["All Users & Spend", "Budget Status"]
tabs = st.tabs(tabs_list)


@st.fragment
def render_by_user_tab():
    st.caption("Credit usage broken down by user and source (CLI vs Snowsight).")
    with st.spinner("Loading user breakdown..."):
        usage_df = get_usage_by_user(ps, pe, selected_user_ids)
    if not usage_df.empty:
        source_predicate = set(source_filter)
        usage_df = usage_df[usage_df["SOURCE"].isin(source_predicate)]
        usage_df = usage_df[usage_df["CREDITS"] > 0]

        user_summary = (
            usage_df.groupby("USER_NAME")
            .agg(CREDITS=("CREDITS", "sum"), REQUESTS=("REQUESTS", "sum"))
            .reset_index()
            .sort_values("CREDITS", ascending=False)
        )

        sources_present = sorted(usage_df["SOURCE"].unique().tolist())
        color_map = {"CLI": "#1f77b4", "SNOWSIGHT": "#ff7f0e"}

        chart = (
            alt.Chart(usage_df)
            .mark_bar()
            .encode(
                x=alt.X("USER_NAME:N", sort="-y", title="User"),
                y=alt.Y("CREDITS:Q", title="Credits"),
                color=alt.Color("SOURCE:N", scale=alt.Scale(
                    domain=sources_present,
                    range=[color_map.get(s, "#999") for s in sources_present]
                )),
                tooltip=["USER_NAME", "SOURCE",
                          alt.Tooltip("CREDITS:Q", format=",.4f"),
                          alt.Tooltip("REQUESTS:Q", format=",")],
            )
            .properties(height=350)
        )
        st.altair_chart(chart, use_container_width=True)

        st.dataframe(
            user_summary.style.format({"CREDITS": "{:,.4f}", "REQUESTS": "{:,}"}),
            hide_index=True,
        )
    else:
        st.info("No usage data for the selected filters.")


@st.fragment
def render_by_model_tab():
    st.caption("Which AI models are consuming the most credits? Expensive models drive costs up fast.")
    with st.spinner("Loading model breakdown..."):
        model_df = get_model_breakdown(ps, pe, selected_user_ids)
    if not model_df.empty:
        model_summary = (
            model_df.groupby("MODEL")
            .agg(CREDITS=("CREDITS", "sum"), REQUESTS=("REQUESTS", "sum"),
                 TOKENS=("TOTAL_TOKENS", "sum"))
            .reset_index()
            .sort_values("CREDITS", ascending=False)
        )

        model_chart = (
            alt.Chart(model_summary)
            .mark_bar()
            .encode(
                x=alt.X("MODEL:N", sort="-y", title="Model"),
                y=alt.Y("CREDITS:Q", title="Credits"),
                color=alt.Color("MODEL:N", legend=None),
                tooltip=["MODEL",
                          alt.Tooltip("CREDITS:Q", format=",.4f"),
                          alt.Tooltip("REQUESTS:Q", format=","),
                          alt.Tooltip("TOKENS:Q", format=",")],
            )
            .properties(height=300)
        )
        st.altair_chart(model_chart, use_container_width=True)

        st.subheader("Cost by Token Type")
        st.caption(
            "Output tokens cost 5-8x more than input tokens. "
            "This chart shows where model costs actually come from."
        )
        with st.spinner("Loading token type breakdown..."):
            tt_df = get_model_token_type_breakdown(ps, pe, selected_user_ids)
        if not tt_df.empty:
            tt_melted = tt_df.melt(
                id_vars=["MODEL"],
                value_vars=["INPUT_CREDITS", "OUTPUT_CREDITS", "CACHE_WRITE_CREDITS", "CACHE_READ_CREDITS"],
                var_name="TOKEN_TYPE", value_name="CREDITS",
            )
            tt_melted["TOKEN_TYPE"] = tt_melted["TOKEN_TYPE"].str.replace("_CREDITS", "")
            type_colors = {"INPUT": "#4e79a7", "OUTPUT": "#e15759", "CACHE_WRITE": "#f28e2b", "CACHE_READ": "#76b7b2"}
            tt_chart = (
                alt.Chart(tt_melted)
                .mark_bar()
                .encode(
                    x=alt.X("MODEL:N", sort="-y", title="Model"),
                    y=alt.Y("CREDITS:Q", title="Credits", stack=True),
                    color=alt.Color("TOKEN_TYPE:N",
                        scale=alt.Scale(domain=list(type_colors.keys()), range=list(type_colors.values())),
                        title="Token Type"),
                    tooltip=["MODEL", "TOKEN_TYPE", alt.Tooltip("CREDITS:Q", format=",.4f")],
                )
                .properties(height=300)
            )
            st.altair_chart(tt_chart, use_container_width=True)

        user_model = (
            model_df.groupby(["USER_NAME", "MODEL"])
            .agg(CREDITS=("CREDITS", "sum"))
            .reset_index()
        )
        if len(user_model["USER_NAME"].unique()) > 1:
            heatmap = (
                alt.Chart(user_model)
                .mark_rect()
                .encode(
                    x=alt.X("MODEL:N", title="Model"),
                    y=alt.Y("USER_NAME:N", title="User"),
                    color=alt.Color("CREDITS:Q", scale=alt.Scale(scheme="blues"),
                                    title="Credits"),
                    tooltip=["USER_NAME", "MODEL",
                              alt.Tooltip("CREDITS:Q", format=",.4f")],
                )
                .properties(height=max(200, len(user_model["USER_NAME"].unique()) * 30))
            )
            st.altair_chart(heatmap, use_container_width=True)

        st.dataframe(
            model_summary.style.format({
                "CREDITS": "{:,.4f}", "REQUESTS": "{:,}", "TOKENS": "{:,.0f}"
            }),
            use_container_width=True, hide_index=True,
        )
    else:
        st.info("No model-level data available.")


@st.fragment
def render_trends_tab():
    st.caption("Daily credit consumption over time. Use this to spot usage spikes or growing trends.")
    default_days = max(7, min(90, (p_end - p_start).days)) if date_mode != "Current Period" else 30
    trend_days = st.slider("Days back", 7, 365, default_days, key="trend_days")
    with st.spinner("Loading trend data..."):
        trend_df = get_daily_trend(trend_days, selected_user_ids)
    if not trend_df.empty:
        trend_df = trend_df[trend_df["SOURCE"].isin(source_filter)]
        trend_df = trend_df[trend_df["DAILY_CREDITS"] > 0]

        daily_totals = (
            trend_df.groupby("USAGE_DATE")
            .agg(DAILY_CREDITS=("DAILY_CREDITS", "sum"))
            .reset_index()
        )
        trend_line = (
            alt.Chart(daily_totals)
            .mark_area(opacity=0.3, line=True)
            .encode(
                x=alt.X("USAGE_DATE:T", title="Date"),
                y=alt.Y("DAILY_CREDITS:Q", title="Credits"),
                tooltip=["USAGE_DATE:T",
                          alt.Tooltip("DAILY_CREDITS:Q", format=",.4f")],
            )
            .properties(height=250)
        )
        st.altair_chart(trend_line, use_container_width=True)

        if len(trend_df["USER_NAME"].dropna().unique()) > 1:
            user_trend = (
                trend_df.groupby(["USAGE_DATE", "USER_NAME"])
                .agg(DAILY_CREDITS=("DAILY_CREDITS", "sum"))
                .reset_index()
            )
            stacked = (
                alt.Chart(user_trend)
                .mark_bar()
                .encode(
                    x=alt.X("USAGE_DATE:T", title="Date"),
                    y=alt.Y("DAILY_CREDITS:Q", title="Credits", stack=True),
                    color="USER_NAME:N",
                    tooltip=["USAGE_DATE:T", "USER_NAME",
                              alt.Tooltip("DAILY_CREDITS:Q", format=",.4f")],
                )
                .properties(height=250)
            )
            st.altair_chart(stacked, use_container_width=True)
    else:
        st.info("No trend data available.")

    st.divider()
    st.subheader("New User Adoption")
    st.caption("When users first started using Cortex Code. Tracks rollout adoption velocity.")
    with st.spinner("Loading onboarding data..."):
        onboard_df = get_new_user_onboarding(trend_days)
    if not onboard_df.empty:
        daily_new = onboard_df.drop_duplicates(subset=["FIRST_USE_DATE"]).copy()
        bar = (
            alt.Chart(daily_new)
            .mark_bar(color="#4e79a7", opacity=0.6)
            .encode(
                x=alt.X("FIRST_USE_DATE:T", title="Date"),
                y=alt.Y("NEW_USERS_DAY:Q", title="Users"),
                tooltip=["FIRST_USE_DATE:T",
                         alt.Tooltip("NEW_USERS_DAY:Q", title="New Users")],
            )
        )
        line = (
            alt.Chart(daily_new)
            .mark_line(color="#e15759", strokeWidth=2)
            .encode(
                x=alt.X("FIRST_USE_DATE:T"),
                y=alt.Y("CUMULATIVE_USERS:Q", title="Cumulative Users"),
                tooltip=["FIRST_USE_DATE:T",
                         alt.Tooltip("CUMULATIVE_USERS:Q", title="Cumulative")],
            )
        )
        combined = alt.layer(bar, line).resolve_scale(y="independent").properties(height=250)
        st.altair_chart(combined, use_container_width=True)

        with st.expander("User first-use dates"):
            st.dataframe(
                onboard_df[["USER_NAME", "FIRST_USE_DATE"]].drop_duplicates().sort_values("FIRST_USE_DATE", ascending=False),
                hide_index=True, use_container_width=True,
            )
    else:
        st.info("No onboarding data available.")


@st.fragment
def render_efficiency_tab():
    st.caption(
        "Cache efficiency and output ratios reveal optimization opportunities. "
        "Multi-turn sessions in the same context can save ~50% on input costs."
    )
    eff_col1, eff_col2 = st.columns(2)
    with eff_col1:
        st.subheader("Cache Efficiency")
        st.caption(
            "Higher cache hit % = better session reuse. Target ≥ 70%. "
            "Low cache hit indicates session churn (many short sessions instead of long multi-turn ones)."
        )
        with st.spinner("Loading cache efficiency..."):
            cache_df = get_cache_efficiency(ps, pe, selected_user_ids)
        if not cache_df.empty:
            health_colors = {"GOOD": "#59a14f", "FAIR": "#f28e2b", "LOW": "#e15759"}
            bars = (
                alt.Chart(cache_df)
                .mark_bar()
                .encode(
                    y=alt.Y("USER_NAME:N", sort="-x", title="User"),
                    x=alt.X("CACHE_HIT_PCT:Q", title="Cache Hit %", scale=alt.Scale(domain=[0, 100])),
                    color=alt.Color("HEALTH:N",
                        scale=alt.Scale(domain=list(health_colors.keys()), range=list(health_colors.values())),
                        title="Health"),
                    tooltip=["USER_NAME", alt.Tooltip("CACHE_HIT_PCT:Q", format=".1f"),
                             "HEALTH", alt.Tooltip("REQUESTS:Q", format=",")],
                )
                .properties(height=max(200, len(cache_df) * 30))
            )
            threshold = (
                alt.Chart(pd.DataFrame({"x": [70]}))
                .mark_rule(strokeDash=[5, 5], color="red", strokeWidth=2)
                .encode(x="x:Q")
            )
            st.altair_chart(bars + threshold, use_container_width=True)
        else:
            st.info("Not enough data for cache efficiency analysis (min 3 requests per user).")

    with eff_col2:
        st.subheader("Output/Input Ratio")
        st.caption(
            "Output tokens cost 5-8x more than input. Ratio = output / (input + cache_read). "
            "Ratio > 3.0 = HIGH cost flag. High ratios mean the model is generating much more than it receives."
        )
        with st.spinner("Loading output ratio..."):
            ratio_df = get_output_ratio(ps, pe, selected_user_ids)
        if not ratio_df.empty:
            flag_colors = {"HIGH": "#e15759", "ELEVATED": "#f28e2b", "NORMAL": "#59a14f"}
            ratio_bars = (
                alt.Chart(ratio_df)
                .mark_bar()
                .encode(
                    y=alt.Y("USER_NAME:N", sort="-x", title="User"),
                    x=alt.X("OUTPUT_INPUT_RATIO:Q", title="Output/Input Ratio"),
                    color=alt.Color("FLAG:N",
                        scale=alt.Scale(domain=list(flag_colors.keys()), range=list(flag_colors.values())),
                        title="Flag"),
                    tooltip=["USER_NAME", alt.Tooltip("OUTPUT_INPUT_RATIO:Q", format=".2f"),
                             "FLAG", alt.Tooltip("REQUESTS:Q", format=",")],
                )
                .properties(height=max(200, len(ratio_df) * 30))
            )
            threshold_3 = (
                alt.Chart(pd.DataFrame({"x": [3.0]}))
                .mark_rule(strokeDash=[5, 5], color="red", strokeWidth=2)
                .encode(x="x:Q")
            )
            st.altair_chart(ratio_bars + threshold_3, use_container_width=True)
        else:
            st.info("Not enough data for ratio analysis (min 5 requests per user).")


@st.fragment
def render_rolling_24h_tab():
    st.caption(
        "Spend in the last 24 hours per user, compared against daily credit limits. "
        "Bridges the gap between period budgets and native rolling 24h enforcement."
    )
    with st.spinner("Loading rolling 24h spend..."):
        r24_df = get_rolling_24h_spend()
    if not r24_df.empty:
        r24_chart = (
            alt.Chart(r24_df)
            .mark_bar(color="#4e79a7")
            .encode(
                x=alt.X("USER_NAME:N", sort="-y", title="User"),
                y=alt.Y("CREDITS_24H:Q", title="Credits (24h)"),
                tooltip=["USER_NAME",
                         alt.Tooltip("CREDITS_24H:Q", format=",.4f"),
                         alt.Tooltip("REQUESTS_24H:Q", format=",")],
            )
            .properties(height=300)
        )
        st.altair_chart(r24_chart, use_container_width=True)
        st.dataframe(
            r24_df.style.format({"CREDITS_24H": "{:,.4f}", "REQUESTS_24H": "{:,}"}),
            hide_index=True, use_container_width=True,
        )
    else:
        st.info("No usage in the last 24 hours.")


@st.fragment
def render_all_users_tab():
    st.caption(
        "Every user in the account with their current spending, budget, and status. "
        "Users without budgets show as 'NO BUDGET' — go to **User Budgets** to assign limits."
    )
    all_spend_df = get_all_users_spend(ps, pe)
    if not all_spend_df.empty:
        over_users = all_spend_df[all_spend_df["STATUS"] == "OVER"]
        warn_users = all_spend_df[all_spend_df["STATUS"] == "WARNING"]
        no_budget_with_usage = all_spend_df[
            (all_spend_df["STATUS"] == "NO BUDGET") & (all_spend_df["TOTAL_USED"] > 0)
        ]

        if not over_users.empty:
            names = ", ".join(over_users["USER_NAME"].tolist())
            st.error(f"**Over budget:** {names}")
        if not warn_users.empty:
            names = ", ".join(warn_users["USER_NAME"].tolist())
            st.warning(f"**Approaching budget:** {names}")
        if not no_budget_with_usage.empty:
            st.info(
                f"**{len(no_budget_with_usage)} user(s)** have usage but no budget configured. "
                f"Go to **User Budgets** to set them up."
            )

        display_cols = [
            "USER_NAME", "EMAIL", "TOTAL_USED", "REQUESTS",
            "EFFECTIVE_BUDGET", "REMAINING", "PCT_USED", "STATUS",
        ]
        available_cols = [c for c in display_cols if c in all_spend_df.columns]

        display_df = all_spend_df[available_cols].copy()
        display_df["EFFECTIVE_BUDGET"] = display_df["EFFECTIVE_BUDGET"].apply(
            lambda x: x if pd.notna(x) and x > 0 else None
        )
        display_df["REMAINING"] = display_df.apply(
            lambda r: r["REMAINING"] if r["STATUS"] != "NO BUDGET" else None, axis=1
        )
        display_df["PCT_USED"] = display_df.apply(
            lambda r: r["PCT_USED"] if r["STATUS"] != "NO BUDGET" else None, axis=1
        )

        st.dataframe(
            display_df,
            use_container_width=True, hide_index=True, height=500,
            column_config={
                "USER_NAME": st.column_config.TextColumn("User"),
                "EMAIL": st.column_config.TextColumn("Email"),
                "TOTAL_USED": st.column_config.NumberColumn("Credits Used", format="%.4f",
                    help="Credits consumed this period based on token-level pricing."),
                "REQUESTS": st.column_config.NumberColumn("Requests", format="%d"),
                "EFFECTIVE_BUDGET": st.column_config.NumberColumn("Budget", format="%.2f",
                    help="Base budget + any top-ups for this period."),
                "REMAINING": st.column_config.NumberColumn("Remaining", format="%.2f"),
                "PCT_USED": st.column_config.ProgressColumn(
                    "% Used", min_value=0, max_value=100, format="%.1f%%"
                ),
                "STATUS": st.column_config.TextColumn("Status",
                    help="OK = under budget, WARNING = past threshold, OVER = exceeded, NO BUDGET = no limit set."),
            },
        )

        s1, s2, s3, s4 = st.columns(4)
        s1.metric("Total Users", len(all_spend_df))
        s2.metric("With Budgets", int((all_spend_df["STATUS"] != "NO BUDGET").sum()))
        s3.metric("Over Budget", len(over_users))
        s4.metric("No Budget (active)", len(no_budget_with_usage))
    else:
        st.info("No user data available.")


@st.fragment
def render_budget_status_tab():
    st.caption(
        "Users who have budgets assigned. Shows budget vs. actual spending this period. "
        "Configure budgets on the **User Budgets** page."
    )
    if not budget_df.empty:
        display_cols = [
            "USER_NAME", "EFFECTIVE_BUDGET", "TOTAL_USED",
            "REMAINING", "PCT_USED", "STATUS",
        ]
        available_cols = [c for c in display_cols if c in budget_df.columns]
        st.dataframe(
            budget_df[available_cols],
            use_container_width=True, hide_index=True,
            column_config={
                "PCT_USED": st.column_config.ProgressColumn(
                    "% Used", min_value=0, max_value=100, format="%d%%"
                ),
                "STATUS": st.column_config.TextColumn("Status"),
            },
        )

        st.divider()
        st.subheader("Cumulative Spend vs. Account Budget")
        with st.spinner("Loading spend trend..."):
            cum_df = get_daily_cumulative_spend(ps, pe)
        if not cum_df.empty:
            acct_b = get_account_budget()
            total_user_budget = budget_df["EFFECTIVE_BUDGET"].sum()
            acct_limit = float(acct_b.iloc[0]["BASE_PERIOD_CREDITS"]) if not acct_b.empty else 0.0
            budget_line_val = acct_limit if acct_limit > 0 else total_user_budget

            spend_chart = (
                alt.Chart(cum_df)
                .mark_area(opacity=0.3, line=True, color="#1f77b4")
                .encode(
                    x=alt.X("USAGE_DATE:T", title="Date"),
                    y=alt.Y("CUMULATIVE_CREDITS:Q", title="Credits"),
                    tooltip=[
                        "USAGE_DATE:T",
                        alt.Tooltip("DAILY_CREDITS:Q", format=",.4f", title="Daily"),
                        alt.Tooltip("CUMULATIVE_CREDITS:Q", format=",.4f", title="Cumulative"),
                    ],
                )
            )

            if budget_line_val > 0:
                budget_rule = (
                    alt.Chart(pd.DataFrame({"y": [budget_line_val]}))
                    .mark_rule(strokeDash=[5, 5], color="red", strokeWidth=2)
                    .encode(y="y:Q")
                )
                budget_text = (
                    alt.Chart(pd.DataFrame({
                        "y": [budget_line_val],
                        "label": [f"Budget: {budget_line_val:,.2f}"]
                    }))
                    .mark_text(align="right", dx=-5, dy=-8, color="red", fontSize=12)
                    .encode(y="y:Q", text="label:N")
                )
                combined = (spend_chart + budget_rule + budget_text).properties(height=280)
            else:
                combined = spend_chart.properties(height=280)

            st.altair_chart(combined, use_container_width=True)
        else:
            st.info("No daily usage data available for trend chart.")
    else:
        st.info("No user budgets configured yet. Go to **User Budgets** to add some.")


with tabs[0]:
    render_by_user_tab()

with tabs[1]:
    render_by_model_tab()

with tabs[2]:
    render_trends_tab()

with tabs[3]:
    render_efficiency_tab()

with tabs[4]:
    render_rolling_24h_tab()

if is_current_period:
    with tabs[5]:
        render_all_users_tab()

    with tabs[6]:
        render_budget_status_tab()
