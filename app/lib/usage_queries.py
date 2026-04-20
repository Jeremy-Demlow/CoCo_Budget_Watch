import streamlit as st
import pandas as pd

from lib.connection import run_query, run_ddl, _float_cols, FQN


CORTEX_CODE_PRICING = {
    "claude-4-sonnet":  {"input": 1.50, "output": 7.50,  "cache_write_input": 1.88, "cache_read_input": 0.15},
    "claude-opus-4-5":  {"input": 2.75, "output": 13.75, "cache_write_input": 3.44, "cache_read_input": 0.28},
    "claude-opus-4-6":  {"input": 2.75, "output": 13.75, "cache_write_input": 3.44, "cache_read_input": 0.28},
    "claude-sonnet-4-5":{"input": 1.65, "output": 8.25,  "cache_write_input": 2.06, "cache_read_input": 0.17},
    "claude-sonnet-4-6":{"input": 1.65, "output": 8.25,  "cache_write_input": 2.07, "cache_read_input": 0.17},
    "openai-gpt-5.2":   {"input": 0.97, "output": 7.70,  "cache_write_input": 0.00, "cache_read_input": 0.10},
}


def _requests_cte() -> str:
    return (
        "requests AS (\n"
        "        SELECT 'CLI' AS SOURCE, REQUEST_ID, PARENT_REQUEST_ID, USER_ID,\n"
        "               USAGE_TIME, TOKENS_GRANULAR,\n"
        "               ROW_NUMBER() OVER (PARTITION BY REQUEST_ID ORDER BY USAGE_TIME DESC) AS _RN\n"
        "        FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY\n"
        "        WHERE TOKENS_GRANULAR IS NOT NULL\n"
        "        UNION ALL\n"
        "        SELECT 'SNOWSIGHT', REQUEST_ID, PARENT_REQUEST_ID, USER_ID,\n"
        "               USAGE_TIME, TOKENS_GRANULAR,\n"
        "               ROW_NUMBER() OVER (PARTITION BY REQUEST_ID ORDER BY USAGE_TIME DESC) AS _RN\n"
        "        FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY\n"
        "        WHERE TOKENS_GRANULAR IS NOT NULL\n"
        "    ),\n"
        "    deduped AS (\n"
        "        SELECT * FROM requests WHERE _RN = 1\n"
        "    )"
    )


def _pricing_cte() -> str:
    rows = []
    for model, rates in CORTEX_CODE_PRICING.items():
        rows.append(
            f"SELECT '{model}' AS MODEL_NAME, "
            f"{rates['input']} AS INPUT_RATE, "
            f"{rates['output']} AS OUTPUT_RATE, "
            f"{rates['cache_write_input']} AS CACHE_WRITE_RATE, "
            f"{rates['cache_read_input']} AS CACHE_READ_RATE"
        )
    return "pricing AS (\n" + "\n        UNION ALL\n        ".join(rows) + "\n    )"


def _credits_sum_expr() -> str:
    return (
        "SUM(\n"
        "            f.INPUT_TOKENS   * COALESCE(p.INPUT_RATE, 0) / 1e6\n"
        "          + f.OUTPUT_TOKENS  * COALESCE(p.OUTPUT_RATE, 0) / 1e6\n"
        "          + f.CACHE_WRITE_TOKENS * COALESCE(p.CACHE_WRITE_RATE, 0) / 1e6\n"
        "          + f.CACHE_READ_TOKENS  * COALESCE(p.CACHE_READ_RATE, 0) / 1e6\n"
        "        )"
    )


def _flatten_cte(where_clause: str) -> str:
    return (
        f"flattened AS (\n"
        f"        SELECT r.REQUEST_ID, r.USER_ID, r.USAGE_TIME, r.SOURCE,\n"
        f"            tk.key AS MODEL_NAME,\n"
        f"            COALESCE(tk.value:input::FLOAT, 0) AS INPUT_TOKENS,\n"
        f"            COALESCE(tk.value:output::FLOAT, 0) AS OUTPUT_TOKENS,\n"
        f"            COALESCE(tk.value:cache_write_input::FLOAT, 0) AS CACHE_WRITE_TOKENS,\n"
        f"            COALESCE(tk.value:cache_read_input::FLOAT, 0) AS CACHE_READ_TOKENS\n"
        f"        FROM deduped r,\n"
        f"            LATERAL FLATTEN(input => r.TOKENS_GRANULAR) tk\n"
        f"        WHERE 1=1\n"
        f"          {where_clause}\n"
        f"    )"
    )


@st.cache_data(ttl=3600, show_spinner=False)
def get_users() -> pd.DataFrame:
    df, _ = run_query(
        "SELECT USER_ID, NAME AS USER_NAME, LOGIN_NAME, EMAIL, "
        "CREATED_ON, LAST_SUCCESS_LOGIN "
        "FROM SNOWFLAKE.ACCOUNT_USAGE.USERS "
        "WHERE DELETED_ON IS NULL "
        "AND LOGIN_NAME NOT LIKE 'SF$SERVICE%' "
        "AND NAME != 'SNOWFLAKE' "
        "ORDER BY NAME"
    )
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def get_coco_active_users(period_start: str, period_end: str) -> pd.DataFrame:
    time_filter = f"AND r.USAGE_TIME >= '{period_start}' AND r.USAGE_TIME < '{period_end}'"
    sql = f"""
    WITH {_requests_cte()},
    {_pricing_cte()},
    {_flatten_cte(time_filter)}
    SELECT
        u.USER_ID, u.NAME AS USER_NAME, u.LOGIN_NAME, u.EMAIL,
        {_credits_sum_expr()} AS TOTAL_CREDITS,
        COUNT(DISTINCT f.REQUEST_ID) AS REQUEST_COUNT,
        SUM(f.INPUT_TOKENS + f.OUTPUT_TOKENS + f.CACHE_WRITE_TOKENS + f.CACHE_READ_TOKENS) AS TOTAL_TOKENS,
        MAX(f.USAGE_TIME) AS LAST_ACTIVITY
    FROM flattened f
    JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON f.USER_ID = u.USER_ID
    LEFT JOIN pricing p ON f.MODEL_NAME = p.MODEL_NAME
    WHERE u.LOGIN_NAME NOT LIKE 'SF$SERVICE%'
      AND u.NAME != 'SNOWFLAKE'
    GROUP BY u.USER_ID, u.NAME, u.LOGIN_NAME, u.EMAIL
    ORDER BY TOTAL_CREDITS DESC
    """
    df, _ = run_query(sql)
    return _float_cols(df, ["TOTAL_CREDITS", "TOTAL_TOKENS"])


@st.cache_data(ttl=3600, show_spinner=False)
def get_usage_by_user(period_start: str, period_end: str,
                      user_ids: tuple | None = None) -> pd.DataFrame:
    user_filter = ""
    if user_ids:
        ids = ", ".join(str(i) for i in user_ids)
        user_filter = f"AND r.USER_ID IN ({ids})"
    time_filter = f"AND r.USAGE_TIME >= '{period_start}' AND r.USAGE_TIME < '{period_end}' {user_filter}"
    sql = f"""
    WITH {_requests_cte()},
    {_pricing_cte()},
    {_flatten_cte(time_filter)}
    SELECT
        u.NAME AS USER_NAME,
        f.USER_ID,
        f.SOURCE,
        {_credits_sum_expr()} AS CREDITS,
        COUNT(DISTINCT f.REQUEST_ID) AS REQUESTS
    FROM flattened f
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON f.USER_ID = u.USER_ID
    LEFT JOIN pricing p ON f.MODEL_NAME = p.MODEL_NAME
    GROUP BY u.NAME, f.USER_ID, f.SOURCE
    ORDER BY CREDITS DESC
    """
    df, _ = run_query(sql)
    return _float_cols(df, ["CREDITS"])


@st.cache_data(ttl=3600, show_spinner=False)
def get_cache_efficiency(period_start: str, period_end: str,
                         user_ids: tuple | None = None) -> pd.DataFrame:
    time_filter = f"AND r.USAGE_TIME >= '{period_start}' AND r.USAGE_TIME < '{period_end}'"
    if user_ids:
        ids = ",".join(str(u) for u in user_ids)
        time_filter += f" AND r.USER_ID IN ({ids})"
    sql = f"""
    WITH {_requests_cte()},
    {_flatten_cte(time_filter)}
    SELECT
        u.NAME AS USER_NAME,
        SUM(f.CACHE_READ_TOKENS) AS CACHE_READ_TOKENS,
        SUM(f.INPUT_TOKENS) AS INPUT_TOKENS,
        SUM(f.CACHE_READ_TOKENS + f.INPUT_TOKENS) AS TOTAL_CACHEABLE,
        CASE WHEN SUM(f.CACHE_READ_TOKENS + f.INPUT_TOKENS) > 0
             THEN ROUND(SUM(f.CACHE_READ_TOKENS) / SUM(f.CACHE_READ_TOKENS + f.INPUT_TOKENS) * 100, 1)
             ELSE 0 END AS CACHE_HIT_PCT,
        COUNT(DISTINCT f.REQUEST_ID) AS REQUESTS
    FROM flattened f
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON f.USER_ID = u.USER_ID
    GROUP BY u.NAME
    HAVING REQUESTS >= 3
    ORDER BY CACHE_HIT_PCT DESC
    """
    df, _ = run_query(sql)
    df = _float_cols(df, ["CACHE_READ_TOKENS", "INPUT_TOKENS", "TOTAL_CACHEABLE", "CACHE_HIT_PCT"])
    if not df.empty:
        df["HEALTH"] = df["CACHE_HIT_PCT"].apply(
            lambda x: "GOOD" if x >= 70 else ("FAIR" if x >= 50 else "LOW")
        )
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def get_output_ratio(period_start: str, period_end: str,
                     user_ids: tuple | None = None) -> pd.DataFrame:
    time_filter = f"AND r.USAGE_TIME >= '{period_start}' AND r.USAGE_TIME < '{period_end}'"
    if user_ids:
        ids = ",".join(str(u) for u in user_ids)
        time_filter += f" AND r.USER_ID IN ({ids})"
    sql = f"""
    WITH {_requests_cte()},
    {_flatten_cte(time_filter)}
    SELECT
        u.NAME AS USER_NAME,
        SUM(f.OUTPUT_TOKENS) AS OUTPUT_TOKENS,
        SUM(f.INPUT_TOKENS + f.CACHE_READ_TOKENS) AS EFFECTIVE_INPUT_TOKENS,
        CASE WHEN SUM(f.INPUT_TOKENS + f.CACHE_READ_TOKENS) > 0
             THEN ROUND(SUM(f.OUTPUT_TOKENS) / SUM(f.INPUT_TOKENS + f.CACHE_READ_TOKENS), 2)
             ELSE 0 END AS OUTPUT_INPUT_RATIO,
        COUNT(DISTINCT f.REQUEST_ID) AS REQUESTS
    FROM flattened f
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON f.USER_ID = u.USER_ID
    GROUP BY u.NAME
    HAVING REQUESTS >= 5
    ORDER BY OUTPUT_INPUT_RATIO DESC
    """
    df, _ = run_query(sql)
    df = _float_cols(df, ["OUTPUT_TOKENS", "EFFECTIVE_INPUT_TOKENS", "OUTPUT_INPUT_RATIO"])
    if not df.empty:
        df["FLAG"] = df["OUTPUT_INPUT_RATIO"].apply(
            lambda x: "HIGH" if x > 3.0 else ("ELEVATED" if x > 2.0 else "NORMAL")
        )
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def get_ai_services_context() -> dict:
    sql = """
    SELECT
        COALESCE(SUM(CASE WHEN SERVICE_TYPE IN ('CORTEX_CODE_CLI','CORTEX_CODE_SNOWSIGHT')
                      THEN CREDITS_USED ELSE 0 END), 0) AS COCO_CREDITS,
        COALESCE(SUM(CREDITS_USED), 0) AS TOTAL_AI_CREDITS
    FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
    WHERE SERVICE_TYPE IN ('AI_SERVICES','CORTEX_CODE_CLI','CORTEX_CODE_SNOWSIGHT','CORTEX_AGENTS')
      AND USAGE_DATE >= DATE_TRUNC('MONTH', CURRENT_DATE())
      AND USAGE_DATE < CURRENT_DATE() + 1
    """
    df, _ = run_query(sql)
    if df.empty:
        return {"coco": 0.0, "total": 0.0}
    return {
        "coco": float(df.iloc[0]["COCO_CREDITS"]),
        "total": float(df.iloc[0]["TOTAL_AI_CREDITS"]),
    }


@st.cache_data(ttl=3600, show_spinner=False)
def get_model_token_type_breakdown(period_start: str, period_end: str,
                                   user_ids: tuple | None = None) -> pd.DataFrame:
    time_filter = f"AND r.USAGE_TIME >= '{period_start}' AND r.USAGE_TIME < '{period_end}'"
    if user_ids:
        ids = ",".join(str(u) for u in user_ids)
        time_filter += f" AND r.USER_ID IN ({ids})"
    sql = f"""
    WITH {_requests_cte()},
    {_pricing_cte()},
    {_flatten_cte(time_filter)}
    SELECT
        f.MODEL_NAME AS MODEL,
        SUM(f.INPUT_TOKENS * COALESCE(p.INPUT_RATE, 0) / 1e6) AS INPUT_CREDITS,
        SUM(f.OUTPUT_TOKENS * COALESCE(p.OUTPUT_RATE, 0) / 1e6) AS OUTPUT_CREDITS,
        SUM(f.CACHE_WRITE_TOKENS * COALESCE(p.CACHE_WRITE_RATE, 0) / 1e6) AS CACHE_WRITE_CREDITS,
        SUM(f.CACHE_READ_TOKENS * COALESCE(p.CACHE_READ_RATE, 0) / 1e6) AS CACHE_READ_CREDITS
    FROM flattened f
    LEFT JOIN pricing p ON f.MODEL_NAME = p.MODEL_NAME
    GROUP BY f.MODEL_NAME
    ORDER BY (INPUT_CREDITS + OUTPUT_CREDITS + CACHE_WRITE_CREDITS + CACHE_READ_CREDITS) DESC
    """
    df, _ = run_query(sql)
    return _float_cols(df, ["INPUT_CREDITS", "OUTPUT_CREDITS", "CACHE_WRITE_CREDITS", "CACHE_READ_CREDITS"])


@st.cache_data(ttl=300, show_spinner=False)
def get_rolling_24h_spend() -> pd.DataFrame:
    sql = f"""
    WITH {_requests_cte()},
    {_pricing_cte()},
    {_flatten_cte("AND r.USAGE_TIME >= DATEADD('hour', -24, CURRENT_TIMESTAMP())")}
    SELECT
        u.NAME AS USER_NAME,
        {_credits_sum_expr()} AS CREDITS_24H,
        COUNT(DISTINCT f.REQUEST_ID) AS REQUESTS_24H
    FROM flattened f
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON f.USER_ID = u.USER_ID
    LEFT JOIN pricing p ON f.MODEL_NAME = p.MODEL_NAME
    GROUP BY u.NAME
    HAVING CREDITS_24H > 0
    ORDER BY CREDITS_24H DESC
    """
    df, _ = run_query(sql)
    return _float_cols(df, ["CREDITS_24H"])


@st.cache_data(ttl=3600, show_spinner=False)
def get_new_user_onboarding(days: int = 90) -> pd.DataFrame:
    sql = f"""
    WITH {_requests_cte()},
    first_use AS (
        SELECT
            r.USER_ID,
            MIN(r.USAGE_TIME)::DATE AS FIRST_USE_DATE
        FROM deduped r
        GROUP BY r.USER_ID
    )
    SELECT
        f.FIRST_USE_DATE,
        u.NAME AS USER_NAME,
        COUNT(*) OVER (PARTITION BY f.FIRST_USE_DATE) AS NEW_USERS_DAY,
        COUNT(*) OVER (ORDER BY f.FIRST_USE_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS CUMULATIVE_USERS
    FROM first_use f
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON f.USER_ID = u.USER_ID
    WHERE f.FIRST_USE_DATE >= DATEADD('day', -{days}, CURRENT_DATE())
    ORDER BY f.FIRST_USE_DATE
    """
    df, _ = run_query(sql)
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def get_daily_cumulative_spend(period_start: str, period_end: str) -> pd.DataFrame:
    time_filter = f"AND r.USAGE_TIME >= '{period_start}' AND r.USAGE_TIME < '{period_end}'"
    sql = f"""
    WITH {_requests_cte()},
    {_pricing_cte()},
    {_flatten_cte(time_filter)}
    SELECT
        DATE(f.USAGE_TIME) AS USAGE_DATE,
        {_credits_sum_expr()} AS DAILY_CREDITS
    FROM flattened f
    LEFT JOIN pricing p ON f.MODEL_NAME = p.MODEL_NAME
    GROUP BY USAGE_DATE
    ORDER BY USAGE_DATE
    """
    df, _ = run_query(sql)
    if df.empty:
        return df
    df = _float_cols(df, ["DAILY_CREDITS"])
    df["CUMULATIVE_CREDITS"] = df["DAILY_CREDITS"].cumsum()
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def get_daily_trend(days: int = 30, user_ids: tuple | None = None) -> pd.DataFrame:
    user_filter = ""
    if user_ids:
        ids = ", ".join(str(i) for i in user_ids)
        user_filter = f"AND r.USER_ID IN ({ids})"
    time_filter = f"AND r.USAGE_TIME >= DATEADD('DAY', -{days}, CURRENT_TIMESTAMP()) {user_filter}"
    sql = f"""
    WITH {_requests_cte()},
    {_pricing_cte()},
    {_flatten_cte(time_filter)}
    SELECT
        DATE(f.USAGE_TIME) AS USAGE_DATE,
        u.NAME AS USER_NAME,
        f.SOURCE,
        {_credits_sum_expr()} AS DAILY_CREDITS,
        COUNT(DISTINCT f.REQUEST_ID) AS REQUESTS
    FROM flattened f
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON f.USER_ID = u.USER_ID
    LEFT JOIN pricing p ON f.MODEL_NAME = p.MODEL_NAME
    GROUP BY USAGE_DATE, u.NAME, f.SOURCE
    ORDER BY USAGE_DATE
    """
    df, _ = run_query(sql)
    return _float_cols(df, ["DAILY_CREDITS"])


@st.cache_data(ttl=3600, show_spinner=False)
def get_model_breakdown(period_start: str, period_end: str,
                        user_ids: tuple | None = None) -> pd.DataFrame:
    user_filter = ""
    if user_ids:
        ids = ", ".join(str(i) for i in user_ids)
        user_filter = f"AND r.USER_ID IN ({ids})"
    time_filter = f"AND r.USAGE_TIME >= '{period_start}' AND r.USAGE_TIME < '{period_end}' {user_filter}"
    sql = f"""
    WITH {_requests_cte()},
    {_pricing_cte()},
    {_flatten_cte(time_filter)}
    SELECT
        f.MODEL_NAME AS MODEL,
        u.NAME AS USER_NAME,
        COUNT(DISTINCT f.REQUEST_ID) AS REQUESTS,
        {_credits_sum_expr()} AS CREDITS,
        SUM(f.INPUT_TOKENS + f.OUTPUT_TOKENS + f.CACHE_WRITE_TOKENS + f.CACHE_READ_TOKENS) AS TOTAL_TOKENS
    FROM flattened f
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON f.USER_ID = u.USER_ID
    LEFT JOIN pricing p ON f.MODEL_NAME = p.MODEL_NAME
    GROUP BY f.MODEL_NAME, u.NAME
    ORDER BY CREDITS DESC
    """
    df, _ = run_query(sql)
    return _float_cols(df, ["CREDITS", "TOTAL_TOKENS"])


@st.cache_data(ttl=3600, show_spinner=False)
def get_all_users_spend(period_start: str, period_end: str) -> pd.DataFrame:
    sql = f"""
    WITH {_requests_cte()},
    {_pricing_cte()},
    {_flatten_cte(f"AND r.USAGE_TIME >= '{period_start}' AND r.USAGE_TIME < '{period_end}'")},
    usage_agg AS (
        SELECT f.USER_ID,
               {_credits_sum_expr()} AS TOTAL_USED,
               COUNT(DISTINCT f.REQUEST_ID) AS REQUESTS,
               MAX(f.USAGE_TIME) AS LAST_ACTIVITY
        FROM flattened f
        LEFT JOIN pricing p ON f.MODEL_NAME = p.MODEL_NAME
        GROUP BY f.USER_ID
    ),
    topup_agg AS (
        SELECT t.USER_ID, SUM(t.CREDITS) AS TOPUP_CREDITS
        FROM {FQN}.BUDGET_TOPUPS t
        WHERE t.TARGET_TYPE = 'USER'
          AND t.EFFECTIVE_START < '{period_end}' AND t.EFFECTIVE_END > '{period_start}'
        GROUP BY t.USER_ID
    )
    SELECT
        u.USER_ID,
        u.NAME AS USER_NAME,
        u.EMAIL,
        COALESCE(ua.TOTAL_USED, 0) AS TOTAL_USED,
        COALESCE(ua.REQUESTS, 0) AS REQUESTS,
        ua.LAST_ACTIVITY,
        b.BASE_PERIOD_CREDITS,
        COALESCE(tp.TOPUP_CREDITS, 0) AS TOPUP_CREDITS,
        b.BASE_PERIOD_CREDITS + COALESCE(tp.TOPUP_CREDITS, 0) AS EFFECTIVE_BUDGET,
        b.BASE_PERIOD_CREDITS + COALESCE(tp.TOPUP_CREDITS, 0) - COALESCE(ua.TOTAL_USED, 0) AS REMAINING,
        CASE
            WHEN b.BASE_PERIOD_CREDITS IS NULL THEN NULL
            WHEN b.BASE_PERIOD_CREDITS + COALESCE(tp.TOPUP_CREDITS, 0) = 0 THEN 0
            ELSE ROUND(COALESCE(ua.TOTAL_USED, 0) / (b.BASE_PERIOD_CREDITS + COALESCE(tp.TOPUP_CREDITS, 0)) * 100, 2)
        END AS PCT_USED,
        CASE
            WHEN b.BASE_PERIOD_CREDITS IS NULL THEN 'NO BUDGET'
            WHEN COALESCE(ua.TOTAL_USED, 0) >= b.BASE_PERIOD_CREDITS + COALESCE(tp.TOPUP_CREDITS, 0) THEN 'OVER'
            WHEN b.BASE_PERIOD_CREDITS + COALESCE(tp.TOPUP_CREDITS, 0) > 0
                 AND COALESCE(ua.TOTAL_USED, 0) >= (b.BASE_PERIOD_CREDITS + COALESCE(tp.TOPUP_CREDITS, 0)) * b.WARNING_THRESHOLD_PCT / 100.0
            THEN 'WARNING'
            ELSE 'OK'
        END AS STATUS,
        b.WARNING_THRESHOLD_PCT,
        b.IS_ACTIVE AS HAS_BUDGET
    FROM SNOWFLAKE.ACCOUNT_USAGE.USERS u
    LEFT JOIN usage_agg ua ON u.USER_ID = ua.USER_ID
    LEFT JOIN {FQN}.USER_BUDGETS b ON u.USER_ID = b.USER_ID AND b.IS_ACTIVE = TRUE
    LEFT JOIN topup_agg tp ON u.USER_ID = tp.USER_ID
    WHERE u.DELETED_ON IS NULL
      AND u.LOGIN_NAME NOT LIKE 'SF$SERVICE%'
      AND u.NAME != 'SNOWFLAKE'
    ORDER BY COALESCE(ua.TOTAL_USED, 0) DESC
    """
    df, _ = run_query(sql)
    return _float_cols(df, [
        "TOTAL_USED", "REQUESTS", "BASE_PERIOD_CREDITS", "TOPUP_CREDITS",
        "EFFECTIVE_BUDGET", "REMAINING", "PCT_USED",
    ])


@st.cache_data(ttl=3600, show_spinner=False)
def get_user_budgets() -> pd.DataFrame:
    df, _ = run_query(
        f"SELECT ub.*, u.NAME AS USER_NAME, u.EMAIL "
        f"FROM {FQN}.USER_BUDGETS ub "
        f"LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON ub.USER_ID = u.USER_ID "
        f"ORDER BY u.NAME"
    )
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def get_account_usage_credits(period_start: str, period_end: str) -> tuple[float, int]:
    time_filter = f"AND r.USAGE_TIME >= '{period_start}' AND r.USAGE_TIME < '{period_end}'"
    sql = f"""
    WITH {_requests_cte()},
    {_pricing_cte()},
    {_flatten_cte(time_filter)}
    SELECT
        {_credits_sum_expr()} AS TOTAL_CREDITS,
        COUNT(DISTINCT f.REQUEST_ID) AS TOTAL_REQUESTS
    FROM flattened f
    LEFT JOIN pricing p ON f.MODEL_NAME = p.MODEL_NAME
    """
    df, err = run_query(sql)
    if err or df.empty:
        return 0.0, 0
    return float(df.iloc[0]["TOTAL_CREDITS"] or 0), int(df.iloc[0]["TOTAL_REQUESTS"] or 0)


@st.cache_data(ttl=3600, show_spinner=False)
def get_account_budget() -> pd.DataFrame:
    df, _ = run_query(
        f"SELECT * FROM {FQN}.ACCOUNT_BUDGET WHERE IS_ACTIVE = TRUE LIMIT 1"
    )
    return df


def get_account_user_breakdown(period_start: str, period_end: str) -> pd.DataFrame:
    time_filter = f"AND r.USAGE_TIME >= '{period_start}' AND r.USAGE_TIME < '{period_end}'"
    sql = f"""
    WITH {_requests_cte()},
    {_pricing_cte()},
    {_flatten_cte(time_filter)}
    SELECT
        u.NAME AS USER_NAME,
        {_credits_sum_expr()} AS CREDITS,
        COUNT(DISTINCT f.REQUEST_ID) AS REQUESTS
    FROM flattened f
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON f.USER_ID = u.USER_ID
    LEFT JOIN pricing p ON f.MODEL_NAME = p.MODEL_NAME
    GROUP BY u.NAME ORDER BY CREDITS DESC LIMIT 20
    """
    df, _ = run_query(sql)
    return _float_cols(df, ["CREDITS"])


def get_available_models() -> list[str]:
    return list(CORTEX_CODE_PRICING.keys())
