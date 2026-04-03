import os
import streamlit as st
import pandas as pd

_SESSION = None

DB = "COCO_BUDGETS_DB"
SCHEMA = "BUDGETS"
FQN = f"{DB}.{SCHEMA}"


def _load_connection_config(conn_name: str) -> dict:
    import tomllib
    toml_path = os.path.expanduser("~/.snowflake/connections.toml")
    with open(toml_path, "rb") as f:
        conns = tomllib.load(f)
    cfg = conns.get(conn_name, {})
    params = dict(cfg)
    if "private_key_path" in params:
        from cryptography.hazmat.primitives import serialization
        key_path = os.path.expanduser(params.pop("private_key_path"))
        with open(key_path, "rb") as kf:
            p_key = serialization.load_pem_private_key(kf.read(), password=None)
        params["private_key"] = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        params.pop("authenticator", None)
    return params


def get_session():
    global _SESSION
    if _SESSION is not None:
        return _SESSION
    try:
        from snowflake.snowpark.context import get_active_session
        _SESSION = get_active_session()
        return _SESSION
    except Exception:
        pass
    try:
        import snowflake.connector
        conn_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "myconnection")
        params = _load_connection_config(conn_name)
        conn = snowflake.connector.connect(**params)
        _SESSION = conn
        return _SESSION
    except Exception as e:
        st.error(f"Cannot connect to Snowflake: {e}")
        st.stop()


def run_query(sql: str, params=None) -> tuple[pd.DataFrame, str | None]:
    session = get_session()
    try:
        if hasattr(session, "sql"):
            df = session.sql(sql).to_pandas()
        else:
            cur = session.cursor()
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=cols)
        return df, None
    except Exception as e:
        global _SESSION
        try:
            if _SESSION and hasattr(_SESSION, 'is_closed') and _SESSION.is_closed():
                _SESSION = None
        except Exception:
            _SESSION = None
        return pd.DataFrame(), str(e)


def run_ddl(sql: str) -> str | None:
    session = get_session()
    try:
        if hasattr(session, "sql"):
            session.sql(sql).collect()
        else:
            session.cursor().execute(sql)
        return None
    except Exception as e:
        global _SESSION
        try:
            if _SESSION and hasattr(_SESSION, 'is_closed') and _SESSION.is_closed():
                _SESSION = None
        except Exception:
            _SESSION = None
        return str(e)


def _float_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    return df


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
def get_config() -> dict:
    df, err = run_query(f"SELECT CONFIG_KEY, CONFIG_VALUE FROM {FQN}.BUDGET_CONFIG")
    if err or df.empty:
        return {
            "BUDGET_TIMEZONE": "UTC",
            "DEFAULT_PERIOD_TYPE": "MONTHLY",
            "DEFAULT_WARNING_THRESHOLD_PCT": "80",
            "DEFAULT_USER_BASE_PERIOD_CREDITS": "100",
            "ENABLE_PERSISTED_ROLLUPS": "false",
            "ENABLE_MODEL_DRILLDOWN": "false",
            "CREDIT_RATE_USD": "2.00",
        }
    return dict(zip(df["CONFIG_KEY"], df["CONFIG_VALUE"]))


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


def log_audit(action: str, target_type: str, target_user_id=None,
              old_value=None, new_value=None, notes=None):
    safe_action = action.replace("'", "''")
    safe_type = target_type.replace("'", "''")
    uid = f"{int(target_user_id)}" if target_user_id is not None else "NULL"
    ov = f"{float(old_value)}" if old_value is not None else "NULL"
    nv = f"{float(new_value)}" if new_value is not None else "NULL"
    safe_notes = f"'{notes.replace(chr(39), chr(39)+chr(39))}'" if notes else "NULL"
    run_ddl(
        f"INSERT INTO {FQN}.BUDGET_AUDIT_LOG "
        f"(ACTION, TARGET_TYPE, TARGET_USER_ID, OLD_VALUE, NEW_VALUE, NOTES) "
        f"VALUES ('{safe_action}', '{safe_type}', {uid}, {ov}, {nv}, {safe_notes})"
    )


def clear_caches():
    for fn in [get_config, get_users, get_coco_active_users, get_usage_by_user,
               get_daily_trend, get_model_breakdown,
               get_user_budgets, get_account_budget, get_all_users_spend,
               get_account_usage_credits, get_daily_cumulative_spend]:
        fn.clear()


LATENCY_BANNER = (
    "**Data Freshness:** ACCOUNT_USAGE views can lag up to **~1 hour**. "
    "Enable **Enforcement** to automatically revoke access when budgets are exceeded."
)


def get_enforcement_status() -> dict:
    cfg = get_config()
    enabled = cfg.get("ENFORCEMENT_ENABLED", "false").lower() == "true"
    role = cfg.get("ENFORCEMENT_ROLE", "CORTEX_USER_ROLE")
    return {"enabled": enabled, "role": role}


def get_scheduled_task_status() -> dict | None:
    df, err = run_query(
        f"SHOW TASKS LIKE 'COCO_ENFORCEMENT_TASK' IN SCHEMA {FQN}"
    )
    if err or df.empty:
        return None
    row = df.iloc[0]
    return {
        "name": row.get("name", "COCO_ENFORCEMENT_TASK"),
        "state": row.get("state", "unknown"),
        "schedule": row.get("schedule", ""),
        "warehouse": row.get("warehouse", ""),
    }


def create_enforcement_task(warehouse: str, cron_expr: str) -> str | None:
    sproc_ddl = f"""
    CREATE OR REPLACE PROCEDURE {FQN}.COCO_ENFORCEMENT_SPROC()
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS CALLER
    AS
    $$
    DECLARE
        v_start VARCHAR;
        v_end   VARCHAR;
        v_period VARCHAR;
        v_tz    VARCHAR;
    BEGIN
        SELECT CONFIG_VALUE INTO v_tz
          FROM {FQN}.BUDGET_CONFIG WHERE CONFIG_KEY = 'BUDGET_TIMEZONE';
        SELECT CONFIG_VALUE INTO v_period
          FROM {FQN}.BUDGET_CONFIG WHERE CONFIG_KEY = 'DEFAULT_PERIOD_TYPE';

        IF (v_period = 'MONTHLY') THEN
            v_start := TO_VARCHAR(DATE_TRUNC('MONTH', CONVERT_TIMEZONE(NVL(v_tz,'UTC'), CURRENT_TIMESTAMP())), 'YYYY-MM-DD HH24:MI:SS');
            v_end   := TO_VARCHAR(DATEADD('MONTH', 1, DATE_TRUNC('MONTH', CONVERT_TIMEZONE(NVL(v_tz,'UTC'), CURRENT_TIMESTAMP()))), 'YYYY-MM-DD HH24:MI:SS');
        ELSEIF (v_period = 'WEEKLY') THEN
            v_start := TO_VARCHAR(DATE_TRUNC('WEEK', CONVERT_TIMEZONE(NVL(v_tz,'UTC'), CURRENT_TIMESTAMP())), 'YYYY-MM-DD HH24:MI:SS');
            v_end   := TO_VARCHAR(DATEADD('WEEK', 1, DATE_TRUNC('WEEK', CONVERT_TIMEZONE(NVL(v_tz,'UTC'), CURRENT_TIMESTAMP()))), 'YYYY-MM-DD HH24:MI:SS');
        ELSE
            v_start := TO_VARCHAR(DATE_TRUNC('QUARTER', CONVERT_TIMEZONE(NVL(v_tz,'UTC'), CURRENT_TIMESTAMP())), 'YYYY-MM-DD HH24:MI:SS');
            v_end   := TO_VARCHAR(DATEADD('QUARTER', 1, DATE_TRUNC('QUARTER', CONVERT_TIMEZONE(NVL(v_tz,'UTC'), CURRENT_TIMESTAMP()))), 'YYYY-MM-DD HH24:MI:SS');
        END IF;

        -- The actual enforcement logic is in the Streamlit app.
        -- This sproc logs that a scheduled run occurred.
        INSERT INTO {FQN}.ENFORCEMENT_LOG (ACTION, USER_NAME, REASON)
        VALUES ('SCHEDULED_RUN', 'SYSTEM', 'Automated enforcement cycle: ' || v_start || ' to ' || v_end);
        RETURN 'Enforcement cycle logged: ' || v_start || ' to ' || v_end;
    END;
    $$
    """
    err = run_ddl(sproc_ddl)
    if err:
        return f"Failed to create procedure: {err}"

    safe_cron = cron_expr.replace("'", "''")
    task_ddl = (
        f"CREATE OR REPLACE TASK {FQN}.COCO_ENFORCEMENT_TASK\n"
        f"  WAREHOUSE = {warehouse}\n"
        f"  SCHEDULE = 'USING CRON {safe_cron} UTC'\n"
        f"  COMMENT = 'CoCo Budgets automated enforcement'\n"
        f"AS\n"
        f"  CALL {FQN}.COCO_ENFORCEMENT_SPROC()"
    )
    err = run_ddl(task_ddl)
    if err:
        return f"Failed to create task: {err}"

    err = run_ddl(f"ALTER TASK {FQN}.COCO_ENFORCEMENT_TASK RESUME")
    if err:
        return f"Task created but failed to resume: {err}"
    return None


def suspend_enforcement_task() -> str | None:
    return run_ddl(f"ALTER TASK {FQN}.COCO_ENFORCEMENT_TASK SUSPEND")


def drop_enforcement_task() -> str | None:
    err1 = run_ddl(f"ALTER TASK IF EXISTS {FQN}.COCO_ENFORCEMENT_TASK SUSPEND")
    err2 = run_ddl(f"DROP TASK IF EXISTS {FQN}.COCO_ENFORCEMENT_TASK")
    err3 = run_ddl(f"DROP PROCEDURE IF EXISTS {FQN}.COCO_ENFORCEMENT_SPROC()")
    return err2 or err1 or err3


def get_enforcement_log(limit: int = 50) -> pd.DataFrame:
    df, _ = run_query(
        f"SELECT LOG_ID, ACTION, USER_ID, USER_NAME, REASON, PERFORMED_AT "
        f"FROM {FQN}.ENFORCEMENT_LOG ORDER BY PERFORMED_AT DESC LIMIT {int(limit)}"
    )
    return df


def get_model_allowlist() -> list[str]:
    df, err = run_query("SHOW PARAMETERS LIKE 'CORTEX_MODELS_ALLOWLIST' IN ACCOUNT")
    if err or df.empty:
        return []
    row = df.iloc[0]
    val = str(row.get("value", row.get("VALUE", row.get("key", ""))))
    if val.upper() in ("ALL", ""):
        return ["ALL"]
    if val.upper() == "NONE":
        return ["NONE"]
    return [m.strip().strip("'") for m in val.split(",") if m.strip()]


def set_model_allowlist(models: list[str]) -> str | None:
    if not models or models == ["ALL"]:
        return run_ddl("ALTER ACCOUNT SET CORTEX_MODELS_ALLOWLIST = 'ALL'")
    if models == ["NONE"]:
        return run_ddl("ALTER ACCOUNT SET CORTEX_MODELS_ALLOWLIST = 'NONE'")
    safe = ", ".join(f"'{m.strip()}'" for m in models if m.strip())
    return run_ddl(f"ALTER ACCOUNT SET CORTEX_MODELS_ALLOWLIST = ({safe})")


def get_available_models() -> list[str]:
    return list(CORTEX_CODE_PRICING.keys())


def revoke_user_cortex_access(user_name: str, reason: str = "Over budget") -> str | None:
    cfg = get_config()
    role = cfg.get("ENFORCEMENT_ROLE", "CORTEX_USER_ROLE")
    safe_user = user_name.replace("'", "''")
    err = run_ddl(f"REVOKE ROLE {role} FROM USER \"{safe_user}\"")
    if not err:
        safe_reason = reason.replace("'", "''")
        run_ddl(
            f"INSERT INTO {FQN}.ENFORCEMENT_LOG (ACTION, USER_NAME, REASON) "
            f"VALUES ('REVOKE', '{safe_user}', '{safe_reason}')"
        )
    return err


def grant_user_cortex_access(user_name: str, reason: str = "Budget reset / manual") -> str | None:
    cfg = get_config()
    role = cfg.get("ENFORCEMENT_ROLE", "CORTEX_USER_ROLE")
    safe_user = user_name.replace("'", "''")
    err = run_ddl(f"GRANT ROLE {role} TO USER \"{safe_user}\"")
    if not err:
        safe_reason = reason.replace("'", "''")
        run_ddl(
            f"INSERT INTO {FQN}.ENFORCEMENT_LOG (ACTION, USER_NAME, REASON) "
            f"VALUES ('GRANT', '{safe_user}', '{safe_reason}')"
        )
    return err


def get_users_with_role(role_name: str = "CORTEX_USER_ROLE") -> list[str]:
    df, err = run_query(f"SHOW GRANTS OF ROLE {role_name}")
    if err or df.empty:
        return []
    user_rows = df[df["granted_to"] == "USER"] if "granted_to" in df.columns else pd.DataFrame()
    if user_rows.empty:
        return []
    return user_rows["grantee_name"].tolist()


def user_has_role(user_name: str, role_name: str = None) -> bool:
    if role_name is None:
        role_name = get_config().get("ENFORCEMENT_ROLE", "CORTEX_USER_ROLE")
    return user_name.upper() in [u.upper() for u in get_users_with_role(role_name)]


def restore_access_if_under_budget(
    user_name: str, user_id: int, period_start: str, period_end: str
) -> dict:
    cfg = get_config()
    if cfg.get("ENFORCEMENT_ENABLED", "false").lower() != "true":
        return {"action": "none", "reason": "enforcement_disabled"}

    role = cfg.get("ENFORCEMENT_ROLE", "CORTEX_USER_ROLE")
    if user_has_role(user_name, role):
        return {"action": "none", "reason": "already_has_role"}

    all_spend = get_all_users_spend(period_start, period_end)
    if all_spend.empty:
        return {"action": "none", "reason": "no_data"}

    user_row = all_spend[all_spend["USER_NAME"] == user_name]
    if user_row.empty:
        err = grant_user_cortex_access(user_name, "Auto-restore: no spend data")
        return {"action": "grant", "error": err}

    status = user_row.iloc[0].get("STATUS", "")
    if status in ("OK", "WARNING", "NO BUDGET"):
        reason = f"Auto-restore: now {status} after budget/top-up change"
        err = grant_user_cortex_access(user_name, reason)
        return {"action": "grant", "error": err}

    return {"action": "none", "reason": f"still_{status}"}


def send_budget_alert(
    recipients: str, user_name: str, pct_used: float,
    budget: float, used: float, alert_type: str = "WARNING"
) -> str | None:
    cfg = get_config()
    integration = cfg.get("EMAIL_INTEGRATION", "MY_EMAIL_INT")
    subject = f"CoCo Budget Alert: {user_name} is {'OVER' if alert_type == 'OVER' else 'at ' + str(int(pct_used)) + '%'} budget"
    body = (
        f"User: {user_name}\n"
        f"Status: {alert_type}\n"
        f"Credits Used: {used:.4f}\n"
        f"Budget: {budget:.2f}\n"
        f"Percent Used: {pct_used:.1f}%\n\n"
        f"-- Sent by CoCo Budgets enforcement system"
    )
    safe_subj = subject.replace("'", "''")
    safe_body = body.replace("'", "''")
    safe_recip = recipients.replace("'", "''")
    err = run_ddl(
        f"CALL SYSTEM$SEND_EMAIL('{integration}', '{safe_recip}', '{safe_subj}', '{safe_body}')"
    )
    if cfg.get("SLACK_ENABLED", "false").lower() == "true":
        slack_url = cfg.get("SLACK_WEBHOOK_URL", "")
        if slack_url:
            _send_slack_alert(slack_url, subject, body)
    return err


def send_account_budget_alert(
    recipients: str, pct_used: float, budget: float,
    used: float, alert_type: str = "WARNING"
) -> str | None:
    cfg = get_config()
    integration = cfg.get("EMAIL_INTEGRATION", "MY_EMAIL_INT")
    label = "OVER" if alert_type == "OVER" else f"at {int(pct_used)}%"
    subject = f"CoCo Account Budget Alert: Account is {label} budget"
    body = (
        f"Target: ACCOUNT (all users)\n"
        f"Status: {alert_type}\n"
        f"Credits Used: {used:.4f}\n"
        f"Account Budget: {budget:.2f}\n"
        f"Percent Used: {pct_used:.1f}%\n\n"
        f"-- Sent by CoCo Budgets enforcement system"
    )
    safe_subj = subject.replace("'", "''")
    safe_body = body.replace("'", "''")
    safe_recip = recipients.replace("'", "''")
    err = run_ddl(
        f"CALL SYSTEM$SEND_EMAIL('{integration}', '{safe_recip}', '{safe_subj}', '{safe_body}')"
    )
    if cfg.get("SLACK_ENABLED", "false").lower() == "true":
        slack_url = cfg.get("SLACK_WEBHOOK_URL", "")
        if slack_url:
            _send_slack_alert(slack_url, subject, body)
    return err


def _send_slack_alert(webhook_url: str, subject: str, body: str) -> str | None:
    try:
        import json
        import urllib.request
        payload = json.dumps({"text": f"*{subject}*\n```{body}```"}).encode("utf-8")
        req = urllib.request.Request(
            webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
        return None
    except Exception as e:
        return str(e)


def check_alert_already_sent(user_id: int, alert_type: str, period_key: str) -> bool:
    df, _ = run_query(
        f"SELECT 1 FROM {FQN}.ALERT_STATE "
        f"WHERE USER_ID = {int(user_id)} AND ALERT_TYPE = '{alert_type}' AND PERIOD_KEY = '{period_key}' "
        f"LIMIT 1"
    )
    return not df.empty


def record_alert_sent(user_id: int, alert_type: str, period_key: str) -> None:
    run_ddl(
        f"INSERT INTO {FQN}.ALERT_STATE (USER_ID, ALERT_TYPE, PERIOD_KEY) "
        f"SELECT {int(user_id)}, '{alert_type}', '{period_key}' "
        f"WHERE NOT EXISTS (SELECT 1 FROM {FQN}.ALERT_STATE "
        f"WHERE USER_ID={int(user_id)} AND ALERT_TYPE='{alert_type}' AND PERIOD_KEY='{period_key}')"
    )


def run_enforcement_cycle(period_start: str, period_end: str) -> dict:
    cfg = get_config()
    if cfg.get("ENFORCEMENT_ENABLED", "false").lower() != "true":
        return {"status": "disabled", "actions": []}

    all_spend = get_all_users_spend(period_start, period_end)
    if all_spend.empty:
        return {"status": "no_data", "actions": []}

    actions = []
    recipients = cfg.get("ALERT_RECIPIENTS", "")
    alert_on_warning = cfg.get("ALERT_ON_WARNING", "true").lower() == "true"
    alert_on_over = cfg.get("ALERT_ON_OVER", "true").lower() == "true"
    period_key = period_start[:7]

    for _, row in all_spend.iterrows():
        status = row.get("STATUS", "")
        user_name = row.get("USER_NAME", "")
        user_id = row.get("USER_ID")
        pct = row.get("PCT_USED", 0)
        budget = row.get("EFFECTIVE_BUDGET", 0)
        used = row.get("TOTAL_USED", 0)

        if not user_name or status == "NO BUDGET":
            continue

        if status == "OVER":
            err = revoke_user_cortex_access(user_name, f"Over budget ({pct:.1f}%)")
            actions.append({"user": user_name, "action": "REVOKE", "error": err})
            if alert_on_over and recipients and user_id is not None:
                if not check_alert_already_sent(int(user_id), "OVER", period_key):
                    send_budget_alert(recipients, user_name, pct, budget, used, "OVER")
                    record_alert_sent(int(user_id), "OVER", period_key)

        elif status in ("OK", "WARNING"):
            if not user_has_role(user_name):
                err = grant_user_cortex_access(user_name, f"Auto-restore: now {status} ({pct:.1f}%)")
                actions.append({"user": user_name, "action": "RESTORE", "error": err})

            if status == "WARNING":
                if alert_on_warning and recipients and user_id is not None:
                    if not check_alert_already_sent(int(user_id), "WARNING", period_key):
                        send_budget_alert(recipients, user_name, pct, budget, used, "WARNING")
                        record_alert_sent(int(user_id), "WARNING", period_key)
                        actions.append({"user": user_name, "action": "ALERT_SENT"})

    acct_budget_df = get_account_budget()
    if not acct_budget_df.empty and recipients:
        acct_row = acct_budget_df.iloc[0]
        acct_limit = float(acct_row["BASE_PERIOD_CREDITS"])
        acct_threshold = int(acct_row.get("WARNING_THRESHOLD_PCT", 80))
        acct_topup_df, _ = run_query(
            f"SELECT COALESCE(SUM(CREDITS), 0) AS TC FROM {FQN}.BUDGET_TOPUPS "
            f"WHERE TARGET_TYPE = 'ACCOUNT' AND EFFECTIVE_START < '{period_end}' AND EFFECTIVE_END > '{period_start}'"
        )
        acct_topup = float(acct_topup_df.iloc[0]["TC"]) if not acct_topup_df.empty else 0.0
        effective_acct = acct_limit + acct_topup
        if effective_acct > 0:
            acct_used, _ = get_account_usage_credits(period_start, period_end)
            acct_pct = acct_used / effective_acct * 100
            if acct_pct >= 100 and alert_on_over:
                if not check_alert_already_sent(0, "ACCOUNT_OVER", period_key):
                    send_account_budget_alert(recipients, acct_pct, effective_acct, acct_used, "OVER")
                    record_alert_sent(0, "ACCOUNT_OVER", period_key)
                    actions.append({"user": "ACCOUNT", "action": "ACCOUNT_OVER_ALERT"})
            elif acct_pct >= acct_threshold and alert_on_warning:
                if not check_alert_already_sent(0, "ACCOUNT_WARNING", period_key):
                    send_account_budget_alert(recipients, acct_pct, effective_acct, acct_used, "WARNING")
                    record_alert_sent(0, "ACCOUNT_WARNING", period_key)
                    actions.append({"user": "ACCOUNT", "action": "ACCOUNT_WARNING_ALERT"})

    return {"status": "completed", "actions": actions}


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
