import streamlit as st

from lib.connection import run_query, run_ddl, FQN


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


def cfg_bool(key: str, default: bool = False) -> bool:
    return get_config().get(key, str(default)).lower() == "true"


def cfg_float(key: str, default: float = 0.0) -> float:
    try:
        return float(get_config().get(key, str(default)))
    except (ValueError, TypeError):
        return default


def cfg_int(key: str, default: int = 0) -> int:
    try:
        return int(get_config().get(key, str(default)))
    except (ValueError, TypeError):
        return default


def cfg_str(key: str, default: str = "") -> str:
    return get_config().get(key, default)


def upsert_config(key: str, value: str) -> str | None:
    safe_v = value.replace("'", "''")
    return run_ddl(
        f"MERGE INTO {FQN}.BUDGET_CONFIG tgt "
        f"USING (SELECT '{key}' AS CK, '{safe_v}' AS CV) src "
        f"ON tgt.CONFIG_KEY = src.CK "
        f"WHEN MATCHED THEN UPDATE SET CONFIG_VALUE = src.CV, UPDATED_AT = CURRENT_TIMESTAMP() "
        f"WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE, UPDATED_AT) "
        f"VALUES (src.CK, src.CV, CURRENT_TIMESTAMP())"
    )


def clear_caches():
    from lib.usage_queries import (
        get_users, get_coco_active_users, get_usage_by_user,
        get_daily_trend, get_model_breakdown, get_all_users_spend,
        get_account_usage_credits, get_daily_cumulative_spend,
        get_cache_efficiency, get_output_ratio, get_model_token_type_breakdown,
        get_rolling_24h_spend, get_new_user_onboarding, get_ai_services_context,
        get_account_user_breakdown, get_user_budgets, get_account_budget,
    )
    from lib.credit_limits import get_all_users_credit_limits

    for fn in [
        get_config,
        get_users, get_coco_active_users, get_usage_by_user,
        get_daily_trend, get_model_breakdown,
        get_user_budgets, get_account_budget, get_all_users_spend,
        get_account_usage_credits, get_daily_cumulative_spend,
        get_cache_efficiency, get_output_ratio, get_model_token_type_breakdown,
        get_rolling_24h_spend, get_new_user_onboarding, get_ai_services_context,
        get_account_user_breakdown, get_all_users_credit_limits,
    ]:
        fn.clear()


LATENCY_BANNER = (
    "**Data Freshness:** ACCOUNT_USAGE views can lag up to **~1 hour**. "
    "Enable **Enforcement** to automatically block access when budgets are exceeded "
    "using Snowflake's native daily credit limit parameters."
)
