import streamlit as st
import pandas as pd

from lib.connection import run_query, run_ddl, FQN
from lib.usage_queries import get_users


PARAM_CLI = "CORTEX_CODE_CLI_DAILY_EST_CREDIT_LIMIT_PER_USER"
PARAM_SNOWSIGHT = "CORTEX_CODE_SNOWSIGHT_DAILY_EST_CREDIT_LIMIT_PER_USER"

SURFACE_PARAMS = {"CLI": PARAM_CLI, "SNOWSIGHT": PARAM_SNOWSIGHT}


def get_account_credit_limits() -> dict:
    result = {}
    for surface, param in SURFACE_PARAMS.items():
        df, err = run_query(f"SHOW PARAMETERS LIKE '{param}' IN ACCOUNT")
        if err or df.empty:
            result[surface] = {"value": -1, "level": "DEFAULT"}
            continue
        row = df.iloc[0]
        val_col = "value" if "value" in df.columns else "VALUE"
        level_col = "level" if "level" in df.columns else "LEVEL"
        result[surface] = {
            "value": float(row.get(val_col, -1)),
            "level": str(row.get(level_col, "DEFAULT")),
        }
    return result


def set_account_credit_limit(surface: str, value: float) -> str | None:
    param = SURFACE_PARAMS.get(surface.upper())
    if not param:
        return f"Unknown surface: {surface}"
    return run_ddl(f"ALTER ACCOUNT SET {param} = {value}")


def unset_account_credit_limit(surface: str) -> str | None:
    param = SURFACE_PARAMS.get(surface.upper())
    if not param:
        return f"Unknown surface: {surface}"
    return run_ddl(f"ALTER ACCOUNT UNSET {param}")


def get_user_credit_limit(user_name: str) -> dict:
    safe_user = user_name.replace('"', '\\"')
    result = {}
    for surface, param in SURFACE_PARAMS.items():
        df, err = run_query(f"SHOW PARAMETERS LIKE '{param}' IN USER \"{safe_user}\"")
        if err or df.empty:
            result[surface] = {"value": -1, "level": "DEFAULT"}
            continue
        row = df.iloc[0]
        val_col = "value" if "value" in df.columns else "VALUE"
        level_col = "level" if "level" in df.columns else "LEVEL"
        result[surface] = {
            "value": float(row.get(val_col, -1)),
            "level": str(row.get(level_col, "DEFAULT")),
        }
    return result


def set_user_credit_limit(user_name: str, surface: str, value: float) -> str | None:
    param = SURFACE_PARAMS.get(surface.upper())
    if not param:
        return f"Unknown surface: {surface}"
    safe_user = user_name.replace('"', '\\"')
    return run_ddl(f'ALTER USER "{safe_user}" SET {param} = {value}')


def unset_user_credit_limit(user_name: str, surface: str) -> str | None:
    param = SURFACE_PARAMS.get(surface.upper())
    if not param:
        return f"Unknown surface: {surface}"
    safe_user = user_name.replace('"', '\\"')
    return run_ddl(f'ALTER USER "{safe_user}" UNSET {param}')


def block_user_cortex_code(user_name: str) -> str | None:
    err1 = set_user_credit_limit(user_name, "CLI", 0)
    err2 = set_user_credit_limit(user_name, "SNOWSIGHT", 0)
    return err1 or err2


def unblock_user_cortex_code(user_name: str) -> str | None:
    err1 = unset_user_credit_limit(user_name, "CLI")
    err2 = unset_user_credit_limit(user_name, "SNOWSIGHT")
    return err1 or err2


def user_is_blocked(user_name: str) -> bool:
    limits = get_user_credit_limit(user_name)
    for surface in ("CLI", "SNOWSIGHT"):
        info = limits.get(surface, {})
        if info.get("level") == "USER" and info.get("value") == 0:
            return True
    return False


def user_has_access(user_name: str) -> bool:
    return not user_is_blocked(user_name)


@st.cache_data(ttl=300, show_spinner=False)
def get_all_users_credit_limits() -> pd.DataFrame:
    users_df = get_users()
    if users_df.empty:
        return pd.DataFrame()
    rows = []
    for _, u in users_df.iterrows():
        uname = u["USER_NAME"]
        limits = get_user_credit_limit(uname)
        cli_info = limits.get("CLI", {})
        ss_info = limits.get("SNOWSIGHT", {})
        if cli_info.get("level") == "USER" or ss_info.get("level") == "USER":
            rows.append({
                "USER_NAME": uname,
                "CLI_LIMIT": cli_info.get("value", -1),
                "CLI_LEVEL": cli_info.get("level", "DEFAULT"),
                "SNOWSIGHT_LIMIT": ss_info.get("value", -1),
                "SNOWSIGHT_LEVEL": ss_info.get("level", "DEFAULT"),
            })
    return pd.DataFrame(rows)
