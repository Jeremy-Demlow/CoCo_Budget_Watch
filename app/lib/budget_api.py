from __future__ import annotations
import pandas as pd
from lib.connection import run_ddl, run_query, FQN

TAG_FQN = f"{FQN}.COST_CENTER"

AI_DOMAINS = [
    "AI FUNCTION",
    "CORTEX CODE",
    "CORTEX AGENT",
    "SNOWFLAKE INTELLIGENCE",
]


def _q(s: str) -> str:
    return s.replace("'", "''")


def _safe_budget_fqn(db: str, schema: str, name: str) -> str:
    return f"{db}.{schema}.{name}"


def check_privileges() -> dict:
    checks = {}

    cu_df, err = run_query("SELECT CURRENT_USER() AS U")
    checks["connection_ok"] = err is None
    current_user = cu_df["U"].iloc[0] if (err is None and not cu_df.empty) else None

    if current_user:
        df, err = run_query(
            f"SHOW PARAMETERS LIKE 'CORTEX_CODE_CLI_DAILY_EST_CREDIT_LIMIT_PER_USER' IN USER \"{current_user}\""
        )
        checks["manage_user"] = err is None
    else:
        checks["manage_user"] = False

    df, err = run_query(f"SHOW TAGS IN SCHEMA {FQN}")
    checks["show_tags"] = err is None

    df, err = run_query(f"SHOW BUDGETS IN SCHEMA {FQN}")
    checks["show_budgets"] = err is None
    budgets_err = (err or "").lower()

    df, err = run_query(
        "SELECT VALUE FROM TABLE(SYSTEM$SHOW_BUDGET_SHARED_RESOURCE_CANDIDATES()) LIMIT 1"
    )
    checks["show_candidates"] = err is None
    candidates_err = (err or "").lower()

    feature_unavailable = (
        "does not exist" in budgets_err
        or "unknown table function" in candidates_err
    )
    checks["budgets_feature_available"] = not feature_unavailable

    return checks


def create_cost_center_tag(tag_value: str | None = None, description: str = "") -> str | None:
    err = run_ddl(
        f"CREATE TAG IF NOT EXISTS {TAG_FQN} "
        f"COMMENT = 'Cost center tag for native AI budget scoping'"
    )
    if err:
        return err

    if tag_value:
        safe_val = _q(tag_value)
        safe_db = _q(FQN.split(".")[0])
        safe_schema = _q(FQN.split(".")[1])
        safe_desc = _q(description)
        err = run_ddl(
            f"INSERT INTO {FQN}.COST_CENTER_TAGS "
            f"(TAG_DB, TAG_SCHEMA, TAG_NAME, TAG_VALUE, DESCRIPTION) "
            f"SELECT '{safe_db}', '{safe_schema}', 'COST_CENTER', '{safe_val}', '{safe_desc}' "
            f"WHERE NOT EXISTS ("
            f"  SELECT 1 FROM {FQN}.COST_CENTER_TAGS "
            f"  WHERE TAG_DB='{safe_db}' AND TAG_SCHEMA='{safe_schema}' "
            f"  AND TAG_NAME='COST_CENTER' AND TAG_VALUE='{safe_val}'"
            f")"
        )
    return err


def list_cost_center_tags() -> pd.DataFrame:
    df, _ = run_query(
        f"SELECT TAG_ID, TAG_VALUE, DESCRIPTION, CREATED_AT, CREATED_BY "
        f"FROM {FQN}.COST_CENTER_TAGS "
        f"ORDER BY TAG_VALUE"
    )
    return df


def delete_cost_center_tag_value(tag_value: str) -> str | None:
    safe_val = _q(tag_value)
    return run_ddl(
        f"DELETE FROM {FQN}.COST_CENTER_TAGS "
        f"WHERE TAG_NAME='COST_CENTER' AND TAG_VALUE='{safe_val}'"
    )


def tag_user_cost_center(user_name: str, user_id: int, tag_value: str) -> str | None:
    safe_user = user_name.replace('"', '\\"')
    safe_val = _q(tag_value)
    err = run_ddl(f'ALTER USER "{safe_user}" SET TAG {TAG_FQN} = \'{safe_val}\'')
    if not err:
        safe_uname = _q(user_name)
        db_part = _q(FQN.split(".")[0])
        schema_part = _q(FQN.split(".")[1])
        run_ddl(
            f"INSERT INTO {FQN}.USER_TAG_ASSIGNMENTS "
            f"(USER_ID, USER_NAME, TAG_DB, TAG_SCHEMA, TAG_NAME, TAG_VALUE, ACTION) "
            f"VALUES ({int(user_id)}, '{safe_uname}', '{db_part}', '{schema_part}', 'COST_CENTER', '{safe_val}', 'SET')"
        )
    return err


def untag_user_cost_center(user_name: str, user_id: int) -> str | None:
    safe_user = user_name.replace('"', '\\"')
    err = run_ddl(f'ALTER USER "{safe_user}" UNSET TAG {TAG_FQN}')
    if not err:
        safe_uname = _q(user_name)
        db_part = _q(FQN.split(".")[0])
        schema_part = _q(FQN.split(".")[1])
        run_ddl(
            f"INSERT INTO {FQN}.USER_TAG_ASSIGNMENTS "
            f"(USER_ID, USER_NAME, TAG_DB, TAG_SCHEMA, TAG_NAME, TAG_VALUE, ACTION) "
            f"VALUES ({int(user_id)}, '{safe_uname}', '{db_part}', '{schema_part}', 'COST_CENTER', NULL, 'UNSET')"
        )
    return err


def get_user_current_tag(user_name: str) -> str | None:
    safe_user = _q(user_name)
    df, err = run_query(
        f"SELECT SYSTEM$GET_TAG('{TAG_FQN}', '{safe_user}', 'USER') AS TAG_VALUE"
    )
    if err or df.empty:
        return None
    val = df.iloc[0]["TAG_VALUE"]
    return str(val) if val is not None else None


def get_tag_assignment_log(limit: int = 100) -> pd.DataFrame:
    df, _ = run_query(
        f"SELECT ASSIGNMENT_ID, USER_NAME, TAG_VALUE, ACTION, ASSIGNED_AT, ASSIGNED_BY "
        f"FROM {FQN}.USER_TAG_ASSIGNMENTS "
        f"ORDER BY ASSIGNED_AT DESC LIMIT {int(limit)}"
    )
    return df


def create_native_budget(
    budget_name: str,
    credit_quota: float,
    description: str = "",
    budget_db: str = "COCO_BUDGETS_DB",
    budget_schema: str = "BUDGETS",
) -> str | None:
    fqn = _safe_budget_fqn(budget_db, budget_schema, budget_name)
    err = run_ddl(f"CREATE SNOWFLAKE.CORE.BUDGET IF NOT EXISTS {fqn} ()")
    if err:
        return err
    err = run_ddl(f"CALL {fqn}!SET_SPENDING_LIMIT({int(credit_quota)})")
    if err:
        return err
    safe_name = _q(budget_name)
    safe_db = _q(budget_db)
    safe_schema = _q(budget_schema)
    safe_desc = _q(description)
    err2 = run_ddl(
        f"INSERT INTO {FQN}.SNOWFLAKE_BUDGET_REGISTRY "
        f"(BUDGET_DB, BUDGET_SCHEMA, BUDGET_NAME, CREDIT_QUOTA, DESCRIPTION) "
        f"SELECT '{safe_db}', '{safe_schema}', '{safe_name}', {float(credit_quota)}, '{safe_desc}' "
        f"WHERE NOT EXISTS ("
        f"  SELECT 1 FROM {FQN}.SNOWFLAKE_BUDGET_REGISTRY "
        f"  WHERE BUDGET_DB='{safe_db}' AND BUDGET_SCHEMA='{safe_schema}' AND BUDGET_NAME='{safe_name}'"
        f")"
    )
    return err2


def alter_native_budget_quota(
    budget_name: str,
    credit_quota: float,
    budget_db: str = "COCO_BUDGETS_DB",
    budget_schema: str = "BUDGETS",
) -> str | None:
    fqn = _safe_budget_fqn(budget_db, budget_schema, budget_name)
    err = run_ddl(f"CALL {fqn}!SET_SPENDING_LIMIT({int(credit_quota)})")
    if not err:
        safe_name = _q(budget_name)
        safe_db = _q(budget_db)
        safe_schema = _q(budget_schema)
        run_ddl(
            f"UPDATE {FQN}.SNOWFLAKE_BUDGET_REGISTRY "
            f"SET CREDIT_QUOTA = {float(credit_quota)} "
            f"WHERE BUDGET_DB='{safe_db}' AND BUDGET_SCHEMA='{safe_schema}' AND BUDGET_NAME='{safe_name}'"
        )
    return err


def drop_native_budget(
    budget_name: str,
    budget_db: str = "COCO_BUDGETS_DB",
    budget_schema: str = "BUDGETS",
) -> str | None:
    fqn = _safe_budget_fqn(budget_db, budget_schema, budget_name)
    native_err = run_ddl(f"DROP SNOWFLAKE.CORE.BUDGET IF EXISTS {fqn}")
    feature_unavailable = native_err and (
        "does not exist" in native_err.lower()
        or "not authorized" in native_err.lower()
    )
    safe_name = _q(budget_name)
    safe_db = _q(budget_db)
    safe_schema = _q(budget_schema)
    reg_err = run_ddl(
        f"DELETE FROM {FQN}.SNOWFLAKE_BUDGET_REGISTRY "
        f"WHERE BUDGET_DB='{safe_db}' AND BUDGET_SCHEMA='{safe_schema}' AND BUDGET_NAME='{safe_name}'"
    )
    if native_err and not feature_unavailable:
        return native_err
    return reg_err


def list_native_budgets(
    budget_db: str = "COCO_BUDGETS_DB",
    budget_schema: str = "BUDGETS",
) -> pd.DataFrame:
    df, _ = run_query(
        f"SELECT BUDGET_ID, BUDGET_DB, BUDGET_SCHEMA, BUDGET_NAME, CREDIT_QUOTA, "
        f"DESCRIPTION, CREATED_AT, CREATED_BY "
        f"FROM {FQN}.SNOWFLAKE_BUDGET_REGISTRY "
        f"ORDER BY CREATED_AT DESC"
    )
    return df


def show_budgets_in_schema(
    budget_db: str = "COCO_BUDGETS_DB",
    budget_schema: str = "BUDGETS",
) -> tuple[pd.DataFrame, str | None]:
    df, err = run_query(f"SHOW BUDGETS IN SCHEMA {budget_db}.{budget_schema}")
    return df, err


def add_shared_resource(
    budget_name: str,
    domain: str,
    budget_db: str = "COCO_BUDGETS_DB",
    budget_schema: str = "BUDGETS",
) -> str | None:
    if domain not in AI_DOMAINS:
        return f"Invalid domain '{domain}'. Must be one of: {AI_DOMAINS}"
    fqn = _safe_budget_fqn(budget_db, budget_schema, budget_name)
    safe_domain = _q(domain)
    return run_ddl(
        f"CALL {fqn}!ADD_SHARED_RESOURCE('{safe_domain}')"
    )


def remove_shared_resource(
    budget_name: str,
    domain: str,
    budget_db: str = "COCO_BUDGETS_DB",
    budget_schema: str = "BUDGETS",
) -> str | None:
    if domain not in AI_DOMAINS:
        return f"Invalid domain '{domain}'. Must be one of: {AI_DOMAINS}"
    fqn = _safe_budget_fqn(budget_db, budget_schema, budget_name)
    safe_domain = _q(domain)
    return run_ddl(
        f"CALL {fqn}!REMOVE_SHARED_RESOURCE('{safe_domain}')"
    )


def set_budget_user_tags(
    budget_name: str,
    tag_value: str,
    mode: str = "UNION",
    budget_db: str = "COCO_BUDGETS_DB",
    budget_schema: str = "BUDGETS",
) -> str | None:
    valid_modes = ("UNION", "INTERSECTION")
    if mode.upper() not in valid_modes:
        return f"Invalid mode '{mode}'. Must be one of: {valid_modes}"
    fqn = _safe_budget_fqn(budget_db, budget_schema, budget_name)
    safe_val = _q(tag_value)
    tag_ref_sql = f"(SELECT SYSTEM$REFERENCE('TAG', '{TAG_FQN}', 'SESSION', 'APPLYBUDGET'))"
    return run_ddl(
        f"CALL {fqn}!SET_USER_TAGS("
        f"[[{tag_ref_sql}, '{safe_val}']], "
        f"'{mode.upper()}')"
    )


def get_budget_scope(
    budget_name: str,
    budget_db: str = "COCO_BUDGETS_DB",
    budget_schema: str = "BUDGETS",
) -> tuple[pd.DataFrame, str | None]:
    fqn = _safe_budget_fqn(budget_db, budget_schema, budget_name)
    df, err = run_query(f"CALL {fqn}!GET_BUDGET_SCOPE()")
    return df, err


def get_shared_resource_candidates() -> tuple[pd.DataFrame, str | None]:
    df, err = run_query(
        "SELECT * FROM TABLE(SYSTEM$SHOW_BUDGET_SHARED_RESOURCE_CANDIDATES())"
    )
    return df, err


def get_budget_usage(
    budget_name: str,
    budget_db: str = "COCO_BUDGETS_DB",
    budget_schema: str = "BUDGETS",
    start_month: str | None = None,
    end_month: str | None = None,
) -> tuple[pd.DataFrame, str | None]:
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    if not start_month:
        start_month = now.strftime("%Y-%m")
    if not end_month:
        end_month = now.strftime("%Y-%m")
    fqn = _safe_budget_fqn(budget_db, budget_schema, budget_name)
    df, err = run_query(f"CALL {fqn}!GET_SERVICE_TYPE_USAGE_V2('{start_month}', '{end_month}')")
    return df, err


def get_user_tags_for_budget(
    budget_name: str,
    budget_db: str = "COCO_BUDGETS_DB",
    budget_schema: str = "BUDGETS",
) -> tuple[pd.DataFrame, str | None]:
    fqn = _safe_budget_fqn(budget_db, budget_schema, budget_name)
    df, err = run_query(f"CALL {fqn}!GET_BUDGET_SCOPE()")
    return df, err


def unset_all_budget_user_tags(
    budget_name: str,
    budget_db: str = "COCO_BUDGETS_DB",
    budget_schema: str = "BUDGETS",
) -> str | None:
    fqn = _safe_budget_fqn(budget_db, budget_schema, budget_name)
    return run_ddl(f"CALL {fqn}!SET_USER_TAGS([], 'UNION')")
