import pandas as pd

from lib.connection import run_query, run_ddl, FQN
from lib.config import get_config
from lib.credit_limits import (
    block_user_cortex_code, unblock_user_cortex_code,
    user_is_blocked,
)
from lib.alerts import (
    send_budget_alert, send_account_budget_alert,
    check_alert_already_sent, record_alert_sent,
)
from lib.usage_queries import get_all_users_spend, get_account_budget, get_account_usage_credits


def get_enforcement_status() -> dict:
    cfg = get_config()
    enabled = cfg.get("ENFORCEMENT_ENABLED", "false").lower() == "true"
    role = cfg.get("ENFORCEMENT_ROLE", "CORTEX_USER_ROLE")
    return {"enabled": enabled, "role": role}


def revoke_user_cortex_access(user_name: str, reason: str = "Over budget") -> str | None:
    safe_user = user_name.replace("'", "''")
    err = block_user_cortex_code(user_name)
    if not err:
        safe_reason = reason.replace("'", "''")
        run_ddl(
            f"INSERT INTO {FQN}.ENFORCEMENT_LOG (ACTION, USER_NAME, REASON) "
            f"VALUES ('BLOCK', '{safe_user}', '{safe_reason}')"
        )
    return err


def grant_user_cortex_access(user_name: str, reason: str = "Budget reset / manual") -> str | None:
    safe_user = user_name.replace("'", "''")
    err = unblock_user_cortex_code(user_name)
    if not err:
        safe_reason = reason.replace("'", "''")
        run_ddl(
            f"INSERT INTO {FQN}.ENFORCEMENT_LOG (ACTION, USER_NAME, REASON) "
            f"VALUES ('UNBLOCK', '{safe_user}', '{safe_reason}')"
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

    from lib.credit_limits import user_has_access
    if user_has_access(user_name):
        return {"action": "none", "reason": "already_has_access"}

    all_spend = get_all_users_spend(period_start, period_end)
    if all_spend.empty:
        return {"action": "none", "reason": "no_data"}

    user_row = all_spend[all_spend["USER_NAME"] == user_name]
    if user_row.empty:
        err = grant_user_cortex_access(user_name, "Auto-restore: no spend data")
        return {"action": "unblock", "error": err}

    status = user_row.iloc[0].get("STATUS", "")
    if status in ("OK", "WARNING", "NO BUDGET"):
        reason = f"Auto-restore: now {status} after budget/top-up change"
        err = grant_user_cortex_access(user_name, reason)
        return {"action": "unblock", "error": err}

    return {"action": "none", "reason": f"still_{status}"}


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
            actions.append({"user": user_name, "action": "BLOCK", "error": err})
            if alert_on_over and recipients and user_id is not None:
                if not check_alert_already_sent(int(user_id), "OVER", period_key):
                    send_budget_alert(recipients, user_name, pct, budget, used, "OVER")
                    record_alert_sent(int(user_id), "OVER", period_key)

        elif status in ("OK", "WARNING"):
            if user_is_blocked(user_name):
                err = grant_user_cortex_access(user_name, f"Auto-restore: now {status} ({pct:.1f}%)")
                actions.append({"user": user_name, "action": "UNBLOCK", "error": err})

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
