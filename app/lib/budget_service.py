from lib.connection import run_query, run_ddl, log_audit, FQN
from lib.config import clear_caches, upsert_config


def create_user_budget(user_id: int, credits: float, period_type: str,
                       warning_threshold_pct: int) -> str | None:
    safe_period = period_type.replace("'", "''")
    err = run_ddl(
        f"INSERT INTO {FQN}.USER_BUDGETS "
        f"(USER_ID, BASE_PERIOD_CREDITS, PERIOD_TYPE, WARNING_THRESHOLD_PCT) "
        f"VALUES ({int(user_id)}, {float(credits)}, '{safe_period}', {int(warning_threshold_pct)})"
    )
    if not err:
        log_audit("CREATE", "USER", user_id, new_value=credits, notes=f"Period={period_type}")
        clear_caches()
    return err


def update_user_budget(user_id: int, credits: float, warning_threshold_pct: int,
                       is_active: bool, period_type: str,
                       old_credits: float | None = None) -> str | None:
    safe_period = period_type.replace("'", "''")
    err = run_ddl(
        f"UPDATE {FQN}.USER_BUDGETS SET "
        f"BASE_PERIOD_CREDITS = {credits}, "
        f"WARNING_THRESHOLD_PCT = {warning_threshold_pct}, "
        f"IS_ACTIVE = {is_active}, "
        f"PERIOD_TYPE = '{safe_period}', "
        f"UPDATED_AT = CURRENT_TIMESTAMP() "
        f"WHERE USER_ID = {user_id}"
    )
    if not err:
        log_audit("UPDATE", "USER", user_id, old_value=old_credits, new_value=credits)
    return err


def bulk_create_user_budgets(user_ids: list[int], credits: float,
                             period_type: str, warning_threshold_pct: int) -> int:
    safe_period = period_type.replace("'", "''")
    count = 0
    for uid in user_ids:
        err = run_ddl(
            f"INSERT INTO {FQN}.USER_BUDGETS "
            f"(USER_ID, BASE_PERIOD_CREDITS, PERIOD_TYPE, WARNING_THRESHOLD_PCT) "
            f"SELECT {int(uid)}, {float(credits)}, '{safe_period}', {int(warning_threshold_pct)} "
            f"WHERE NOT EXISTS (SELECT 1 FROM {FQN}.USER_BUDGETS WHERE USER_ID={int(uid)})"
        )
        if not err:
            count += 1
    if count:
        log_audit("BULK_CREATE", "USER", notes=f"Added {count} users at {credits} credits")
        clear_caches()
    return count


def grant_user_topup(user_id: int, credits: float,
                     period_start: str, period_end: str,
                     notes: str = "") -> str | None:
    notes_val = notes.replace("'", "''") if notes else ""
    err = run_ddl(
        f"INSERT INTO {FQN}.BUDGET_TOPUPS "
        f"(TARGET_TYPE, USER_ID, CREDITS, EFFECTIVE_START, EFFECTIVE_END, NOTES) "
        f"VALUES ('USER', {user_id}, {credits}, '{period_start}', '{period_end}', '{notes_val}')"
    )
    if not err:
        log_audit("TOPUP", "USER", user_id, new_value=credits, notes=notes)
        clear_caches()
    return err


def save_account_budget(budget: float, period_type: str,
                        warning_threshold_pct: int,
                        old_budget: float = 0.0) -> str | None:
    run_ddl(
        f"UPDATE {FQN}.ACCOUNT_BUDGET SET "
        f"IS_ACTIVE = FALSE, EFFECTIVE_END = CURRENT_TIMESTAMP() "
        f"WHERE IS_ACTIVE = TRUE"
    )
    safe_period = period_type.replace("'", "''")
    err = run_ddl(
        f"INSERT INTO {FQN}.ACCOUNT_BUDGET "
        f"(IS_ACTIVE, BASE_PERIOD_CREDITS, PERIOD_TYPE, WARNING_THRESHOLD_PCT) "
        f"VALUES (TRUE, {float(budget)}, '{safe_period}', {int(warning_threshold_pct)})"
    )
    if not err:
        log_audit("UPDATE", "ACCOUNT", old_value=old_budget, new_value=budget)
        clear_caches()
    return err


def grant_account_topup(credits: float, period_start: str,
                        period_end: str, notes: str = "") -> str | None:
    notes_val = notes.replace("'", "''") if notes else ""
    err = run_ddl(
        f"INSERT INTO {FQN}.BUDGET_TOPUPS "
        f"(TARGET_TYPE, CREDITS, EFFECTIVE_START, EFFECTIVE_END, NOTES) "
        f"VALUES ('ACCOUNT', {credits}, '{period_start}', '{period_end}', '{notes_val}')"
    )
    if not err:
        log_audit("TOPUP", "ACCOUNT", new_value=credits, notes=notes)
        clear_caches()
    return err


def save_config_batch(updates: dict[str, str]) -> list[str]:
    errors = []
    for k, v in updates.items():
        err = upsert_config(k, v)
        if err:
            errors.append(f"{k}: {err}")
    if not errors:
        clear_caches()
    return errors
