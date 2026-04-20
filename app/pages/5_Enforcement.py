import streamlit as st
import pandas as pd

from lib.connection import run_query, run_ddl, log_audit, FQN
from lib.config import get_config, clear_caches
from lib.usage_queries import get_users, get_all_users_spend, CORTEX_CODE_PRICING
from lib.enforcement import (
    get_enforcement_status, get_enforcement_log, get_model_allowlist,
    set_model_allowlist, revoke_user_cortex_access,
    grant_user_cortex_access, run_enforcement_cycle,
    get_scheduled_task_status, create_enforcement_task,
    suspend_enforcement_task, drop_enforcement_task,
)
from lib.alerts import send_budget_alert, _send_slack_alert
from lib.credit_limits import (
    get_account_credit_limits, set_account_credit_limit, unset_account_credit_limit,
    get_all_users_credit_limits, user_is_blocked, get_user_credit_limit,
    set_user_credit_limit, unset_user_credit_limit,
    block_user_cortex_code, unblock_user_cortex_code,
)
from lib.usage_queries import get_available_models
from lib.time import get_period_bounds, format_period

st.header("Enforcement & Controls")

st.markdown(
    "Manage **Cortex Code cost controls** at the account and user level, "
    "**enforce budgets** automatically, **restrict models**, and **send alerts**."
)

cfg = get_config()
tz = cfg.get("BUDGET_TIMEZONE", "UTC")
period_type = cfg.get("DEFAULT_PERIOD_TYPE", "MONTHLY")
p_start, p_end = get_period_bounds(period_type, tz)
ps = p_start.strftime("%Y-%m-%d %H:%M:%S")
pe = p_end.strftime("%Y-%m-%d %H:%M:%S")

tab_controls, tab_enforce, tab_models, tab_alerts, tab_log = st.tabs([
    "Cost Controls", "Budget Enforcement", "Model Allowlist", "Email Alerts", "Enforcement Log"
])

# ─── Tab 1: Cost Controls ───────────────────────────────────────────────────

with tab_controls:
    st.subheader("Cortex Code Cost Controls")

    st.info(
        "Snowflake provides **native daily credit limit parameters** that cap how many credits "
        "a user can consume in a rolling 24-hour window. These work at two levels:\n\n"
        "- **Account-level** — the default cap for **all** users\n"
        "- **User-level** — overrides the account default for **specific** users\n\n"
        "User-level settings **always take priority** over account-level. "
        "This page lets you manage both.",
        icon="ℹ️"
    )

    # ── Section 1: Account-Level Defaults ────────────────────────────────

    st.markdown("---")
    st.subheader("Account-Level Defaults")
    st.caption(
        "These apply to **every user** in your account unless they have a user-level override. "
        "Snowflake enforces these natively in a rolling 24-hour window."
    )

    acct_limits = get_account_credit_limits()
    cli_limit = acct_limits["CLI"]["value"]
    ss_limit = acct_limits["SNOWSIGHT"]["value"]

    col_guide, col_empty = st.columns([2, 1])
    with col_guide:
        st.markdown(
            "| Value | Meaning |\n"
            "|:------|:--------|\n"
            "| **-1** | Unlimited (no cap) |\n"
            "| **0** | Completely blocked |\n"
            "| **> 0** | Daily credit cap (rolling 24h) |"
        )

    col_cli, col_ss = st.columns(2)
    with col_cli:
        new_cli = st.number_input(
            "CLI Daily Limit (credits)", value=cli_limit,
            min_value=-1.0, step=1.0, key="acct_cli_limit",
            help="Controls Cortex Code CLI / Desktop usage for all users"
        )
    with col_ss:
        new_ss = st.number_input(
            "Snowsight Daily Limit (credits)", value=ss_limit,
            min_value=-1.0, step=1.0, key="acct_ss_limit",
            help="Controls Cortex Code browser (Snowsight) usage for all users"
        )

    with st.expander("What does Apply Account Defaults do?", expanded=False):
        st.markdown(
            "Sets the **account-level** daily credit limit for **both** CLI and Snowsight.\n\n"
            "- Runs `ALTER ACCOUNT SET CORTEX_CODE_CLI_DAILY_EST_CREDIT_LIMIT_PER_USER = <value>`\n"
            "- Runs `ALTER ACCOUNT SET CORTEX_CODE_SNOWSIGHT_DAILY_EST_CREDIT_LIMIT_PER_USER = <value>`\n"
            "- Setting **-1** removes the limit (runs `ALTER ACCOUNT UNSET ...`)\n"
            "- Setting **0** blocks **all** users who don't have a user-level override\n\n"
            "**Scope:** Every user in the account (unless they have a per-user override).\n\n"
            "**Reversible:** Yes — set back to -1 or change the value at any time."
        )

    if st.button("Apply Account Defaults", type="primary", key="apply_acct_limits"):
        errors = []
        if new_cli != cli_limit:
            if new_cli == -1:
                err = unset_account_credit_limit("CLI")
            else:
                err = set_account_credit_limit("CLI", new_cli)
            if err:
                errors.append(f"CLI: {err}")
        if new_ss != ss_limit:
            if new_ss == -1:
                err = unset_account_credit_limit("SNOWSIGHT")
            else:
                err = set_account_credit_limit("SNOWSIGHT", new_ss)
            if err:
                errors.append(f"Snowsight: {err}")
        if errors:
            st.error("Failed: " + "; ".join(errors))
        else:
            log_audit("UPDATE", "ACCOUNT_LIMITS", notes=f"CLI={new_cli}, Snowsight={new_ss}")
            clear_caches()
            st.success("Account-level defaults updated.")
            st.rerun()

    # ── Section 2: User-Level Overrides ──────────────────────────────────

    st.markdown("---")
    st.subheader("Per-User Credit Limit Overrides")
    st.caption(
        "Override the account default for specific users. "
        "User-level settings **always take priority** over account-level defaults."
    )

    users_df = get_users()
    user_names = sorted(users_df["USER_NAME"].tolist()) if not users_df.empty else []

    if user_names:
        override_user = st.selectbox("Select User", user_names, key="override_user_select")

        current_user_limits = get_user_credit_limit(override_user)
        cli_info = current_user_limits.get("CLI", {})
        ss_info = current_user_limits.get("SNOWSIGHT", {})
        cli_has_override = cli_info.get("level") == "USER"
        ss_has_override = ss_info.get("level") == "USER"

        if cli_has_override or ss_has_override:
            cli_display = cli_info.get("value", -1)
            ss_display = ss_info.get("value", -1)
            status_parts = []
            if cli_has_override:
                if cli_display == 0:
                    status_parts.append("CLI: **BLOCKED**")
                else:
                    status_parts.append(f"CLI: **{cli_display:.1f}** credits/day")
            else:
                status_parts.append("CLI: using account default")
            if ss_has_override:
                if ss_display == 0:
                    status_parts.append("Snowsight: **BLOCKED**")
                else:
                    status_parts.append(f"Snowsight: **{ss_display:.1f}** credits/day")
            else:
                status_parts.append("Snowsight: using account default")
            st.warning(f"**{override_user}** has user-level overrides: {' | '.join(status_parts)}")
        else:
            st.success(f"**{override_user}** is using account defaults (no overrides).")

        oc1, oc2 = st.columns(2)
        with oc1:
            user_cli_val = st.number_input(
                "CLI Daily Limit", value=cli_info.get("value", -1),
                min_value=-1.0, step=1.0, key="user_cli_override",
                help="-1 = unlimited, 0 = blocked, positive = daily cap"
            )
        with oc2:
            user_ss_val = st.number_input(
                "Snowsight Daily Limit", value=ss_info.get("value", -1),
                min_value=-1.0, step=1.0, key="user_ss_override",
                help="-1 = unlimited, 0 = blocked, positive = daily cap"
            )

        with st.expander("What do these buttons do?", expanded=False):
            st.markdown(
                "**Apply User Override**\n\n"
                "- Runs `ALTER USER \"<user>\" SET CORTEX_CODE_CLI_DAILY_EST_CREDIT_LIMIT_PER_USER = <value>`\n"
                "- Runs `ALTER USER \"<user>\" SET CORTEX_CODE_SNOWSIGHT_DAILY_EST_CREDIT_LIMIT_PER_USER = <value>`\n"
                "- This **overrides** the account default for this specific user\n"
                "- Setting -1 removes the override (runs `UNSET`)\n\n"
                "**Remove All Overrides**\n\n"
                "- Runs `ALTER USER \"<user>\" UNSET` for both parameters\n"
                "- The user reverts to using the account-level defaults\n\n"
                "**Reversible:** Yes — you can re-apply or remove overrides at any time."
            )

        bc1, bc2 = st.columns(2)
        with bc1:
            if st.button("Apply User Override", type="primary", key="apply_user_override"):
                errors = []
                if user_cli_val == -1:
                    err = unset_user_credit_limit(override_user, "CLI")
                else:
                    err = set_user_credit_limit(override_user, "CLI", user_cli_val)
                if err:
                    errors.append(f"CLI: {err}")
                if user_ss_val == -1:
                    err = unset_user_credit_limit(override_user, "SNOWSIGHT")
                else:
                    err = set_user_credit_limit(override_user, "SNOWSIGHT", user_ss_val)
                if err:
                    errors.append(f"Snowsight: {err}")
                if errors:
                    st.error("Failed: " + "; ".join(errors))
                else:
                    log_audit("UPDATE", "USER_LIMITS", notes=f"user={override_user}, CLI={user_cli_val}, Snowsight={user_ss_val}")
                    clear_caches()
                    st.success(f"Overrides applied for **{override_user}**.")
                    st.rerun()
        with bc2:
            if st.button("Remove All Overrides", key="remove_user_override",
                         disabled=not (cli_has_override or ss_has_override)):
                err = unblock_user_cortex_code(override_user)
                if err:
                    st.error(f"Failed: {err}")
                else:
                    log_audit("REMOVE_OVERRIDE", "USER_LIMITS", notes=f"user={override_user}")
                    clear_caches()
                    st.success(f"Removed overrides for **{override_user}** (now using account defaults).")
                    st.rerun()
    else:
        st.info("No users found in this account.")

    # ── Users with Overrides Table ───────────────────────────────────────

    st.markdown("---")
    st.subheader("All Users with Overrides")
    st.caption("Users who have user-level overrides set (by enforcement or manually). Everyone else uses account defaults.")

    with st.spinner("Scanning user-level overrides..."):
        overrides_df = get_all_users_credit_limits()

    if not overrides_df.empty:
        st.write(f"**{len(overrides_df)}** user(s) with user-level overrides:")

        display = overrides_df.copy()
        def _format_status(val):
            if val == 0:
                return "BLOCKED"
            elif val == -1:
                return "Unlimited"
            else:
                return f"{val:.1f} credits/day"

        display["CLI"] = display["CLI_LIMIT"].apply(_format_status)
        display["Snowsight"] = display["SNOWSIGHT_LIMIT"].apply(_format_status)
        display = display[["USER_NAME", "CLI", "Snowsight"]]
        st.dataframe(display, hide_index=True, use_container_width=True, height=250)
    else:
        st.success("No user-level overrides. All users are following account defaults.")


# ─── Tab 2: Budget Enforcement ───────────────────────────────────────────────

with tab_enforce:
    st.subheader("Budget Enforcement")

    st.info(
        "Budget enforcement **automatically** blocks users who exceed their period budget "
        "by setting their user-level daily credit limits to **0**. When a user is back "
        "under budget (e.g., after a top-up or budget increase), enforcement removes the "
        "override so they fall back to account defaults.\n\n"
        "This uses the **same native parameters** you see in the Cost Controls tab.",
        icon="ℹ️"
    )

    with st.expander("How does enforcement work?", expanded=False):
        st.markdown("""
**The enforcement cycle checks each user's cumulative spend against their period budget:**

1. **Over budget?** Set user-level CLI + Snowsight limits to **0** (blocked)
2. **Under budget but currently blocked?** **Remove** user-level overrides (restores account defaults)
3. **Under budget and not blocked?** No action needed

**Important notes:**
- Data from ACCOUNT_USAGE views can lag up to ~1 hour
- For **instant** cost control, use the **Model Allowlist** tab or set user limits directly in **Cost Controls**
- Enforcement only sets limits to 0 (block) or removes them (unblock) — it does NOT set custom daily caps
""")

    enforcement = get_enforcement_status()
    enabled = enforcement["enabled"]

    new_enabled = st.toggle(
        "Enforcement Enabled", value=enabled, key="enf_toggle",
        help="When ON, the enforcement cycle will block over-budget users."
    )

    if new_enabled != enabled:
        if st.button("Save Enforcement Setting", type="primary", key="save_enf"):
            safe_v = str(new_enabled).lower()
            run_ddl(
                f"MERGE INTO {FQN}.BUDGET_CONFIG tgt "
                f"USING (SELECT 'ENFORCEMENT_ENABLED' AS CK, '{safe_v}' AS CV) src "
                f"ON tgt.CONFIG_KEY = src.CK "
                f"WHEN MATCHED THEN UPDATE SET CONFIG_VALUE = src.CV, UPDATED_AT = CURRENT_TIMESTAMP() "
                f"WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE, UPDATED_AT) "
                f"VALUES (src.CK, src.CV, CURRENT_TIMESTAMP())"
            )
            log_audit("UPDATE", "ENFORCEMENT", notes=f"enabled={new_enabled}")
            clear_caches()
            st.success("Enforcement setting saved.")
            st.rerun()

    st.divider()

    col_run, col_manual = st.columns(2)

    with col_run:
        st.subheader("Run Enforcement Now")
        st.caption(
            "Checks all users against their budgets. "
            "Over-budget users get blocked; under-budget users with overrides get restored."
        )
        with st.expander("What does Run Enforcement do?", expanded=False):
            st.markdown(
                "Compares each user's **cumulative period spend** to their budget:\n\n"
                "1. **Over budget →** `ALTER USER SET ... = 0` for CLI + Snowsight (blocked)\n"
                "2. **Under budget but blocked →** `ALTER USER UNSET ...` (restores account defaults)\n"
                "3. **Under budget, not blocked →** No action\n\n"
                "**Data lag:** ACCOUNT_USAGE views can lag up to ~1 hour, so enforcement "
                "won't catch the very latest usage.\n\n"
                "**Reversible:** Yes — unblock via Quick Unblock or remove overrides."
            )
        if not enabled:
            st.warning("Enforcement is disabled. Enable it above first.")
        if st.button("Run Enforcement Cycle", type="primary", key="run_enforce",
                     disabled=not enabled):
            with st.spinner("Running enforcement..."):
                result = run_enforcement_cycle(ps, pe)
            if result["status"] == "completed":
                actions = result["actions"]
                if actions:
                    for a in actions:
                        if a.get("error"):
                            st.error(f"{a['user']}: {a['action']} failed — {a['error']}")
                        else:
                            st.success(f"{a['user']}: {a['action']}")
                else:
                    st.info("No enforcement actions needed — all users are within budget.")
                clear_caches()
            else:
                st.info(f"Enforcement result: {result['status']}")

    with col_manual:
        st.subheader("Quick Block / Unblock")
        st.caption("Quickly block or unblock a user (sets limits to 0 or removes overrides).")
        with st.expander("What do Block / Unblock do?", expanded=False):
            st.markdown(
                "**Block**\n\n"
                "- Runs `ALTER USER SET ... = 0` for **both** CLI and Snowsight\n"
                "- The user immediately loses Cortex Code access on both surfaces\n\n"
                "**Unblock**\n\n"
                "- Runs `ALTER USER UNSET ...` for **both** parameters\n"
                "- The user reverts to account-level defaults (which may still have a cap)\n\n"
                "**Reversible:** Yes — block and unblock at any time."
            )
        all_spend = get_all_users_spend(ps, pe)
        manual_user_names = sorted(all_spend["USER_NAME"].dropna().unique().tolist()) if not all_spend.empty else []

        if manual_user_names:
            manual_user = st.selectbox("User", manual_user_names, key="manual_user")
            manual_reason = st.text_input("Reason", value="Manual action", key="manual_reason")

            is_blocked = user_is_blocked(manual_user)
            if is_blocked:
                st.warning(f"**{manual_user}** is currently **BLOCKED** (daily limits = 0).")
            else:
                st.success(f"**{manual_user}** has access (using account defaults).")

            c1, c2 = st.columns(2)
            with c1:
                if st.button("Unblock", key="manual_grant", disabled=not is_blocked):
                    err = grant_user_cortex_access(manual_user, manual_reason)
                    if err:
                        st.error(f"Failed: {err}")
                    else:
                        log_audit("UNBLOCK", "ENFORCEMENT", notes=f"user={manual_user}")
                        st.success(f"Unblocked {manual_user}")
                        clear_caches()
                        st.rerun()
            with c2:
                if st.button("Block", key="manual_revoke", type="secondary", disabled=is_blocked):
                    err = revoke_user_cortex_access(manual_user, manual_reason)
                    if err:
                        st.error(f"Failed: {err}")
                    else:
                        log_audit("BLOCK", "ENFORCEMENT", notes=f"user={manual_user}")
                        st.success(f"Blocked {manual_user}")
                        clear_caches()
                        st.rerun()
        else:
            st.info("No users found.")

    st.divider()

    col_reset, col_sched = st.columns(2)

    with col_reset:
        st.subheader("Period Reset")
        st.caption("Remove all user-level overrides — restoring everyone to account defaults. Use at the start of a new period.")
        with st.expander("What does Period Reset do?", expanded=False):
            st.markdown(
                "- Finds every user who has a **user-level override** (set by enforcement or manually)\n"
                "- Runs `ALTER USER UNSET ...` for both CLI and Snowsight on **each** of those users\n"
                "- All users revert to the **account-level defaults**\n\n"
                "**When to use:** At the start of a new budget period so previously-blocked "
                "users get a fresh start.\n\n"
                "**Reversible:** Yes — but you'd need to re-apply individual overrides manually "
                "or run an enforcement cycle to re-block over-budget users."
            )
        overrides_df_enforce = get_all_users_credit_limits()
        if st.button("Reset All Users (Remove Overrides)", key="period_reset"):
            if not overrides_df_enforce.empty:
                count = 0
                for _, row in overrides_df_enforce.iterrows():
                    uname = row.get("USER_NAME", "")
                    if uname:
                        err = unblock_user_cortex_code(uname)
                        if not err:
                            count += 1
                            run_ddl(
                                f"INSERT INTO {FQN}.ENFORCEMENT_LOG (ACTION, USER_NAME, REASON) "
                                f"VALUES ('PERIOD_RESET', '{uname.replace(chr(39), chr(39)+chr(39))}', 'Period reset: removed user-level overrides')"
                            )
                log_audit("RESET", "ENFORCEMENT", notes=f"Cleared overrides for {count} users")
                clear_caches()
                st.success(f"Removed overrides for {count} users.")
                st.rerun()
            else:
                st.info("No user-level overrides to reset.")

    with col_sched:
        st.subheader("Scheduled Enforcement")
        st.caption("Run enforcement automatically on a schedule using a Snowflake Task.")
        with st.expander("What does Scheduled Enforcement do?", expanded=False):
            st.markdown(
                "**Create Scheduled Task**\n\n"
                "- Creates a Snowflake stored procedure + Task in `COCO_BUDGETS_DB.BUDGETS`\n"
                "- The task runs the enforcement cycle automatically on your chosen schedule\n"
                "- Uses the specified warehouse for compute\n\n"
                "**Suspend / Resume Task**\n\n"
                "- Pauses or resumes the scheduled task without deleting it\n\n"
                "**Remove Task**\n\n"
                "- Drops the task and stored procedure entirely\n\n"
                "**Reversible:** Yes — you can create, suspend, resume, or remove the task at any time."
            )

        task_info = get_scheduled_task_status()
        if task_info:
            state_color = "🟢" if task_info["state"] == "started" else "🔴"
            st.info(
                f"{state_color} **Task:** `{task_info['name']}` — "
                f"**State:** {task_info['state']} — "
                f"**Schedule:** {task_info['schedule']} — "
                f"**Warehouse:** {task_info['warehouse']}"
            )
            tc1, tc2 = st.columns(2)
            with tc1:
                if task_info["state"] == "started":
                    if st.button("Suspend Task", key="suspend_task"):
                        err = suspend_enforcement_task()
                        if err:
                            st.error(f"Failed: {err}")
                        else:
                            log_audit("SUSPEND", "ENFORCEMENT_TASK")
                            st.success("Task suspended.")
                            st.rerun()
                else:
                    if st.button("Resume Task", key="resume_task"):
                        err = run_ddl(f"ALTER TASK {FQN}.COCO_ENFORCEMENT_TASK RESUME")
                        if err:
                            st.error(f"Failed: {err}")
                        else:
                            log_audit("RESUME", "ENFORCEMENT_TASK")
                            st.success("Task resumed.")
                            st.rerun()
            with tc2:
                if st.button("Remove Task", key="drop_task", type="secondary"):
                    err = drop_enforcement_task()
                    if err:
                        st.error(f"Failed: {err}")
                    else:
                        log_audit("DROP", "ENFORCEMENT_TASK")
                        st.success("Task and procedure removed.")
                        st.rerun()
        else:
            sched_options = {
                "Every hour": "0 * * * *",
                "Every 4 hours": "0 */4 * * *",
                "Every 12 hours": "0 */12 * * *",
                "Daily at midnight": "0 0 * * *",
                "Daily at 8am": "0 8 * * *",
            }
            sched_choice = st.selectbox(
                "Schedule", list(sched_options.keys()), key="sched_choice",
                help="How often to run the enforcement cycle automatically."
            )
            sched_wh = st.text_input("Warehouse", value="COMPUTE_WH", key="sched_wh")
            if st.button("Create Scheduled Task", type="primary", key="create_task"):
                if not enabled:
                    st.error("Enable enforcement first before scheduling.")
                else:
                    cron = sched_options[sched_choice]
                    err = create_enforcement_task(sched_wh, cron)
                    if err:
                        st.error(f"Failed: {err}")
                    else:
                        log_audit("CREATE", "ENFORCEMENT_TASK", notes=f"cron={cron}")
                        st.success(f"Enforcement task created with schedule: **{sched_choice}**")
                        st.rerun()


# ─── Tab 3: Model Allowlist ──────────────────────────────────────────────────

with tab_models:
    st.subheader("Model Allowlist")

    st.markdown(
        "Control which Cortex AI models are available in your account. "
        "This is the **fastest cost control lever** — it takes effect immediately, "
        "unlike enforcement which depends on ACCOUNT_USAGE lag."
    )

    with st.expander("Understanding model costs", expanded=False):
        st.markdown("""
Different models have dramatically different costs. For example:
- **claude-opus** models cost **~2x more** than **claude-sonnet** models
- **Output tokens** cost **~5x more** than input tokens
- Restricting to only sonnet-class models can significantly reduce costs

Use the pricing table below to decide which models to allow.
""")

    with st.expander("Tiered RBAC governance pattern", expanded=False):
        st.markdown("""
A recommended approach for managing model access at scale is to create **tiered database roles**
that group models by cost level. This allows you to grant different user groups access to
appropriate model tiers.

| Tier | Role Name | Models Included | Use Case |
|:---|:---|:---|:---|
| **Standard** | `CORTEX_CODE_STANDARD` | `openai-gpt-5.2`, `claude-4-sonnet` | Default for all users |
| **Enhanced** | `CORTEX_CODE_ENHANCED` | Standard + `claude-sonnet-4-5`, `claude-sonnet-4-6` | Power users, approved teams |
| **Premium** | `CORTEX_CODE_PREMIUM` | Enhanced + `claude-opus-4-5`, `claude-opus-4-6` | By approval only |

**Example SQL to implement tiers:**
```sql
-- Create custom roles for each tier
CREATE ROLE IF NOT EXISTS CORTEX_CODE_STANDARD;
CREATE ROLE IF NOT EXISTS CORTEX_CODE_ENHANCED;
CREATE ROLE IF NOT EXISTS CORTEX_CODE_PREMIUM;

-- Assign users to appropriate tiers
GRANT ROLE CORTEX_CODE_STANDARD TO USER analyst_user;
GRANT ROLE CORTEX_CODE_ENHANCED TO USER senior_engineer;
GRANT ROLE CORTEX_CODE_PREMIUM TO USER ml_lead;
```

**How it works with the Model Allowlist:**
- Use the allowlist below to set the **account-wide maximum** (e.g., all models allowed)
- Use roles + row-access policies on internal tables to enforce per-user tier restrictions
- Combine with daily credit limits for defense-in-depth cost control
""")

    current_list = get_model_allowlist()
    is_all = current_list == ["ALL"]
    is_none = current_list == ["NONE"]

    if is_all:
        st.success("All models are currently allowed.")
    elif is_none:
        st.error("All models are currently **BLOCKED** (allowlist = NONE). No one can use Cortex AI.")
    else:
        st.info(f"**{len(current_list)}** model(s) allowed: {', '.join(current_list)}")

    st.divider()

    all_known = get_available_models()

    st.markdown("**Cortex Code Model Pricing** (credits per 1M tokens):")
    pricing_rows = []
    for m, r in CORTEX_CODE_PRICING.items():
        pricing_rows.append({
            "Model": m,
            "Input": r["input"],
            "Output": r["output"],
            "Cache Write": r["cache_write_input"],
            "Cache Read": r["cache_read_input"],
        })
    st.dataframe(pd.DataFrame(pricing_rows), hide_index=True, use_container_width=True)

    st.divider()

    mode = st.radio(
        "Allowlist Mode",
        ["Allow All", "Allow Selected Models", "Block All"],
        index=0 if is_all else (2 if is_none else 1),
        key="allowlist_mode",
        help="'Allow All' = no restrictions. 'Allow Selected' = only chosen models. 'Block All' = no Cortex AI."
    )

    if mode == "Allow Selected Models":
        defaults = current_list if not is_all and not is_none else all_known
        selected_models = st.multiselect(
            "Select allowed models",
            options=all_known,
            default=[m for m in defaults if m in all_known],
            key="model_select",
            help="Only these models will be available for Cortex Code."
        )
    else:
        selected_models = []

    if mode == "Block All":
        st.error("**This will block ALL Cortex AI usage across the entire account.**")

    with st.expander("What does Apply Model Allowlist do?", expanded=False):
        st.markdown(
            "- Runs `ALTER ACCOUNT SET CORTEX_MODELS_ALLOWLIST = '<model_list>'`\n\n"
            "- **Allow All:** Removes model restrictions entirely\n"
            "- **Allow Selected:** Only the chosen models will be usable\n"
            "- **Block All:** Sets the allowlist to NONE \u2014 **no one** can use Cortex AI\n\n"
            "**Takes effect immediately** (unlike budget enforcement which has data lag).\n\n"
            "**Reversible:** Yes \u2014 change the allowlist at any time."
        )

    if st.button("Apply Model Allowlist", type="primary", key="apply_allowlist"):
        if mode == "Allow All":
            err = set_model_allowlist(["ALL"])
        elif mode == "Block All":
            err = set_model_allowlist(["NONE"])
        else:
            if not selected_models:
                st.error("Select at least one model.")
                st.stop()
            err = set_model_allowlist(selected_models)

        if err:
            st.error(f"Failed: {err}")
        else:
            log_audit("UPDATE", "MODEL_ALLOWLIST", notes=f"mode={mode}")
            st.success(f"Model allowlist updated: **{mode}**")
            st.rerun()


# ─── Tab 4: Email Alerts ─────────────────────────────────────────────────────

with tab_alerts:
    st.subheader("Email Spending Alerts")

    st.markdown(
        "Get notified by email when users approach or exceed their budgets. "
        "Alerts use Snowflake's built-in `SYSTEM$SEND_EMAIL` and are "
        "**deduplicated** — each alert type is sent only once per user per period."
    )

    with st.expander("Alert types explained", expanded=False):
        st.markdown("""
| Alert Type | Trigger | Purpose |
|:---|:---|:---|
| **Warning** | User reaches their warning threshold (default 80%) | Proactive heads-up |
| **Over Budget** | User exceeds their credit limit | Urgent notification |

Alerts are sent during enforcement cycles. Each user receives at most
**one warning** and **one over-budget** alert per budget period.
""")

    current_integration = cfg.get("EMAIL_INTEGRATION", "MY_EMAIL_INT")
    current_recipients = cfg.get("ALERT_RECIPIENTS", "")
    alert_warning = cfg.get("ALERT_ON_WARNING", "true").lower() == "true"
    alert_over = cfg.get("ALERT_ON_OVER", "true").lower() == "true"

    integration_df, _ = run_query("SHOW NOTIFICATION INTEGRATIONS")
    available_integrations = []
    if not integration_df.empty and "name" in integration_df.columns:
        email_ints = integration_df[integration_df["type"] == "EMAIL"]
        available_integrations = email_ints["name"].tolist() if not email_ints.empty else []

    if available_integrations:
        new_integration = st.selectbox(
            "Notification Integration",
            available_integrations,
            index=available_integrations.index(current_integration) if current_integration in available_integrations else 0,
            key="alert_integration",
            help="The Snowflake notification integration used to send emails. Must be type EMAIL."
        )
    else:
        st.warning("No email notification integrations found. Use the setup wizard below to create one.")
        new_integration = st.text_input("Integration Name", value=current_integration, key="alert_integration_manual")

    wizard_label = "Create New Email Integration" if available_integrations else "Setup Wizard: Create Email Notification Integration"
    with st.expander(wizard_label, expanded=not available_integrations):
        st.markdown(
            "Snowflake needs a **notification integration** before it can send emails via `SYSTEM$SEND_EMAIL`. "
            "Fill in the details below and click **Create Integration** to set it up."
        )

        wiz_name = st.text_input(
            "New Integration Name",
            value="COCO_BUDGET_EMAIL_INT",
            key="wiz_int_name",
            help="Name for the new notification integration object."
        )
        wiz_emails = st.text_input(
            "Allowed Email Addresses (comma-separated)",
            key="wiz_allowed_emails",
            help="Only these email addresses will be able to receive emails from this integration."
        )

        if wiz_emails:
            emails_list = [e.strip() for e in wiz_emails.split(",") if e.strip()]
            formatted_emails = ", ".join(f"'{e}'" for e in emails_list)
            create_sql = (
                f"CREATE NOTIFICATION INTEGRATION IF NOT EXISTS {wiz_name}\n"
                f"  TYPE = EMAIL\n"
                f"  ENABLED = TRUE\n"
                f"  ALLOWED_RECIPIENTS = ({formatted_emails})"
            )
            st.code(create_sql, language="sql")

            if st.button("Create Integration", type="primary", key="wiz_create_int"):
                err = run_ddl(create_sql)
                if err:
                    st.error(f"Failed to create integration: {err}")
                else:
                    for k, v in [("EMAIL_INTEGRATION", wiz_name), ("ALERT_RECIPIENTS", wiz_emails)]:
                        safe_v = v.replace("'", "''")
                        run_ddl(
                            f"MERGE INTO {FQN}.BUDGET_CONFIG tgt "
                            f"USING (SELECT '{k}' AS CK, '{safe_v}' AS CV) src "
                            f"ON tgt.CONFIG_KEY = src.CK "
                            f"WHEN MATCHED THEN UPDATE SET CONFIG_VALUE = src.CV, UPDATED_AT = CURRENT_TIMESTAMP() "
                            f"WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE, UPDATED_AT) "
                            f"VALUES (src.CK, src.CV, CURRENT_TIMESTAMP())"
                        )
                    log_audit("CREATE", "EMAIL_INTEGRATION", notes=f"name={wiz_name}")
                    clear_caches()
                    st.success(f"Integration **{wiz_name}** created and saved to config.")
                    st.rerun()
        else:
            st.caption("Enter at least one email address to generate the SQL.")

    new_recipients = st.text_input(
        "Alert Recipients (comma-separated emails)",
        value=current_recipients,
        key="alert_recipients",
        help="Who should receive budget alerts? Separate multiple emails with commas."
    )

    col_w, col_o = st.columns(2)
    with col_w:
        new_alert_warning = st.checkbox("Send Warning Alerts", value=alert_warning, key="alert_warn_cb")
    with col_o:
        new_alert_over = st.checkbox("Send Over-Budget Alerts", value=alert_over, key="alert_over_cb")

    if st.button("Save Alert Settings", type="primary", key="save_alerts"):
        updates = {
            "EMAIL_INTEGRATION": new_integration,
            "ALERT_RECIPIENTS": new_recipients,
            "ALERT_ON_WARNING": str(new_alert_warning).lower(),
            "ALERT_ON_OVER": str(new_alert_over).lower(),
        }
        for k, v in updates.items():
            safe_v = v.replace("'", "''")
            run_ddl(
                f"MERGE INTO {FQN}.BUDGET_CONFIG tgt "
                f"USING (SELECT '{k}' AS CK, '{safe_v}' AS CV) src "
                f"ON tgt.CONFIG_KEY = src.CK "
                f"WHEN MATCHED THEN UPDATE SET CONFIG_VALUE = src.CV, UPDATED_AT = CURRENT_TIMESTAMP() "
                f"WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE, UPDATED_AT) "
                f"VALUES (src.CK, src.CV, CURRENT_TIMESTAMP())"
            )
        log_audit("UPDATE", "ALERT_CONFIG", notes=f"recipients={new_recipients}")
        clear_caches()
        st.success("Alert settings saved.")
        st.rerun()

    st.divider()
    st.subheader("Slack Notifications")
    st.caption(
        "Optionally send budget alerts to a Slack channel via an Incoming Webhook. "
        "Both email and Slack alerts fire during enforcement cycles."
    )

    current_slack_url = cfg.get("SLACK_WEBHOOK_URL", "")
    slack_enabled = cfg.get("SLACK_ENABLED", "false").lower() == "true"

    new_slack_enabled = st.toggle("Enable Slack Alerts", value=slack_enabled, key="slack_toggle")
    new_slack_url = st.text_input(
        "Slack Webhook URL",
        value=current_slack_url,
        key="slack_url",
        type="password",
        help="Create an Incoming Webhook in your Slack workspace and paste the URL here."
    )

    if st.button("Save Slack Settings", type="primary", key="save_slack"):
        for k, v in [("SLACK_ENABLED", str(new_slack_enabled).lower()),
                      ("SLACK_WEBHOOK_URL", new_slack_url)]:
            safe_v = v.replace("'", "''")
            run_ddl(
                f"MERGE INTO {FQN}.BUDGET_CONFIG tgt "
                f"USING (SELECT '{k}' AS CK, '{safe_v}' AS CV) src "
                f"ON tgt.CONFIG_KEY = src.CK "
                f"WHEN MATCHED THEN UPDATE SET CONFIG_VALUE = src.CV, UPDATED_AT = CURRENT_TIMESTAMP() "
                f"WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE, UPDATED_AT) "
                f"VALUES (src.CK, src.CV, CURRENT_TIMESTAMP())"
            )
        log_audit("UPDATE", "SLACK_CONFIG", notes=f"enabled={new_slack_enabled}")
        clear_caches()
        st.success("Slack settings saved.")
        st.rerun()

    if new_slack_url and new_slack_enabled:
        if st.button("Send Test Slack Message", key="test_slack"):
            err = _send_slack_alert(
                new_slack_url,
                "CoCo Budget Test Alert",
                "This is a test notification from CoCo Budgets."
            )
            if err:
                st.error(f"Slack test failed: {err}")
            else:
                st.success("Test message sent to Slack!")

    st.divider()
    st.subheader("Test Alert")
    st.caption("Send a test email to verify your notification integration and recipients are working.")
    if st.button("Send Test Alert", key="test_alert"):
        if not new_recipients:
            st.error("Set alert recipients first.")
        else:
            err = send_budget_alert(
                new_recipients, "TEST_USER", 85.0, 100.0, 85.0, "WARNING"
            )
            if err:
                st.error(f"Failed to send test alert: {err}")
            else:
                st.success(f"Test alert sent to {new_recipients}")

    st.divider()
    st.subheader("Alert History")
    st.caption("All alerts sent this period. Each alert type is sent only once per user per period.")
    alert_df, alert_err = run_query(
        f"SELECT a.ALERT_ID, u.NAME AS USER_NAME, a.ALERT_TYPE, a.PERIOD_KEY, a.SENT_AT "
        f"FROM {FQN}.ALERT_STATE a "
        f"LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON a.USER_ID = u.USER_ID "
        f"ORDER BY a.SENT_AT DESC LIMIT 50"
    )
    if not alert_err and not alert_df.empty:
        st.dataframe(alert_df, hide_index=True, use_container_width=True)
    else:
        st.info("No alerts sent yet. Alerts are triggered during enforcement cycles.")


# ─── Tab 5: Enforcement Log ──────────────────────────────────────────────────

with tab_log:
    st.subheader("Enforcement Log")
    st.caption("Every block, unblock, and override action performed by enforcement or manually.")
    log_limit = st.number_input("Rows", value=50, min_value=10, max_value=500, key="log_limit")
    log_df = get_enforcement_log(log_limit)
    if not log_df.empty:
        st.dataframe(log_df, hide_index=True, use_container_width=True)
    else:
        st.info("No enforcement actions logged yet. Actions appear here when enforcement runs or you manually block/unblock access.")
