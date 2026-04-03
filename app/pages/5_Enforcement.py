import streamlit as st
import pandas as pd

from lib.db import (
    run_query, run_ddl, get_config, clear_caches, log_audit,
    get_enforcement_status, get_enforcement_log, get_model_allowlist,
    set_model_allowlist, get_available_models, revoke_user_cortex_access,
    grant_user_cortex_access, get_users_with_role, send_budget_alert,
    get_all_users_spend, run_enforcement_cycle, check_alert_already_sent,
    record_alert_sent, FQN, LATENCY_BANNER, CORTEX_CODE_PRICING,
    _send_slack_alert, get_scheduled_task_status, create_enforcement_task,
    suspend_enforcement_task, drop_enforcement_task,
)
from lib.time import get_period_bounds, format_period

st.header("Enforcement & Controls")

st.markdown(
    "This page lets you go beyond monitoring — **actively enforce** credit budgets, "
    "**restrict available models**, and **send email alerts** when budgets are breached."
)

cfg = get_config()
tz = cfg.get("BUDGET_TIMEZONE", "UTC")
period_type = cfg.get("DEFAULT_PERIOD_TYPE", "MONTHLY")
p_start, p_end = get_period_bounds(period_type, tz)
ps = p_start.strftime("%Y-%m-%d %H:%M:%S")
pe = p_end.strftime("%Y-%m-%d %H:%M:%S")

tab_enforce, tab_models, tab_alerts, tab_log = st.tabs([
    "Budget Enforcement", "Model Allowlist", "Email Alerts", "Enforcement Log"
])

with tab_enforce:
    st.subheader("Budget Enforcement")

    with st.expander("How does enforcement work?", expanded=False):
        st.markdown("""
**Enforcement automatically manages Cortex AI access based on budget status.**

The mechanism:
1. Snowflake's `SNOWFLAKE.CORTEX_USER` database role controls access to all Cortex AI features
2. A managed role (default: `CORTEX_USER_ROLE`) is granted `SNOWFLAKE.CORTEX_USER`
3. When a user **exceeds** their budget, enforcement **revokes** `CORTEX_USER_ROLE` from that user
4. When a user is **back under budget** (e.g., after a top-up or budget increase), enforcement **automatically restores** their access
5. Granting a top-up or increasing a budget on the **User Budgets** page also triggers an immediate access check

**Prerequisites:**
- The enforcement role must exist and have `SNOWFLAKE.CORTEX_USER` granted to it
- For full enforcement, `SNOWFLAKE.CORTEX_USER` should be **revoked from the PUBLIC role** — otherwise all users can still access Cortex AI regardless
- Users must have the enforcement role granted to them (not just PUBLIC access)

**Important:** Data from ACCOUNT_USAGE views can lag up to ~1 hour, so enforcement
checks act on slightly delayed data. For **instant** cost control, use the **Model Allowlist** tab.
""")

    public_check_df, pub_err = run_query(
        "SHOW GRANTS OF DATABASE ROLE SNOWFLAKE.CORTEX_USER"
    )
    if not pub_err and not public_check_df.empty:
        public_has_role = not public_check_df[
            (public_check_df.get("grantee_name", pd.Series()) == "PUBLIC")
        ].empty if "grantee_name" in public_check_df.columns else False
        if public_has_role:
            st.warning(
                "**The PUBLIC role still has `SNOWFLAKE.CORTEX_USER` granted.** "
                "This means ALL users can access Cortex AI regardless of enforcement. "
                "To enable real enforcement, run:\n\n"
                "```sql\n"
                "REVOKE DATABASE ROLE SNOWFLAKE.CORTEX_USER FROM ROLE PUBLIC;\n"
                "```\n\n"
                "Then grant `CORTEX_USER_ROLE` to each user who should have access."
            )

    enforcement = get_enforcement_status()
    enabled = enforcement["enabled"]
    role = enforcement["role"]

    col_toggle, col_role = st.columns(2)
    with col_toggle:
        new_enabled = st.toggle(
            "Enforcement Enabled", value=enabled, key="enf_toggle",
            help="When ON, the 'Run Enforcement Cycle' button will revoke access from over-budget users."
        )
    with col_role:
        new_role = st.text_input(
            "Enforcement Role", value=role, key="enf_role",
            help="The Snowflake role that gets granted/revoked to control Cortex AI access."
        )

    if new_enabled != enabled or new_role != role:
        if st.button("Save Enforcement Settings", type="primary", key="save_enf"):
            for k, v in [("ENFORCEMENT_ENABLED", str(new_enabled).lower()),
                         ("ENFORCEMENT_ROLE", new_role)]:
                safe_v = v.replace("'", "''")
                run_ddl(
                    f"MERGE INTO {FQN}.BUDGET_CONFIG tgt "
                    f"USING (SELECT '{k}' AS CK, '{safe_v}' AS CV) src "
                    f"ON tgt.CONFIG_KEY = src.CK "
                    f"WHEN MATCHED THEN UPDATE SET CONFIG_VALUE = src.CV, UPDATED_AT = CURRENT_TIMESTAMP() "
                    f"WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE, UPDATED_AT) "
                    f"VALUES (src.CK, src.CV, CURRENT_TIMESTAMP())"
                )
            log_audit("UPDATE", "ENFORCEMENT", notes=f"enabled={new_enabled}, role={new_role}")
            clear_caches()
            st.success("Enforcement settings saved.")
            st.rerun()

    st.divider()

    st.subheader("Current Role Grants")
    st.caption(f"Users who currently have the `{role}` role (and therefore Cortex AI access).")
    users_with_role = get_users_with_role(role)
    if users_with_role:
        st.write(f"**{len(users_with_role)}** users have the `{role}` role:")
        role_df = pd.DataFrame({"USER": users_with_role})
        st.dataframe(role_df, hide_index=True, use_container_width=True, height=200)
    else:
        st.warning(f"No users currently have the `{role}` role.")

    st.divider()

    col_run, col_manual = st.columns(2)

    with col_run:
        st.subheader("Run Enforcement Now")
        st.caption(
            "Checks all users against their budgets. "
            "Over-budget users have their access revoked; warning-level users get email alerts (if configured)."
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
        st.subheader("Manual Access Control")
        st.caption("Grant or revoke Cortex AI access for a specific user.")
        all_spend = get_all_users_spend(ps, pe)
        user_names = sorted(all_spend["USER_NAME"].dropna().unique().tolist()) if not all_spend.empty else []

        if user_names:
            manual_user = st.selectbox("User", user_names, key="manual_user")
            manual_reason = st.text_input("Reason", value="Manual action", key="manual_reason")
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Grant Access", key="manual_grant"):
                    err = grant_user_cortex_access(manual_user, manual_reason)
                    if err:
                        st.error(f"Failed: {err}")
                    else:
                        log_audit("GRANT", "ENFORCEMENT", notes=f"user={manual_user}")
                        st.success(f"Granted `{role}` to {manual_user}")
                        clear_caches()
            with c2:
                if st.button("Revoke Access", key="manual_revoke", type="secondary"):
                    err = revoke_user_cortex_access(manual_user, manual_reason)
                    if err:
                        st.error(f"Failed: {err}")
                    else:
                        log_audit("REVOKE", "ENFORCEMENT", notes=f"user={manual_user}")
                        st.success(f"Revoked `{role}` from {manual_user}")
                        clear_caches()
        else:
            st.info("No users found.")

    st.divider()
    st.subheader("Period Reset")
    st.caption("Re-grants the enforcement role to ALL users who have active budgets — use this at the start of a new period.")
    if st.button("Reset All Users (Re-grant Access)", key="period_reset"):
        if all_spend.empty:
            st.info("No users to reset.")
        else:
            budgeted = all_spend[all_spend["STATUS"] != "NO BUDGET"]
            count = 0
            for _, row in budgeted.iterrows():
                uname = row.get("USER_NAME", "")
                if uname:
                    err = grant_user_cortex_access(uname, "Period reset")
                    if not err:
                        count += 1
            log_audit("RESET", "ENFORCEMENT", notes=f"Re-granted {count} users")
            clear_caches()
            st.success(f"Re-granted access to {count} users.")

    st.divider()
    st.subheader("Scheduled Enforcement")
    st.caption(
        "Run enforcement automatically on a schedule using a Snowflake Task. "
        "The task calls a stored procedure that logs each scheduled run."
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

with tab_log:
    st.subheader("Enforcement Log")
    st.caption("Every grant and revoke action performed by the enforcement system or manually.")
    log_limit = st.number_input("Rows", value=50, min_value=10, max_value=500, key="log_limit")
    log_df = get_enforcement_log(log_limit)
    if not log_df.empty:
        st.dataframe(log_df, hide_index=True, use_container_width=True)
    else:
        st.info("No enforcement actions logged yet. Actions appear here when enforcement runs or you manually grant/revoke access.")
