import streamlit as st

try:
    from lib.budget_api import (
        AI_DOMAINS, TAG_FQN,
        check_privileges,
        create_cost_center_tag, list_cost_center_tags, delete_cost_center_tag_value,
        tag_user_cost_center, untag_user_cost_center,
        get_user_current_tag, get_tag_assignment_log,
        create_native_budget, alter_native_budget_quota, drop_native_budget,
        list_native_budgets, show_budgets_in_schema,
        add_shared_resource, remove_shared_resource,
        set_budget_user_tags, get_budget_scope, get_budget_usage,
        get_user_tags_for_budget, unset_all_budget_user_tags,
        get_shared_resource_candidates,
    )
    from lib.config import get_config
    from lib.usage_queries import get_users
    from lib.connection import FQN
    _IMPORT_OK = True
    _IMPORT_ERR = None
except Exception as _e:
    _IMPORT_OK = False
    _IMPORT_ERR = str(_e)

st.title("AI Budgets (Native)")

if not _IMPORT_OK:
    st.error(f"Failed to load native budget module: {_IMPORT_ERR}")
    st.info("The rest of the app is unaffected. Check that `app/lib/budget_api.py` exists.")
    st.stop()

with st.expander("ℹ️ How CoCo Budgets works — two systems side by side", expanded=False):
    st.markdown(
        "**Two complementary budget systems run side by side in CoCo Budgets:**\n\n"
        "| System | What it does | Blocking? |\n"
        "|---|---|---|\n"
        "| **Advisory** (User Budgets + Enforcement) | Per-user credit limits, warning alerts, auto-block via `CORTEX_CODE_*_DAILY_EST_CREDIT_LIMIT_PER_USER` | ✅ Yes |\n"
        "| **Native** (this page) | Snowflake Budget objects scoped to teams via cost center tags. Tracks spend across AI Functions, Cortex Code, Cortex Agents, Snowflake Intelligence | ❌ No (tracking only) |\n\n"
        "Native budgets do **not** block users — they provide Snowflake-managed attribution and visibility by team. "
        "Use Enforcement + User Budgets to enforce hard limits."
    )

tab1, tab2, tab3, tab4 = st.tabs([
    "1️⃣ Privilege Check & Tag Setup",
    "2️⃣ Cost Center Assignment",
    "3️⃣ Native Budgets",
    "4️⃣ Verify & Status",
])

with tab1:
    st.subheader("Privilege Check")
    with st.expander("ℹ️ What this does & why it matters", expanded=False):
        st.markdown(
            "Verifies that the active role has all grants needed to create and manage native budgets.\n\n"
            "- **MANAGE USER** — required to set user tags and daily credit limits\n"
            "- **SHOW TAGS** — required to read and apply cost center tags\n"
            "- **SHOW BUDGETS** — required to create and view native Budget objects\n"
            "- **SHOW BUDGET CANDIDATES** — required to add AI domains to a budget\n\n"
            "Run `deploy/rbac.sql` as ACCOUNTADMIN if any grants fail. "
            "Note: if the account-level Budget feature is not enabled, Tabs 3 & 4 will be unavailable "
            "regardless of grants — contact an ORGADMIN or Snowflake Support to enable it."
        )

    if st.button("Run Privilege Check", type="primary"):
        with st.spinner("Checking privileges..."):
            checks = check_privileges()
            st.session_state["privilege_checks"] = checks

        feature_available = checks.get("budgets_feature_available", True)

        if not feature_available:
            st.warning(
                "**Native Snowflake Budget objects are not enabled on this account.**\n\n"
                "`SHOW BUDGETS` and `SYSTEM$SHOW_BUDGET_SHARED_RESOURCE_CANDIDATES()` are "
                "unavailable. This is an **account-level feature gate**, not a grants issue — "
                "re-running `deploy/rbac.sql` will not fix it.\n\n"
                "**To enable:** An ORGADMIN on this account (or Snowflake Support) must enable "
                "the Native Budgets feature.\n\n"
                "✅ **Cost center TAG setup and user tagging (Tabs 1 & 2) work normally.**  \n"
                "❌ **Native Budget creation and AI domain tracking (Tabs 3 & 4) require the "
                "Budget feature to be enabled first.**",
                icon="⚠️",
            )

        budget_hint = (
            "Account-level feature not enabled — contact ORGADMIN or Snowflake Support"
            if not feature_available
            else "Grant: GRANT CREATE BUDGET ON SCHEMA after bootstrap"
        )
        candidates_hint = (
            "Account-level feature not enabled — contact ORGADMIN or Snowflake Support"
            if not feature_available
            else "Requires ACCOUNTADMIN or SYSADMIN lineage"
        )

        rows = [
            ("Snowflake connection", checks.get("connection_ok"), "Required for all operations"),
            ("MANAGE USER (ALTER USER params + tags)", checks.get("manage_user"), "Grant: GRANT MANAGE USER TO ROLE COCO_BUDGETS_OWNER"),
            ("SHOW TAGS in schema", checks.get("show_tags"), "Grant: GRANT CREATE TAG ON SCHEMA after bootstrap"),
            ("SHOW BUDGETS in schema", checks.get("show_budgets"), budget_hint),
            ("SYSTEM$SHOW_BUDGET_SHARED_RESOURCE_CANDIDATES", checks.get("show_candidates"), candidates_hint),
        ]

        grants_ok = all(checks.get(k) for k in ("connection_ok", "manage_user", "show_tags"))

        if feature_available and all(v for _, v, _ in rows):
            st.success("All privilege checks passed. Native budgets are ready to use.")
        elif not feature_available and grants_ok:
            st.success("Tag operation grants ✅ — Native Budget feature is unavailable at the account level (see above).")
        elif not feature_available:
            st.warning("Some grants are missing AND the Native Budget feature is not enabled. Fix grants first, then work with ORGADMIN to enable Budgets.")
        else:
            st.warning("Some checks failed. Review the grants below and re-run `deploy/rbac.sql`.")

        for label, ok, hint in rows:
            col1, col2 = st.columns([3, 2])
            with col1:
                if ok:
                    st.success(f"✅ {label}")
                else:
                    st.error(f"❌ {label}")
            with col2:
                if not ok:
                    st.caption(hint)

    st.divider()
    st.subheader("Cost Center Tag — Initial Setup")
    with st.expander("ℹ️ What this does & why it matters", expanded=False):
        st.markdown(
            f"Creates the Snowflake TAG object `{TAG_FQN}` and registers the allowed cost center values "
            "(e.g. ENGINEERING, FINANCE, SALES).\n\n"
            "**Why it matters:** Native Snowflake Budgets use tags to scope which users' consumption counts "
            "toward a budget. A user tagged `COST_CENTER = 'ENGINEERING'` will have their AI spend "
            "counted against the Engineering budget — giving you team-level visibility without blocking anyone.\n\n"
            "This step only needs to be done once. Adding values is safe to repeat."
        )

    with st.form("create_tag_form"):
        st.markdown(f"Creates the Snowflake TAG object at `{TAG_FQN}` (idempotent, safe to re-run).")
        submitted = st.form_submit_button("Create TAG object", type="primary")
        if submitted:
            with st.spinner("Creating tag..."):
                err = create_cost_center_tag()
            if err:
                st.error(f"Error: {err}")
            else:
                st.success(f"TAG `{TAG_FQN}` created (or already exists).")
                st.info("Add cost center values in the 'Manage Cost Center Values' section below.")

    st.divider()
    st.subheader("Manage Cost Center Values")

    col_add, col_list = st.columns([1, 1])

    with col_add:
        with st.form("add_tag_value_form"):
            add_val = st.text_input("New cost center value", placeholder="e.g. FINANCE")
            add_desc = st.text_input("Description", placeholder="Finance team")
            if st.form_submit_button("Add value"):
                if not add_val.strip():
                    st.error("Value cannot be empty.")
                else:
                    err = create_cost_center_tag(tag_value=add_val.strip(), description=add_desc.strip())
                    if err:
                        st.error(f"Error: {err}")
                    else:
                        st.success(f"Added `{add_val.strip()}`.")
                        st.rerun()

    with col_list:
        tags_df = list_cost_center_tags()
        if tags_df.empty:
            st.info("No cost center values registered yet.")
        else:
            st.dataframe(tags_df, use_container_width=True, hide_index=True)
            del_val = st.selectbox("Remove value", [""] + tags_df["TAG_VALUE"].tolist())
            if del_val and st.button("Remove", type="secondary"):
                err = delete_cost_center_tag_value(del_val)
                if err:
                    st.error(f"Error: {err}")
                else:
                    st.success(f"Removed `{del_val}` from registry.")
                    st.rerun()


with tab2:
    st.subheader("Assign Cost Centers to Users")
    with st.expander("ℹ️ What this does & why it matters", expanded=False):
        st.markdown(
            "Runs `ALTER USER ... SET TAG COST_CENTER = 'VALUE'` on the selected user.\n\n"
            "**Why it matters:** This is what connects a user to a native budget. Once tagged, any AI spend "
            "(Cortex Code, AI Functions, Cortex Agents, Snowflake Intelligence) that user generates will be "
            "attributed to their cost center and counted against the matching budget.\n\n"
            "⚠️ **This directly modifies a live Snowflake user object.** "
            "The change takes effect immediately and is permanent until explicitly revoked. "
            "All operations are recorded in `USER_TAG_ASSIGNMENTS` for audit purposes."
        )

    users_df = get_users()
    tags_df = list_cost_center_tags()

    if users_df.empty:
        st.info("No users found.")
    else:
        col_assign, col_current = st.columns([1, 1])

        with col_assign:
            st.markdown("**Assign a cost center**")
            user_options = users_df["USER_NAME"].tolist()
            selected_user = st.selectbox("User", user_options, key="assign_user")
            user_row = users_df[users_df["USER_NAME"] == selected_user].iloc[0]
            user_id = int(user_row["USER_ID"])

            if tags_df.empty:
                st.warning("No cost center values defined. Create them in tab 1 first.")
            else:
                selected_tag = st.selectbox(
                    "Cost center value",
                    tags_df["TAG_VALUE"].tolist(),
                    key="assign_tag",
                )

                current_tag = get_user_current_tag(selected_user)
                if current_tag:
                    st.info(f"Current tag: **{current_tag}**")

                st.markdown("---")
                confirm_assign = st.checkbox(
                    f"I confirm: set `{TAG_FQN}` = `{selected_tag}` on user `{selected_user}`",
                    key="confirm_assign",
                )
                if st.button("Apply Tag", type="primary", disabled=not confirm_assign):
                    with st.spinner("Applying tag..."):
                        err = tag_user_cost_center(selected_user, user_id, selected_tag)
                    if err:
                        st.error(f"Error: {err}")
                    else:
                        st.success(f"User `{selected_user}` tagged as `{selected_tag}`.")
                        st.rerun()

                st.markdown("---")
                confirm_untag = st.checkbox(
                    f"I confirm: REMOVE cost center tag from `{selected_user}`",
                    key="confirm_untag",
                )
                if st.button("Remove Tag", type="secondary", disabled=not confirm_untag):
                    with st.spinner("Removing tag..."):
                        err = untag_user_cost_center(selected_user, user_id)
                    if err:
                        st.error(f"Error: {err}")
                    else:
                        st.success(f"Tag removed from `{selected_user}`.")
                        st.rerun()

        with col_current:
            st.markdown("**Recent tag operations (audit log)**")
            log_df = get_tag_assignment_log(limit=30)
            if log_df.empty:
                st.info("No tag operations recorded yet.")
            else:
                st.dataframe(log_df, use_container_width=True, hide_index=True)


with tab3:
    st.subheader("Manage Native Snowflake Budgets")
    with st.expander("ℹ️ What this does & why it matters", expanded=False):
        st.markdown(
            "Creates and configures Snowflake-native Budget objects that track AI spend by team.\n\n"
            "**Setup order:**\n"
            "1. **Create a budget** — gives it a name and monthly credit quota\n"
            "2. **Add shared resources** — choose which AI domains count (AI Functions, Cortex Code, Cortex Agent, Snowflake Intelligence)\n"
            "3. **Set user tag scope** — link a cost center tag value so only tagged users' spend counts\n\n"
            "**Why it matters:** Unlike the Advisory system (which blocks users), native budgets provide "
            "Snowflake-managed spend visibility per team. Use them for chargebacks, showbacks, and "
            "team-level AI cost reporting — without enforcing hard limits.\n\n"
            "Requires the Native Budgets account-level feature to be enabled (check Tab 1)."
        )

    _pc = st.session_state.get("privilege_checks", {})
    if _pc and not _pc.get("budgets_feature_available", True):
        st.error(
            "**Native Budget objects are not available on this account.** "
            "Run the Privilege Check in Tab 1 for details. "
            "Actions on this tab will fail until an ORGADMIN enables the Budget feature.",
            icon="⛔",
        )

    cfg = get_config()
    default_quota = float(cfg.get("DEFAULT_NATIVE_BUDGET_QUOTA", 1000))

    _feature_ok = not (_pc and not _pc.get("budgets_feature_available", True))

    col_create, col_manage = st.columns([1, 1])

    with col_create:
        st.markdown("**Create a new budget**")
        with st.form("create_budget_form"):
            b_name = st.text_input("Budget name", placeholder="e.g. TEAM_AI_BUDGET", disabled=not _feature_ok)
            b_quota = st.number_input(
                "Credit quota",
                min_value=1.0,
                value=default_quota,
                step=100.0,
                help="Monthly credit limit for this budget scope.",
                disabled=not _feature_ok,
            )
            b_desc = st.text_input("Description", placeholder="e.g. Engineering team AI spend", disabled=not _feature_ok)
            if st.form_submit_button("Create Budget", type="primary", disabled=not _feature_ok):
                if not b_name.strip():
                    st.error("Budget name is required.")
                else:
                    with st.spinner("Creating budget..."):
                        err = create_native_budget(
                            budget_name=b_name.strip().upper(),
                            credit_quota=b_quota,
                            description=b_desc.strip(),
                        )
                    if err:
                        st.error(f"Error: {err}")
                    else:
                        st.success(f"Budget `{b_name.strip().upper()}` created with {b_quota:,.0f} credit quota.")
                        st.rerun()

    with col_manage:
        st.markdown("**Existing budgets**")
        budgets_df = list_native_budgets()
        if budgets_df.empty:
            st.info("No budgets registered yet.")
        else:
            st.dataframe(
                budgets_df[["BUDGET_NAME", "CREDIT_QUOTA", "DESCRIPTION", "CREATED_AT"]],
                use_container_width=True,
                hide_index=True,
            )

    st.divider()

    if not budgets_df.empty:
        budget_names = budgets_df["BUDGET_NAME"].tolist()
        selected_budget = st.selectbox("Select budget to configure", budget_names)

        tab_resources, tab_tags, tab_quota, tab_drop = st.tabs([
            "Shared Resources", "User Tag Scope", "Update Quota", "Drop Budget"
        ])

        with tab_resources:
            st.markdown(f"**Add / remove AI domains tracked by `{selected_budget}`**")
            st.caption(
                "Each shared resource corresponds to one AI service domain. "
                "Credits from that domain consumed by scoped users will count towards this budget."
            )
            if _feature_ok:
                candidates_df, c_err = get_shared_resource_candidates()
                if c_err:
                    st.warning(f"Could not list candidates: {c_err}")

            for domain in AI_DOMAINS:
                col_d, col_add_btn, col_rm_btn = st.columns([2, 1, 1])
                with col_d:
                    st.markdown(f"`{domain}`")
                with col_add_btn:
                    if st.button("Add", key=f"add_{domain}_{selected_budget}"):
                        with st.spinner(f"Adding {domain}..."):
                            err = add_shared_resource(selected_budget, domain)
                        if err:
                            st.error(f"Error: {err}")
                        else:
                            st.success(f"Added `{domain}` to `{selected_budget}`.")
                with col_rm_btn:
                    if st.button("Remove", key=f"rm_{domain}_{selected_budget}"):
                        with st.spinner(f"Removing {domain}..."):
                            err = remove_shared_resource(selected_budget, domain)
                        if err:
                            st.error(f"Error: {err}")
                        else:
                            st.success(f"Removed `{domain}` from `{selected_budget}`.")

        with tab_tags:
            st.markdown(f"**Scope `{selected_budget}` to tagged users**")
            st.caption(
                "Links a cost center tag value to this budget. "
                "Only users tagged with the selected value will be counted. "
                "Mode `UNION` adds to existing scope; `INTERSECTION` narrows scope to users matching both conditions."
            )
            tags_df2 = list_cost_center_tags()
            if tags_df2.empty:
                st.warning("No cost center values defined. Go to tab 1 to create them.")
            else:
                scope_val = st.selectbox("Cost center value", tags_df2["TAG_VALUE"].tolist())
                scope_mode = st.selectbox("Mode", ["UNION", "INTERSECTION"], index=0)

                col_set, col_clear = st.columns(2)
                with col_set:
                    if st.button("Set User Tag Scope", type="primary"):
                        with st.spinner("Setting tag scope..."):
                            err = set_budget_user_tags(selected_budget, scope_val, mode=scope_mode)
                        if err:
                            st.error(f"Error: {err}")
                        else:
                            st.success(f"Scope set: `{scope_val}` ({scope_mode}).")

                with col_clear:
                    if st.button("Clear All Tag Scopes", type="secondary"):
                        with st.spinner("Clearing scopes..."):
                            err = unset_all_budget_user_tags(selected_budget)
                        if err:
                            st.error(f"Error: {err}")
                        else:
                            st.success("All tag scopes cleared.")

                st.markdown("**Current tag scope**")
                scope_df, scope_err = get_user_tags_for_budget(selected_budget)
                if scope_err:
                    st.caption(f"Could not retrieve scope: {scope_err}")
                elif scope_df.empty:
                    st.caption("No tag scope set.")
                else:
                    st.dataframe(scope_df, use_container_width=True, hide_index=True)

        with tab_quota:
            st.markdown(f"**Update credit quota for `{selected_budget}`**")
            row = budgets_df[budgets_df["BUDGET_NAME"] == selected_budget].iloc[0]
            current_quota = float(row["CREDIT_QUOTA"])
            new_quota = st.number_input(
                "New credit quota",
                min_value=1.0,
                value=current_quota,
                step=100.0,
            )
            if st.button("Update Quota", type="primary"):
                with st.spinner("Updating..."):
                    err = alter_native_budget_quota(selected_budget, new_quota)
                if err:
                    st.error(f"Error: {err}")
                else:
                    st.success(f"Quota updated to {new_quota:,.0f} credits.")
                    st.rerun()

        with tab_drop:
            st.markdown(f"**Drop budget `{selected_budget}`**")
            st.error(
                "Dropping a budget is irreversible. The budget object will be removed from Snowflake "
                "and from the CoCo registry. Usage history is retained in Snowflake ACCOUNT_USAGE.",
                icon="⛔",
            )
            confirm_drop = st.checkbox(
                f"I confirm: permanently drop `{selected_budget}` and all its configuration.",
                key="confirm_drop",
            )
            if st.button("Drop Budget", type="secondary", disabled=not confirm_drop):
                with st.spinner("Dropping budget..."):
                    err = drop_native_budget(selected_budget)
                if err:
                    st.error(f"Error: {err}")
                else:
                    st.success(f"Budget `{selected_budget}` dropped.")
                    st.rerun()


with tab4:
    st.subheader("Verify & Status")
    with st.expander("ℹ️ What this does & why it matters", expanded=False):
        st.markdown(
            "Inspects the live state of native Budget objects directly from Snowflake — "
            "not from the local CoCo registry.\n\n"
            "- **SHOW BUDGETS** — lists all budget objects visible to the current role\n"
            "- **Resource candidates** — shows which AI domains are available to add to a budget\n"
            "- **Get scope** — shows which users are currently in a budget's scope\n"
            "- **Get usage** — shows real-time credit consumption by AI service type\n\n"
            "The Tag Audit Log and Native Budget Registry at the bottom always reflect local CoCo state "
            "and work even when the account-level Budget feature is unavailable."
        )

    _pc4 = st.session_state.get("privilege_checks", {})
    _feature_ok4 = not (_pc4 and not _pc4.get("budgets_feature_available", True))

    budgets_df2 = list_native_budgets()
    budget_names4 = budgets_df2["BUDGET_NAME"].tolist() if not budgets_df2.empty else []

    col_run, col_sel = st.columns([1, 2])
    with col_run:
        run_checks = st.button("Run All Checks", type="primary")
    with col_sel:
        verify_budget = st.selectbox(
            "Budget to inspect (for scope & usage checks)",
            ["— none —"] + budget_names4,
            key="verify_budget",
            disabled=not budget_names4,
        )
    selected_b4 = verify_budget if verify_budget != "— none —" else None

    if run_checks:
        checks4 = {}

        with st.spinner("Running checks..."):
            if _feature_ok4:
                live_df2, live_err = show_budgets_in_schema()
                checks4["budgets_in_schema"] = {"ok": live_err is None, "err": live_err, "df": live_df2}
            else:
                checks4["budgets_in_schema"] = {"ok": None, "err": "Native Budget feature not enabled on this account", "df": None}

            if _feature_ok4:
                cand_df4, cand_err4 = get_shared_resource_candidates()
                checks4["resource_candidates"] = {"ok": cand_err4 is None, "err": cand_err4, "df": cand_df4 if cand_err4 is None else None}
            else:
                checks4["resource_candidates"] = {"ok": None, "err": "Native Budget feature not enabled on this account", "df": None}

            if selected_b4:
                scope_df4, scope_err4 = get_budget_scope(selected_b4)
                checks4["budget_scope"] = {"ok": scope_err4 is None, "err": scope_err4, "df": scope_df4 if scope_err4 is None else None, "budget": selected_b4}
                usage_df4, usage_err4 = get_budget_usage(selected_b4)
                checks4["budget_usage"] = {"ok": usage_err4 is None, "err": usage_err4, "df": usage_df4 if usage_err4 is None else None, "budget": selected_b4}
            else:
                checks4["budget_scope"] = {"ok": None, "err": "No budget selected", "df": None}
                checks4["budget_usage"] = {"ok": None, "err": "No budget selected", "df": None}

            checks4["tag_log"] = {"ok": True, "count": len(get_tag_assignment_log(limit=1000))}


            reg_df4 = list_native_budgets()
            checks4["registry"] = {"ok": True, "count": len(reg_df4)}

        st.session_state["tab4_checks"] = checks4

    saved = st.session_state.get("tab4_checks")

    if saved:
        st.divider()
        st.subheader("Check Results")

        def _status_row(label: str, result: dict, detail: str = ""):
            ok = result.get("ok")
            err = result.get("err", "")
            col1, col2, col3 = st.columns([3, 1, 3])
            with col1:
                st.markdown(f"**{label}**")
                if detail:
                    st.caption(detail)
            with col2:
                if ok is True:
                    st.success("✅ Pass")
                elif ok is False:
                    st.error("❌ Fail")
                else:
                    st.warning("⚠️ N/A")
            with col3:
                if err:
                    st.caption(err)

        st.markdown("**Live Snowflake checks** *(require Native Budget feature)*")
        _status_row("SHOW BUDGETS in schema", saved["budgets_in_schema"], "Lists budget objects visible to current role")
        _status_row("Resource candidates available", saved["resource_candidates"], "SYSTEM$SHOW_BUDGET_SHARED_RESOURCE_CANDIDATES()")

        b_label = f"for `{saved['budget_scope'].get('budget', '')}`" if saved.get("budget_scope", {}).get("budget") else ""
        _status_row(f"Budget scope {b_label}", saved["budget_scope"], "GET_BUDGET_SCOPE() — which users are in scope")
        _status_row(f"Budget usage {b_label}", saved["budget_usage"], "GET_SERVICE_TYPE_USAGE_V2() — credits by AI service type")

        st.markdown("**Local CoCo state** *(always available)*")
        tag_ok = saved["tag_log"]["ok"]
        reg_ok = saved["registry"]["ok"]
        _status_row(f"Tag assignment log ({saved['tag_log']['count']} entries)", {"ok": tag_ok}, "USER_TAG_ASSIGNMENTS table")
        _status_row(f"Budget registry ({saved['registry']['count']} budgets)", {"ok": reg_ok}, "SNOWFLAKE_BUDGET_REGISTRY table")

        st.divider()
        for key, label in [
            ("budgets_in_schema", "Budgets in Schema"),
            ("resource_candidates", "Resource Candidates"),
            ("budget_scope", "Budget Scope"),
            ("budget_usage", "Budget Usage by Service Type"),
        ]:
            r = saved.get(key, {})
            if r.get("ok") and r.get("df") is not None and not r["df"].empty:
                with st.expander(f"View data — {label}", expanded=False):
                    st.dataframe(r["df"], use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Tag Assignment Audit Log")
    log_df2 = get_tag_assignment_log(limit=50)
    if log_df2.empty:
        st.info("No tag operations recorded.")
    else:
        st.dataframe(log_df2, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Native Budget Registry")
    budgets_reg = list_native_budgets()
    if budgets_reg.empty:
        st.info("No budgets in registry.")
    else:
        st.dataframe(budgets_reg, use_container_width=True, hide_index=True)
