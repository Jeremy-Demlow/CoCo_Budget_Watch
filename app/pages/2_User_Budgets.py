import streamlit as st
import pandas as pd

from lib.connection import FQN
from lib.config import get_config, clear_caches, LATENCY_BANNER
from lib.usage_queries import get_users, get_user_budgets
from lib.credit_limits import user_is_blocked
from lib.enforcement import (
    get_enforcement_status, restore_access_if_under_budget,
)
from lib.budget_service import (
    create_user_budget, update_user_budget,
    bulk_create_user_budgets, grant_user_topup,
)
from lib.time import get_period_bounds

st.header("User Budgets")

st.markdown(
    "Set per-user credit limits for Cortex Code usage. When a user reaches their "
    "**warning threshold**, they appear as 'WARNING' on the Dashboard. When they "
    "exceed the limit, they show as 'OVER' — and if **Enforcement** is enabled, "
    "their Cortex Code daily credit limits are automatically set to 0 (blocked)."
)

cfg = get_config()
default_credits = float(cfg.get("DEFAULT_USER_BASE_PERIOD_CREDITS", "100"))
default_period = cfg.get("DEFAULT_PERIOD_TYPE", "MONTHLY")
default_threshold = int(cfg.get("DEFAULT_WARNING_THRESHOLD_PCT", "80"))
tz = cfg.get("BUDGET_TIMEZONE", "UTC")

tab_add, tab_edit, tab_bulk, tab_topup = st.tabs(
    ["Add Budget", "Edit Budgets", "Bulk Onboard", "Grant Top-up"]
)

users_df = get_users()
budgets_df = get_user_budgets()

budgeted_ids = set()
if not budgets_df.empty and "USER_ID" in budgets_df.columns:
    budgeted_ids = set(budgets_df["USER_ID"].tolist())

with tab_add:
    st.subheader("Add User Budget")
    st.caption("Assign a credit budget to a single user who doesn't have one yet.")

    if users_df.empty:
        st.warning("No users found in ACCOUNT_USAGE.USERS.")
    else:
        available = users_df[~users_df["USER_ID"].isin(budgeted_ids)]
        if available.empty:
            st.success("All users already have budgets configured.")
        else:
            user_options = {}
            for _, r in available.iterrows():
                email_part = f" — {r['EMAIL']}" if pd.notna(r.get("EMAIL")) and r["EMAIL"] else ""
                label = f"{r['USER_NAME']}{email_part} ({r['LOGIN_NAME']})"
                user_options[label] = r["USER_ID"]

            selected = st.selectbox("User", list(user_options.keys()), key="add_user_select")
            credits = st.number_input(
                "Period Credit Budget", value=default_credits,
                min_value=0.0, step=10.0, key="add_credits",
                help="Maximum credits this user can consume per period before hitting the limit."
            )
            period = st.selectbox("Period Type", ["MONTHLY", "WEEKLY", "QUARTERLY"],
                                  index=["MONTHLY","WEEKLY","QUARTERLY"].index(default_period),
                                  key="add_period",
                                  help="How often the budget resets. Most teams use MONTHLY.")
            threshold = st.slider(
                "Warning Threshold %", 0, 100, default_threshold, key="add_thresh",
                help="At what % of the budget to show a warning (e.g., 80% means a warning appears when 80 of 100 credits are used)."
            )

            if st.button("Add Budget", type="primary", key="add_btn"):
                uid = user_options[selected]
                err = create_user_budget(uid, credits, period, threshold)
                if err:
                    st.error(f"Failed: {err}")
                else:
                    st.success(f"Budget added for {selected}")
                    st.rerun()

with tab_edit:
    st.subheader("Edit User Budgets")
    st.caption("Modify existing budgets — change the credit limit, threshold, or deactivate a budget.")

    if budgets_df.empty:
        st.info("No user budgets to edit. Add one first using the **Add Budget** tab.")
    else:
        edit_cols = ["USER_NAME", "EMAIL", "USER_ID", "BASE_PERIOD_CREDITS",
                     "WARNING_THRESHOLD_PCT", "IS_ACTIVE", "PERIOD_TYPE"]
        available_edit = [c for c in edit_cols if c in budgets_df.columns]
        edited = st.data_editor(
            budgets_df[available_edit],
            disabled=["USER_NAME", "EMAIL", "USER_ID"],
            use_container_width=True,
            key="budget_editor",
            num_rows="fixed",
        )

        if st.button("Save Changes", type="primary", key="save_edits"):
            changes = 0
            for idx, row in edited.iterrows():
                orig = budgets_df.loc[idx]
                if (row.get("BASE_PERIOD_CREDITS") != orig.get("BASE_PERIOD_CREDITS") or
                    row.get("WARNING_THRESHOLD_PCT") != orig.get("WARNING_THRESHOLD_PCT") or
                    row.get("IS_ACTIVE") != orig.get("IS_ACTIVE") or
                    row.get("PERIOD_TYPE") != orig.get("PERIOD_TYPE")):
                    uid = row["USER_ID"]
                    err = update_user_budget(
                        uid, row["BASE_PERIOD_CREDITS"],
                        row["WARNING_THRESHOLD_PCT"], row["IS_ACTIVE"],
                        row["PERIOD_TYPE"], old_credits=orig.get("BASE_PERIOD_CREDITS"),
                    )
                    if not err:
                        changes += 1
            if changes:
                clear_caches()
                st.success(f"Saved {changes} change(s).")

                enforcement = get_enforcement_status()
                if enforcement["enabled"]:
                    p_start, p_end = get_period_bounds(default_period, tz)
                    ps = p_start.strftime("%Y-%m-%d %H:%M:%S")
                    pe = p_end.strftime("%Y-%m-%d %H:%M:%S")
                    restored = []
                    for idx, row in edited.iterrows():
                        orig = budgets_df.loc[idx]
                        budget_increased = (
                            row.get("BASE_PERIOD_CREDITS", 0) > orig.get("BASE_PERIOD_CREDITS", 0)
                        )
                        reactivated = (
                            row.get("IS_ACTIVE") == True and orig.get("IS_ACTIVE") != True
                        )
                        if budget_increased or reactivated:
                            uname = row.get("USER_NAME", "")
                            if uname and user_is_blocked(uname):
                                result = restore_access_if_under_budget(
                                    uname, int(row["USER_ID"]), ps, pe
                                )
                                if result["action"] == "unblock" and not result.get("error"):
                                    restored.append(uname)
                    if restored:
                        st.success(f"Access restored for: {', '.join(restored)}")

                st.rerun()
            else:
                st.info("No changes detected.")

with tab_bulk:
    st.subheader("Bulk Onboard Users")
    st.caption("Quickly assign the same default budget to all users who don't have one yet.")

    unbudgeted = users_df[~users_df["USER_ID"].isin(budgeted_ids)]
    st.write(f"**{len(unbudgeted)}** users without budgets")

    if not unbudgeted.empty:
        st.dataframe(
            unbudgeted[["USER_NAME", "LOGIN_NAME", "EMAIL"]],
            use_container_width=True, hide_index=True, height=200,
        )
        bulk_credits = st.number_input("Credit Budget for all", value=default_credits,
                                       min_value=0.0, step=10.0, key="bulk_credits")
        bulk_period = st.selectbox("Period Type", ["MONTHLY", "WEEKLY", "QUARTERLY"],
                                   key="bulk_period")

        if st.button("Apply Default Budget to All", type="primary", key="bulk_btn"):
            uids = [int(u["USER_ID"]) for _, u in unbudgeted.iterrows()]
            count = bulk_create_user_budgets(uids, bulk_credits, bulk_period, default_threshold)
            st.success(f"Added budgets for {count} users.")
            st.rerun()
    else:
        st.success("All users already have budgets.")

with tab_topup:
    st.subheader("Grant One-Time Top-up")
    st.caption(
        "Need to give a user extra credits mid-period? A top-up increases their effective "
        "budget for the current period without changing their base budget."
    )

    if budgets_df.empty:
        st.info("Add user budgets first before granting top-ups.")
    else:
        topup_options = {}
        for _, r in budgets_df.iterrows():
            email_part = f" — {r['EMAIL']}" if pd.notna(r.get("EMAIL")) and r["EMAIL"] else ""
            label = f"{r['USER_NAME']}{email_part}"
            topup_options[label] = r["USER_ID"]

        tu_selected = st.selectbox("User", list(topup_options.keys()), key="topup_user")

        enforcement = get_enforcement_status()
        if enforcement["enabled"]:
            selected_user_name = tu_selected.split(" — ")[0]
            if user_is_blocked(selected_user_name):
                st.warning(
                    f"**{selected_user_name}** currently has Cortex Code access **blocked** (daily limits set to 0). "
                    f"If this top-up brings them under budget, access will be automatically restored."
                )

        tu_credits = st.number_input("Additional Credits", value=50.0,
                                     min_value=0.01, step=10.0, key="topup_credits")
        tu_notes = st.text_input("Notes (optional)", key="topup_notes")

        p_start, p_end = get_period_bounds(default_period, tz)
        st.caption(f"Top-up applies to current period: {p_start.strftime('%Y-%m-%d')} – {p_end.strftime('%Y-%m-%d')}")

        if st.button("Grant Top-up", type="primary", key="topup_btn"):
            uid = topup_options[tu_selected]
            err = grant_user_topup(
                uid, tu_credits,
                p_start.strftime('%Y-%m-%d %H:%M:%S'),
                p_end.strftime('%Y-%m-%d %H:%M:%S'),
                tu_notes or "",
            )
            if err:
                st.error(f"Failed: {err}")
            else:
                st.success(f"Granted {tu_credits} credits to {tu_selected}")

                enforcement = get_enforcement_status()
                if enforcement["enabled"]:
                    user_name_for_restore = tu_selected.split(" — ")[0]
                    ps = p_start.strftime("%Y-%m-%d %H:%M:%S")
                    pe = p_end.strftime("%Y-%m-%d %H:%M:%S")
                    result = restore_access_if_under_budget(
                        user_name_for_restore, int(uid), ps, pe
                    )
                    if result["action"] == "unblock" and not result.get("error"):
                        st.success(
                            f"Access automatically restored — {user_name_for_restore} is now under budget."
                        )
                    elif result["action"] == "unblock" and result.get("error"):
                        st.warning(f"Tried to restore access but failed: {result['error']}")

                st.rerun()
