---
name: deploy-coco-budgets
description: "Deploy, configure, and troubleshoot CoCo Budgets — a Streamlit-in-Snowflake admin console for Cortex Code credit management. Use when: deploying CoCo Budgets, setting up RBAC, bootstrapping backend tables, configuring enforcement, running locally, or troubleshooting deploy issues. Triggers: deploy coco budgets, deploy app, set up coco budgets, install coco budgets, coco budgets setup, deploy streamlit, bootstrap backend, rbac setup, configure enforcement, run locally, deploy to snowflake, coco budgets troubleshoot."
---
# Deploy CoCo Budgets
CoCo Budgets is a Streamlit-in-Snowflake (SIS) admin console for monitoring, budgeting, and enforcing Cortex Code (CLI + Snowsight) credit spend. This skill guides deployment end-to-end.
## Prerequisites
- **Snowflake CLI** (`snow`) v3.14.0+ installed
- **ACCOUNTADMIN** role (or a role with the privileges listed in `deploy/rbac.sql`)
- A warehouse (default: `COMPUTE_WH`)
- A Snowflake connection configured in `~/.snowflake/connections.toml`
## Workflow
```
Start
  |
  v
Step 1: Verify prerequisites
  |
  v
Step 2: Choose deploy path
  |
  +---> SIS Deploy (Snowflake) ---> Step 3A
  |
  +---> Local Dev               ---> Step 3B
  |
  v
Step 4: RBAC setup (optional)
  |
  v
Step 5: Verify deployment
  |
  v
Step 6: Post-deploy configuration
```
### Step 1: Verify Prerequisites
**Goal:** Confirm environment is ready.
**Actions:**
1. **Check** Snowflake CLI is installed:
   ```bash
   snow --version
   ```
2. **Check** a Snowflake connection exists:
   ```bash
   snow connection list
   ```
3. **Check** the user has ACCOUNTADMIN or equivalent:
   ```sql
   SELECT CURRENT_ROLE();
   SHOW GRANTS TO USER CURRENT_USER();
   ```
**If CLI is missing:** Install via `pip install snowflake-cli` or `brew install snowflake-cli`.
**If no connection:** Guide user to create one:
```bash
snow connection add
```
### Step 2: Choose Deploy Path
**Ask** user which deployment mode they want:
1. **Streamlit in Snowflake (SIS)** — production deployment, accessible via Snowsight UI
2. **Local development** — run on localhost for development/testing
**⚠️ STOP**: Wait for user selection before proceeding.
### Step 3A: Deploy to Streamlit in Snowflake
**Goal:** Deploy the app to SIS.
**Actions:**
1. **Create the stage** (first time only):
   ```bash
   snow sql -f deploy/sis_prereqs.sql --connection <CONNECTION_NAME>
   ```
   This creates `COCO_BUDGETS_DB.BUDGETS.COCO_BUDGETS_STAGE`.
2. **Deploy the app**:
   ```bash
   cd <REPO_ROOT>/app
   snow streamlit deploy --connection <CONNECTION_NAME> --replace
   ```
3. The app self-bootstraps on first load — it creates all backend tables automatically:
   - `COCO_BUDGETS_DB.BUDGETS.USER_BUDGETS`
   - `COCO_BUDGETS_DB.BUDGETS.ACCOUNT_BUDGET`
   - `COCO_BUDGETS_DB.BUDGETS.BUDGET_TOPUPS`
   - `COCO_BUDGETS_DB.BUDGETS.BUDGET_CONFIG`
   - `COCO_BUDGETS_DB.BUDGETS.BUDGET_AUDIT_LOG`
   - `COCO_BUDGETS_DB.BUDGETS.ENFORCEMENT_LOG`
   - `COCO_BUDGETS_DB.BUDGETS.ALERT_STATE`
   - `COCO_BUDGETS_DB.BUDGETS.COST_CENTER_TAGS`
   - `COCO_BUDGETS_DB.BUDGETS.USER_TAG_ASSIGNMENTS`
   - `COCO_BUDGETS_DB.BUDGETS.SNOWFLAKE_BUDGET_REGISTRY`
4. **Alternative** — pre-create tables before deploy:
   ```bash
   snow sql -f deploy/backend.sql --connection <CONNECTION_NAME>
   ```
**Output:** App available at:
`https://app.snowflake.com/<ORG>/<ACCOUNT>/#/streamlit-apps/COCO_BUDGETS_DB.BUDGETS.COCO_BUDGETS`
**Continue** to Step 4 (RBAC) or Step 5 (Verify).
### Step 3B: Run Locally
**Goal:** Run the app on localhost for development.
**Actions:**
1. **Create conda environment**:
   ```bash
   conda env create -f local_environment.yml
   conda activate coco_budgets
   ```
2. **Set connection**:
   ```bash
   export SNOWFLAKE_CONNECTION_NAME=<CONNECTION_NAME>
   ```
3. **Run the app**:
   ```bash
   cd <REPO_ROOT>/app
   streamlit run streamlit_app.py
   ```
   App opens at `http://localhost:8501`.
4. The app auto-detects local mode and connects via `snowflake.connector` instead of Snowpark session.
**Continue** to Step 5 (Verify).
### Step 4: RBAC Setup (Optional)
**Goal:** Create least-privilege roles for multi-user environments.
**When to use:** Production deployments where ACCOUNTADMIN shouldn't be the day-to-day operating role.
**Actions:**
1. **Run RBAC script** as ACCOUNTADMIN:
   ```bash
   snow sql -f deploy/rbac.sql --connection <CONNECTION_NAME>
   ```
2. This creates three roles in a hierarchy:
   | Role | Purpose | Key Privileges |
   |------|---------|----------------|
   | `COCO_BUDGETS_OWNER` | Full app maintainer | IMPORTED PRIVILEGES, MANAGE USER, APPLY TAG, CREATE DATABASE |
   | `COCO_BUDGETS_APP_USER` | Budget manager | DML on all budget tables |
   | `COCO_BUDGETS_READER` | Read-only dashboards | SELECT on all tables |
   Hierarchy: `READER` → `APP_USER` → `OWNER` → `SYSADMIN`
3. **After first app bootstrap**, uncomment and run the post-bootstrap grants section in `deploy/rbac.sql` (section 8) to grant database/schema access to sub-roles.
4. **Assign users**:
   ```sql
   GRANT ROLE COCO_BUDGETS_OWNER    TO USER <admin_user>;
   GRANT ROLE COCO_BUDGETS_APP_USER TO USER <manager_user>;
   GRANT ROLE COCO_BUDGETS_READER   TO USER <viewer_user>;
   ```
**Note:** `ALTER ACCOUNT SET CORTEX_MODELS_ALLOWLIST` (Model Allowlist) always requires ACCOUNTADMIN — this is a Snowflake platform constraint.
### Step 5: Verify Deployment
**Goal:** Confirm the app is working.
**Actions:**
1. **Open the app** in Snowsight (SIS) or browser (local).
2. **Check** the sidebar loads with connection info and role selector.
3. **Navigate** to Dashboard — verify it shows Cortex Code usage data.
4. **Verify tables exist**:
   ```sql
   USE DATABASE COCO_BUDGETS_DB;
   SHOW TABLES IN SCHEMA BUDGETS;
   ```
   Should show 10 tables.
5. **Verify config seeded**:
   ```sql
   SELECT * FROM COCO_BUDGETS_DB.BUDGETS.BUDGET_CONFIG;
   ```
   Should have default keys: `BUDGET_TIMEZONE`, `DEFAULT_PERIOD_TYPE`, `CREDIT_RATE_USD`, etc.
**If no usage data appears:** ACCOUNT_USAGE views have ~1 hour latency. If the account has no Cortex Code usage yet, the dashboard will be empty.
### Step 6: Post-Deploy Configuration
**Goal:** Configure the app for the customer's environment.
**Key settings** (all configurable in the Settings page):
| Setting | Default | Description |
|---------|---------|-------------|
| `BUDGET_TIMEZONE` | `UTC` | Timezone for period boundaries |
| `DEFAULT_PERIOD_TYPE` | `MONTHLY` | Budget period (MONTHLY/WEEKLY/QUARTERLY) |
| `CREDIT_RATE_USD` | `2.00` | USD per credit |
| `ENFORCEMENT_ENABLED` | `false` | Enable auto-enforcement |
| `NATIVE_BUDGETS_ENABLED` | `false` | Enable AI Budgets (native Budget objects) tab |
**Enforcement setup flow:**
1. Create user budgets on the **User Budgets** page
2. Set an account budget on the **Account Budget** page
3. Configure alerts (email/Slack) on the **Enforcement** page
4. Enable enforcement and optionally create a scheduled Task
5. Set `ENFORCEMENT_ENABLED` to `true` in Settings
## Key Files
| File | Purpose |
|------|---------|
| `app/snowflake.yml` | SIS project definition — lists all artifact files |
| `app/environment.yml` | SIS conda dependencies |
| `app/streamlit_app.py` | Entry point, bootstrap DDL, sidebar |
| `app/pages/0_Setup.py` | Setup wizard — diagnostics, fix buttons, copy-paste SQL |
| `deploy/backend.sql` | Backend DDL (idempotent) |
| `deploy/rbac.sql` | Least-privilege roles |
| `deploy/sis_prereqs.sql` | Stage creation |
| `deploy/ai_budgets_setup.sql` | Optional native budget quick-start |
| `local_environment.yml` | Local dev conda dependencies |
## Stopping Points
- ✋ Step 2: Deploy path selection
- ✋ Step 4: Before running RBAC (confirm roles needed)
- ✋ Step 6: Before enabling enforcement (confirm user budgets are set)
## Troubleshooting
### Deploy fails: "stage not found"
Run `deploy/sis_prereqs.sql` first to create `COCO_BUDGETS_STAGE`.
### Deploy fails: "insufficient privileges"
Ensure the connection uses ACCOUNTADMIN or a role with CREATE STREAMLIT and USAGE on the warehouse. For RBAC setups, `COCO_BUDGETS_OWNER` needs CREATE DATABASE.
### App loads but dashboard is empty
ACCOUNT_USAGE views have ~1 hour latency. Verify Cortex Code has been used:
```sql
SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY
WHERE START_TIMESTAMP > DATEADD('day', -7, CURRENT_TIMESTAMP());
```
### Tables not created on first load
The bootstrap DDL runs in `streamlit_app.py` but requires CREATE DATABASE privilege. If the first user to open the app lacks this privilege, bootstrap fails silently.

**Self-healing:** Navigate to the **Setup** page (appears first in nav when bootstrap is incomplete). The wizard:
1. Runs read-only diagnostic checks on every backend object
2. Shows a pass/fail checklist for database, schema, 10 tables, and config seed
3. Provides "Run This Step" buttons for users with sufficient privileges
4. Offers a "Copy Full Setup SQL" block that any user can hand to their admin

**Alternative:** Pre-create all objects before deploying:
```bash
snow sql -f deploy/backend.sql --connection <CONNECTION_NAME>
```
### Enforcement blocks aren't working
1. Verify `ENFORCEMENT_ENABLED` is `true` in BUDGET_CONFIG
2. Verify user budgets exist in USER_BUDGETS
3. Verify the operating role has `MANAGE USER` privilege
4. Check ENFORCEMENT_LOG for recent actions
### Model Allowlist changes fail
This requires ACCOUNTADMIN — it cannot be delegated via RBAC.
### New lib module missing after redeploy
Ensure `app/snowflake.yml` `artifacts` list includes all files in `lib/`. Current required files:
- `lib/__init__.py`, `lib/connection.py`, `lib/config.py`, `lib/usage_queries.py`
- `lib/credit_limits.py`, `lib/alerts.py`, `lib/enforcement.py`, `lib/budget_service.py`
- `lib/db.py`, `lib/budget_api.py`, `lib/time.py`
## Output
A fully deployed CoCo Budgets app with:
- Backend tables auto-created or pre-provisioned
- Optional RBAC roles for least-privilege access
- Configurable enforcement and alerting ready to activate
