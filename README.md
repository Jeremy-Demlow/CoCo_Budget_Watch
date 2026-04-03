<p align="center">
  <h1 align="center">CoCo Budgets</h1>
  <p align="center">
    <strong>Cortex Code Credit Budget Manager for Snowflake</strong>
  </p>
  <p align="center">
    Monitor, budget, and enforce credit spending for
    <a href="https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-code">Cortex Code</a>
    (CLI + Snowsight) across your entire Snowflake account.
  </p>
</p>

---

> **This is not an official Snowflake product.** See [LEGAL.md](LEGAL.md) for full disclaimer.

## Why CoCo Budgets?

Cortex Code (Snowflake's AI coding assistant) charges credits based on token usage across multiple AI models. Without visibility, costs can grow quickly as adoption scales. CoCo Budgets gives administrators:

- **Real-time dashboards** showing credit spend by user, model, and source (CLI vs Snowsight)
- **Per-user and account-level budgets** with configurable warning thresholds
- **Automated enforcement** that revokes Cortex AI access when users exceed budgets
- **Email and Slack alerts** for warning and over-budget notifications
- **Scheduled enforcement cycles** via Snowflake Tasks for hands-off management

## Screenshots

### Dashboard
KPI metrics, usage charts by user/model, trend analysis, and budget status tracking.

![Dashboard](docs/images/dashboard.png)

### User Budgets
Add, edit, and bulk-onboard per-user credit budgets with configurable thresholds.

![User Budgets](docs/images/user_budgets.png)

### Account Budget
Set account-wide credit caps with real-time progress tracking and spend breakdown.

![Account Budget](docs/images/account_budget.png)

### Enforcement & Controls
Enforce budgets by revoking Cortex AI access, manage model allowlists, configure email/Slack alerts, and schedule automated enforcement.

![Enforcement](docs/images/enforcement.png)

### Settings
Configure budget defaults, credit-to-USD rates, timezone, and view audit logs.

![Settings](docs/images/settings.png)

## Architecture

```
+----------------------------------------------------------------------+
|                         Snowflake Account                            |
|                                                                      |
|  +-------------------------------+  +------------------------------+ |
|  | SNOWFLAKE.ACCOUNT_USAGE       |  | COCO_BUDGETS_DB.BUDGETS      | |
|  |                               |  |                              | |
|  | CORTEX_CODE_CLI_USAGE_HISTORY |  | USER_BUDGETS       (table)   | |
|  | CORTEX_CODE_SNOWSIGHT_USAGE_  |  | ACCOUNT_BUDGET     (table)   | |
|  |   HISTORY                     |  | BUDGET_TOPUPS      (table)   | |
|  | USERS                         |  | BUDGET_AUDIT_LOG   (table)   | |
|  |                               |  | BUDGET_CONFIG      (table)   | |
|  | (~1 hour data latency)        |  | ENFORCEMENT_LOG    (table)   | |
|  +---------------+---------------+  | ALERT_STATE        (table)   | |
|                  |                  +---------------+--------------+ |
|                  v                                  v                |
|  +--------------------------------------------------------------+   |
|  |           Streamlit in Snowflake (or local)                   |   |
|  |                                                               |   |
|  |  +----------+ +-------------+ +------------+ +-------------+ |   |
|  |  | Dashboard| |User Budgets | | Account    | | Enforcement | |   |
|  |  | - KPIs   | | - Add/Edit  | | Budget     | | - Enforce   | |   |
|  |  | - Charts | | - Bulk Add  | | - Set Cap  | | - Allowlist | |   |
|  |  | - Trends | | - Top-ups   | | - Progress | | - Alerts    | |   |
|  |  | - Status | |             | | - Breakdown| | - Schedule  | |   |
|  |  +----------+ +-------------+ +------------+ +-------------+ |   |
|  +--------------------------------------------------------------+   |
+----------------------------------------------------------------------+
```

### Data Sources

| View | Purpose | Latency |
|------|---------|---------|
| `SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY` | CLI credit usage per request | ~1 hour |
| `SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY` | Snowsight credit usage per request | ~1 hour |
| `SNOWFLAKE.ACCOUNT_USAGE.USERS` | Maps USER_ID to login name | ~1 hour |

Both usage views retain data for the **last 365 days**. The app unions CLI and Snowsight data with a `SOURCE` column for unified reporting.

### Credit Calculation

Credits are computed from the `TOKENS_GRANULAR` variant column using Snowflake's published token-to-credit rates per model. This approach is more accurate than `TOKEN_CREDITS` alone (which can be `0` for Snowsight rows). The CTE chain: `_requests_cte()` -> `_pricing_cte()` -> `_flatten_cte()` handles all credit math.

## Features

| Feature | Description |
|---------|-------------|
| **Multi-source tracking** | Unified view of CLI and Snowsight usage with source filtering |
| **Per-user budgets** | Monthly/weekly credit limits with warning thresholds |
| **Account budget** | Global credit cap independent of user budgets |
| **Budget top-ups** | One-time credit grants with date-bounded effectiveness |
| **Bulk onboarding** | Add budgets for all active users in one click |
| **Automated enforcement** | Revoke `CORTEX_USER_ROLE` from over-budget users |
| **Model allowlist** | View and document which AI models are enabled |
| **Email alerts** | Notification integration for warning/over-budget events |
| **Slack alerts** | Webhook-based Slack notifications |
| **Scheduled enforcement** | Snowflake Task with CRON schedule for hands-off operation |
| **Audit logging** | Every budget change tracked with old/new values |
| **Configurable credit rate** | Adjust USD-per-credit for cross-region pricing |
| **Progressive loading** | Skeleton UI pattern for responsive feel during data loads |
| **Self-bootstrapping** | App creates its own backend tables on first run |
| **Dual-mode deployment** | Runs locally or in Streamlit in Snowflake |

## Important Limitations

| Limitation | Detail |
|------------|--------|
| **Advisory budgets** | Snowflake does not expose an API to block Cortex Code usage. Enforcement works by revoking a database role, which requires proper RBAC setup. |
| **~1 hour data lag** | `ACCOUNT_USAGE` views have up to 1 hour latency. Enforcement decisions are based on lagged data. |
| **365-day retention** | Source views only cover the last year. |
| **Estimated costs** | USD amounts are approximations. Always validate against your official Snowflake invoice. |
| **SIS Streamlit version** | The app requires Streamlit >= 1.39.0 (for `st.Page`/`st.navigation`). Ensure `environment.yml` specifies a compatible version. |

## Quick Start

### Prerequisites

- **ACCOUNTADMIN** role (or a custom role with `IMPORTED PRIVILEGES` on the `SNOWFLAKE` database)
- A virtual warehouse (default: `COMPUTE_WH`)
- [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index) v3.14.0+ (for SIS deployment)

### 1. Deploy the Backend

Run the backend DDL to create the database, schema, tables, and seed configuration:

```bash
snow sql -f deploy/backend.sql --connection <your_connection>
```

Or paste the contents of `deploy/backend.sql` into a Snowsight worksheet and execute.

### 2. Deploy to Streamlit in Snowflake

```bash
# Create the stage (first time only)
snow sql -f deploy/sis_prereqs.sql --connection <your_connection>

# Deploy the app
cd app
snow streamlit deploy --connection <your_connection> --replace
```

The app will be available in Snowsight under **Streamlit** > **COCO_BUDGETS**.

### 3. (Optional) RBAC Setup

For least-privilege deployments, run `deploy/rbac.sql` to create:

| Role | Purpose |
|------|---------|
| `COCO_BUDGETS_OWNER` | Owns DB objects; runs DDL; has IMPORTED PRIVILEGES on SNOWFLAKE |
| `COCO_BUDGETS_APP_USER` | DML on budget tables; can run app and manage budgets |
| `COCO_BUDGETS_READER` | Read-only dashboard access |

### Self-Bootstrap (Alternative)

Skip steps 1-2 entirely. Create any Streamlit app pointing at `app/streamlit_app.py`. On first load, the app detects missing backend tables and creates them automatically.

## Local Development

```bash
# Create conda environment
conda env create -f local_environment.yml
conda activate coco_budgets

# Set your Snowflake connection
export SNOWFLAKE_CONNECTION_NAME=<your_connection>

# Run the app
cd app
streamlit run streamlit_app.py
```

The app auto-detects whether it's running locally or in SIS and uses the appropriate connection method.

## Project Structure

```
CoCo_Budgets/
+-- app/
|   +-- streamlit_app.py          # Entry point + bootstrap DDL
|   +-- environment.yml           # SIS conda dependencies
|   +-- snowflake.yml             # Snowflake CLI project definition
|   +-- lib/
|   |   +-- db.py                 # All SQL queries + Snowflake connection logic
|   |   +-- time.py               # Period boundary calculations
|   +-- pages/
|       +-- 1_Dashboard.py        # KPIs, charts, trends, budget status
|       +-- 2_User_Budgets.py     # Add/edit/bulk user budgets + top-ups
|       +-- 3_Account_Budget.py   # Account-level budget + progress
|       +-- 4_Settings.py         # Configuration, audit log, data freshness
|       +-- 5_Enforcement.py      # Enforcement, allowlist, alerts, scheduling
+-- deploy/
|   +-- backend.sql               # Full backend DDL (tables + config seed)
|   +-- rbac.sql                  # Least-privilege role setup
|   +-- sis_prereqs.sql           # Stage creation for SIS deployment
+-- local_environment.yml         # Local dev conda environment
+-- LEGAL.md                      # Disclaimer and legal notice
+-- README.md                     # This file
```

## Enforcement Setup

To enable real enforcement (not just advisory budgets):

1. **Revoke PUBLIC access** to Cortex AI:
   ```sql
   REVOKE DATABASE ROLE SNOWFLAKE.CORTEX_USER FROM ROLE PUBLIC;
   ```

2. **Create an enforcement role** (default: `CORTEX_USER_ROLE`):
   ```sql
   CREATE ROLE IF NOT EXISTS CORTEX_USER_ROLE;
   GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE CORTEX_USER_ROLE;
   ```

3. **Grant the role** to users who should have Cortex Code access:
   ```sql
   GRANT ROLE CORTEX_USER_ROLE TO USER <username>;
   ```

4. **Enable enforcement** in the app's Enforcement & Controls page.

When a user exceeds their budget, the app revokes `CORTEX_USER_ROLE` from them. When their budget resets or is increased, the role can be re-granted.

## Email & Slack Alerts

### Email
Configure in **Enforcement & Controls > Email Alerts**:
1. Select an existing Snowflake notification integration
2. Add recipient email addresses
3. Choose alert triggers (warning threshold, over budget)

### Slack
Configure in **Enforcement & Controls > Email Alerts > Slack**:
1. Create an [Incoming Webhook](https://api.slack.com/messaging/webhooks) in your Slack workspace
2. Paste the webhook URL in the app
3. Enable Slack notifications

## Configuration Reference

All settings are stored in `COCO_BUDGETS_DB.BUDGETS.BUDGET_CONFIG`:

| Key | Default | Description |
|-----|---------|-------------|
| `BUDGET_TIMEZONE` | `UTC` | Timezone for period boundary calculations |
| `DEFAULT_PERIOD_TYPE` | `MONTHLY` | Default budget period (MONTHLY/WEEKLY) |
| `DEFAULT_WARNING_THRESHOLD_PCT` | `80` | Default warning threshold percentage |
| `DEFAULT_USER_BASE_PERIOD_CREDITS` | `100` | Default credit budget for new users |
| `CREDIT_RATE_USD` | `2.00` | USD per credit ($2.00 cross-region, $2.20 standard) |
| `ENFORCEMENT_ENABLED` | `false` | Enable automatic role revocation |
| `ENFORCEMENT_ROLE` | `CORTEX_USER_ROLE` | Role to revoke from over-budget users |
| `EMAIL_INTEGRATION` | `MY_EMAIL_INT` | Snowflake notification integration name |
| `ALERT_RECIPIENTS` | (empty) | Comma-separated email addresses |
| `SLACK_ENABLED` | `false` | Enable Slack webhook notifications |
| `SLACK_WEBHOOK_URL` | (empty) | Slack incoming webhook URL |

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Test locally and in SIS
4. Submit a pull request

## License

Apache License 2.0. See [LEGAL.md](LEGAL.md) for full terms and Snowflake trademark notice.
