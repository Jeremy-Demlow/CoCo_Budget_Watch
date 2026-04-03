# Legal Notice

## Disclaimer

**CoCo Budgets** is an **unofficial, community-developed tool**. It is **not a supported Snowflake product** and is not covered by any Snowflake support agreement, SLA, or warranty.

### No Warranty

THIS SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT, OR OTHERWISE, ARISING FROM, OUT OF, OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

### Not a Snowflake Product

- This tool is **not developed, maintained, or endorsed by Snowflake Inc.**
- It is not part of the Snowflake platform and should not be treated as an official feature.
- Snowflake Support will not provide assistance for issues related to this tool.
- The tool's behavior may change or break as Snowflake evolves its platform and APIs.

### Data Accuracy

- Credit usage data is sourced from `SNOWFLAKE.ACCOUNT_USAGE` views, which have an inherent latency of **up to 1 hour**.
- Credit calculations are based on publicly documented token-to-credit rates at the time of development. These rates may change without notice.
- The estimated USD costs displayed are approximations based on a configurable credit rate and **may not match your actual invoice**.

### Budget Enforcement Limitations

- Enforcement uses Snowflake's **native daily credit limit parameters** (`CORTEX_CODE_CLI_DAILY_EST_CREDIT_LIMIT_PER_USER` and `CORTEX_CODE_SNOWSIGHT_DAILY_EST_CREDIT_LIMIT_PER_USER`) to block over-budget users by setting their limits to `0`.
- These parameters enforce a **rolling 24-hour window**, which is different from the app's period-based (monthly/weekly/quarterly) budget tracking.
- `ACCOUNT_USAGE` views have up to **~1 hour data lag**. Enforcement decisions are based on lagged data, so a user may consume credits beyond their budget before being blocked.
- For instant cost control, use the **Model Allowlist** (`CORTEX_ENABLED_CROSS_REGION`) which takes effect immediately.
- `ALTER ACCOUNT SET` and `ALTER USER SET` for these parameters require **ACCOUNTADMIN** privileges.

### Your Responsibility

By using this tool, you acknowledge that:

1. You are solely responsible for monitoring and managing your Snowflake credit consumption.
2. This tool supplements but does not replace Snowflake's built-in cost management features (Resource Monitors, Budgets, etc.).
3. You should validate all credit and cost figures against your official Snowflake billing.
4. You assume all risk associated with deploying and using this tool in your Snowflake account.

### Snowflake Trademarks

Snowflake, the Snowflake logo, and all other Snowflake product or service names are trademarks or registered trademarks of Snowflake Inc. in the United States and/or other countries. Use of these trademarks in this project is for identification purposes only and does not imply endorsement.

## License

This project is released under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).

Copyright 2024-2026 Contributors.
