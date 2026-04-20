from lib.connection import run_query, run_ddl, FQN
from lib.config import get_config


def send_budget_alert(
    recipients: str, user_name: str, pct_used: float,
    budget: float, used: float, alert_type: str = "WARNING"
) -> str | None:
    cfg = get_config()
    integration = cfg.get("EMAIL_INTEGRATION", "MY_EMAIL_INT")
    subject = f"CoCo Budget Alert: {user_name} is {'OVER' if alert_type == 'OVER' else 'at ' + str(int(pct_used)) + '%'} budget"
    body = (
        f"User: {user_name}\n"
        f"Status: {alert_type}\n"
        f"Credits Used: {used:.4f}\n"
        f"Budget: {budget:.2f}\n"
        f"Percent Used: {pct_used:.1f}%\n\n"
        f"-- Sent by CoCo Budgets enforcement system"
    )
    safe_subj = subject.replace("'", "''")
    safe_body = body.replace("'", "''")
    safe_recip = recipients.replace("'", "''")
    err = run_ddl(
        f"CALL SYSTEM$SEND_EMAIL('{integration}', '{safe_recip}', '{safe_subj}', '{safe_body}')"
    )
    if cfg.get("SLACK_ENABLED", "false").lower() == "true":
        slack_url = cfg.get("SLACK_WEBHOOK_URL", "")
        if slack_url:
            _send_slack_alert(slack_url, subject, body)
    return err


def send_account_budget_alert(
    recipients: str, pct_used: float, budget: float,
    used: float, alert_type: str = "WARNING"
) -> str | None:
    cfg = get_config()
    integration = cfg.get("EMAIL_INTEGRATION", "MY_EMAIL_INT")
    label = "OVER" if alert_type == "OVER" else f"at {int(pct_used)}%"
    subject = f"CoCo Account Budget Alert: Account is {label} budget"
    body = (
        f"Target: ACCOUNT (all users)\n"
        f"Status: {alert_type}\n"
        f"Credits Used: {used:.4f}\n"
        f"Account Budget: {budget:.2f}\n"
        f"Percent Used: {pct_used:.1f}%\n\n"
        f"-- Sent by CoCo Budgets enforcement system"
    )
    safe_subj = subject.replace("'", "''")
    safe_body = body.replace("'", "''")
    safe_recip = recipients.replace("'", "''")
    err = run_ddl(
        f"CALL SYSTEM$SEND_EMAIL('{integration}', '{safe_recip}', '{safe_subj}', '{safe_body}')"
    )
    if cfg.get("SLACK_ENABLED", "false").lower() == "true":
        slack_url = cfg.get("SLACK_WEBHOOK_URL", "")
        if slack_url:
            _send_slack_alert(slack_url, subject, body)
    return err


def _send_slack_alert(webhook_url: str, subject: str, body: str) -> str | None:
    try:
        import json
        import urllib.request
        payload = json.dumps({"text": f"*{subject}*\n```{body}```"}).encode("utf-8")
        req = urllib.request.Request(
            webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
        return None
    except Exception as e:
        return str(e)


def check_alert_already_sent(user_id: int, alert_type: str, period_key: str) -> bool:
    df, _ = run_query(
        f"SELECT 1 FROM {FQN}.ALERT_STATE "
        f"WHERE USER_ID = {int(user_id)} AND ALERT_TYPE = '{alert_type}' AND PERIOD_KEY = '{period_key}' "
        f"LIMIT 1"
    )
    return not df.empty


def record_alert_sent(user_id: int, alert_type: str, period_key: str) -> None:
    run_ddl(
        f"INSERT INTO {FQN}.ALERT_STATE (USER_ID, ALERT_TYPE, PERIOD_KEY) "
        f"SELECT {int(user_id)}, '{alert_type}', '{period_key}' "
        f"WHERE NOT EXISTS (SELECT 1 FROM {FQN}.ALERT_STATE "
        f"WHERE USER_ID={int(user_id)} AND ALERT_TYPE='{alert_type}' AND PERIOD_KEY='{period_key}')"
    )
