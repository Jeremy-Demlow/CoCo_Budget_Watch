import os
import pandas as pd

_SESSION = None
_ACTIVE_CONNECTION = None
_CONNECTION_ERROR = None

DB = "COCO_BUDGETS_DB"
SCHEMA = "BUDGETS"
FQN = f"{DB}.{SCHEMA}"


def _load_connection_config(conn_name: str) -> dict:
    import tomllib
    toml_path = os.path.expanduser("~/.snowflake/connections.toml")
    with open(toml_path, "rb") as f:
        conns = tomllib.load(f)
    cfg = conns.get(conn_name, {})
    params = dict(cfg)
    if "private_key_path" in params or "private_key_file" in params:
        from cryptography.hazmat.primitives import serialization
        key_path = os.path.expanduser(params.pop("private_key_path", params.pop("private_key_file", "")))
        with open(key_path, "rb") as kf:
            p_key = serialization.load_pem_private_key(kf.read(), password=None)
        params["private_key"] = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        params.pop("authenticator", None)
    return params


def is_local_mode() -> bool:
    try:
        from snowflake.snowpark.context import get_active_session
        get_active_session()
        return False
    except Exception:
        return True


def list_connections() -> dict[str, str]:
    try:
        import tomllib
        toml_path = os.path.expanduser("~/.snowflake/connections.toml")
        with open(toml_path, "rb") as f:
            conns = tomllib.load(f)
        result = {}
        for name, cfg in conns.items():
            if isinstance(cfg, dict) and "account" in cfg:
                result[name] = cfg.get("account", "")
        return result
    except Exception:
        return {}


def get_active_connection_name() -> str:
    global _ACTIVE_CONNECTION
    if _ACTIVE_CONNECTION:
        return _ACTIVE_CONNECTION
    return os.getenv("SNOWFLAKE_CONNECTION_NAME", "myconnection")


def switch_connection(conn_name: str) -> None:
    global _SESSION, _ACTIVE_CONNECTION, _CONNECTION_ERROR
    if conn_name == _ACTIVE_CONNECTION:
        return
    if _SESSION is not None:
        try:
            if hasattr(_SESSION, "close"):
                _SESSION.close()
        except Exception:
            pass
    _SESSION = None
    _CONNECTION_ERROR = None
    _ACTIVE_CONNECTION = conn_name
    from lib.config import clear_caches
    clear_caches()


def get_connection_error() -> str | None:
    return _CONNECTION_ERROR


def get_session():
    global _SESSION, _ACTIVE_CONNECTION, _CONNECTION_ERROR
    if _SESSION is not None:
        return _SESSION
    _CONNECTION_ERROR = None
    try:
        from snowflake.snowpark.context import get_active_session
        _SESSION = get_active_session()
        return _SESSION
    except Exception:
        pass
    try:
        import snowflake.connector
        conn_name = _ACTIVE_CONNECTION or os.getenv("SNOWFLAKE_CONNECTION_NAME", "myconnection")
        _ACTIVE_CONNECTION = conn_name
        params = _load_connection_config(conn_name)
        conn = snowflake.connector.connect(**params)
        _SESSION = conn
        return _SESSION
    except Exception as e:
        _CONNECTION_ERROR = str(e)
        return None


def run_query(sql: str, params=None) -> tuple[pd.DataFrame, str | None]:
    session = get_session()
    if session is None:
        return pd.DataFrame(), _CONNECTION_ERROR or "Not connected"
    try:
        if hasattr(session, "sql"):
            df = session.sql(sql).to_pandas()
        else:
            cur = session.cursor()
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=cols)
        return df, None
    except Exception as e:
        global _SESSION
        try:
            if _SESSION and hasattr(_SESSION, 'is_closed') and _SESSION.is_closed():
                _SESSION = None
        except Exception:
            _SESSION = None
        return pd.DataFrame(), str(e)


def run_ddl(sql: str) -> str | None:
    session = get_session()
    if session is None:
        return _CONNECTION_ERROR or "Not connected"
    try:
        if hasattr(session, "sql"):
            session.sql(sql).collect()
        else:
            session.cursor().execute(sql)
        return None
    except Exception as e:
        global _SESSION
        try:
            if _SESSION and hasattr(_SESSION, 'is_closed') and _SESSION.is_closed():
                _SESSION = None
        except Exception:
            _SESSION = None
        return str(e)


def _float_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    return df


def get_current_role() -> str:
    df, err = run_query("SELECT CURRENT_ROLE() AS ROLE")
    if err:
        global _SESSION
        _SESSION = None
        df, err = run_query("SELECT CURRENT_ROLE() AS ROLE")
    if err or df.empty:
        return "UNKNOWN"
    return str(df.iloc[0]["ROLE"])


def get_available_roles() -> list[str]:
    df, err = run_query("SELECT CURRENT_USER() AS U")
    if err or df.empty:
        return []
    user = df.iloc[0]["U"]
    df, err = run_query(f"SHOW GRANTS TO USER {user}")
    if err or df.empty:
        return []
    col = "role" if "role" in df.columns else "ROLE" if "ROLE" in df.columns else None
    if col is None:
        return []
    return sorted(df[col].astype(str).unique().tolist())


def use_role(role_name: str) -> str | None:
    err = run_ddl(f"USE ROLE {role_name}")
    if not err:
        from lib.config import clear_caches
        clear_caches()
    return err


def log_audit(action: str, target_type: str, target_user_id=None,
              old_value=None, new_value=None, notes=None):
    safe_action = action.replace("'", "''")
    safe_type = target_type.replace("'", "''")
    uid = f"{int(target_user_id)}" if target_user_id is not None else "NULL"
    ov = f"{float(old_value)}" if old_value is not None else "NULL"
    nv = f"{float(new_value)}" if new_value is not None else "NULL"
    safe_notes = f"'{notes.replace(chr(39), chr(39)+chr(39))}'" if notes else "NULL"
    run_ddl(
        f"INSERT INTO {FQN}.BUDGET_AUDIT_LOG "
        f"(ACTION, TARGET_TYPE, TARGET_USER_ID, OLD_VALUE, NEW_VALUE, NOTES) "
        f"VALUES ('{safe_action}', '{safe_type}', {uid}, {ov}, {nv}, {safe_notes})"
    )
