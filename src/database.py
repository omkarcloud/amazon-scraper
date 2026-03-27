import json
import socket
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pymysql
from sshtunnel import SSHTunnelForwarder

from .config import get_database_settings, get_ssh_tunnel_settings


DEFAULT_RAW_PRODUCTS_TABLE = "amazon_product_raw"
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_START_TUNNEL_SCRIPT = _PROJECT_ROOT / "scripts" / "start-rds-tunnel.sh"

_tunnel: Optional[SSHTunnelForwarder] = None


def create_db_connection(
    host: Optional[str] = None,
    port: Optional[int] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
    use_ssh_tunnel: Optional[bool] = None,
) -> pymysql.Connection:
    db = get_database_settings()
    resolved = {
        "host": host or db["host"],
        "port": port or db["port"],
        "user": user or db["user"],
        "password": password or db["password"],
        "database": database or db["database"],
        "charset": db["charset"],
    }

    ssh = get_ssh_tunnel_settings()
    should_tunnel = use_ssh_tunnel if use_ssh_tunnel is not None else bool(ssh["ssh_host"])

    if should_tunnel:
        return _connect_via_ssh_tunnel(resolved, ssh)

    _ensure_local_tunnel_if_needed(resolved)
    try:
        return pymysql.connect(**resolved)
    except pymysql.err.OperationalError as exc:
        # If the local SSH tunnel disappeared between checks, restart it once
        # and retry the direct local connection.
        if _is_local_tunnel_target(resolved):
            _ensure_local_tunnel_if_needed(resolved, force_restart=True)
            return pymysql.connect(**resolved)
        raise exc


def _connect_via_ssh_tunnel(
    db_settings: Dict[str, Any],
    ssh_settings: Dict[str, Any],
) -> pymysql.Connection:
    global _tunnel

    if _tunnel is None or not _tunnel.is_active:
        _tunnel = SSHTunnelForwarder(
            (ssh_settings["ssh_host"], ssh_settings["ssh_port"]),
            ssh_username=ssh_settings["ssh_username"],
            ssh_pkey=ssh_settings["ssh_pkey"],
            remote_bind_address=(db_settings["host"], db_settings["port"]),
        )
        _tunnel.start()

    return pymysql.connect(
        host="127.0.0.1",
        port=_tunnel.local_bind_port,
        user=db_settings["user"],
        password=db_settings["password"],
        database=db_settings["database"],
        charset=db_settings["charset"],
    )


def close_tunnel() -> None:
    global _tunnel
    if _tunnel is not None and _tunnel.is_active:
        _tunnel.stop()
        _tunnel = None


def _is_local_tunnel_target(db_settings: Dict[str, Any]) -> bool:
    host = str(db_settings.get("host", "")).strip().lower()
    port = int(db_settings.get("port", 0) or 0)
    return host in {"127.0.0.1", "localhost"} and port == 3307


def _is_port_listening(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def _ensure_local_tunnel_if_needed(
    db_settings: Dict[str, Any],
    force_restart: bool = False,
) -> None:
    if not _is_local_tunnel_target(db_settings):
        return

    host = str(db_settings["host"])
    port = int(db_settings["port"])
    if not force_restart and _is_port_listening(host, port):
        return

    if not _START_TUNNEL_SCRIPT.exists():
        return

    subprocess.run(
        ["bash", str(_START_TUNNEL_SCRIPT)],
        cwd=str(_PROJECT_ROOT),
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )


def ensure_raw_products_table(
    connection: pymysql.Connection,
    table_name: str = DEFAULT_RAW_PRODUCTS_TABLE,
) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                asin VARCHAR(32) NOT NULL,
                record_create_timestamp DATETIME NOT NULL,
                source_endpoint VARCHAR(64) NOT NULL,
                marketplace_country VARCHAR(8) NOT NULL DEFAULT 'US',
                search_query TEXT,
                request_metadata JSON,
                api_payload JSON NOT NULL,
                INDEX idx_asin (asin),
                INDEX idx_record_create_timestamp (record_create_timestamp)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )
    connection.commit()


def insert_raw_product_rows(
    connection: pymysql.Connection,
    rows: Iterable[Dict[str, Any]],
    table_name: str = DEFAULT_RAW_PRODUCTS_TABLE,
    commit: bool = True,
) -> int:
    normalized_rows: List[Dict[str, Any]] = list(rows)
    if not normalized_rows:
        return 0

    sql = f"""
        INSERT INTO `{table_name}` (
            asin,
            record_create_timestamp,
            source_endpoint,
            marketplace_country,
            search_query,
            request_metadata,
            api_payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """

    params = [
        (
            row["asin"],
            row["record_create_timestamp"],
            row["source_endpoint"],
            row["marketplace_country"],
            row.get("search_query"),
            json.dumps(row.get("request_metadata", {}), ensure_ascii=False),
            json.dumps(row["api_payload"], ensure_ascii=False),
        )
        for row in normalized_rows
    ]

    with connection.cursor() as cursor:
        cursor.executemany(sql, params)

    if commit:
        connection.commit()

    return len(normalized_rows)
