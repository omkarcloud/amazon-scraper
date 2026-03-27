import os
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=True)

DEFAULT_RAPIDAPI_HOST = "real-time-amazon-data.p.rapidapi.com"
DEFAULT_RAPIDAPI_BASE_URL = f"https://{DEFAULT_RAPIDAPI_HOST}"


def get_rapidapi_settings(api_key: Optional[str] = None) -> Dict[str, Any]:
    resolved_api_key = api_key or os.getenv("RAPIDAPI_KEY")
    if not resolved_api_key:
        raise ValueError(
            "RapidAPI key is required. Set RAPIDAPI_KEY or pass key/api_key explicitly."
        )

    return {
        "api_key": resolved_api_key,
        "api_host": os.getenv("RAPIDAPI_HOST", DEFAULT_RAPIDAPI_HOST),
        "base_url": os.getenv("RAPIDAPI_BASE_URL", DEFAULT_RAPIDAPI_BASE_URL),
        "timeout": int(os.getenv("RAPIDAPI_TIMEOUT", "30")),
    }


def get_database_settings() -> Dict[str, Any]:
    return {
        "host": os.getenv("DB_HOST", os.getenv("AWS_SE1B_RDS_01_END_POINT", "")),
        "port": int(os.getenv("DB_PORT", "3306")),
        "database": os.getenv("DB_NAME", "gurysk_src"),
        "user": os.getenv("DB_USER", os.getenv("AWS_SE1B_RDS_01_USER", "")),
        "password": os.getenv("DB_PASSWORD", os.getenv("AWS_SE1B_RDS_01_PWD", "")),
        "charset": os.getenv("DB_CHARSET", "utf8mb4"),
    }


def get_ssh_tunnel_settings() -> Dict[str, Any]:
    return {
        "ssh_host": os.getenv("SSH_HOST", os.getenv("AWS_SE1B_CICD_HOST", "")),
        "ssh_port": int(os.getenv("SSH_PORT", os.getenv("AWS_SE1B_CICD_SSH_PORT", "22"))),
        "ssh_username": os.getenv("SSH_USERNAME", os.getenv("AWS_SE1B_CICD_USERNAME", "ec2-user")),
        "ssh_pkey": os.getenv("SSH_PKEY", os.path.expanduser("~/.ssh/id_rsa")),
    }


def get_rds_conn() -> Dict[str, Any]:
    """Build a connection config dict compatible with etlprocessor's MySqlClient."""
    db = get_database_settings()
    ssh = get_ssh_tunnel_settings()

    if ssh["ssh_host"]:
        return {
            "client_type": "mysqlssh",
            "db_host": db["host"],
            "db_port": db["port"],
            "db_user": db["user"],
            "db_password": db["password"],
            "db_database": db["database"],
            "ssh_host": ssh["ssh_host"],
            "ssh_port": ssh["ssh_port"],
            "ssh_username": ssh["ssh_username"],
            "ssh_pkey": ssh["ssh_pkey"],
        }

    return {
        "client_type": "mysql",
        "host": db["host"],
        "port": db["port"],
        "user": db["user"],
        "password": db["password"],
        "database": db["database"],
    }


# Module-level config dicts consumed by dashboard/pages/config.py and others.
_db = get_database_settings()
_ssh = get_ssh_tunnel_settings()

DB_CONFIG: Dict[str, Any] = {
    "host": _db["host"],
    "port": _db["port"],
    "user": _db["user"],
    "password": _db["password"],
    "database": _db["database"],
}

SSH_CONFIG: Dict[str, Any] = {
    "enabled": bool(_ssh["ssh_host"]),
    "host": _ssh["ssh_host"],
    "port": _ssh["ssh_port"],
    "user": _ssh["ssh_username"],
    "key_file": _ssh["ssh_pkey"],
}
