"""Shared database connection utility."""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
import pyodbc

# Load .env from parent directory (projects/dbstudio/.env)
_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(_ENV_PATH)


def get_config():
    """Return database configuration from environment variables."""
    return {
        "server": os.environ.get("DB_SERVER", ""),
        "user": os.environ.get("DB_USER", ""),
        "password": os.environ.get("DB_PASSWORD", ""),
        "database": os.environ.get("DB_NAME", "VARIAN"),
    }


def connect(config=None):
    """Create a pyodbc connection to the database."""
    if config is None:
        config = get_config()
    conn_str = (
        f"DRIVER={{SQL Server}};"
        f"SERVER={config['server']};"
        f"DATABASE={config['database']};"
        f"UID={config['user']};"
        f"PWD={config['password']}"
    )
    return pyodbc.connect(conn_str, timeout=10)


def format_type(type_name, max_length, precision, scale):
    """Format SQL Server column type string."""
    if type_name in ("nvarchar", "nchar"):
        return f"{type_name}(max)" if max_length == -1 else f"{type_name}({max_length // 2})"
    if type_name in ("varchar", "char", "varbinary", "binary"):
        return f"{type_name}(max)" if max_length == -1 else f"{type_name}({max_length})"
    if type_name in ("decimal", "numeric"):
        return f"{type_name}({precision},{scale})"
    return type_name
