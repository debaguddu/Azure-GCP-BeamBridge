"""
PostgreSQL Connection Helper
============================
Provides reusable helper functions to get PostgreSQL configurations
and connection objects from environment variables.

Does NOT create tables or load data. Only establishes and verifies connections.

Usage:
    from postgres_connection import get_connection, get_config

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Path resolution: check Source_Data_Setup/.env first, then parent project root .env
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent

ENV_CANDIDATES = [
    BASE_DIR / ".env",
    PROJECT_ROOT / ".env",
]

for candidate in ENV_CANDIDATES:
    if candidate.is_file():
        load_dotenv(dotenv_path=candidate, override=False)

# Driver resolution
DRIVER_NAME = None
try:
    import psycopg2
    DRIVER_NAME = "psycopg2"
except ImportError:
    try:
        import pg8000.dbapi
        DRIVER_NAME = "pg8000"
    except ImportError:
        pass


def get_config():
    """Retrieve PostgreSQL connection parameters from environment variables."""
    return {
        "host": os.getenv("PG_HOST") or os.getenv("POSTGRES_HOST") or "localhost",
        "port": int(os.getenv("PG_PORT") or os.getenv("POSTGRES_PORT") or 5432),
        "database": os.getenv("PG_DATABASE") or os.getenv("POSTGRES_DB") or "postgres",
        "user": os.getenv("PG_USER") or os.getenv("POSTGRES_USER") or "postgres",
        "password": os.getenv("PG_PASSWORD") or os.getenv("POSTGRES_PASSWORD") or "",
        "schema": os.getenv("PG_SCHEMA", "public").strip(),
        "sslmode": os.getenv("PG_SSLMODE", "prefer").strip(),
        "timeout": int(os.getenv("PG_TIMEOUT", "10")),
    }


def get_connection(config=None):
    """
    Establish and return an active PostgreSQL connection.
    Does not modify database schemas or tables.
    """
    cfg = config or get_config()

    if DRIVER_NAME == "psycopg2":
        return psycopg2.connect(
            host=cfg["host"],
            port=cfg["port"],
            dbname=cfg["database"],
            user=cfg["user"],
            password=cfg["password"],
            sslmode=cfg["sslmode"],
            connect_timeout=cfg["timeout"],
        )
    elif DRIVER_NAME == "pg8000":
        return pg8000.dbapi.connect(
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            user=cfg["user"],
            password=cfg["password"],
            timeout=cfg["timeout"],
        )
    else:
        raise RuntimeError(
            "No PostgreSQL driver available. Please install psycopg2:\n"
            "    pip install psycopg2-binary"
        )


def test_connection():
    """Quick sanity check of database connection."""
    cfg = get_config()
    masked_pwd = (cfg['password'][0] + '***' + cfg['password'][-1]) if len(cfg['password']) > 2 else '***'
    print(f"Connecting to {cfg['user']}@{cfg['host']}:{cfg['port']}/{cfg['database']} (pwd: {masked_pwd})...")
    try:
        conn = get_connection(cfg)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT version(), current_database(), current_user;")
        row = cur.fetchone()
        cur.close()
        conn.close()
        print(f"[SUCCESS] Connected to PostgreSQL!")
        print(f"  Database: {row[1]}")
        print(f"  User:     {row[2]}")
        print(f"  Version:  {row[0].splitlines()[0]}")
        return True
    except Exception as e:
        print(f"[FAILED] Could not connect: {e}")
        return False


if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
