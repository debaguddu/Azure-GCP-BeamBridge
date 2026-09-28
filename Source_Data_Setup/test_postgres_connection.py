"""
PostgreSQL Connectivity Diagnostic Tool
=======================================
Verifies database connection settings, performs health checks,
inspects target schema, and lists existing tables.

Usage:
    python test_postgres_connection.py
    python test_postgres_connection.py --host localhost --port 5432 --user postgres --dbname postgres
"""

import os
import sys
import time
import argparse
from pathlib import Path
from dotenv import load_dotenv

# Path resolution: check Source_Data_Setup/.env first, then parent project root .env
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent

ENV_CANDIDATES = [
    BASE_DIR / ".env",
    PROJECT_ROOT / ".env",
]

env_loaded_from = None
for candidate in ENV_CANDIDATES:
    if candidate.is_file():
        load_dotenv(dotenv_path=candidate, override=False)
        if env_loaded_from is None:
            env_loaded_from = candidate

# Determine available PostgreSQL driver
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


def get_pg_config(args=None):
    """Retrieve PostgreSQL connection settings from arguments or environment variables."""
    args = args or argparse.Namespace()

    host = getattr(args, "host", None) or os.getenv("PG_HOST") or os.getenv("POSTGRES_HOST") or "localhost"
    port = getattr(args, "port", None) or os.getenv("PG_PORT") or os.getenv("POSTGRES_PORT") or "5432"
    dbname = getattr(args, "dbname", None) or os.getenv("PG_DATABASE") or os.getenv("POSTGRES_DB") or "postgres"
    user = getattr(args, "user", None) or os.getenv("PG_USER") or os.getenv("POSTGRES_USER") or "postgres"
    password = getattr(args, "password", None) or os.getenv("PG_PASSWORD") or os.getenv("POSTGRES_PASSWORD") or ""
    schema = getattr(args, "schema", None) or os.getenv("PG_SCHEMA") or "public"
    sslmode = getattr(args, "sslmode", None) or os.getenv("PG_SSLMODE") or "prefer"
    timeout = getattr(args, "timeout", None) or int(os.getenv("PG_TIMEOUT", "10"))

    return {
        "host": host.strip(),
        "port": int(str(port).strip()),
        "dbname": dbname.strip(),
        "user": user.strip(),
        "password": password.strip(),
        "schema": schema.strip(),
        "sslmode": sslmode.strip(),
        "timeout": timeout,
    }


def mask_password(pwd):
    """Safely mask password for terminal output."""
    if not pwd:
        return "(empty / none)"
    if len(pwd) <= 2:
        return "**"
    return pwd[0] + "*" * (len(pwd) - 2) + pwd[-1]


def connect_postgres(config):
    """Establish connection using available driver."""
    if DRIVER_NAME == "psycopg2":
        return psycopg2.connect(
            host=config["host"],
            port=config["port"],
            dbname=config["dbname"],
            user=config["user"],
            password=config["password"],
            sslmode=config["sslmode"],
            connect_timeout=config["timeout"],
        )
    elif DRIVER_NAME == "pg8000":
        return pg8000.dbapi.connect(
            host=config["host"],
            port=config["port"],
            database=config["dbname"],
            user=config["user"],
            password=config["password"],
            timeout=config["timeout"],
        )
    else:
        raise RuntimeError(
            "Neither 'psycopg2' nor 'pg8000' driver is installed.\n"
            "Please install one by running:\n"
            "    pip install psycopg2-binary\n"
            " or pip install pg8000"
        )


def test_connection(args=None):
    """Run comprehensive connection test and diagnostics."""
    config = get_pg_config(args)

    print("=" * 70)
    print(" PostgreSQL Connectivity Test")
    print("=" * 70)
    if env_loaded_from:
        print(f" Loaded env file: {env_loaded_from}")
    else:
        print(" [INFO] No .env file found; using defaults / OS environment variables.")

    print(f" Driver:         {DRIVER_NAME or 'NONE (missing driver)'}")
    print(f" Host:           {config['host']}:{config['port']}")
    print(f" Database:       {config['dbname']}")
    print(f" User:           {config['user']}")
    print(f" Password:       {mask_password(config['password'])}")
    print(f" Target Schema:  {config['schema']}")
    print(f" SSL Mode:       {config['sslmode']}")
    print("-" * 70)

    if not DRIVER_NAME:
        print("[ERROR] No PostgreSQL driver available. Run: pip install psycopg2-binary")
        print("=" * 70)
        return False

    t0 = time.time()
    try:
        conn = connect_postgres(config)
        conn.autocommit = True
        elapsed_conn = (time.time() - t0) * 1000

        print(f"[SUCCESS] Handshake established in {elapsed_conn:.1f} ms!")
        print("-" * 70)

        cursor = conn.cursor()

        # 1. Version
        cursor.execute("SELECT version();")
        version_str = cursor.fetchone()[0]
        print(f" Server Version: {version_str.splitlines()[0]}")

        # 2. Database & User Context
        cursor.execute("SELECT current_database(), current_user, session_user;")
        cur_db, cur_user, sess_user = cursor.fetchone()
        print(f" Current DB:     {cur_db}")
        print(f" Current User:   {cur_user} (session: {sess_user})")

        # 3. Superuser status
        try:
            cursor.execute("SELECT usesuper FROM pg_user WHERE usename = current_user;")
            row = cursor.fetchone()
            is_super = row[0] if row else False
            print(f" Superuser:      {'Yes' if is_super else 'No'}")
        except Exception:
            pass

        # 4. Encoding & Collation
        cursor.execute(
            "SELECT pg_encoding_to_char(encoding), datcollate FROM pg_database WHERE datname = current_database();"
        )
        enc, collate = cursor.fetchone()
        print(f" DB Encoding:    {enc} (Collation: {collate})")

        # 5. Timestamp & Timezone
        cursor.execute("SELECT NOW(), current_setting('TIMEZONE');")
        now_ts, tz = cursor.fetchone()
        print(f" Server Time:    {now_ts} (TZ: {tz})")

        # 6. Schema & Tables Inspection
        print("-" * 70)
        cursor.execute(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s;",
            (config["schema"],)
        )
        schema_exists = cursor.fetchone() is not None
        if not schema_exists:
            print(f" [WARNING] Schema '{config['schema']}' does NOT exist in database '{cur_db}'.")
        else:
            print(f" Target Schema '{config['schema']}': EXISTS")
            cursor.execute(
                """
                SELECT table_name, table_type
                FROM information_schema.tables 
                WHERE table_schema = %s
                ORDER BY table_name;
                """,
                (config["schema"],)
            )
            tables = cursor.fetchall()
            if tables:
                print(f" Tables Found ({len(tables)}):")
                for tbl_name, tbl_type in tables:
                    # Optional row count probe
                    count_str = ""
                    try:
                        cursor.execute(f'SELECT COUNT(*) FROM "{config["schema"]}"."{tbl_name}";')
                        row_cnt = cursor.fetchone()[0]
                        count_str = f"({row_cnt:,} rows)"
                    except Exception:
                        pass
                    print(f"   - {tbl_name:<30} [{tbl_type}] {count_str}")
            else:
                print("   (No tables currently in this schema)")

        cursor.close()
        conn.close()
        print("=" * 70)
        print(" [ALL CHECKS PASSED] PostgreSQL connection is fully functional!")
        print("=" * 70)
        return True

    except Exception as exc:
        elapsed = (time.time() - t0) * 1000
        print(f"[FAILED] Could not connect to PostgreSQL after {elapsed:.1f} ms.")
        print(f"Error Message: {exc}")
        print("\nTroubleshooting Tips:")
        err_msg = str(exc).lower()
        if "password authentication failed" in err_msg:
            print("  * Authentication failed: Check PG_USER and PG_PASSWORD in .env")
            print(f"    Current attempt: user='{config['user']}', password='{mask_password(config['password'])}'")
        elif "connection refused" in err_msg or "could not connect" in err_msg:
            print("  * Connection refused: Verify the PostgreSQL service is running.")
            print(f"    Check host '{config['host']}' and port {config['port']}.")
            print("    On Windows: check Services for 'postgresql-x64-<version>'.")
        elif "database" in err_msg and "does not exist" in err_msg:
            print(f"  * Database '{config['dbname']}' does not exist on this PostgreSQL instance.")
            print("    Verify PG_DATABASE or create the database in pgAdmin/psql first.")
        elif "timeout" in err_msg:
            print("  * Connection timed out: Check firewall settings and host accessibility.")
        else:
            print("  * Check your .env configuration and server logs for further details.")
        print("=" * 70)
        return False


def main():
    parser = argparse.ArgumentParser(description="Test PostgreSQL database connectivity.")
    parser.add_argument("--host", help="PostgreSQL server hostname/IP (default: from .env or localhost)")
    parser.add_argument("--port", type=int, help="PostgreSQL port (default: from .env or 5432)")
    parser.add_argument("--dbname", help="PostgreSQL database name (default: from .env or postgres)")
    parser.add_argument("--user", help="PostgreSQL username (default: from .env or postgres)")
    parser.add_argument("--password", help="PostgreSQL password (default: from .env)")
    parser.add_argument("--schema", help="Target schema to inspect (default: public)")
    parser.add_argument("--sslmode", help="SSL mode: disable, allow, prefer, require (default: prefer)")
    parser.add_argument("--timeout", type=int, help="Connection timeout in seconds (default: 10)")
    args = parser.parse_args()

    success = test_connection(args)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
