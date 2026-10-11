import os
import sys
import time
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import init_environment, print_header, print_success, print_error, print_info, print_warning

def test_postgres_connection() -> bool:
    """Test connectivity to PostgreSQL database."""
    init_environment()
    print_header("Testing PostgreSQL Database Connection")

    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5432")
    database = os.getenv("PG_DATABASE", "postgres")
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASSWORD", "")
    schema = os.getenv("PG_SCHEMA", "public")
    sslmode = os.getenv("PG_SSLMODE", "prefer")

    print_info(f"Host: {host}:{port}")
    print_info(f"Database: {database}")
    print_info(f"User: {user}")
    print_info(f"Schema: {schema}")
    print_info(f"SSL Mode: {sslmode}")

    if not host or not database or not user:
        print_error("Missing required PostgreSQL parameters in .env file.")
        return False

    try:
        import psycopg2

        print_info(f"Connecting to PostgreSQL database at {host}:{port}...")
        start_time = time.time()
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password,
            sslmode=sslmode,
            connect_timeout=10,
        )
        elapsed = time.time() - start_time
        print_success(f"Connected to PostgreSQL in {elapsed:.2f} seconds!")

        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version(), current_database(), current_user, clock_timestamp();")
                row = cur.fetchone()
                if row:
                    pg_version = row[0].split(",")[0]
                    print_info(f"PostgreSQL Version: {pg_version}")
                    print_info(f"Connected Database: {row[1]}")
                    print_info(f"Current User: {row[2]}")
                    print_info(f"Server Timestamp: {row[3]}")

                # Query table count in target schema
                cur.execute(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = %s;",
                    (schema,)
                )
                count = cur.fetchone()[0]
                print_info(f"Tables in schema '{schema}': {count}")

                if count > 0:
                    cur.execute(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema = %s LIMIT 5;",
                        (schema,)
                    )
                    tables = [r[0] for r in cur.fetchall()]
                    print_info(f"Sample Tables: {tables}")

        conn.close()
        print_success("PostgreSQL connection test PASSED.")
        return True

    except Exception as e:
        err_msg = str(e)
        print_error(f"PostgreSQL connection failed: {err_msg}")
        if "could not connect to server" in err_msg.lower():
            print_warning(f"Could not reach PostgreSQL server on {host}:{port}. Ensure service is running.")
        elif "password authentication failed" in err_msg.lower():
            print_warning("Authentication failed. Please verify PG_USER and PG_PASSWORD.")
        return False

if __name__ == "__main__":
    success = test_postgres_connection()
    sys.exit(0 if success else 1)
