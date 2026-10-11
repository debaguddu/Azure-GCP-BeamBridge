import os
import sys
import time
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import init_environment, print_header, print_success, print_error, print_info, print_warning

def test_asql_connection() -> bool:
    """Test connectivity to Azure SQL Server database."""
    init_environment()
    print_header("Testing Azure SQL Server (ASQL) Connection")

    server = os.getenv("AZURE_SQL_SERVER")
    port = os.getenv("AZURE_SQL_PORT", "1433")
    database = os.getenv("AZURE_SQL_DATABASE")
    user = os.getenv("AZURE_SQL_USER")
    password = os.getenv("AZURE_SQL_PASSWORD")
    driver = os.getenv("AZURE_SQL_DRIVER", "ODBC Driver 18 for SQL Server")
    trust_cert = os.getenv("AZURE_SQL_TRUST_SERVER_CERTIFICATE", "yes")

    print_info(f"Server: {server}:{port}")
    print_info(f"Database: {database}")
    print_info(f"Username: {user}")
    print_info(f"Configured Driver: {driver}")

    if not server or not database or not user or not password:
        print_error("Missing required Azure SQL parameters in .env file.")
        return False

    try:
        import pyodbc

        available_drivers = pyodbc.drivers()
        print_info(f"Available ODBC Drivers: {available_drivers}")

        # Choose the best installed SQL Server driver
        selected_driver = driver
        if driver not in available_drivers:
            # Check for alternative ODBC Driver 17 or SQL Server
            sql_drivers = [d for d in available_drivers if "SQL Server" in d]
            if sql_drivers:
                selected_driver = sql_drivers[0]
                print_warning(f"Driver '{driver}' not found. Using installed driver: '{selected_driver}'")
            else:
                print_error(f"No SQL Server ODBC drivers found in system! Please install '{driver}'.")
                return False

        # Construct connection string
        conn_str = (
            f"DRIVER={{{selected_driver}}};"
            f"SERVER=tcp:{server},{port};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate={trust_cert};"
        )

        print_info("Connecting to Azure SQL Server (timeout 30s)...")
        start_time = time.time()
        with pyodbc.connect(conn_str, timeout=30) as conn:
            elapsed = time.time() - start_time
            print_success(f"Connected to Azure SQL Server in {elapsed:.2f} seconds!")

            with conn.cursor() as cursor:
                cursor.execute("SELECT @@VERSION AS version, DB_NAME() AS current_db, CURRENT_USER AS db_user, GETDATE() AS server_time")
                row = cursor.fetchone()
                if row:
                    version_summary = row[0].split("\n")[0]
                    print_info(f"Database Engine: {version_summary}")
                    print_info(f"Connected Database: {row[1]}")
                    print_info(f"DB User: {row[2]}")
                    print_info(f"Server Time: {row[3]}")

                # Query table count
                cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
                table_count = cursor.fetchone()[0]
                print_info(f"Total Base Tables in Database: {table_count}")

                if table_count > 0:
                    cursor.execute("SELECT TOP 5 TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
                    sample_tables = [f"{r[0]}.{r[1]}" for r in cursor.fetchall()]
                    print_info(f"Sample Tables: {sample_tables}")

        print_success("Azure SQL Server connection test PASSED.")
        return True

    except Exception as e:
        err_msg = str(e)
        print_error(f"Azure SQL Server connection failed: {err_msg}")
        if "40613" in err_msg or "not currently available" in err_msg:
            print_warning("Azure SQL Database appears to be in auto-paused state (Serverless tier). Azure SQL takes ~30-60s to resume. Please retry shortly.")
        elif "Cannot open server" in err_msg or "firewall" in err_msg.lower():
            print_warning("Client IP might not be whitelisted in Azure SQL Server firewall settings.")
        elif "Login failed" in err_msg:
            print_warning("Authentication failed. Please verify AZURE_SQL_USER and AZURE_SQL_PASSWORD.")
        return False

if __name__ == "__main__":
    success = test_asql_connection()
    sys.exit(0 if success else 1)
