"""
High-Performance ETL Loader: 4 Flat Files to SQL Server
======================================================
Loads categories.csv, customers.csv, orders.csv, and order_items.csv
into Microsoft SQL Server / Azure SQL with automatic schema creation,
fast streaming batch insertion (fast_executemany), constraints, and verification.
"""

import os
import sys
import time
import csv
import itertools
import argparse
from pathlib import Path
from dotenv import load_dotenv

# Try importing pyodbc
try:
    import pyodbc
except ImportError:
    print("[ERROR] pyodbc is not installed. Please run: pip install pyodbc")
    sys.exit(1)


# Path configurations
BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env" if (BASE_DIR / ".env").exists() else BASE_DIR.parent / ".env"

# Load environment variables
load_dotenv(dotenv_path=ENV_PATH)



def get_config():
    """Retrieve database connection settings from environment variables."""
    return {
        "server": os.getenv("DB_SERVER", "localhost").strip(),
        "port": os.getenv("DB_PORT", "1433").strip(),
        "database": os.getenv("DB_NAME", "").strip(),
        "trusted_connection": os.getenv("DB_TRUSTED_CONNECTION", "no").strip().lower() in ("yes", "true", "1"),
        "user": os.getenv("DB_USER", "sa").strip(),
        "password": os.getenv("DB_PASSWORD", "").strip(),
        "driver": os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server").strip(),
        "trust_cert": os.getenv("DB_TRUST_SERVER_CERTIFICATE", "yes").strip(),
        "batch_size": int(os.getenv("BATCH_SIZE", "50000")),
        "drop_existing": os.getenv("DROP_EXISTING", "yes").strip().lower() in ("yes", "true", "1"),
        "add_constraints": os.getenv("ADD_CONSTRAINTS", "yes").strip().lower() in ("yes", "true", "1"),
    }


def build_connection_string(config):
    """Construct ODBC connection string based on config."""
    server_str = config["server"]
    if config["port"] and "\\" not in server_str and "," not in server_str:
        server_str = f"{server_str},{config['port']}"

    parts = [
        f"DRIVER={{{config['driver']}}}",
        f"SERVER={server_str}",
        f"DATABASE={config['database']}",
    ]

    if config["trusted_connection"]:
        parts.append("Trusted_Connection=yes")
    else:
        parts.append(f"UID={config['user']}")
        parts.append(f"PWD={config['password']}")

    # Encryption and certificate settings
    parts.append("Encrypt=yes")
    if config["trust_cert"].lower() in ("yes", "true", "1"):
        parts.append("TrustServerCertificate=yes")
    else:
        parts.append("TrustServerCertificate=no")

    return ";".join(parts) + ";"


def get_connection(config):
    """Establish and return a database connection."""
    conn_str = build_connection_string(config)
    conn = pyodbc.connect(conn_str, autocommit=False)
    conn.timeout = 600
    return conn


# Table definitions matching user's exact DDL specifications
TABLE_SCHEMAS = {
    "categories": {
        "file": "categories.csv",
        "drop_order": 4,  # dropped last due to FK dependencies
        "load_order": 1,  # loaded first
        "create_sql": """
            CREATE TABLE categories (
                category_id INT PRIMARY KEY,
                category_name VARCHAR(255),
                department VARCHAR(255)
            );
        """,
        "insert_sql": "INSERT INTO categories (category_id, category_name, department) VALUES (?, ?, ?)",
        "parser": lambda r: (int(r[0]), r[1], r[2]),
        "expected_cols": 3,
    },
    "customers": {
        "file": "customers.csv",
        "drop_order": 3,
        "load_order": 2,
        "create_sql": """
            CREATE TABLE customers (
                customer_id INT PRIMARY KEY,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                email VARCHAR(255),
                signup_date DATE,
                city VARCHAR(100)
            );
        """,
        "insert_sql": "INSERT INTO customers (customer_id, first_name, last_name, email, signup_date, city) VALUES (?, ?, ?, ?, ?, ?)",
        "parser": lambda r: (int(r[0]), r[1], r[2], r[3], r[4], r[5]),
        "expected_cols": 6,
    },
    "orders": {
        "file": "orders.csv",
        "drop_order": 2,
        "load_order": 3,
        "create_sql": """
            CREATE TABLE orders (
                order_id INT PRIMARY KEY,
                customer_id INT FOREIGN KEY REFERENCES customers(customer_id),
                store_id INT,
                order_date DATETIME,
                total_amount DECIMAL(10,2),
                status VARCHAR(50)
            );
        """,
        "insert_sql": "INSERT INTO orders (order_id, customer_id, store_id, order_date, total_amount, status) VALUES (?, ?, ?, ?, ?, ?)",
        "parser": lambda r: (int(r[0]), int(r[1]), int(r[2]), r[3], float(r[4]), r[5]),
        "expected_cols": 6,
    },
    "order_items": {
        "file": "order_items.csv",
        "drop_order": 1,  # dropped first
        "load_order": 4,  # loaded last
        "create_sql": """
            CREATE TABLE order_items (
                order_item_id BIGINT PRIMARY KEY,
                order_id INT FOREIGN KEY REFERENCES orders(order_id),
                product_id INT,
                quantity INT,
                unit_price DECIMAL(10,2),
                discount DECIMAL(5,2)
            );
        """,
        "insert_sql": "INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, discount) VALUES (?, ?, ?, ?, ?, ?)",
        "parser": lambda r: (int(r[0]), int(r[1]), int(r[2]), int(r[3]), float(r[4]), float(r[5])),
        "expected_cols": 6,
    },
}

# Post-load Non-Clustered Indexes (foreign keys are defined directly in table DDL)
POST_LOAD_CONSTRAINTS = [
    (
        "IX_orders_customer_id",
        "CREATE NONCLUSTERED INDEX IX_orders_customer_id ON orders(customer_id);"
    ),
    (
        "IX_orders_order_date",
        "CREATE NONCLUSTERED INDEX IX_orders_order_date ON orders(order_date);"
    ),
    (
        "IX_order_items_order_id",
        "CREATE NONCLUSTERED INDEX IX_order_items_order_id ON order_items(order_id);"
    ),
    (
        "IX_order_items_product_id",
        "CREATE NONCLUSTERED INDEX IX_order_items_product_id ON order_items(product_id);"
    ),
]


def count_file_rows(file_path):
    """Quickly counts total data rows (excluding header) in a file."""
    with open(file_path, "r", encoding="utf-8") as f:
        return max(0, sum(1 for _ in f) - 1)


def chunked_iterable(iterable, size):
    """Yield successive chunks from iterable without loading everything into memory."""
    it = iter(iterable)
    while True:
        chunk = list(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


def test_connection():
    """Verify database connection with details from .env."""
    config = get_config()
    print("=" * 65)
    print("Testing SQL Server Connection")
    print("=" * 65)
    print(f" Server:    {config['server']}:{config['port']}")
    print(f" Database:  {config['database']}")
    print(f" Auth Mode: {'Windows Authentication' if config['trusted_connection'] else f'SQL Authentication (User: {config['user']})'}")
    print(f" Driver:    {config['driver']}")
    print("-" * 65)

    if not config["database"] or config["database"] == "YourDatabaseName":
        print("[WARNING] DB_NAME is not set or still set to 'YourDatabaseName'.")
        print("Please edit '.env' with your actual database name.")
        return False

    try:
        t0 = time.time()
        conn = get_connection(config)
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION, DB_NAME();")
        version, db_name = cursor.fetchone()
        conn.close()
        elapsed = time.time() - t0

        print(f"[SUCCESS] Connected in {elapsed:.2f}s!")
        print(f" Connected DB: {db_name}")
        print(f" SQL Server:   {version.splitlines()[0]}")
        print("=" * 65)
        return True
    except Exception as e:
        print(f"[FAILED] Could not connect to SQL Server:\n{e}")
        print("\nTroubleshooting tips:")
        print("1. Check if SQL Server service is running and accessible.")
        print("2. Check server name, port, database, and credentials in '.env'.")
        print("3. Ensure TCP/IP protocol is enabled in SQL Server Configuration Manager.")
        print("4. Check if the specified DB_DRIVER matches an installed driver on this PC.")
        print("=" * 65)
        return False


def drop_and_create_tables(conn, config, selected_tables=None):
    """Drop and recreate tables in appropriate dependency order."""
    cursor = conn.cursor()

    # Determine tables to process
    tables_to_process = selected_tables or list(TABLE_SCHEMAS.keys())

    if config["drop_existing"]:
        print("\n--- Dropping Existing Constraints & Tables ---")

        # 1. Drop any foreign keys referencing or belonging to target tables first
        tbl_names = "', '".join(tables_to_process)
        fk_query = f"""
            SELECT 
                SCHEMA_NAME(pt.schema_id) AS parent_schema,
                pt.name AS parent_table,
                fk.name AS fk_name
            FROM sys.foreign_keys fk
            JOIN sys.tables pt ON fk.parent_object_id = pt.object_id
            JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
            WHERE pt.name IN ('{tbl_names}') OR rt.name IN ('{tbl_names}')
        """
        cursor.execute(fk_query)
        fks = cursor.fetchall()
        for schema, table, fk_name in fks:
            try:
                cursor.execute(f"ALTER TABLE [{schema}].[{table}] DROP CONSTRAINT [{fk_name}];")
                print(f"  [OK] Dropped constraint: [{schema}].[{table}].[{fk_name}]")
            except Exception as e:
                print(f"  [WARNING] Could not drop constraint {fk_name}: {e}")
        conn.commit()

        # 2. Drop in reverse dependency order
        drop_list = sorted(tables_to_process, key=lambda t: TABLE_SCHEMAS[t]["drop_order"])
        for table in drop_list:
            check_sql = f"IF OBJECT_ID(N'dbo.{table}', N'U') IS NOT NULL DROP TABLE dbo.{table};"
            cursor.execute(check_sql)
            print(f"  [OK] Dropped table (if existed): {table}")
        conn.commit()

    print("\n--- Creating New Tables ---")
    # Create in forward dependency order
    create_list = sorted(tables_to_process, key=lambda t: TABLE_SCHEMAS[t]["load_order"])
    for table in create_list:
        spec = TABLE_SCHEMAS[table]
        cursor.execute(f"SELECT COUNT(*) FROM sys.tables WHERE name = '{table}'")
        exists = cursor.fetchone()[0] > 0
        if not exists:
            cursor.execute(spec["create_sql"])
            print(f"  [OK] Created table: {table}")
        else:
            print(f"  [INFO] Table already exists: {table} (skipping creation)")
    conn.commit()


def load_table(conn, table_name, spec, batch_size):
    """Load a single flat file into its corresponding SQL Server table using fast_executemany."""
    file_path = BASE_DIR / spec["file"]
    if not file_path.exists():
        print(f"[ERROR] File not found: {file_path}")
        return 0, 0, False

    print(f"\nProcessing '{table_name}' from {spec['file']}...")
    print("  Counting rows in CSV...")
    total_rows = count_file_rows(file_path)
    print(f"  Total CSV rows: {total_rows:,}")

    cursor = conn.cursor()
    cursor.fast_executemany = True

    parser = spec["parser"]
    insert_sql = spec["insert_sql"]

    start_time = time.time()
    rows_inserted = 0

    def row_generator():
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            for row in reader:
                yield parser(row)

    # Temporarily disable constraints during bulk insert for maximum throughput
    try:
        cursor.execute(f"ALTER TABLE dbo.{table_name} NOCHECK CONSTRAINT ALL;")
        conn.commit()
    except Exception:
        pass

    print(f"  Streaming batches (batch size: {batch_size:,})...")
    for chunk in chunked_iterable(row_generator(), batch_size):
        cursor.executemany(insert_sql, chunk)
        conn.commit()
        rows_inserted += len(chunk)

        elapsed = time.time() - start_time
        speed = rows_inserted / elapsed if elapsed > 0 else 0
        percent = (rows_inserted / total_rows * 100) if total_rows > 0 else 100
        eta = (total_rows - rows_inserted) / speed if speed > 0 else 0

        # Live terminal progress indicator
        sys.stdout.write(
            f"\r  Progress: {rows_inserted:,}/{total_rows:,} ({percent:5.1f}%) | "
            f"Rate: {speed:,.0f} rows/s | ETA: {eta:4.1f}s"
        )
        sys.stdout.flush()

    total_time = time.time() - start_time
    avg_speed = rows_inserted / total_time if total_time > 0 else 0
    print(f"\n  [DONE] Completed in {total_time:.2f}s (Average: {avg_speed:,.0f} rows/s)")

    # Re-enable and validate all constraints
    try:
        cursor.execute(f"ALTER TABLE dbo.{table_name} WITH CHECK CHECK CONSTRAINT ALL;")
        conn.commit()
    except Exception as e:
        print(f"  [WARNING] Could not re-enable constraints on {table_name}: {e}")

    # Verification: query actual count from SQL Server
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
    db_count = cursor.fetchone()[0]

    status = (db_count == total_rows)
    print(f"  Verification: CSV = {total_rows:,} | SQL Server = {db_count:,} | Match: {'YES' if status else 'NO'}")

    return rows_inserted, total_time, status


def apply_constraints_and_indexes(conn):
    """Apply foreign keys and non-clustered performance indexes after bulk load."""
    print("\n--- Applying Foreign Keys & Performance Indexes ---")
    cursor = conn.cursor()
    for name, sql in POST_LOAD_CONSTRAINTS:
        try:
            t0 = time.time()
            cursor.execute(sql)
            conn.commit()
            print(f"  [OK] Applied: {name} ({time.time() - t0:.2f}s)")
        except Exception as e:
            print(f"  [WARNING] Could not apply {name}: {e}")
            conn.rollback()


def main():
    parser = argparse.ArgumentParser(description="Load 4 CSV flat files into SQL Server.")
    parser.add_argument("--test-connection", action="store_true", help="Test database connection and exit.")
    parser.add_argument("--tables", nargs="+", help="Specific tables to load (categories, customers, orders, order_items)")
    parser.add_argument("--no-drop", action="store_true", help="Do not drop existing tables.")
    parser.add_argument("--no-constraints", action="store_true", help="Do not add foreign keys/indexes after load.")
    parser.add_argument("--batch-size", type=int, help="Override batch size for bulk inserts.")
    args = parser.parse_args()

    if args.test_connection:
        success = test_connection()
        sys.exit(0 if success else 1)

    config = get_config()
    if args.no_drop:
        config["drop_existing"] = False
    if args.no_constraints:
        config["add_constraints"] = False
    if args.batch_size:
        config["batch_size"] = args.batch_size

    # Check database setting
    if not config["database"] or config["database"] == "YourDatabaseName":
        print("\n" + "=" * 65)
        print("[CONFIGURATION NEEDED]")
        print("Please configure your SQL Server details in '.env':")
        print(f"File location: {ENV_PATH}")
        print("Required fields:")
        print("  - DB_SERVER (e.g. localhost or instance name)")
        print("  - DB_NAME (your database name)")
        print("  - DB_USER and DB_PASSWORD (or DB_TRUSTED_CONNECTION=yes)")
        print("=" * 65 + "\n")
        sys.exit(1)

    print("=" * 65)
    print("ETL Flat Files to SQL Server Loader")
    print("=" * 65)
    print(f"Database:   {config['database']} on {config['server']}")
    print(f"Batch Size: {config['batch_size']:,}")
    print(f"Drop First: {'Yes' if config['drop_existing'] else 'No'}")
    print(f"Add FK/Idx: {'Yes' if config['add_constraints'] else 'No'}")
    print("=" * 65)

    try:
        conn = get_connection(config)
    except Exception as e:
        print(f"\n[ERROR] Connection failed: {e}")
        print("Run with --test-connection to inspect details.")
        sys.exit(1)

    selected = args.tables or ["categories", "customers", "orders", "order_items"]
    invalid = [t for t in selected if t not in TABLE_SCHEMAS]
    if invalid:
        print(f"[ERROR] Invalid table names: {invalid}. Options: {list(TABLE_SCHEMAS.keys())}")
        sys.exit(1)

    overall_start = time.time()

    try:
        # Step 1: Drop & Create Tables
        drop_and_create_tables(conn, config, selected)

        # Step 2: Load Data in Dependency Order
        load_list = sorted(selected, key=lambda t: TABLE_SCHEMAS[t]["load_order"])
        summary = []

        for table in load_list:
            spec = TABLE_SCHEMAS[table]
            rows, duration, match = load_table(conn, table, spec, config["batch_size"])
            summary.append({
                "table": table,
                "rows": rows,
                "duration": duration,
                "match": match
            })

        # Step 3: Apply Foreign Keys & Indexes
        if config["add_constraints"] and set(selected) == set(TABLE_SCHEMAS.keys()):
            apply_constraints_and_indexes(conn)

        total_elapsed = time.time() - overall_start
        total_rows_loaded = sum(s["rows"] for s in summary)

        print("\n" + "=" * 65)
        print("ETL EXECUTION SUMMARY")
        print("=" * 65)
        print(f"{'Table':<15} {'Rows Loaded':<15} {'Time':<10} {'Status':<10}")
        print("-" * 65)
        for s in summary:
            status_str = "SUCCESS" if s["match"] else "MISMATCH"
            print(f"{s['table']:<15} {s['rows']:<15,} {s['duration']:<9.2f}s {status_str:<10}")
        print("-" * 65)
        print(f"Total Rows:     {total_rows_loaded:,}")
        print(f"Total Time:     {total_elapsed:.2f}s")
        if total_elapsed > 0:
            print(f"Overall Rate:   {total_rows_loaded / total_elapsed:,.0f} rows/s")
        print("=" * 65)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
