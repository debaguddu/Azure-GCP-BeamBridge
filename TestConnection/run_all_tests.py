import sys
import time
from pathlib import Path

# Add current folder to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_gcs import test_gcs_connection
from test_bigquery import test_bigquery_connection
from test_pubsub import test_pubsub_connection
from test_asql import test_asql_connection
from test_postgres import test_postgres_connection
from test_secret_manager import test_secret_manager_connection
from test_kms import test_kms_connection

def main():
    print("=" * 75)
    print("   GCP BEAMBRIDGE - COMPREHENSIVE INFRASTRUCTURE CONNECTIVITY SUITE")
    print("=" * 75)

    tests = [
        ("Google Cloud Storage (GCS)", test_gcs_connection),
        ("Google Cloud BigQuery", test_bigquery_connection),
        ("Google Cloud Pub/Sub", test_pubsub_connection),
        ("Azure SQL Server (ASQL)", test_asql_connection),
        ("PostgreSQL Database", test_postgres_connection),
        ("GCP Secret Manager", test_secret_manager_connection),
        ("GCP Cloud KMS", test_kms_connection),
    ]

    results = []

    for name, test_func in tests:
        start_time = time.time()
        try:
            passed = test_func()
        except Exception as e:
            print(f"[ERROR] Unexpected exception in {name}: {e}")
            passed = False
        duration = time.time() - start_time
        results.append((name, passed, duration))

    # Print Summary Table
    print("\n" + "=" * 75)
    print("                        TEST RESULTS SUMMARY")
    print("=" * 75)
    print(f"{'Service / Component':<35} | {'Status':<12} | {'Duration (s)':<12}")
    print("-" * 75)

    total_passed = 0
    for name, passed, duration in results:
        status_text = "PASSED [OK]" if passed else "FAILED [X]"
        print(f"{name:<35} | {status_text:<12} | {duration:>10.2f}s")
        if passed:
            total_passed += 1

    print("-" * 75)
    print(f"Total: {len(tests)} | Passed: {total_passed} | Failed: {len(tests) - total_passed}")
    print("=" * 75 + "\n")

    return 0 if total_passed == len(tests) else 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
