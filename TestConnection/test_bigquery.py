import os
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import init_environment, print_header, print_success, print_error, print_info, print_warning

def test_bigquery_connection() -> bool:
    """Test connectivity to Google Cloud BigQuery."""
    init_environment()
    print_header("Testing Google Cloud BigQuery Connection")

    project_id = os.getenv("GCP_PROJECT_ID")
    target_dataset = os.getenv("BIGQUERY_DATASET")
    bronze_dataset = os.getenv("BIGQUERY_BRONZE_DATASET")
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    print_info(f"Project ID: {project_id}")
    print_info(f"Credentials File: {creds_path}")
    print_info(f"Configured Dataset: {target_dataset}")

    if not project_id:
        print_error("GCP_PROJECT_ID is not configured in .env.")
        return False

    try:
        from google.cloud import bigquery
        from google.cloud.exceptions import NotFound, Forbidden

        client = bigquery.Client(project=project_id)

        # Test 1: Execute a lightweight query
        print_info("Executing validation query: SELECT CURRENT_TIMESTAMP(), SESSION_USER()...")
        query = "SELECT CURRENT_TIMESTAMP() AS server_time, SESSION_USER() AS session_user"
        query_job = client.query(query)
        results = list(query_job.result())

        for row in results:
            print_success(f"Query succeeded! Server Time: {row['server_time']} | Authenticated as: {row['session_user']}")

        # Test 2: Check configured datasets
        datasets_to_check = [d for d in [target_dataset, bronze_dataset] if d]
        for ds_id in datasets_to_check:
            try:
                dataset = client.get_dataset(ds_id)
                print_success(f"Dataset '{ds_id}' verified (Location: {dataset.location}, Created: {dataset.created})")
            except NotFound:
                print_warning(f"Dataset '{ds_id}' does not exist yet in project '{project_id}'.")
            except Forbidden as e:
                print_warning(f"Access to dataset '{ds_id}' restricted: {e}")

        # Test 3: List accessible datasets
        print_info("Listing accessible datasets in project...")
        datasets = list(client.list_datasets(max_results=5))
        if datasets:
            print_success(f"Accessible datasets ({len(datasets)}): {[d.dataset_id for d in datasets]}")
        else:
            print_info("No existing datasets found in this project.")

        print_success("Google Cloud BigQuery connection test PASSED.")
        return True

    except Exception as e:
        print_error(f"BigQuery connection test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_bigquery_connection()
    sys.exit(0 if success else 1)
