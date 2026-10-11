import os
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import init_environment, print_header, print_success, print_error, print_info, print_warning

def test_gcs_connection() -> bool:
    """Test connectivity to Google Cloud Storage (GCS)."""
    init_environment()
    print_header("Testing Google Cloud Storage (GCS) Connection")

    project_id = os.getenv("GCP_PROJECT_ID")
    landing_bucket_name = os.getenv("GCS_LANDING_BUCKET")
    base_path = os.getenv("GCS_BASE_PATH", "")
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    print_info(f"Project ID: {project_id}")
    print_info(f"Credentials File: {creds_path}")
    print_info(f"Target Landing Bucket: {landing_bucket_name}")

    if not project_id:
        print_error("GCP_PROJECT_ID is not set in environment or .env file.")
        return False

    try:
        from google.cloud import storage
        from google.cloud.exceptions import NotFound, Forbidden

        client = storage.Client(project=project_id)

        # Test 1: Verify landing bucket access
        if landing_bucket_name:
            try:
                bucket = client.get_bucket(landing_bucket_name)
                print_success(f"Successfully connected to bucket '{landing_bucket_name}'!")
                print_info(f"Bucket Location: {bucket.location}")
                print_info(f"Storage Class: {bucket.storage_class}")
                print_info(f"Created Time: {bucket.time_created}")

                # Test listing objects with prefix
                prefix = f"{base_path}/" if base_path and not base_path.endswith("/") else base_path
                blobs = list(client.list_blobs(bucket, prefix=prefix, max_results=5))
                print_info(f"Sample objects in '{landing_bucket_name}/{prefix}': {len(blobs)} found")
                for blob in blobs:
                    print_info(f"  - {blob.name} ({blob.size} bytes)")

            except NotFound:
                print_warning(f"Bucket '{landing_bucket_name}' was not found in project '{project_id}'.")
                print_info("Listing available accessible buckets in project...")
                buckets = list(client.list_buckets(max_results=5))
                if buckets:
                    print_success(f"Accessible buckets ({len(buckets)}): {[b.name for b in buckets]}")
                else:
                    print_info("No other buckets found or accessible.")
            except Forbidden as e:
                print_error(f"Permission denied accessing bucket '{landing_bucket_name}': {e}")
                return False
        else:
            # Fallback: List available buckets
            print_info("GCS_LANDING_BUCKET not specified. Listing buckets in project...")
            buckets = list(client.list_buckets(max_results=5))
            print_success(f"GCS client connected. Found buckets: {[b.name for b in buckets]}")

        print_success("Google Cloud Storage connection test PASSED.")
        return True

    except Exception as e:
        print_error(f"Google Cloud Storage connection failed: {e}")
        return False

if __name__ == "__main__":
    success = test_gcs_connection()
    sys.exit(0 if success else 1)
