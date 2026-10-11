import os
import sys
import json
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import init_environment, print_header, print_success, print_error, print_info, print_warning

def test_secret_manager_connection() -> bool:
    """Test connectivity to Google Cloud Secret Manager."""
    init_environment()
    print_header("Testing GCP Secret Manager Connection")

    project_id = os.getenv("GCP_PROJECT_ID")
    secret_resource_name = os.getenv("GCP_SECRET_AZURE_SQL")
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    print_info(f"Project ID: {project_id}")
    print_info(f"Credentials File: {creds_path}")
    print_info(f"Target Secret: {secret_resource_name}")

    if not project_id:
        print_error("GCP_PROJECT_ID is not configured in .env.")
        return False

    try:
        from google.cloud import secretmanager
        from google.api_core.exceptions import NotFound, PermissionDenied

        client = secretmanager.SecretManagerServiceClient()
        parent = f"projects/{project_id}"

        # Test 1: List secrets in the project
        print_info(f"Listing secrets in project '{parent}'...")
        try:
            secrets_list = []
            for i, secret in enumerate(client.list_secrets(request={"parent": parent})):
                secret_id = secret.name.split("/")[-1]
                secrets_list.append(secret_id)
                if i >= 4:
                    break
            print_success(f"Secret Manager API accessible! Secrets found: {secrets_list if secrets_list else 'None'}")
        except PermissionDenied as e:
            print_warning(f"Permission denied listing secrets: {e}")

        # Test 2: Access target secret payload if configured
        if secret_resource_name:
            print_info(f"Accessing secret version: '{secret_resource_name}'...")
            try:
                response = client.access_secret_version(request={"name": secret_resource_name})
                payload_str = response.payload.data.decode("UTF-8")
                print_success(f"Successfully retrieved payload for '{secret_resource_name}'!")

                # Validate payload format safely without exposing secrets
                try:
                    payload_json = json.loads(payload_str)
                    masked_keys = list(payload_json.keys())
                    print_info(f"Secret format is valid JSON. Detected keys: {masked_keys}")
                    if "server" in payload_json:
                        print_info(f"  Server defined: {payload_json['server']}")
                    if "database" in payload_json:
                        print_info(f"  Database defined: {payload_json['database']}")
                    if "username" in payload_json:
                        print_info(f"  Username defined: {payload_json['username']}")
                except json.JSONDecodeError:
                    print_info("Secret payload is plain text string (length: {len(payload_str)} characters).")

            except NotFound:
                print_warning(f"Secret version '{secret_resource_name}' was not found.")
            except PermissionDenied as e:
                print_error(f"Permission denied accessing secret '{secret_resource_name}': {e}")
                return False

        print_success("GCP Secret Manager connection test PASSED.")
        return True

    except Exception as e:
        print_error(f"Secret Manager test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_secret_manager_connection()
    sys.exit(0 if success else 1)
