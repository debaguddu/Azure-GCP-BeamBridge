import os
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import init_environment, print_header, print_success, print_error, print_info, print_warning

def test_kms_connection() -> bool:
    """Test connectivity to Google Cloud Key Management Service (KMS)."""
    init_environment()
    print_header("Testing GCP Cloud KMS Connection")

    project_id = os.getenv("GCP_PROJECT_ID")
    region = os.getenv("GCP_REGION", "asia-south1")
    kms_key_id = os.getenv("GCP_KMS_KEY_ID")
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    print_info(f"Project ID: {project_id}")
    print_info(f"Region: {region}")
    print_info(f"Credentials File: {creds_path}")
    print_info(f"Target KMS Key: {kms_key_id}")

    if not project_id:
        print_error("GCP_PROJECT_ID is not configured in .env.")
        return False

    try:
        from google.cloud import kms_v1
        from google.api_core.exceptions import NotFound, PermissionDenied

        client = kms_v1.KeyManagementServiceClient()
        parent_location = f"projects/{project_id}/locations/{region}"

        # Test 1: List KeyRings in the location
        print_info(f"Listing KeyRings in '{parent_location}'...")
        try:
            keyrings = list(client.list_key_rings(request={"parent": parent_location}))
            keyring_names = [kr.name.split("/")[-1] for kr in keyrings]
            print_success(f"Cloud KMS API reachable! KeyRings found: {keyring_names if keyring_names else 'None'}")
        except PermissionDenied as e:
            print_warning(f"Permission denied listing KeyRings: {e}")

        # Test 2: Check target crypto key
        if kms_key_id:
            print_info(f"Checking CryptoKey: '{kms_key_id}'...")
            try:
                crypto_key = client.get_crypto_key(request={"name": kms_key_id})
                purpose_name = kms_v1.CryptoKey.CryptoKeyPurpose(crypto_key.purpose).name
                print_success(f"CryptoKey verified: {crypto_key.name.split('/')[-1]}")
                print_info(f"Key Purpose: {purpose_name}")
                if crypto_key.primary:
                    state_name = kms_v1.CryptoKeyVersion.CryptoKeyVersionState(crypto_key.primary.state).name
                    print_info(f"Primary Version State: {state_name}")
                    print_info(f"Algorithm: {crypto_key.primary.algorithm.name}")
            except NotFound:
                print_warning(f"KMS Key '{kms_key_id}' was not found. Key can be created via Terraform.")
            except PermissionDenied as e:
                print_warning(f"Permission denied accessing key '{kms_key_id}': {e}")

        print_success("GCP Cloud KMS connection test PASSED.")
        return True

    except Exception as e:
        print_error(f"Cloud KMS connection test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_kms_connection()
    sys.exit(0 if success else 1)
