import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Determine Project Root Directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / ".env"

def init_environment():
    """Load .env file and set absolute path for GOOGLE_APPLICATION_CREDENTIALS."""
    if ENV_PATH.exists():
        load_dotenv(dotenv_path=ENV_PATH, override=True)
    else:
        print(f"[!] Warning: .env file not found at {ENV_PATH}")

    # Ensure GOOGLE_APPLICATION_CREDENTIALS points to an absolute path
    cred_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_file and not os.path.isabs(cred_file):
        abs_cred_file = (PROJECT_ROOT / cred_file).resolve()
        if abs_cred_file.exists():
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(abs_cred_file)

# Console Output Formatting
def print_header(title: str):
    print("\n" + "=" * 70)
    print(f"  {title.upper()}")
    print("=" * 70)

def print_success(message: str):
    print(f"[SUCCESS] {message}")

def print_error(message: str):
    print(f"[FAILED]  {message}")

def print_info(message: str):
    print(f"[INFO]    {message}")

def print_warning(message: str):
    print(f"[WARNING] {message}")
