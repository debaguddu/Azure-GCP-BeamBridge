import os
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import init_environment, print_header, print_success, print_error, print_info, print_warning

def test_pubsub_connection() -> bool:
    """Test connectivity to Google Cloud Pub/Sub."""
    init_environment()
    print_header("Testing Google Cloud Pub/Sub Connection")

    project_id = os.getenv("GCP_PROJECT_ID")
    ingest_topic = os.getenv("PUBSUB_ORDERS_INGEST_TOPIC")
    dlq_topic = os.getenv("PUBSUB_DLQ_ALERTS_TOPIC")
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    print_info(f"Project ID: {project_id}")
    print_info(f"Credentials File: {creds_path}")
    print_info(f"Ingest Topic Config: {ingest_topic}")

    if not project_id:
        print_error("GCP_PROJECT_ID is not configured in .env.")
        return False

    try:
        from google.cloud import pubsub_v1
        from google.api_core.exceptions import NotFound, PermissionDenied

        publisher = pubsub_v1.PublisherClient()
        subscriber = pubsub_v1.SubscriberClient()
        project_path = f"projects/{project_id}"

        # Test 1: List existing topics in project
        print_info(f"Listing topics in project '{project_path}'...")
        try:
            topics_page = publisher.list_topics(request={"project": project_path})
            topics_list = []
            for i, topic in enumerate(topics_page):
                topics_list.append(topic.name.split("/")[-1])
                if i >= 4:
                    break
            print_success(f"Pub/Sub API reachable! Found topics: {topics_list if topics_list else 'None'}")
        except PermissionDenied as e:
            print_warning(f"Permission denied listing topics: {e}")

        # Test 2: Check configured topics
        topics_to_check = [t for t in [ingest_topic, dlq_topic] if t]
        for topic_entry in topics_to_check:
            # Format full topic path if needed
            if topic_entry.startswith("projects/"):
                topic_path = topic_entry
            else:
                topic_path = publisher.topic_path(project_id, topic_entry)

            try:
                topic_obj = publisher.get_topic(request={"topic": topic_path})
                print_success(f"Topic verified: {topic_obj.name}")
            except NotFound:
                print_warning(f"Topic not found: {topic_path} (Topic can be created via Terraform/Console)")
            except PermissionDenied as e:
                print_warning(f"Permission denied accessing topic '{topic_path}': {e}")

        # Test 3: List subscriptions
        try:
            subs_page = subscriber.list_subscriptions(request={"project": project_path})
            subs_list = []
            for i, sub in enumerate(subs_page):
                subs_list.append(sub.name.split("/")[-1])
                if i >= 4:
                    break
            print_info(f"Existing subscriptions: {subs_list if subs_list else 'None'}")
        except Exception:
            pass

        print_success("Google Cloud Pub/Sub connection test PASSED.")
        return True

    except Exception as e:
        print_error(f"Google Cloud Pub/Sub connection test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_pubsub_connection()
    sys.exit(0 if success else 1)
