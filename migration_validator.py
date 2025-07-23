"""
# Pull the full Appwrite project state (databases, collections, attributes, documents, functions, buckets, files)
python migration_validator.py --pull --output prod_snapshot.json

# Compare two pulled project states and print migration diff
python migration_validator.py --compare --source prod_snapshot.json --destination staging_snapshot.json

# With resume and checkpointing logic to resume from in between
python migration_validator.py --pull --output des_snapshot.json --resume

# seedin appwrite from the json
python migration_validator.py --seed prod_snapshot.json
"""
import os
import time
import json
import argparse
from dotenv import load_dotenv
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.services.functions import Functions
from appwrite.services.storage import Storage
from deepdiff import DeepDiff
from appwrite.query import Query
import pickle

# Load .env
load_dotenv()
ENDPOINT = "https://fra.cloud.appwrite.io/v1"
PROJECT_ID = "685e9d7e000715a6e67f"
API_KEY = "standard_326fc4ccf37f4e2cfe08f3b2de0de604851ea6ce841695c95970c091ff49be07214b7887967400207417716f59ca8ad0e2363b73e295f52f95afef66636bf9f64b3f1f70ee399130808716ace33fab37f93f007b1c1273adc2dd05aa2701e4609c41fc28e1ef0e51aade4cbc8ce7d780d42b978bb6cd60f00fa4c127737d7f28"

# Appwrite Setup
client = Client()
client.set_endpoint(ENDPOINT).set_project(PROJECT_ID).set_key(API_KEY)

databases = Databases(client)
functions = Functions(client)
storage = Storage(client)

def fetch_all_documents(db_id, col_id, resume=False, checkpoint_dir="checkpoints", logs=None):
    if logs is None:
        logs = []
    all_docs = []
    limit = 100
    offset = 0
    allowed_keys = {"$id", "$sequence"}
    checkpoint_file = os.path.join(checkpoint_dir, f"checkpoint_{db_id}_{col_id}.pkl")
    completed = False

    # Resume logic
    if resume and os.path.exists(checkpoint_file):
        with open(checkpoint_file, "rb") as f:
            checkpoint = pickle.load(f)
            all_docs = checkpoint.get("all_docs", [])
            offset = checkpoint.get("offset", 0)
            logs = checkpoint.get("logs", logs)
            completed = checkpoint.get("completed", False)
        logs.append(f"[RESUME] Resuming {db_id}/{col_id} from offset {offset}")
    else:
        logs.append(f"{db_id}/{col_id} started")

    os.makedirs(checkpoint_dir, exist_ok=True)

    while True:
        try:
            result = databases.list_documents(
                database_id=db_id,
                collection_id=col_id,
                queries=[Query.limit(limit), Query.offset(offset), Query.order_desc("")]
            )
            docs = result.get("documents", [])
            if not docs:
                completed = True
                logs.append(f"{db_id}/{col_id} ended")
                break

            # Append only schema fields (excluding Appwrite system keys like $id)
            all_docs.extend([
                {k: v for k, v in doc.items() if not k.startswith('$') or k in allowed_keys}
                for doc in docs
            ])
            offset += limit
            logs.append(f"{db_id}/{col_id}: {offset} docs done")

            # Save checkpoint every 100 docs
            with open(checkpoint_file, "wb") as f:
                pickle.dump({
                    "all_docs": all_docs,
                    "offset": offset,
                    "logs": logs,
                    "completed": False
                }, f)
        except Exception as e:
            logs.append(f"⚠️ Failed to fetch documents from {col_id}: {e}")
            break

    # Final checkpoint with completion status
    with open(checkpoint_file, "wb") as f:
        pickle.dump({
            "all_docs": all_docs,
            "offset": offset,
            "logs": logs,
            "completed": completed
        }, f)
    return all_docs, logs, completed


def pull_full_project_state(resume=False, checkpoint_dir="checkpoints"):
    project = {
        "databases": {},
        "functions": [],
        "storage": {
            "buckets": {}
        },
        "completed": False
    }
    completed_resources = []
    logs = []  # logs are now only in memory, not in project dict

    # Databases
    try:
        dbs = databases.list()["databases"]
        for db in dbs:
            db_id = db["$id"]
            db_data = {"name": db["name"], "collections": {}}
            logs.append(f"Database {db['name']} started")
            collections = databases.list_collections(database_id=db_id)["collections"]
            for col in collections:
                col_id = col["$id"]
                col_data = {
                    "name": col["name"],
                    "attributes": [],
                    "documents": []
                }
                try:
                    attr = databases.list_attributes(database_id=db_id, collection_id=col_id)
                    col_data["attributes"] = attr.get("attributes", [])
                except Exception as e:
                    logs.append(f"⚠️ Couldn't fetch attributes for {col_id}: {e}")
                try:
                    docs, doc_logs, completed = fetch_all_documents(db_id, col_id, resume=resume, checkpoint_dir=checkpoint_dir)
                    col_data["documents"] = docs
                    logs.extend(doc_logs)
                    if completed:
                        completed_resources.append(f"{db['name']}::{col['name']}")
                except Exception as e:
                    logs.append(f"⚠️ Couldn't fetch documents for {col_id}: {e}")
                db_data["collections"][col_id] = col_data
                logs.append(f"Collection {col['name']} ended")
            project["databases"][db_id] = db_data
            logs.append(f"Database {db['name']} ended")
    except Exception as e:
        logs.append(f"❌ Error fetching databases: {e}")

    # Functions
    logs.append("Functions started")
    try:
        funcs = functions.list()["functions"]
        project["functions"] = [{"$id": f["$id"], "name": f["name"], "runtime": f["runtime"]} for f in funcs]
        logs.append("Functions ended")
        completed_resources.append("functions")
    except Exception as e:
        logs.append(f"⚠️ Couldn't fetch functions: {e}")

    # Storage Buckets
    logs.append("Storage started")
    try:
        buckets = storage.list_buckets()["buckets"]
        for bucket in buckets:
            bucket_id = bucket["$id"]
            bucket_data = {
                "name": bucket["name"],
                "files": []
            }
            try:
                files = storage.list_files(bucket_id=bucket_id)["files"]
                bucket_data["files"] = [
                    {"$id": f["$id"], "name": f["name"], "sizeOriginal": f["sizeOriginal"]} for f in files
                ]
                completed_resources.append(f"bucket::{bucket['name']}")
            except Exception as fe:
                logs.append(f"⚠️ Couldn't fetch files in bucket {bucket_id}: {fe}")
            project["storage"]["buckets"][bucket_id] = bucket_data
        logs.append("Storage ended")
    except Exception as e:
        logs.append(f"⚠️ Couldn't fetch storage buckets: {e}")

    # Mark completion
    project["completed"] = True
    project["completed_resources"] = completed_resources
    return project, logs

def wait_for_collection_ready(db_id, col_id, timeout=10):
    """Wait until all attributes in the collection are indexed."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            collection = databases.get_collection(database_id=db_id, collection_id=col_id)
            if collection["status"] == "available":
                return True
        except Exception:
            pass
        time.sleep(0.5)
    print(f"⚠️ Timeout while waiting for collection {col_id} to become available")
    return False


def seed_from_snapshot(snapshot_path):
    with open(snapshot_path, "r") as f:
        snapshot = json.load(f)

    created_resources = []

    # Step 1: Databases and collections
    for db_id, db_data in snapshot.get("databases", {}).items():
        try:
            databases.create(database_id=db_id, name=db_data["name"])
            created_resources.append(f"Database: {db_data['name']} ({db_id})")
        except Exception as e:
            print(f"⚠️ Failed to create database {db_id}: {e}")

        for col_id, col_data in db_data.get("collections", {}).items():
            try:
                databases.create_collection(
                    database_id=db_id,
                    collection_id=col_id,
                    name=col_data["name"]
                )
                created_resources.append(f"Collection: {col_data['name']} ({col_id})")
            except Exception as e:
                print(f"⚠️ Failed to create collection {col_id}: {e}")

    # Step 2: Attributes
    for db_id, db_data in snapshot.get("databases", {}).items():
        for col_id, col_data in db_data.get("collections", {}).items():
            for attr in col_data.get("attributes", []):
                attr_type = attr["type"]
                attr_id = attr["key"]
                attr_required = attr.get("required", False)
                attr_default = attr.get("default")
                try:
                    if attr_type == "string":
                        databases.create_string_attribute(
                            database_id=db_id,
                            collection_id=col_id,
                            key=attr_id,
                            size=attr.get("size", 255),
                            required=attr_required,
                            default=attr_default,
                            array=attr.get("array", False)
                        )
                    elif attr_type == "integer":
                        databases.create_integer_attribute(
                            database_id=db_id,
                            collection_id=col_id,
                            key=attr_id,
                            required=attr_required,
                            default=attr_default,
                            min=attr.get("min"),
                            max=attr.get("max"),
                            array=attr.get("array", False)
                        )
                    elif attr_type == "float":
                        databases.create_float_attribute(
                            database_id=db_id,
                            collection_id=col_id,
                            key=attr_id,
                            required=attr_required,
                            default=attr_default,
                            min=attr.get("min"),
                            max=attr.get("max"),
                            array=attr.get("array", False)
                        )
                    elif attr_type == "boolean":
                        databases.create_boolean_attribute(
                            database_id=db_id,
                            collection_id=col_id,
                            key=attr_id,
                            required=attr_required,
                            default=attr_default,
                            array=attr.get("array", False)
                        )
                    elif attr_type == "email":
                        databases.create_email_attribute(
                            database_id=db_id,
                            collection_id=col_id,
                            key=attr_id,
                            required=attr_required,
                            default=attr_default,
                            array=attr.get("array", False)
                        )
                    elif attr_type == "url":
                        databases.create_url_attribute(
                            database_id=db_id,
                            collection_id=col_id,
                            key=attr_id,
                            required=attr_required,
                            default=attr_default,
                            array=attr.get("array", False)
                        )
                    elif attr_type == "ip":
                        databases.create_ip_attribute(
                            database_id=db_id,
                            collection_id=col_id,
                            key=attr_id,
                            required=attr_required,
                            default=attr_default,
                            array=attr.get("array", False)
                        )
                    elif attr_type == "enum":
                        databases.create_enum_attribute(
                            database_id=db_id,
                            collection_id=col_id,
                            key=attr_id,
                            elements=attr.get("elements", []),
                            required=attr_required,
                            default=attr_default,
                            array=attr.get("array", False)
                        )
                    elif attr_type == "datetime":
                        databases.create_datetime_attribute(
                            database_id=db_id,
                            collection_id=col_id,
                            key=attr_id,
                            required=attr_required,
                            default=attr_default,
                            array=attr.get("array", False)
                        )
                    elif attr_type == "relationship":
                        databases.create_relationship_attribute(
                            database_id=db_id,
                            collection_id=col_id,
                            related_collection_id=attr["relatedCollection"],
                            type=attr["relationType"],
                            key=attr_id,
                            two_way=attr.get("twoWay", False),
                            two_way_key=attr.get("twoWayKey"),
                            on_delete=attr.get("onDelete", "restrict")
                        )
                    else:
                        print(f"⚠️ Unknown attribute type {attr_type} for key {attr_id}")
                    created_resources.append(f"  └─ Attribute: {attr_id} ({attr_type})")
                except Exception as e:
                    print(f"⚠️ Failed to create attribute {attr_id} in {col_id}: {e}")

    # Step 3: Wait for all collections to become ready
    for db_id, db_data in snapshot.get("databases", {}).items():
        for col_id in db_data.get("collections", {}):
            wait_for_collection_ready(db_id, col_id)

    # Step 4: Documents
    for db_id, db_data in snapshot.get("databases", {}).items():
        for col_id, col_data in db_data.get("collections", {}).items():
            for doc in col_data.get("documents", []):
                try:
                    doc_id = doc.get("$id")
                    databases.create_document(
                        database_id=db_id,
                        collection_id=col_id,
                        document_id=doc_id,
                        data={k: v for k, v in doc.items() if k != "$id"}
                    )
                    created_resources.append(f"  └─ Document: {doc_id}")
                except Exception as e:
                    print(f"⚠️ Failed to create document {doc_id}: {e}")

    # Step 5: Functions
    if SEED_FUNCTIONS:
        for fn in snapshot.get("functions", []):
            try:
                functions.create(
                    function_id=fn["$id"],
                    name=fn["name"],
                    runtime=fn["runtime"]
                )
                created_resources.append(f"Function: {fn['name']}")
            except Exception as e:
                print(f"⚠️ Failed to create function {fn['name']}: {e}")

    # Step 6: Buckets (Files not handled)
    if SEED_STORAGE:
        for bucket_id, bucket in snapshot.get("storage", {}).get("buckets", {}).items():
            try:
                storage.create_bucket(bucket_id=bucket_id, name=bucket["name"])
                created_resources.append(f"Bucket: {bucket['name']}")
            except Exception as e:
                print(f"⚠️ Failed to create bucket {bucket['name']}: {e}")

    return created_resources

def compare_project_states(source, destination):
    diff = DeepDiff(source, destination, ignore_order=True)
    return diff if diff else "✅ Project states match!"


def save_to_file(data, path):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def load_from_file(path):
    with open(path, "r") as f:
        return json.load(f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Appwrite Migration Validator CLI")
    parser.add_argument("--pull", action="store_true", help="Pull full Appwrite project state")
    parser.add_argument("--output", type=str, default="project_state.json", help="Path to save pulled state")
    parser.add_argument("--compare", action="store_true", help="Compare two pulled project states")
    parser.add_argument("--source", type=str, help="Path to source JSON")
    parser.add_argument("--destination", type=str, help="Path to destination JSON")
    parser.add_argument("--resume", action="store_true", help="Resume from last checkpoint if available")
    parser.add_argument("--checkpoint_dir", type=str, default="checkpoints", help="Directory to store checkpoints")
    parser.add_argument("--seed", type=str, help="Seed local Appwrite project using snapshot JSON")

    args = parser.parse_args()

    if args.pull:
        try:
            state, logs = pull_full_project_state(resume=args.resume, checkpoint_dir=args.checkpoint_dir)
            save_to_file(state, args.output)
            print(f"📄 Full project state saved to `{args.output}`")
            print("\n--- LOGS ---")
            for log in logs:
                print(log)
            print("\n--- COMPLETED RESOURCES ---")
            for res in state.get("completed_resources", []):
                print(res)
            if not state.get("completed", False):
                print("\n⚠️ Migration not fully completed. Resume with --resume.")
        except Exception as e:
            # Save partial state with completed: False
            partial_state = locals().get('state', {"completed": False})
            partial_state["completed"] = False
            save_to_file(partial_state, args.output)
            print(f"❌ Error during pull: {e}")
            print("\n--- LOGS ---")
            logs = locals().get('logs', [])
            for log in logs:
                print(log)
            print("\n--- COMPLETED RESOURCES ---")
            for res in partial_state.get("completed_resources", []):
                print(res)
            print("\n⚠️ Migration not fully completed. Resume with --resume.")

    if args.compare and args.source and args.destination:
        src = load_from_file(args.source)
        dest = load_from_file(args.destination)
        diff = compare_project_states(src, dest)
        print("🧾 Migration Comparison Result:")
        print(diff)


    if args.seed:
        print(f"🌱 Seeding from snapshot: {args.seed}")
        created = seed_from_snapshot(args.seed)
        print("✅ Seeding complete. Resources created:")
        for item in created:
            print(item)