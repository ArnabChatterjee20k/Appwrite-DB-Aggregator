"""
# Pull the full Appwrite project state (databases, collections, attributes, documents, functions, buckets, files)
python migration_validator.py --pull --output prod_snapshot.json

# Compare two pulled project states and print migration diff
python migration_validator.py --compare --source prod_snapshot.json --destination staging_snapshot.json

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

# Load .env
load_dotenv()
ENDPOINT = os.getenv("APPWRITE_ENDPOINT")
PROJECT_ID = os.getenv("APPWRITE_PROJECT_ID")
API_KEY = os.getenv("APPWRITE_API_KEY")

# Appwrite Setup
client = Client()
client.set_endpoint(ENDPOINT).set_project(PROJECT_ID).set_key(API_KEY)

databases = Databases(client)
functions = Functions(client)
storage = Storage(client)

def fetch_all_documents(db_id, col_id):
    all_docs = []
    limit = 100
    offset = 0
    allowed_keys = {"$id", "$sequence"}

    while True:
        try:
            result = databases.list_documents(
                database_id=db_id,
                collection_id=col_id,
                queries=[Query.limit(limit), Query.offset(offset),Query.order_desc("")]
            )
            docs = result.get("documents", [])
            if not docs:
                break

            # Append only schema fields (excluding Appwrite system keys like $id)
            all_docs.extend([
                {k: v for k, v in doc.items() if not k.startswith('$') or k in allowed_keys}
                for doc in docs
            ])
            offset += limit
        except Exception as e:
            print(f"⚠️ Failed to fetch documents from {col_id}: {e}")
            break

    return all_docs


def pull_full_project_state():
    project = {
        "databases": {},
        "functions": [],
        "storage": {
            "buckets": {}
        }
    }

    # Databases
    try:
        dbs = databases.list()["databases"]
        for db in dbs:
            db_id = db["$id"]
            db_data = {"name": db["name"], "collections": {}}
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
                    print(f"⚠️ Couldn't fetch attributes for {col_id}: {e}")

                try:
                    col_data["documents"] = fetch_all_documents(db_id, col_id)
                except Exception as e:
                    print(f"⚠️ Couldn't fetch documents for {col_id}: {e}")

                db_data["collections"][col_id] = col_data

            project["databases"][db_id] = db_data
    except Exception as e:
        print(f"❌ Error fetching databases: {e}")

    # Functions
    try:
        funcs = functions.list()["functions"]
        project["functions"] = [{"$id": f["$id"], "name": f["name"], "runtime": f["runtime"]} for f in funcs]
    except Exception as e:
        print(f"⚠️ Couldn't fetch functions: {e}")

    # Storage Buckets
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
            except Exception as fe:
                print(f"⚠️ Couldn't fetch files in bucket {bucket_id}: {fe}")

            project["storage"]["buckets"][bucket_id] = bucket_data
    except Exception as e:
        print(f"⚠️ Couldn't fetch storage buckets: {e}")

    return project


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
    args = parser.parse_args()

    if args.pull:
        state = pull_full_project_state()
        save_to_file(state, args.output)
        print(f"📄 Full project state saved to `{args.output}`")

    if args.compare and args.source and args.destination:
        src = load_from_file(args.source)
        dest = load_from_file(args.destination)
        diff = compare_project_states(src, dest)
        print("🧾 Migration Comparison Result:")
        print(diff)
