"""
# First time: initialize schema
python bulk_appwrite_tool.py --init-schema

# Generate and upload 1000 docs/collection
python bulk_appwrite_tool.py --generate

# Generate 500 posts only and save to file, no upload
python bulk_appwrite_tool.py --generate --count 500 --collections posts --dry-run --output posts.json

# Upload from file later and compare
python bulk_appwrite_tool.py --compare --collections posts --output posts.json

# Initialize only the 'csv' collection (if not exists) with all required attributes
python db_faker.py --init-csv-collection

# Create 'csv' collection (if not exists) and generate CSV for import with target size (default 10MB)
python db_faker.py --csv-with-a-size
python db_faker.py --csv-with-a-size --csv-size 5 --csv-output my_import.csv
"""
import os
import re
import time
import json
import random
import argparse
import csv
from faker import Faker
from dotenv import load_dotenv
from appwrite.client import Client
from appwrite.id import ID
from appwrite.services.databases import Databases
from appwrite.permission import Permission
from appwrite.role import Role
from deepdiff import DeepDiff

# Load environment
load_dotenv()
ENDPOINT = "https://fra.cloud.appwrite.io/v1"
PROJECT_ID = "693125a800378378faf0"
API_KEY = "standard_06782f4f6529ce57af946db0b6e45926a0befed8ee23fe5aa1f80b20c2adc2fa252d5ed4b106584f8c9da63938065388348a0f1b638d382c0f1ca962c3c1b2fd9e3e5462fac9d1c728f5ad195c66345382d35bb6eb03eedb71df0ef03a5fad741c22757ad7fc92d5a362af6240c46d774b881e06434942c505df4bcd55cb5358"
DATABASE_ID = os.getenv("APPWRITE_DATABASE_ID") or "auto-generated-db"

# Appwrite setup
client = Client()
client.set_endpoint(ENDPOINT).set_project(PROJECT_ID).set_key(API_KEY)
databases = Databases(client)
faker = Faker()

# Collection/Attribute Schema
COLLECTIONS = {
    "users": ["name", "email", "age", "username", "bio"],
    "products": ["name", "price", "in_stock", "brand", "category"],
    "posts": ["title", "content", "likes", "tags", "published"],
    "events": ["title", "location", "date", "organizer", "attendees"],
    "csv": [
        "id", "name", "email", "description",
        "extra_1", "extra_2", "extra_3", "extra_4", "extra_5", "extra_6",
        "extra_7", "extra_8",
    ],
}

# Field type mapping for schema generation
FIELD_TYPE_MAP = {
    "name": "string",
    "email": "email",
    "age": "integer",
    "username": "string",
    "bio": "string",
    "price": "float",
    "in_stock": "boolean",
    "brand": "string",
    "category": "string",
    "title": "string",
    "content": "string",
    "likes": "integer",
    "tags": "string",   # stored as CSV
    "published": "boolean",
    "location": "string",
    "date": "string",
    "organizer": "string",
    "attendees": "integer",
    "id": "string",
    "description": "string",
    **{f"extra_{i}": "string" for i in range(1, 9)},
}

# Field value generators
GENERATOR_MAP = {
    "name": faker.name,
    "email": faker.email,
    "age": lambda: random.randint(18, 65),
    "username": faker.user_name,
    "bio": faker.text,
    "price": lambda: round(random.uniform(10, 500), 2),
    "in_stock": lambda: random.choice([True, False]),
    "brand": faker.company,
    "category": lambda: random.choice(["Electronics", "Clothing", "Home", "Sports"]),
    "title": faker.sentence,
    "content": faker.paragraph,
    "likes": lambda: random.randint(0, 10000),
    "tags": lambda: ", ".join(faker.words(nb=random.randint(2, 5))),
    "published": lambda: random.choice([True, False]),
    "location": faker.city,
    "date": faker.date,
    "organizer": faker.company,
    "attendees": lambda: random.randint(0, 1000),
    "id": lambda: "",  # filled by row index in CSV generator
    "description": lambda: "This is a long description field to increase file size. " + faker.text(max_nb_chars=200),
    **{f"extra_{i}": faker.sentence for i in range(1, 9)},
}

CHUNK_SIZE = 100

def init_database():
    try:
        databases.create(database_id=DATABASE_ID, name="Auto Generated DB")
        print(f"📁 Created database `{DATABASE_ID}`")
    except Exception as e:
        print(f"ℹ️ Database may already exist: {e}")

def ensure_csv_collection():
    """Create database and 'csv' collection with many string attributes if they don't exist."""
    try:
        databases.create(database_id=DATABASE_ID, name="Auto Generated DB")
        print(f"📁 Created database `{DATABASE_ID}`")
    except Exception as e:
        print(f"ℹ️ Database may already exist: {e}")

    collection_id = "csv"
    fields = COLLECTIONS[collection_id]
    try:
        databases.create_collection(
            database_id=DATABASE_ID,
            collection_id=collection_id,
            name=collection_id,
            document_security=False,
            permissions=[
                Permission.read(Role.any()),
                Permission.update(Role.any()),
                Permission.delete(Role.any()),
            ],
        )
        print(f"📦 Created collection `{collection_id}`")
    except Exception as e:
        print(f"ℹ️ Collection `{collection_id}` may already exist: {e}")

    for field in fields:
        try:
            # All csv collection fields are string-like; use large size for description
            size = 4096 if field == "description" else 1024
            databases.create_string_attribute(
                database_id=DATABASE_ID,
                collection_id=collection_id,
                key=field,
                size=size,
                required=False,
            )
        except Exception as e:
            print(f"ℹ️ Attribute `{field}` may already exist: {e}")
    print(f"✅ Schema ready for collection `{collection_id}`")


def generate_csv_with_size(target_size_bytes, output_path="large_file.csv", seed=None):
    """Generate a CSV file with columns matching the 'csv' collection until file reaches target size."""
    if seed is not None:
        random.seed(seed)
        Faker.seed(seed)

    fields = COLLECTIONS["csv"]
    # Add _import prefix before file extension
    if output_path.endswith('.csv'):
        filename = output_path[:-4] + '_import.csv'
    else:
        filename = output_path + '_import.csv'
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(fields)
        i = 0
        while os.path.getsize(filename) < target_size_bytes:
            row = [
                str(i),
                faker.name(),
                faker.email(),
                "This is a long description field to increase file size. " + faker.text(max_nb_chars=200),
            ]
            row += [faker.sentence() for _ in range(8)]
            writer.writerow(row)
            i += 1

    size_mb = os.path.getsize(filename) / (1024 * 1024)
    print("CSV generated:", round(size_mb, 2), "MB", f"({i} rows)")
    return filename


def init_collections():
    for collection, fields in COLLECTIONS.items():
        try:
            databases.create_collection(
                database_id=DATABASE_ID,
                collection_id=collection,
                name=collection,
                document_security=False,
                permissions=[
                    Permission.read(Role.any()),
                    Permission.update(Role.any()),
                    Permission.delete(Role.any())
                ]
            )
            print(f"📦 Created collection `{collection}`")

            for field in fields:
                field_type = FIELD_TYPE_MAP[field]
                is_array = False

                # We can mark some fields like tags as optional array (CSV fallback here)
                if field == "tags":
                    is_array = False

                databases.create_string_attribute(
                    database_id=DATABASE_ID,
                    collection_id=collection,
                    key=field,
                    size=256,
                    required=False
                ) if field_type in ["string", "email"] else \
                databases.create_integer_attribute(
                    database_id=DATABASE_ID,
                    collection_id=collection,
                    key=field,
                    required=False
                ) if field_type == "integer" else \
                databases.create_float_attribute(
                    database_id=DATABASE_ID,
                    collection_id=collection,
                    key=field,
                    required=False
                ) if field_type == "float" else \
                databases.create_boolean_attribute(
                    database_id=DATABASE_ID,
                    collection_id=collection,
                    key=field,
                    required=False
                )
            print(f"✅ Schema created for `{collection}`")
        except Exception as e:
            print(f"⚠️ Collection `{collection}` may already exist: {e}")

def generate_documents(count, collections_filter=None, seed=None):
    if seed:
        random.seed(seed)
        Faker.seed(seed)

    data = {}
    targets = {k: v for k, v in COLLECTIONS.items() if (not collections_filter or k in collections_filter)}
    for collection, fields in targets.items():
        data[collection] = []
        for _ in range(count):
            doc = {field: GENERATOR_MAP[field]() for field in fields}
            data[collection].append(doc)
    return data

def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def upload_documents(data, max_attempts=10):
    for collection, docs in data.items():
        print(f"⬆ Uploading {len(docs)} docs to `{collection}`")
        for chunk in chunked(docs, CHUNK_SIZE):
            documents = [{"$id": ID.unique(), **doc} for doc in chunk]

            for attempt in range(1, max_attempts + 1):
                try:
                    databases.create_documents(
                        database_id=DATABASE_ID,
                        collection_id=collection,
                        documents=documents
                    )
                    break  # ✅ Success
                except Exception as e:
                    wait = min(2 ** attempt + random.uniform(0, 1), 30)  # max wait cap 30s
                    print(f"⚠️ Attempt {attempt} failed for chunk in `{collection}`: {e}")
                    if attempt == max_attempts:
                        print(f"❌ Failed to upload after {max_attempts} attempts.")
                    else:
                        print(f"⏳ Retrying in {wait:.2f}s...")
                        time.sleep(wait)
        print(f"✅ Upload complete for `{collection}`")

def delete_dbs():
    for db in databases.list()['databases']:
        databases.delete(db['$id'])

def save_to_file(data, path):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def load_from_file(path):
    with open(path, "r") as f:
        return json.load(f)

def pull_from_appwrite(collections_filter=None):
    remote = {}
    targets = {k: v for k, v in COLLECTIONS.items() if (not collections_filter or k in collections_filter)}
    for collection in targets:
        result = databases.list_documents(database_id=DATABASE_ID, collection_id=collection)
        docs = []
        for doc in result["documents"]:
            docs.append({k: v for k, v in doc["data"].items()})
        remote[collection] = docs
    return remote

def compare(local, remote):
    diff = DeepDiff(local, remote, ignore_order=True)
    return diff if diff else "✅ Local and remote data match!"

def publish_event():
    import time, random
    while True:
        upload_documents(generate_documents(1))
        print("triggerd")
        time.sleep(random.choice([2,4,6]) + random.random())

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--init-schema", action="store_true", help="Create DB and collections/schema")
    parser.add_argument("--generate", action="store_true", help="Generate and upload documents")
    parser.add_argument("--compare", action="store_true", help="Compare remote data with local file")
    parser.add_argument("--count", type=int, default=1000, help="Docs per collection")
    parser.add_argument("--collections", type=str, help="Comma-separated collection names")
    parser.add_argument("--output", type=str, default="generated.json", help="Path to save generated data")
    parser.add_argument("--attempts", type=int, default=10, help="Max retry attempts for upload")
    parser.add_argument("--dry-run", action="store_true", help="Skip upload, only generate and save")
    parser.add_argument("--seed", type=int, help="Seed for repeatable generation")
    parser.add_argument("--delete", type=bool, help="delete db of project id")
    parser.add_argument("--realtime", type=bool, help="create documents in the db at each interval. make sure to run index.html first to see")
    parser.add_argument("--init-csv-collection", action="store_true", help="Create 'csv' collection (if not exists) with all required string attributes")
    parser.add_argument("--csv-with-a-size", action="store_true", help="Create 'csv' collection (if not exists) and generate a CSV file for import with target size")
    parser.add_argument("--csv-size", type=float, default=10, help="Target CSV size in MB (default: 10)")
    parser.add_argument("--csv-output", type=str, default="large_file.csv", help="Output path for generated CSV (will be suffixed with _import before .csv extension, default: large_file_import.csv)")
    parser.add_argument("--csv-seed", type=int, help="Optional seed for repeatable CSV generation")
    args = parser.parse_args()

    selected_collections = args.collections.split(",") if args.collections else None

    if args.init_schema:
        init_database()
        init_collections()

    if args.generate:
        docs = generate_documents(args.count, selected_collections, args.seed)
        save_to_file(docs, args.output)
        print(f"📄 Data saved to `{args.output}`")
        if not args.dry_run:
            upload_documents(docs)

    if args.compare:
        local = load_from_file(args.output)
        remote = pull_from_appwrite(selected_collections)
        print(compare(local, remote))

    if args.realtime:
        publish_event()

    if args.delete:
        delete_dbs()

    if args.init_csv_collection:
        ensure_csv_collection()

    if args.csv_with_a_size:
        ensure_csv_collection()
        target_bytes = int(args.csv_size * 1024 * 1024)
        generate_csv_with_size(
            target_size_bytes=target_bytes,
            output_path=args.csv_output,
            seed=args.csv_seed,
        )