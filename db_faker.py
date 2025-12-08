"""
# First time: initialize schema
python bulk_appwrite_tool.py --init-schema

# Generate and upload 1000 docs/collection
python bulk_appwrite_tool.py --generate

# Generate 500 posts only and save to file, no upload
python bulk_appwrite_tool.py --generate --count 500 --collections posts --dry-run --output posts.json

# Upload from file later and compare
python bulk_appwrite_tool.py --compare --collections posts --output posts.json
"""
import os
import time
import json
import random
import argparse
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
    "events": ["title", "location", "date", "organizer", "attendees"]
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
    "attendees": "integer"
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
}

CHUNK_SIZE = 100

def init_database():
    try:
        databases.create(database_id=DATABASE_ID, name="Auto Generated DB")
        print(f"📁 Created database `{DATABASE_ID}`")
    except Exception as e:
        print(f"ℹ️ Database may already exist: {e}")

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