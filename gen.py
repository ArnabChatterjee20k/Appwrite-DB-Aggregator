import json
import uuid
import random
from faker import Faker

fake = Faker()

def generate_id():
    return str(uuid.uuid4())

def generate_string_attribute(max_size):
    possible_sizes = [*range(1,1500)]
    # Filter sizes that fit in remaining max_size
    allowed_sizes = [s for s in possible_sizes if s <= max_size]
    if not allowed_sizes:
        return None, 0

    size = random.choice(allowed_sizes)
    return {
        "key": fake.unique.word(),
        "type": "string",
        "required": random.choice([True, False]),
        "array": False,
        "size": size,
        "default": None
    }, size

def generate_collection(database_id):
    total_size = 0
    max_total_size = 58000  # leave 4-5 KB buffer for system fields & metadata
    attributes = []

    while total_size < max_total_size and len(attributes) < 5:
        remaining_size = max_total_size - total_size
        attr, size = generate_string_attribute(remaining_size)
        if not attr:
            break  # no sizes left that fit
        attributes.append(attr)
        total_size += size

    collection = {
        "$id": generate_id(),
        "databaseId": database_id,
        "name": fake.unique.word(),
        "enabled": True,
        "documentSecurity": False,
        "attributes": attributes,
        "indexes": []
    }
    return collection

def generate_database():
    database_id = generate_id()
    num_collections = random.randint(2, 5)
    collections = [generate_collection(database_id) for _ in range(num_collections)]

    database = {
        "$id": database_id,
        "name": fake.unique.word(),
        "enabled": True
    }
    return database, collections

def generate_appwrite_config(num_databases=3):
    databases = []
    collections = []

    for _ in range(num_databases):
        db, cols = generate_database()
        databases.append(db)
        collections.extend(cols)

    config = {
        "databases": databases,
        "collections": collections
    }

    return config

# Generate config
config = generate_appwrite_config(num_databases=5)

# Save to JSON file
with open("appwrite_config.json", "w") as f:
    json.dump(config, f, indent=4)

print("✅ Appwrite config JSON generated and saved as 'appwrite_config.json'")
