import time
import os
from appwrite.client import Client
from appwrite.services.account import Account
from appwrite.services.databases import Databases
from appwrite.services.functions import Functions
from appwrite.id import ID
from appwrite.permission import Permission
from appwrite.role import Role
from dotenv import load_dotenv
from appwrite.exception import AppwriteException

load_dotenv()

def assert_true(condition, message):
    if not condition:
        raise AssertionError(message)

def assert_raises(fn, message):
    try:
        fn()
        raise AssertionError(f"Expected failure: {message}")
    except AppwriteException:
        pass

# -------------------- Client Setup --------------------

APPWRITE_ENDPOINT = os.environ.get("APPWRITE_ENDPOINT")
APPWRITE_PROJECT_ID = os.environ.get("APPWRITE_PROJECT_ID")
API_KEY = os.environ.get("APPWRITE_API_KEY")

client = Client()
client.set_endpoint(APPWRITE_ENDPOINT)
client.set_project(APPWRITE_PROJECT_ID)
client.set_key(API_KEY)

account = Account(client)
databases = Databases(client)
functions = Functions(client)

# -------------------- Test Constants --------------------

TEST_EMAIL = f"test_{int(time.time())}@example.com"
TEST_PASSWORD = "password123!"
TEST_NAME = "Test User"

DATABASE_ID = ID.unique()
COLLECTION_ID = ID.unique()
DOCUMENT_ID = ID.unique()

# -------------------- Helpers --------------------

def log(step, data=None):
    print(f"\n✅ {step}")
    if data:
        print(data)

# -------------------- Tests --------------------

def run():
    # ========= ACCOUNTS =========

    user = account.create(
        user_id=ID.unique(),
        email=TEST_EMAIL,
        password=TEST_PASSWORD,
        name=TEST_NAME
    )
    user_id = user["$id"]
    log("User created", user_id)

    session = account.create_email_password_session(
        email=TEST_EMAIL,
        password=TEST_PASSWORD
    )
    log("User logged in")

    # User-authenticated client
    user_client = Client()
    user_client.set_endpoint(APPWRITE_ENDPOINT)
    user_client.set_project(APPWRITE_PROJECT_ID)
    user_client.set_session(session["secret"])

    user_account = Account(user_client)
    user_databases = Databases(user_client)

    me = user_account.get()
    assert_true(me["$id"] == user_id, "User should read own account")
    log("User can read own account")

    # ========= DATABASES =========

    db = databases.create(DATABASE_ID, "Auth Test DB")
    collection = databases.create_collection(
        DATABASE_ID,
        COLLECTION_ID,
        "Auth Test Collection",
        permissions=[
            Permission.read(Role.user(user_id)),
            Permission.create(Role.user(user_id)),
            Permission.update(Role.user(user_id)),
            # ❌ No delete permission on purpose
        ]
    )

    log("Collection created with restricted permissions")

    # # Assert permissions config
    # perms = collection["permissions"]
    # print(collection)
    # assert_true(
    #     any("user:" in p for p in perms),
    #     "Collection should be user-restricted"
    # )

    databases.create_string_attribute(
        DATABASE_ID, COLLECTION_ID, "title", 255, True
    )

    time.sleep(2)

    # ========= PERMISSION ASSERTIONS =========

    # ✅ User can create
    doc = user_databases.create_document(
        DATABASE_ID,
        COLLECTION_ID,
        DOCUMENT_ID,
        {"title": "Auth Test"}
    )
    log("User can create document")

    # ✅ User can read
    user_databases.get_document(DATABASE_ID, COLLECTION_ID, DOCUMENT_ID)
    log("User can read document")

    # ✅ User can update
    user_databases.update_document(
        DATABASE_ID,
        COLLECTION_ID,
        DOCUMENT_ID,
        {"title": "Updated"}
    )
    log("User can update document")

    # ❌ User cannot delete
    assert_raises(
        lambda: user_databases.delete_document(
            DATABASE_ID, COLLECTION_ID, DOCUMENT_ID
        ),
        "User should NOT be able to delete document"
    )
    log("Delete correctly forbidden")

    # ========= GUEST ASSERTIONS =========

    guest_client = Client()
    guest_client.set_endpoint(APPWRITE_ENDPOINT)
    guest_client.set_project(APPWRITE_PROJECT_ID)

    guest_db = Databases(guest_client)

    assert_raises(
        lambda: guest_db.get_document(
            DATABASE_ID, COLLECTION_ID, DOCUMENT_ID
        ),
        "Guest must not read protected document"
    )
    log("Guest access correctly denied")

    print("\n🎉 AUTH + PERMISSIONS TESTS PASSED")


# -------------------- Run --------------------

if __name__ == "__main__":
    run()
