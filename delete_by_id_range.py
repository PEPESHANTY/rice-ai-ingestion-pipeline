import os
from dotenv import load_dotenv
from qdrant_client import QdrantClient

# --- Load environment variables ---
load_dotenv()
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
COLLECTION_NAME = os.getenv("QDRANT_COLLECTION_NAME", "rice_knowledge")

# --- Connect to Qdrant ---
client = QdrantClient(
    url=QDRANT_URL,
    api_key=QDRANT_API_KEY
)

# --- Define the ID range to delete ---
START_ID = 23113
END_ID = 23274

# --- Convert to explicit list of IDs ---
ids_to_delete = list(range(START_ID, END_ID + 1))

# --- Perform the deletion ---
print(f"🚨 Deleting {len(ids_to_delete)} points from ID {START_ID} to {END_ID}...")

result = client.delete(
    collection_name=COLLECTION_NAME,
    points_selector=ids_to_delete
)

print("✅ Deletion completed.")
