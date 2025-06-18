import os
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from qdrant_client.models import Filter

load_dotenv()

QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
COLLECTION_NAME = os.getenv("QDRANT_COLLECTION_NAME")

client = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)

# ✅ Delete all points from the collection (but keep the collection)
client.delete(
    collection_name=COLLECTION_NAME,
    points_selector=Filter(must=[])  # this means "match all"
)

print(f"🧹 All points deleted from collection: {COLLECTION_NAME}")
