import os
import json
import shutil
import time
import qdrant_client
from tqdm import tqdm
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct

# --- Load .env variables ---
load_dotenv()
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
COLLECTION_NAME = os.getenv("QDRANT_COLLECTION_NAME", "rice_knowledge")

BASE_DIR = "final_data/app_qdrant_chunks"

client = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)

def upload_session_to_qdrant(session_path):
    json_files = [f for f in os.listdir(session_path) if f.endswith(".json")]
    if not json_files:
        print(f"⚠️ No JSON files in {session_path}. Skipping.")
        return 0

    batch = []
    for file in tqdm(json_files, desc=f"Uploading {os.path.basename(session_path)}", unit="chunk"):
        with open(os.path.join(session_path, file), encoding="utf-8") as f:
            data = json.load(f)
            batch.append(PointStruct(id=data["id"], vector=data["vector"], payload=data["payload"]))

    client.upload_points(collection_name=COLLECTION_NAME, points=batch)
    print(f"✅ Uploaded {len(batch)} chunks from {os.path.basename(session_path)}")

    shutil.rmtree(session_path)
    print(f"🧹 Deleted session folder: {session_path}")
    return len(batch)

def upload_all_sessions_to_qdrant():
    all_folders = sorted(f for f in os.listdir(BASE_DIR) if f.startswith("session_"))
    if not all_folders:
        print("🚫 No session folders found.")
        return

    total_chunks = 0
    for folder in all_folders:
        full_path = os.path.join(BASE_DIR, folder)
        count = upload_session_to_qdrant(full_path)
        total_chunks += count

    print(f"\n✅ Total {total_chunks} chunks uploaded to Qdrant.")
    return total_chunks

if __name__ == "__main__":
    start = time.time()
    upload_all_sessions_to_qdrant()
    print(f"\n⏱ Completed in {round(time.time() - start, 2)} seconds.")
