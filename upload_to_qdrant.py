import os
import json
import glob
import time
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance

# ------------------ CONFIG ------------------

load_dotenv()
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
COLLECTION_NAME = os.getenv("QDRANT_COLLECTION_NAME")
VECTOR_SIZE = 1536
CHUNK_DIR = "final_data/qdrant_chunks"
BATCH_SIZE = 25

# ------------------ INIT CLIENT ------------------

client = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)

if COLLECTION_NAME not in [c.name for c in client.get_collections().collections]:
    client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(size=VECTOR_SIZE, distance=Distance.COSINE),
    )

# ------------------ LOAD CHUNKS ------------------

chunk_files = glob.glob(os.path.join(CHUNK_DIR, "*.json"))
print(f"🔍 Found {len(chunk_files)} chunk files to upload.")

# ------------------ UPLOAD ------------------

def upload_in_batches(files, batch_size=BATCH_SIZE):
    failed = []

    for i in range(0, len(files), batch_size):
        batch_files = files[i:i + batch_size]
        points = []

        for file_path in batch_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    obj = json.load(f)
                    points.append(PointStruct(
                        id=obj["id"],
                        vector=obj["vector"],
                        payload=obj["payload"]
                    ))
            except Exception as e:
                print(f"❌ JSON read failed: {file_path} | {e}")
                failed.append(file_path)

        if not points:
            continue

        uploaded = False
        try:
            client.upsert(collection_name=COLLECTION_NAME, points=points)
            uploaded = True
        except Exception as e:
            print(f"⚠️ Upload failed (batch {i // batch_size + 1}), retrying in 2s: {e}")
            time.sleep(2)
            try:
                client.upsert(collection_name=COLLECTION_NAME, points=points)
                uploaded = True
                print(f"🔁 Retry succeeded (batch {i // batch_size + 1})")
            except Exception as e2:
                print(f"❌ Retry failed (batch {i // batch_size + 1}): {e2}")
                failed.extend(batch_files)

        if uploaded:
            print(f"✅ Uploaded batch {i // batch_size + 1} ({len(points)} chunks)")

    return failed

# ------------------ MAIN ------------------

if chunk_files:
    failed_files = upload_in_batches(chunk_files)
    print(f"\n🎉 All chunks processed. ✅ Success: {len(chunk_files) - len(failed_files)} | ❌ Failed: {len(failed_files)}")

    if failed_files:
        with open("failed_chunks.txt", "w") as f:
            for path in failed_files:
                f.write(path + "\n")
        print("📄 Failed chunk paths saved to failed_chunks.txt")
else:
    print("⚠️ No chunks found to upload.")


# import os
# import json
# import glob
# from dotenv import load_dotenv
# from qdrant_client import QdrantClient
# from qdrant_client.models import PointStruct, VectorParams, Distance
#
# load_dotenv()
#
# QDRANT_URL = os.getenv("QDRANT_URL")
# QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
# COLLECTION_NAME = os.getenv("QDRANT_COLLECTION_NAME")
# VECTOR_SIZE = 1536
# CHUNK_DIR = "final_data/qdrant_chunks"
#
# client = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
#
# # Create collection if it doesn’t exist
# if COLLECTION_NAME not in [c.name for c in client.get_collections().collections]:
#     client.create_collection(
#         collection_name=COLLECTION_NAME,
#         vectors_config=VectorParams(size=VECTOR_SIZE, distance=Distance.COSINE),
#     )
#
# # Load JSON chunk files
# chunk_files = glob.glob(os.path.join(CHUNK_DIR, "*.json"))
# print(f"🔍 Found {len(chunk_files)} chunk files to upload.")
#
# def upload_in_batches(points, batch_size=20):
#     for i in range(0, len(points), batch_size):
#         batch = points[i:i + batch_size]
#         try:
#             client.upsert(collection_name=COLLECTION_NAME, points=batch)
#             print(f"✅ Uploaded batch {i // batch_size + 1} ({len(batch)} chunks)")
#         except Exception as e:
#             print(f"❌ Failed batch {i // batch_size + 1}: {e}")
#
# # Prepare points
# points = []
# for file in chunk_files:
#     with open(file, "r", encoding="utf-8") as f:
#         obj = json.load(f)
#         points.append(PointStruct(
#             id=obj["id"],
#             vector=obj["vector"],
#             payload=obj["payload"]
#         ))
#
# # Upload
# if points:
#     upload_in_batches(points)
#     print("🎉 All demo chunks uploaded to Qdrant.")
# else:
#     print("⚠️ No chunks found to upload.")
