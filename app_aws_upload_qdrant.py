import os, json, shutil
import boto3
from tqdm import tqdm
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct

load_dotenv()
bucket = os.getenv("S3_BUCKET_NAME")
region = os.getenv("AWS_REGION")
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
COLLECTION_NAME = os.getenv("QDRANT_COLLECTION_NAME", "rice_knowledge")

s3 = boto3.client("s3")

CHUNK_ID_FILE = "last_chunk_id.txt"

def get_next_global_chunk_id():
    try:
        obj = s3.get_object(Bucket=bucket, Key=CHUNK_ID_FILE)
        last = int(obj["Body"].read().decode("utf-8").strip())
    except:
        last = 0
    return last + 1

def save_global_chunk_id(new_id: int):
    s3.put_object(Bucket=bucket, Key=CHUNK_ID_FILE, Body=str(new_id).encode("utf-8"))

qdrant = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)

def upload_session_from_s3(session_prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=session_prefix)
    if "Contents" not in response:
        print(f"❌ No files found under {session_prefix}")
        return 0

    json_keys = [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".json")]
    batch = []
    next_chunk_id = get_next_global_chunk_id()

    for key in tqdm(json_keys, desc=f"Uploading {session_prefix}"):
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(obj["Body"].read())
        data["id"] = next_chunk_id
        batch.append(PointStruct(id=next_chunk_id, vector=data["vector"], payload=data["payload"]))
        next_chunk_id += 1

    if batch:
        qdrant.upload_points(collection_name=COLLECTION_NAME, points=batch)
        save_global_chunk_id(next_chunk_id - 1)

        print(f"✅ Uploaded {len(batch)} chunks from {session_prefix}")

    # Cleanup
    for key in json_keys:
        s3.delete_object(Bucket=bucket, Key=key)
        # Remove crawl_done.flag if present
        s3.delete_object(Bucket=bucket, Key=f"{session_prefix}crawl_done.flag")

        # Remove the prefix folder marker if any
        try:
            s3.delete_object(Bucket=bucket, Key=session_prefix)
        except:
            pass

    print(f"🧹 Deleted session folder from S3: {session_prefix}")
    return len(batch)

def run_upload():
    paginator = s3.get_paginator("list_objects_v2")
    session_folders = set()
    for page in paginator.paginate(Bucket=bucket, Prefix="session_"):
        for obj in page.get("Contents", []):
            folder = obj["Key"].split("/")[0]
            if folder.startswith("session_"):
                session_folders.add(folder)

    total_uploaded = 0
    for folder in sorted(session_folders):
        count = upload_session_from_s3(folder + "/")
        total_uploaded += count

    print(f"\n✅ Total {total_uploaded} chunks uploaded to Qdrant.")
    return total_uploaded

if __name__ == "__main__":
    run_upload()
