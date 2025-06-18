# run_web_ingestion.py
import os, sys, json, asyncio, boto3
from ingest_aws_for_app import process_single_urls
from dotenv import load_dotenv

# === Load environment and S3 setup ===
load_dotenv()
bucket = os.getenv("S3_BUCKET_NAME")
s3 = boto3.client("s3")

# === Get session path from config ===
with open("session_config.json") as f:
    session_path = json.load(f)["current_session_dir"]  # e.g. session_5/

async def run(urls):
    print(f"🌐 Crawling {len(urls)} URLs...")
    await process_single_urls(urls)
    print(f"✅ Done! JSON chunks uploaded to S3 under {session_path}")

    # ✅ Upload crawl_done.flag to S3
    s3.put_object(
        Bucket=bucket,
        Key=f"{session_path}crawl_done.flag",
        Body="done"
    )
    print(f"📁 Uploaded crawl_done.flag to s3://{bucket}/{session_path}")

if __name__ == "__main__":
    urls = sys.argv[1:]
    if not urls:
        print("⚠️ No URLs provided.")
        sys.exit(1)
    asyncio.run(run(urls))

