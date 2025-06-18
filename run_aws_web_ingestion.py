# run_web_ingestion.py
import os
import json
import sys
import asyncio
from ingest_aws_for_app import process_single_urls
from dotenv import load_dotenv
import boto3

# === Load environment and S3 setup ===
load_dotenv()
bucket = os.getenv("S3_BUCKET_NAME")
s3 = boto3.client("s3")

import subprocess
subprocess.run(["playwright", "install", "chromium"], check=True)

def install_playwright_deps():
    try:
        # Only run on Linux
        if sys.platform.startswith("linux"):
            print("🔧 Installing Playwright Linux dependencies...")
            subprocess.run([
                "apt-get", "update"
            ], check=True)

            subprocess.run([
                "apt-get", "install", "-y",
                "libnss3", "libnspr4", "libatk1.0-0", "libatk-bridge2.0-0", "libcups2",
                "libxkbcommon0", "libatspi2.0-0", "libxcomposite1", "libxdamage1", "libxfixes3",
                "libxrandr2", "libgbm1", "libpango-1.0-0", "libcairo2", "libasound2"
            ], check=True)

            print("✅ System dependencies installed.")
        else:
            print("⚠️ Skipping Playwright deps installation (not on Linux).")
    except Exception as e:
        print("❌ Failed to install system dependencies:", e)

# Run installation before anything else
install_playwright_deps()

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

