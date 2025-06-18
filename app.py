import os
import json
import time
import streamlit as st
import asyncio
import subprocess
import boto3
from dotenv import load_dotenv

# === Setup ===
load_dotenv()
s3 = boto3.client("s3")
bucket = os.getenv("S3_BUCKET_NAME")

# === UI Setup ===
st.set_page_config(page_title="📚 RAG S3 Pipeline", layout="centered")
st.title("📦 S3-based Chunk Ingestion App")

# === Upload Section ===
st.header("📄 Upload PDF(s)")
uploaded_files = st.file_uploader("Choose PDF files", type="pdf", accept_multiple_files=True)

st.header("🌐 Add Website URLs")
url_input = st.text_area("Enter one or more URLs (one per line)", height=150)
urls = [u.strip() for u in url_input.strip().splitlines() if u.strip()]

# === Helper to create new session folder name ===
def get_next_session_prefix():
    existing = s3.list_objects_v2(Bucket=bucket, Prefix="session_")
    if "Contents" not in existing:
        return "session_1/"
    sessions = [obj["Key"].split("/")[0] for obj in existing["Contents"] if obj["Key"].startswith("session_")]
    max_id = max([int(s.split("_")[1]) for s in sessions if s.split("_")[1].isdigit()], default=0)
    return f"session_{max_id + 1}/"

# === Main Convert Button ===
if st.button("🚀 Convert to Chunks"):
    from ingest_aws_for_app import process_pdf_file

    session_prefix = get_next_session_prefix()

    # Write session prefix to config
    with open("session_config.json", "w") as f:
        json.dump({"current_session_dir": session_prefix}, f)

    async def run_pdf_ingestion():
        if uploaded_files:
            st.info("📥 Uploading PDFs to S3...")
            for file in uploaded_files:
                local_path = os.path.join("data", file.name)
                with open(local_path, "wb") as f_out:
                    f_out.write(file.read())
                await process_pdf_file(local_path)
            st.success("✅ PDF upload complete.")

    if uploaded_files:
        asyncio.run(run_pdf_ingestion())

    if urls:
        clean_urls = [u for u in urls if u.startswith("http://") or u.startswith("https://")]
        if not clean_urls:
            st.error("❌ No valid HTTP/HTTPS URLs found.")
        else:
            st.info("🌐 Crawling started in background...")
            url_args = " ".join(f'"{u}"' for u in clean_urls)
            command = f"python run_aws_web_ingestion.py {url_args}"
            subprocess.Popen(command, shell=True)
            st.warning("⚠️ Refresh after a while to see newly ingested URLs.")

            # Poll crawl_done.flag in S3
            flag_key = f"{session_prefix}crawl_done.flag"
            waited = 0
            while waited < 300:
                if any(obj["Key"] == flag_key for obj in s3.list_objects_v2(Bucket=bucket, Prefix=session_prefix).get("Contents", [])):
                    st.success("✅ URL ingestion complete.")
                    break
                time.sleep(3)
                waited += 3

# === Upload to Qdrant ===
if st.button("📤 Upload All Session Folders to Qdrant"):
    try:
        from app_aws_upload_qdrant import run_upload
        count = run_upload()
        st.success(f"✅ Upload to Qdrant complete. {count} chunks uploaded.")
    except Exception as e:
        st.error("❌ Upload failed.")
        st.exception(e)
