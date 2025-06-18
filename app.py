import io
import os
import sys
import json
import time
import asyncio
import streamlit as st
import subprocess
import boto3

from dotenv import load_dotenv

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

load_dotenv()
bucket = os.getenv("S3_BUCKET_NAME")
region = os.getenv("AWS_REGION")
s3 = boto3.client("s3")

st.set_page_config(page_title="S3 Ingestion UI", layout="centered")
st.title("📦 S3-Based Ingestion App")

st.header("📤 Upload PDF files")
uploaded_files = st.file_uploader("Choose PDF or TXT files", type=["pdf", "txt"], accept_multiple_files=True)

st.header("🌐 Add Website URLs")
url_input = st.text_area("Enter one or more URLs (one per line)", height=150)
urls = [u.strip() for u in url_input.strip().splitlines() if u.strip()]

def get_next_session_prefix():
    existing = s3.list_objects_v2(Bucket=bucket, Prefix="session_")
    if "Contents" not in existing:
        return "session_1/"
    sessions = [obj["Key"].split("/")[0] for obj in existing["Contents"] if obj["Key"].startswith("session_")]
    max_id = max([int(s.split("_")[1]) for s in sessions if s.split("_")[1].isdigit()], default=0)
    return f"session_{max_id + 1}/"

if st.button("🚀 Convert to Chunks"):
    session_prefix = get_next_session_prefix()

    with open("session_config.json", "w") as f:
        json.dump({"current_session_dir": session_prefix}, f)

    from ingest_aws_for_app import process_pdf_file
    #
    async def run_pdf_ingestion():
        for file in uploaded_files:
            file_stream = io.BytesIO(file.read())  # No disk write, in-memory processing
            await process_pdf_file(file_stream, file.name)
        st.success("✅ PDF upload complete.")


    # Phase 1: Process PDFs
    # Phase 1: Process PDFs/TXT in-memory
    if uploaded_files:
        async def run_ingestion():
            for file in uploaded_files:
                file_stream = io.BytesIO(file.read())  # Read in memory
                await process_pdf_file(file_stream, file.name)


        try:
            try:
                # Preferred: run with existing running loop
                loop = asyncio.get_running_loop()
                task = loop.create_task(run_ingestion())
            except RuntimeError:
                # Fallback: no running loop found
                asyncio.run(run_ingestion())
            st.success("✅ PDF upload complete.")
        except Exception as e:
            st.error("❌ PDF ingestion failed.")
            st.exception(e)

    if urls:
        clean_urls = [u for u in urls if u.startswith("http://") or u.startswith("https://")]
        if not clean_urls:
            st.error("❌ No valid HTTP/HTTPS URLs found.")
        else:
            url_args = " ".join(f'"{u}"' for u in clean_urls)
            command = f"{sys.executable} run_aws_web_ingestion.py {url_args}"
            subprocess.Popen(command, shell=True)
            st.info("🌐 Crawling started in background...")
            st.warning("⚠️ Refresh after a while to see newly ingested URLs.")

            flag_key = session_prefix + "crawl_done.flag"
            waited = 0
            while waited < 300:
                try:
                    s3.head_object(Bucket=bucket, Key=flag_key)
                    st.success("✅ URL ingestion complete.")
                    break
                except:
                    time.sleep(3)
                    waited += 3

if st.button("🚀 Upload All Session Folders to Qdrant"):
    try:
        from app_aws_upload_qdrant import run_upload
        count = run_upload()
        st.success(f"✅ Upload to Qdrant complete. {count} chunks uploaded.")
    except Exception as e:
        st.error("❌ Upload failed. Check logs below.")
        st.exception(e)
