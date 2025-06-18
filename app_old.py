import os
import sys
import json
import time
import subprocess
import streamlit as st
import asyncio

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# === CONFIGURATION ===
UPLOAD_DIR = "data"
BASE_CHUNK_DIR = "final_data/app_qdrant_chunks"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(BASE_CHUNK_DIR, exist_ok=True)

# === DYNAMIC SESSION FOLDER CREATION ===
def get_next_session_folder(base_dir):
    existing = [f for f in os.listdir(base_dir) if f.startswith("session_")]
    numbers = [int(f.split("_")[1]) for f in existing if f.split("_")[1].isdigit()]
    next_index = max(numbers, default=0) + 1
    folder = os.path.join(base_dir, f"session_{next_index}")
    os.makedirs(folder, exist_ok=True)
    return folder


# === Streamlit UI Setup ===
st.set_page_config(page_title="RAG Ingestion", layout="centered")
st.title("📚 Rice Knowledge Ingestion App")

# --- PDF Upload Section ---
st.header("📄 Upload PDF(s)")
uploaded_files = st.file_uploader("Choose PDF files", type="pdf", accept_multiple_files=True)

# --- Website URLs Section ---
st.header("🌐 Add Website URLs")
url_input = st.text_area("Enter one or more URLs (one per line)", height=150)
urls = [u.strip() for u in url_input.strip().splitlines() if u.strip()]

# --- Ingestion ---
if st.button("🚀 Convert to Chunks"):
    from ingest_for_app import process_pdf_file

    SESSION_CHUNK_DIR = get_next_session_folder(BASE_CHUNK_DIR)

    # 🔒 Save session config for use in run_web_ingestion.py
    with open("session_config.json", "w") as f:
        json.dump({"current_session_dir": SESSION_CHUNK_DIR}, f)

    # === Inject override for CHUNK_DIR ===
    import ingest_for_app

    ingest_for_app.CHUNK_DIR = SESSION_CHUNK_DIR


    async def run_pdf_ingestion():
        if uploaded_files:
            st.info("📥 Ingesting PDFs...")
            for file in uploaded_files:
                save_path = os.path.join(UPLOAD_DIR, file.name)
                with open(save_path, "wb") as f:
                    f.write(file.read())
                await process_pdf_file(save_path)
            st.success("✅ PDF ingestion complete.")

    # Phase 1: Process PDFs
    if uploaded_files:
        asyncio.run(run_pdf_ingestion())

    # Phase 2: Start subprocess for URL ingestion
    if urls:
        clean_urls = [u for u in urls if u.startswith("http://") or u.startswith("https://")]
        if not clean_urls:
            st.error("❌ No valid HTTP/HTTPS URLs found.")
        else:
            url_args = " ".join(f'"{u}"' for u in clean_urls)  # wrap each URL in quotes
            command = f"python run_web_ingestion.py {url_args}"
            subprocess.Popen(command, shell=True)
            st.info("🌐 Crawling started in background...")
            st.warning("⚠️ Refresh after a while to see newly ingested URLs.")


            # ✅ Poll for crawl_done.flag
            # Poll for crawl_done.flag with timeout
            crawl_flag = os.path.join(SESSION_CHUNK_DIR, "crawl_done.flag")
            max_wait = 300  # seconds
            poll_interval = 3  # seconds
            waited = 0

            while waited < max_wait:
                if os.path.exists(crawl_flag):
                    st.success("✅ URL ingestion complete.")
                    break
                time.sleep(poll_interval)
                waited += poll_interval

    # if urls:
    #     url_args = " ".join(urls)
    #     command = f"python run_web_ingestion.py {SESSION_CHUNK_DIR} {url_args}"
    #     subprocess.Popen(command, shell=True)
    #     st.info("🌐 Crawling started in background...")
    #     st.warning("⚠️ Refresh after a while to see newly ingested URLs.")

# === Display Ingested Content ===
st.header("📂 Already Ingested Content")

def get_ingested_sources(base_dir, source_type):
    seen = set()
    for folder in os.listdir(base_dir):
        folder_path = os.path.join(base_dir, folder)
        if not os.path.isdir(folder_path): continue
        for file in os.listdir(folder_path):
            if file.endswith(".json"):
                with open(os.path.join(folder_path, file), encoding="utf-8") as f:
                    try:
                        data = json.load(f)
                        payload = data.get("payload", {})
                        if payload.get("source") == source_type:
                            if source_type == "pdf_import":
                                slug = os.path.basename(payload.get("url", "").replace("pdf://", ""))
                                seen.add(slug)
                            elif source_type == "web_crawl":
                                seen.add(payload.get("url", ""))
                    except:
                        continue
    return sorted(seen)

with st.expander("📘 Ingested PDFs"):
    pdf_slugs = get_ingested_sources(BASE_CHUNK_DIR, "pdf_import")
    if pdf_slugs:
        for name in pdf_slugs:
            st.markdown(f"- {name}")
    else:
        st.info("No PDFs ingested yet.")

with st.expander("🌍 Ingested URLs"):
    url_slugs = get_ingested_sources(BASE_CHUNK_DIR, "web_crawl")
    if url_slugs:
        for url in url_slugs:
            st.markdown(f"- [{url}]({url})")
    else:
        st.info("No URLs ingested yet.")

# 🔁 Refresh
if st.button("🔄 Refresh Ingested URLs"):
    st.toast("Refreshing...", icon="🔁")
    time.sleep(1)
    st.rerun()

# 📤 Upload to Qdrant Section
#st.header("📤 Upload to Qdrant")

if st.button("🚀 Upload All Session Folders to Qdrant"):
    try:
        from app_upload_qdrant import upload_all_sessions_to_qdrant
        count = upload_all_sessions_to_qdrant()
        st.success(f"✅ Upload to Qdrant complete. {count} chunks uploaded.")
    except Exception as e:
        st.error("❌ Upload failed. Check logs below.")
        st.exception(e)

