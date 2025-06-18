# run_web_ingestion.py
import os, sys, json, asyncio
from ingest_for_app import process_single_urls

with open("session_config.json") as f:
    session_path = json.load(f)["current_session_dir"]

import ingest_for_app
ingest_for_app.CHUNK_DIR = session_path

async def run(urls):
    print(f"🌐 Crawling {len(urls)} URLs...")
    await process_single_urls(urls)
    print(f"✅ Done! JSON chunks saved to {session_path}")

    # ✅ Write completion flag
    with open(os.path.join(session_path, "crawl_done.flag"), "w") as f:
        f.write("done")

if __name__ == "__main__":
    urls = sys.argv[1:]
    if not urls:
        print("⚠️ No URLs provided.")
        sys.exit(1)
    asyncio.run(run(urls))
