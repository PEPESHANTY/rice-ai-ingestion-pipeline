import os, json, asyncio
import fitz
import re
import boto3
import io
from hashlib import md5
from pathlib import Path
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from urllib.parse import urlparse
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from ingest_chunks import get_embedding, get_title_summary, detect_lang, translate_vi_en, postprocess_text, chunk_text

load_dotenv()
s3 = boto3.client("s3")
bucket = os.getenv("S3_BUCKET_NAME")
region = os.getenv("AWS_REGION")

def get_last_global_id():
    try:
        obj = s3.get_object(Bucket=bucket, Key="last_chunk_id.txt")
        return int(obj["Body"].read().decode().strip())
    except:
        return 0

GLOBAL_CHUNK_ID = get_last_global_id()


def update_last_global_id(new_id: int):
    s3.put_object(Body=str(new_id), Bucket=bucket, Key="last_chunk_id.txt")



def get_next_session_prefix():
    existing = s3.list_objects_v2(Bucket=bucket, Prefix="session_")
    if "Contents" not in existing:
        return "session_1/"
    sessions = [obj["Key"].split("/")[0] for obj in existing["Contents"] if obj["Key"].startswith("session_")]
    max_id = max([int(s.split("_")[1]) for s in sessions if s.split("_")[1].isdigit()], default=0)
    return f"session_{max_id + 1}/"

SESSION_PREFIX = get_next_session_prefix()

async def save_chunk_to_s3(chunk, meta, url, chunk_id, source, slug, path_hash):
    global GLOBAL_CHUNK_ID  # <- ensure we reference the global
    lang = detect_lang(chunk)
    translated = translate_vi_en(meta["summary"]) if lang == "vi" else meta["summary"]
    parsed = urlparse(url)
    vector = await get_embedding(meta["summary"] + " " + chunk)

    payload = {
        "id": GLOBAL_CHUNK_ID,
        "vector": vector,
        "payload": {
            "title": meta["title"],
            "summary": meta["summary"],
            "translated_summary": translated,
            "content": chunk,
            "url": url,
            "source": source,
            "lang": lang,
            "chunk_number": GLOBAL_CHUNK_ID,
            "url_path": parsed.path,
            "chunk_id": f"{slug}_{path_hash}_chunk{chunk_id}"
        }
    }

    key = f"{SESSION_PREFIX}{slug}_{path_hash}_chunk{GLOBAL_CHUNK_ID}.json"
    s3.put_object(Body=json.dumps(payload, ensure_ascii=False), Bucket=bucket, Key=key)
    print(f"✅ Uploaded: {key}")

    GLOBAL_CHUNK_ID += 1
    update_last_global_id(GLOBAL_CHUNK_ID)

async def process_pdf_file(file_stream: io.BytesIO, filename: str):
    ext = Path(filename).suffix.lower()

    if ext == ".pdf":
        doc = fitz.open(stream=file_stream, filetype="pdf")
        text = "\n".join([page.get_text() for page in doc])
    elif ext == ".txt":
        text = file_stream.read().decode("utf-8")
    else:
        raise ValueError("Unsupported file type. Only .pdf and .txt are allowed.")

    from ingest_chunks import chunk_text, postprocess_text, get_title_summary

    chunks = chunk_text(postprocess_text(text))

    slug = Path(filename).stem.replace(" ", "_").lower()
    path_hash = md5(filename.encode()).hexdigest()[:6]
    url = f"{ext[1:]}://{slug}"

    for i, chunk in enumerate(chunks):
        meta = await get_title_summary(chunk, url)
        await save_chunk_to_s3(chunk, meta, url, i, "pdf_import", slug, path_hash)

async def process_single_urls(urls: list):
    crawler = AsyncWebCrawler(config=BrowserConfig(headless=True))
    await crawler.start()
    try:
        for url in urls:
            result = await crawler.arun(url=url, config=CrawlerRunConfig(cache_mode=CacheMode.BYPASS), session_id="single_page")
            if result.success:
                await process_and_save_web(url, result.html)
    finally:
        await crawler.close()

async def process_and_save_web(url: str, html: str):
    text = postprocess_text(BeautifulSoup(html, "html.parser").get_text())
    chunks = chunk_text(text)
    parsed = urlparse(url)
    slug = parsed.netloc.replace(".", "_")
    path_hash = md5(parsed.path.encode()).hexdigest()[:6]

    for i, chunk in enumerate(chunks):
        meta = await get_title_summary(chunk, url)
        await save_chunk_to_s3(chunk, meta, url, i, "web_crawl", slug, path_hash)
