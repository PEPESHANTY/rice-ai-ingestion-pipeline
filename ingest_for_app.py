# ingest_chunks.py

import os
import json
import re
from pathlib import Path
from urllib.parse import urlparse
from hashlib import md5
import fitz  # PyMuPDF
from bs4 import BeautifulSoup
from langdetect import detect
from dotenv import load_dotenv
from openai import AsyncOpenAI
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

from test_translate import translate_vi_en

# Load env vars
load_dotenv()
openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Paths and globals
CHUNK_DIR = "final_data/app_qdrant_chunks"
PDF_DIR = "data"
os.makedirs(CHUNK_DIR, exist_ok=True)
os.makedirs(PDF_DIR, exist_ok=True)


CHUNK_ID_TRACKER = "final_data/last_chunk_id.txt"

def get_next_global_chunk_id():
    if os.path.exists(CHUNK_ID_TRACKER):
        with open(CHUNK_ID_TRACKER, "r") as f:
            try:
                return int(f.read().strip()) + 1
            except:
                return 0
    return 0

def save_global_chunk_id(val):
    with open(CHUNK_ID_TRACKER, "w") as f:
        f.write(str(val))

GLOBAL_CHUNK_COUNTER = get_next_global_chunk_id()


# -------------------- UTILS --------------------
def clean_html(raw_html: str) -> str:
    soup = BeautifulSoup(raw_html, "html.parser")
    for tag in soup(["script", "style", "header", "footer", "nav"]):
        tag.decompose()
    return soup.get_text(separator="\n", strip=True)

def postprocess_text(text: str) -> str:
    text = re.sub(r'\\[nrt]', ' ', text)
    text = re.sub(r'\\+', '', text)
    text = text.replace('\n', ' ')
    text = re.sub(r'https?://\S+', '', text)
    text = re.sub(r'\s{2,}', ' ', text)
    return text.strip()

def chunk_text(text: str, chunk_size: int = 3000, overlap: int = 200) -> list:
    chunks, start = [], 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        start += chunk_size - overlap
    return chunks

def detect_lang(text: str) -> str:
    try:
        return detect(text)
    except:
        return "en"

def clean_id(text):
    return re.sub(r'[^a-zA-Z0-9_-]+', '_', text).strip('_')

async def get_title_summary(chunk: str, url: str) -> dict:
    system_prompt = "Extract a relevant title and concise English summary for this text chunk. Return JSON with 'title' and 'summary'."
    try:
        response = await openai_client.chat.completions.create(
            model=os.getenv("LLM_MODEL", "gpt-4o-mini"),
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"URL: {url}\n\nContent:\n{chunk[:1000]}..."}
            ],
            response_format={"type": "json_object"}
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"❌ GPT summary error: {e}")
        return {"title": "Error", "summary": chunk[:100]}

async def get_embedding(text: str) -> list:
    try:
        response = await openai_client.embeddings.create(
            model="text-embedding-3-small",
            input=text
        )
        return response.data[0].embedding
    except Exception as e:
        print(f"❌ Embedding error: {e}")
        return [0.0] * 1536

async def save_chunk(chunk: str, meta: dict, url: str, chunk_number: int, source: str, slug: str, path_hash: str):
    global GLOBAL_CHUNK_COUNTER
    chunk_id = clean_id(f"{slug}_{path_hash}_chunk{GLOBAL_CHUNK_COUNTER}")
    vector = await get_embedding(meta["summary"] + " " + chunk)
    lang = detect_lang(chunk)
    translated_summary = translate_vi_en(meta["summary"]) if lang == "vi" else meta["summary"]
    parsed = urlparse(url)

    json_obj = {
        "id": GLOBAL_CHUNK_COUNTER,
        "vector": vector,
        "payload": {
            "title": meta["title"],
            "summary": meta["summary"],
            "translated_summary": translated_summary,
            "content": chunk,
            "url": url,
            "source": source,
            "lang": lang,
            "chunk_number": chunk_number,
            "url_path": parsed.path,
            "chunk_id": chunk_id
        }
    }

    outpath = os.path.join(CHUNK_DIR, f"{chunk_id}.json")
    with open(outpath, "w", encoding="utf-8") as f:
        json.dump(json_obj, f, ensure_ascii=False, indent=2)
    print(f"✅ Saved: {outpath.replace('\\', '/')}")
    GLOBAL_CHUNK_COUNTER += 1
    save_global_chunk_id(GLOBAL_CHUNK_COUNTER)


# -------------------- PUBLIC FUNCTIONS --------------------
async def process_pdf_file(filepath: str):
    doc = fitz.open(filepath)
    text = "\n".join([page.get_text() for page in doc])
    chunks = chunk_text(postprocess_text(text))
    slug = Path(filepath).stem.replace(" ", "_").lower()
    path_hash = md5(filepath.encode()).hexdigest()[:6]
    url = f"pdf://{slug}"
    for i, chunk in enumerate(chunks):
        meta = await get_title_summary(chunk, url)
        await save_chunk(chunk, meta, url, i, "pdf_import", slug, path_hash)

async def process_multiple_pdfs(pdf_paths: list):
    for path in pdf_paths:
        await process_pdf_file(path)

async def process_single_urls(urls: list):
    crawler = AsyncWebCrawler(config=BrowserConfig(headless=True))
    await crawler.start()
    try:
        for url in urls:
            result = await crawler.arun(url=url, config=CrawlerRunConfig(cache_mode=CacheMode.BYPASS),
                                        session_id="single_page")
            if result.success:
                await process_and_save_web(url, result.html)
    finally:
        await crawler.close()

async def process_and_save_web(url: str, html: str):
    clean = postprocess_text(clean_html(html))
    chunks = chunk_text(clean)
    parsed = urlparse(url)
    slug = parsed.netloc.replace(".", "_")
    path_hash = md5(parsed.path.encode()).hexdigest()[:6]
    for i, chunk in enumerate(chunks):
        meta = await get_title_summary(chunk, url)
        await save_chunk(chunk, meta, url, i, "web_crawl", slug, path_hash)
