# Unified RAG Ingestion Script for PDFs and Websites (Qdrant-Ready)

import os
import json
import re
from pathlib import Path
from xml.etree import ElementTree


import fitz  # PyMuPDF
import requests
from urllib.parse import urlparse, urljoin
from hashlib import md5
from bs4 import BeautifulSoup
from langdetect import detect
from dotenv import load_dotenv
from openai import AsyncOpenAI
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

from test_translate import translate_vi_en

# ------------------ SETUP ------------------

load_dotenv()
openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# gemini = ChatGoogleGenerativeAI(model="gemini-1.5-pro", google_api_key=os.getenv("GEMINI_API_KEY"))

CHUNK_DIR = "final_data/qdrant_chunks"
os.makedirs(CHUNK_DIR, exist_ok=True)

PDF_DIR = "data"
GLOBAL_CHUNK_COUNTER = 2727


# ------------------ UTILITIES ------------------

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
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        start += chunk_size - overlap
    return chunks


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


def detect_lang(text: str) -> str:
    try:
        return detect(text)
    except:
        return "en"

def clean_id(text):
    return re.sub(r'[^a-zA-Z0-9_-]+', '_', text).strip('_')

# async def translate_summary_with_gemini(text):
#     try:
#         response = await gemini.ainvoke(f"Translate this Vietnamese summary into English:\n\n{text}")
#         return response.content.strip()
#     except Exception as e:
#         print(f"❌ Gemini translation failed: {e}")
#         return "[Translation Failed]"


# ------------------ MAIN SAVE FUNCTION ------------------

async def save_chunk(chunk: str, meta: dict, url: str, chunk_number: int, source: str, slug: str, path_hash: str):
    global GLOBAL_CHUNK_COUNTER
    chunk_id = clean_id(f"{slug}_{path_hash}_chunk{GLOBAL_CHUNK_COUNTER}")
    # chunk_id = f"{slug}_{path_hash}_chunk{GLOBAL_CHUNK_COUNTER}"
    vector = await get_embedding(meta["summary"] + " " + chunk)
    lang = detect_lang(chunk)
    # translated_summary = meta["summary"] if lang == "en" else "[Vietnamese Translation Needed]"
    if lang == "vi":
        translated_summary = translate_vi_en(meta["summary"])
    else:
        translated_summary = meta["summary"]

    parsed = urlparse(url)
    json_obj = {
        "id": GLOBAL_CHUNK_COUNTER, #chunk_id,
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


# ------------------ PDF PARSER ------------------

async def process_pdf_file(filepath: str):
    doc = fitz.open(filepath)
    text = "\n".join([page.get_text() for page in doc])
    clean = postprocess_text(text)
    chunks = chunk_text(clean)
    slug = Path(filepath).stem.replace(" ", "_").lower()
    path_hash = md5(filepath.encode()).hexdigest()[:6]
    url = f"pdf://{slug}"
    for i, chunk in enumerate(chunks):
        meta = await get_title_summary(chunk, url)
        await save_chunk(chunk, meta, url, i, "pdf_import", slug, path_hash)


async def process_all_pdfs(pdf_dir: str = PDF_DIR):
    print(f"\n📄 Scanning PDF files in: {pdf_dir}\n")
    pdf_files = [f for f in os.listdir(pdf_dir) if f.lower().endswith(".pdf")]

    if not pdf_files:
        print("⚠️  No PDF files found.\n")
        return

    for file in pdf_files:
        print(f"📘 Processing PDF: {file}")
        filepath = os.path.join(pdf_dir, file)
        await process_pdf_file(filepath)
    print(f"\n✅ Finished processing {len(pdf_files)} PDF files.\n")


# ------------------ WEBSITE ------------------

async def process_and_save_web(url: str, html: str):
    clean = postprocess_text(clean_html(html))
    chunks = chunk_text(clean)
    parsed = urlparse(url)
    slug = parsed.netloc.replace(".", "_")
    path_hash = md5(parsed.path.encode()).hexdigest()[:6]
    for i, chunk in enumerate(chunks):
        meta = await get_title_summary(chunk, url)
        await save_chunk(chunk, meta, url, i, "web_crawl", slug, path_hash)


async def crawl_single_page(urls: list):
    crawler = AsyncWebCrawler(config=BrowserConfig(headless=True))
    config = CrawlerRunConfig(cache_mode=CacheMode.BYPASS)
    await crawler.start()
    try:
        for url in urls:
            result = await crawler.arun(url=url, config=config, session_id="single_page")
            if result.success:
                await process_and_save_web(url, result.html)
    finally:
        await crawler.close()


async def crawl_recursive(seed_url: str, max_depth: int = 2):
    visited, to_visit = set(), [(seed_url, 0)]
    crawler = AsyncWebCrawler(config=BrowserConfig(headless=True))
    await crawler.start()
    try:
        while to_visit:
            url, depth = to_visit.pop(0)
            if url in visited or depth > max_depth:
                continue
            result = await crawler.arun(url=url, config=CrawlerRunConfig(cache_mode=CacheMode.BYPASS),
                                        session_id="recursive")
            if result.success:
                await process_and_save_web(url, result.html)
                links = extract_internal_links(result.html, url)
                to_visit.extend([(l, depth + 1) for l in links if l not in visited])
                visited.add(url)
    finally:
        await crawler.close()

def get_urls_from_sitemap(sitemap_url: str, seen=None):
    if seen is None:
        seen = set()
    urls = []
    try:
        if sitemap_url in seen:
            return []
        seen.add(sitemap_url)
        resp = requests.get(sitemap_url, timeout=10)
        resp.raise_for_status()
        root = ElementTree.fromstring(resp.content)

        if root.tag.endswith("sitemapindex"):
            for sitemap in root.findall(".//{*}sitemap/{*}loc"):
                nested_url = sitemap.text.strip()
                urls.extend(get_urls_from_sitemap(nested_url, seen))
        elif root.tag.endswith("urlset"):
            urls.extend([loc.text.strip() for loc in root.findall(".//{*}url/{*}loc")])
    except Exception as e:
        print(f"❌ Error reading {sitemap_url}: {e}")
    return urls


def extract_internal_links(html: str, base_url: str):
    soup = BeautifulSoup(html, "html.parser")
    base_domain = urlparse(base_url).netloc
    internal_links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if (href.startswith("/") or base_domain in href) and ":" not in href and "#" not in href:
            full_url = urljoin(base_url, href)
            if urlparse(full_url).netloc == base_domain:
                internal_links.add(full_url)
    return list(internal_links)

# ------------------ MAIN ------------------

if __name__ == "__main__":
    import time
    import asyncio

    sitemap_urls = [
         "https://khuyennongvn.gov.vn/sitemap.xml",
         "https://viegoglobal.com/category-sitemap.xml"
    ]

    single_page_urls = [
        # "https://en.wikipedia.org/wiki/Rice_production_in_Vietnam",
        # "https://en.wikipedia.org/wiki/Irrigation_in_Vietnam",
        # "https://saigontourism.com.vn/best-rice-fields-places-in-vietnam/",
        # "https://vietnamagriculture.nongnghiep.vn/smart-rice-farming-the-key-to-success-for-farmers-d378590.html"
    ]

    recursive_urls = [
         # "http://www.knowledgebank.irri.org/"
    ]


    async def run_all():
        print(f"🚀 Starting complete RAG ingestion pipeline...\n")

        # Phase 0: PDFs
        print(f"📄 Phase 0: Ingesting local PDF files...")
        start = time.time()
        await process_all_pdfs()
        print(f"⏱️ Completed Phase 0 in {round(time.time() - start, 2)} seconds\n")

        # Phase 3: Specific single-page crawling
        if single_page_urls:
            print(f"🧱 Phase 1: Crawling {len(single_page_urls)} single-page URLs...")
            await crawl_single_page(single_page_urls)
            print("✅ Phase 1 complete\n")

        # Phase 4: Recursive crawling
        if recursive_urls:
            print(f"🌐 Phase 2: Crawling recursively from seed URLs...")
            for url in recursive_urls:
                print(f"🔁 Crawling recursively from: {url}")
                await crawl_recursive(url, max_depth=2)
            print("✅ Phase 2 complete\n")

        # Phase 3: Sitemap parsing
        all_sitemap_urls = []
        print(f"📥 Phase 3: Parsing sitemap.xml files...")
        for sitemap in sitemap_urls:
            urls = get_urls_from_sitemap(sitemap)
            print(f"🗺️ Sitemap found {len(urls)} URLs: {sitemap}")
            all_sitemap_urls.extend(urls)
        print(f"📦 Collected {len(all_sitemap_urls)} total URLs from sitemaps\n")

        # Phase 4: Sitemap-based crawling
        if all_sitemap_urls:
            print(f"🤖 Phase 4: Crawling {len(all_sitemap_urls)} sitemap URLs...")
            await crawl_single_page(all_sitemap_urls)
            print("✅ Phase 4 complete\n")


        print("🎯 All ingestion phases completed. Ready for Qdrant upload or querying.\n")


    asyncio.run(run_all())