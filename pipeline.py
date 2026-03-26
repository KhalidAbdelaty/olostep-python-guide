"""
From Web Scraping Scripts to Web Data APIs: Complete Pipeline
=============================================================
Companion code for the DataCamp article by Khalid Alyahmadi.

Before running:
    1. Copy .env.example to .env
    2. Add your Olostep API key to .env
    3. pip install -r requirements.txt

Tested with: Python 3.9+, requests 2.31+, pandas 2.0+, tenacity 8.x
"""

import os
import json
import time
import hashlib

import requests
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_KEY = os.environ.get("OLOSTEP_API_KEY")
if not API_KEY:
    raise EnvironmentError(
        "OLOSTEP_API_KEY is not set. Copy .env.example to .env and add your key."
    )

BASE_URL = "https://api.olostep.com"
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
}


# ---------------------------------------------------------------------------
# 1. Single-page scrape  —  /v1/scrapes
# ---------------------------------------------------------------------------

def single_page_scrape(url: str) -> dict:
    """Scrape a single URL and return Markdown + HTML content."""
    payload = {
        "url_to_scrape": url,
        "formats": ["markdown", "html"],
    }
    response = requests.post(
        f"{BASE_URL}/v1/scrapes", json=payload, headers=HEADERS
    )
    response.raise_for_status()
    data = response.json()

    return {
        "retrieve_id": data["retrieve_id"],
        "markdown": data["result"]["markdown_content"],
        "html": data["result"]["html_content"],
    }


# ---------------------------------------------------------------------------
# 2. Batch submission  —  /v1/batches
# ---------------------------------------------------------------------------

def submit_batch(urls: list[str]) -> str:
    """Submit a list of URLs as a batch job. Returns the batch ID."""
    items = [
        {
            "custom_id": hashlib.sha256(url.encode()).hexdigest()[:12],
            "url": url,
        }
        for url in urls
    ]
    response = requests.post(
        f"{BASE_URL}/v1/batches", json={"items": items}, headers=HEADERS
    )
    response.raise_for_status()
    batch_id = response.json()["id"]
    print(f"Batch submitted: {batch_id} ({len(items)} URLs)")
    return batch_id


# ---------------------------------------------------------------------------
# 3. Batch polling with timeout
# ---------------------------------------------------------------------------

def wait_for_batch(batch_id: str, timeout: int = 600) -> None:
    """Poll batch status until completed, with a timeout guard."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = requests.get(
            f"{BASE_URL}/v1/batches/{batch_id}", headers=HEADERS
        )
        resp.raise_for_status()
        status = resp.json()["status"]

        if status == "completed":
            print("Batch completed.")
            return
        elif status != "in_progress":
            raise RuntimeError(f"Unexpected batch status: {status}")

        print(f"  Status: {status} — waiting 30s...")
        time.sleep(30)

    raise TimeoutError(f"Batch {batch_id} did not complete within {timeout}s")


# ---------------------------------------------------------------------------
# 4. Batch result retrieval  —  /v1/retrieve
# ---------------------------------------------------------------------------

def retrieve_batch_results(batch_id: str) -> list[dict]:
    """Fetch all completed items from a batch using /v1/retrieve."""
    items_resp = requests.get(
        f"{BASE_URL}/v1/batches/{batch_id}/items", headers=HEADERS
    )
    items_resp.raise_for_status()
    batch_items = items_resp.json()["items"]

    results = []
    for item in batch_items:
        if not item.get("retrieve_id"):
            print(f"  Skipping item with no retrieve_id: {item['custom_id']}")
            continue

        retrieve_resp = requests.get(
            f"{BASE_URL}/v1/retrieve",
            params={
                "retrieve_id": item["retrieve_id"],
                "formats": ["markdown"],
            },
            headers=HEADERS,
        )
        retrieve_resp.raise_for_status()
        content = retrieve_resp.json()

        results.append({
            "custom_id": item["custom_id"],
            "url": item["url"],
            "markdown": content.get("markdown_content", ""),
        })

    return results


# ---------------------------------------------------------------------------
# 5. Structured JSON extraction  —  llm_extract (20 credits per request)
# ---------------------------------------------------------------------------

def extract_structured(url: str, schema: dict) -> dict:
    """
    Extract structured data from a URL using llm_extract.

    Note: schema-based llm_extract has a known bug on Olostep's end
    that is actively being fixed. If you get a 500 extraction_error,
    temporarily pass an empty schema ({}) as a workaround.
    """
    payload = {
        "url_to_scrape": url,
        "formats": ["json"],
        "llm_extract": {"schema": schema},
    }
    response = requests.post(
        f"{BASE_URL}/v1/scrapes", json=payload, headers=HEADERS
    )
    response.raise_for_status()
    data = response.json()

    # json_content is a stringified JSON string — must parse it
    raw_json = data["result"]["json_content"]
    return json.loads(raw_json)


# ---------------------------------------------------------------------------
# 6. Web-grounded Q&A  —  /v1/answers (20 credits per request)
# ---------------------------------------------------------------------------

def ask_web(task: str, response_schema: dict | None = None) -> dict:
    """Ask a natural language question grounded on live web data."""
    payload = {"task": task}
    if response_schema:
        payload["json"] = response_schema

    response = requests.post(
        f"{BASE_URL}/v1/answers", json=payload, headers=HEADERS
    )
    response.raise_for_status()
    data = response.json()

    raw = data["result"]["json_content"]
    return json.loads(raw) if raw else data["result"]


# ---------------------------------------------------------------------------
# 7. Retry-wrapped scrape  —  tenacity
# ---------------------------------------------------------------------------

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
)
def scrape_with_retry(url: str) -> dict:
    """Scrape a URL with automatic retry on transient failures."""
    payload = {"url_to_scrape": url, "formats": ["markdown"]}
    response = requests.post(
        f"{BASE_URL}/v1/scrapes", json=payload, headers=HEADERS
    )
    response.raise_for_status()
    return response.json()


# ---------------------------------------------------------------------------
# Main: run the full pipeline
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("STEP 1: Single-page scrape")
    print("=" * 60)
    result = single_page_scrape("https://books.toscrape.com/")
    print(f"  retrieve_id : {result['retrieve_id']}")
    print(f"  Markdown preview : {result['markdown'][:200]}...")
    print()

    print("=" * 60)
    print("STEP 2: Batch pipeline")
    print("=" * 60)
    urls = [
        "https://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html",
        "https://books.toscrape.com/catalogue/tipping-the-velvet_999/index.html",
        "https://books.toscrape.com/catalogue/soumission_998/index.html",
    ]
    batch_id = submit_batch(urls)
    wait_for_batch(batch_id)
    results = retrieve_batch_results(batch_id)

    df = pd.DataFrame(results)
    df.to_csv("scraped_results.csv", index=False)
    print(f"  Saved {len(df)} results to scraped_results.csv")
    print(df[["custom_id", "url"]].to_string(index=False))
    print()

    print("=" * 60)
    print("STEP 3: Structured JSON extraction")
    print("=" * 60)
    schema = {
        "title": {"type": "string"},
        "price": {"type": "string"},
        "availability": {"type": "string"},
        "rating": {"type": "string"},
    }
    try:
        product = extract_structured(
            "https://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html",
            schema,
        )
        print(f"  Extracted: {json.dumps(product, indent=2)}")
        pd.DataFrame([product]).to_csv("products.csv", index=False)
        print("  Saved to products.csv")
    except Exception as e:
        print(f"  llm_extract with schema failed: {e}")
        print("  Note: schema-based llm_extract has a known server-side bug being")
        print("  fixed by Olostep. This step will work once the fix is deployed.")
    print()

    print("=" * 60)
    print("STEP 4: /v1/answers query")
    print("=" * 60)
    answer = ask_web(
        "What is the current population of Cairo, Egypt?",
        {"city": "", "population": "", "source": ""},
    )
    print(f"  Answer: {json.dumps(answer, indent=2)}")
    print()

    print("=" * 60)
    print("STEP 5: Retry-wrapped scrape")
    print("=" * 60)
    retry_result = scrape_with_retry("https://books.toscrape.com/")
    print(f"  retrieve_id: {retry_result['retrieve_id']}")
    print()

    print("Pipeline complete.")


if __name__ == "__main__":
    main()
