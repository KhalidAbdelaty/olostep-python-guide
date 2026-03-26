# From Web Scraping Scripts to Web Data APIs — Python Examples

Companion code for the DataCamp article **"From Web Scraping Scripts to Web Data APIs: A Practical Python Guide"** by Khalid Alyahmadi.

This repository contains a fully working Python pipeline that demonstrates how to replace fragile scraping scripts with clean, API-driven data extraction using [Olostep](https://www.olostep.com/).

---

## What This Covers

| Step | Description |
|---|---|
| Single-page scrape | Extract Markdown and HTML from any URL via `/v1/scrapes` |
| Batch processing | Submit hundreds of URLs in one request via `/v1/batches` |
| Polling & retrieval | Wait for batch completion and fetch content via `/v1/retrieve` |
| Structured extraction | Pull structured JSON using `llm_extract` |
| Web Q&A | Ask natural language questions grounded on live web data via `/v1/answers` |
| Retry logic | Production-ready retry with exponential backoff via `tenacity` |

---

## Prerequisites

- Python 3.9+
- An [Olostep](https://www.olostep.com/) account — the free tier gives you 500 requests/month with no credit card required

---

## Setup

**1. Clone the repository**

```bash
git clone https://github.com/YOUR_USERNAME/olostep-python-guide.git
cd olostep-python-guide
```

**2. Install dependencies**

```bash
pip install -r requirements.txt
```

**3. Configure your API key**

```bash
cp .env.example .env
```

Open `.env` and replace `your_api_key_here` with your actual Olostep API key, which you can find in your dashboard.

```
OLOSTEP_API_KEY=your_api_key_here
```

---

## Running the Pipeline

```bash
python pipeline.py
```

This runs all five steps in sequence and saves two output files:

- `scraped_results.csv` — Markdown content from the batch job
- `products.csv` — Structured product data from `llm_extract`

---

## Project Structure

```
olostep-python-guide/
├── pipeline.py        # Main script with all examples
├── requirements.txt   # Python dependencies
├── .env.example       # API key template
├── .gitignore
└── README.md
```

---

## Notes

- `llm_extract` costs **20 credits** per request. The free tier includes 500 credits/month.
- `retrieve_id` values are valid for **7 days** from the time of scraping.
- New accounts have a default batch limit of **100 URLs**. Contact Olostep support to raise it to 10,000.
- The test URLs used in this example (`books.toscrape.com`) are a sandbox site specifically designed for scraping practice.

---

## Learn More

- [Olostep Documentation](https://docs.olostep.com)
- [Full Article on DataCamp](https://www.datacamp.com)
