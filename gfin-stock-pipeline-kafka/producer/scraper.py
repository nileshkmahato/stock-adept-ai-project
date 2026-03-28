"""
=============================================================================
Google Finance Async Scraper
=============================================================================
This module provides an asynchronous web scraper to extract real-time stock 
market data from Google Finance. It uses `aiohttp` for efficient, non-blocking 
HTTP requests and `BeautifulSoup` for HTML parsing.

Key Features:
1. CSS Selector Mapping: Uses predefined extraction rules (`EXTRACT_RULES`) to 
   reliably locate specific financial metrics (price, market cap, PE ratio, etc.).
2. Async Execution: Designed to be called concurrently for multiple tickers 
   to maximize pipeline throughput.
3. Data Cleaning: Parses and formats raw text strings into structured, clean 
   numerical data before returning it as a dictionary payload for Kafka.
=============================================================================
"""
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from common.utils import clean_number, parse_change

HEADERS = {"User-Agent": "Mozilla/5.0"}

EXTRACT_RULES = {
    "price": 'div[jsname="ip75Cb"] div.YMlKec.fxKbKc',
    "change": 'div[jsname="CGyduf"] span.P2Luy',
    "previous_close": 'div.eYanAe > div.gyFHrc:nth-of-type(2) > div',
    "day_range": 'div.eYanAe > div.gyFHrc:nth-of-type(3) > div',
    "year_range": 'div.eYanAe > div.gyFHrc:nth-of-type(4) > div',
    "market_cap": 'div.eYanAe > div.gyFHrc:nth-of-type(5) > div',
    "avg_volume": 'div.eYanAe > div.gyFHrc:nth-of-type(6) > div',
    "pe_ratio": 'div.eYanAe > div.gyFHrc:nth-of-type(7) > div',
}


async def scrape_stock(session, ticker, exchange):
    url = f"https://www.google.com/finance/quote/{ticker}:{exchange}"
    async with session.get(url, headers=HEADERS, timeout=10) as r:
        soup = BeautifulSoup(await r.text(), "html.parser")

    raw = {k: soup.select_one(v).text if soup.select_one(v) else None
           for k, v in EXTRACT_RULES.items()}

    chg, pct = parse_change(raw["change"])

    return {
        "ticker": ticker,
        "exchange": exchange,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "price": clean_number(raw["price"]),
        "price_change": chg,
        "price_change_pct": pct,
        "previous_close": raw["previous_close"],
        "day_range": raw["day_range"],
        "year_range": raw["year_range"],
        "market_cap": raw["market_cap"],
        "avg_volume": raw["avg_volume"],
        "pe_ratio": clean_number(raw["pe_ratio"]),
    }
