#!/usr/bin/env python3
"""
WorldPulse data updater.

GitHub Actions runs this every 15 minutes. It:
  1. fetches news from every source in SOURCES (public RSS feeds),
  2. merges duplicate stories and tags each with its likely market impact,
  3. downloads prices for world indices, commodities and currencies (Yahoo Finance),
  4. writes data/news.json and data/prices.json, which index.html reads.

Everything you might want to change (sources, markets, keyword rules) is in the SETTINGS section below.
Run it yourself:  pip install -r requirements.txt  &&  python scripts/update_data.py
"""
from __future__ import annotations

import hashlib
import html
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import feedparser
import requests

OUT_DIR = os.environ.get("WORLDPULSE_OUT", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data"))
KEEP_HOURS = 72          # stories older than this drop off the site
MAX_STORIES = 1500

# =============================================================================
# SETTINGS
# =============================================================================
APP_NAME = "WorldPulse"

# ---------------------------------------------------------------------------
# NEWS SOURCES  (public RSS feeds – free, no key needed)
# region: where the story is mostly about / published
# category: World, Business, Markets, Tech, Politics
# Add a line here to add a source. It appears on the site after the next update.
# ---------------------------------------------------------------------------
def _gn(topic: str, hl: str, gl: str) -> str:
    return f"https://news.google.com/rss/headlines/section/topic/{topic}?hl={hl}&gl={gl}&ceid={gl}:{hl.split('-')[0]}"

def _gns(query: str, hl: str, gl: str) -> str:
    q = query.replace(" ", "+")
    return f"https://news.google.com/rss/search?q={q}&hl={hl}&gl={gl}&ceid={gl}:{hl.split('-')[0]}"

SOURCES = [
    # --- Google News editions: one feed pulls from thousands of publishers per country ---
    {"name": "Google News · World (US edition)", "url": _gn("WORLD", "en-US", "US"), "region": "Global", "category": "World"},
    {"name": "Google News · Business (US)", "url": _gn("BUSINESS", "en-US", "US"), "region": "United States", "category": "Business"},
    {"name": "Google News · Business (India)", "url": _gn("BUSINESS", "en-IN", "IN"), "region": "India", "category": "Business"},
    {"name": "Google News · World (India)", "url": _gn("WORLD", "en-IN", "IN"), "region": "Global", "category": "World"},
    {"name": "Google News · Business (UK)", "url": _gn("BUSINESS", "en-GB", "GB"), "region": "Europe", "category": "Business"},
    {"name": "Google News · Business (Singapore)", "url": _gn("BUSINESS", "en-SG", "SG"), "region": "Asia", "category": "Business"},
    {"name": "Google News · Business (Australia)", "url": _gn("BUSINESS", "en-AU", "AU"), "region": "Asia", "category": "Business"},
    {"name": "Google News · Technology (US)", "url": _gn("TECHNOLOGY", "en-US", "US"), "region": "Global", "category": "Tech"},
    {"name": "Google News · Stock market (India)", "url": _gns("stock market Sensex Nifty", "en-IN", "IN"), "region": "India", "category": "Markets"},
    {"name": "Google News · Wall Street", "url": _gns("Wall Street stocks", "en-US", "US"), "region": "United States", "category": "Markets"},
    {"name": "Google News · China economy", "url": _gns("China economy markets", "en-US", "US"), "region": "China", "category": "Markets"},
    {"name": "Google News · Japan markets", "url": _gns("Nikkei Japan markets", "en-US", "US"), "region": "Japan", "category": "Markets"},
    {"name": "Google News · Europe markets", "url": _gns("European stocks ECB", "en-GB", "GB"), "region": "Europe", "category": "Markets"},
    {"name": "Google News · Middle East", "url": _gns("Middle East oil Gulf", "en-US", "US"), "region": "Middle East", "category": "World"},
    {"name": "Google News · Commodities", "url": _gns("crude oil gold prices", "en-US", "US"), "region": "Global", "category": "Markets"},
    # --- Direct publisher feeds ---
    {"name": "BBC World", "url": "https://feeds.bbci.co.uk/news/world/rss.xml", "region": "Global", "category": "World"},
    {"name": "BBC Business", "url": "https://feeds.bbci.co.uk/news/business/rss.xml", "region": "Europe", "category": "Business"},
    {"name": "Al Jazeera", "url": "https://www.aljazeera.com/xml/rss/all.xml", "region": "Middle East", "category": "World"},
    {"name": "The Guardian · World", "url": "https://www.theguardian.com/world/rss", "region": "Global", "category": "World"},
    {"name": "The Guardian · Business", "url": "https://www.theguardian.com/business/rss", "region": "Europe", "category": "Business"},
    {"name": "DW (Germany)", "url": "https://rss.dw.com/rdf/rss-en-all", "region": "Europe", "category": "World"},
    {"name": "France 24", "url": "https://www.france24.com/en/rss", "region": "Europe", "category": "World"},
    {"name": "NYT · Business", "url": "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml", "region": "United States", "category": "Business"},
    {"name": "NYT · World", "url": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml", "region": "Global", "category": "World"},
    {"name": "CNBC · World", "url": "https://www.cnbc.com/id/100727362/device/rss/rss.html", "region": "Global", "category": "Markets"},
    {"name": "CNBC · Top news", "url": "https://www.cnbc.com/id/100003114/device/rss/rss.html", "region": "United States", "category": "Markets"},
    {"name": "Economic Times · Markets", "url": "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms", "region": "India", "category": "Markets"},
    {"name": "Livemint · Markets", "url": "https://www.livemint.com/rss/markets", "region": "India", "category": "Markets"},
    {"name": "Business Standard · Markets", "url": "https://www.business-standard.com/rss/markets-106.rss", "region": "India", "category": "Markets"},
    {"name": "Moneycontrol · Latest", "url": "https://www.moneycontrol.com/rss/latestnews.xml", "region": "India", "category": "Markets"},
]

REGIONS = ["Global", "United States", "India", "China", "Japan", "Europe", "Asia", "Middle East"]
CATEGORIES = ["World", "Business", "Markets", "Tech", "Politics"]

# ---------------------------------------------------------------------------
# MARKETS  (Yahoo Finance tickers – free via the yfinance library)
# ---------------------------------------------------------------------------
INDICES = {
    # name: (ticker, region)
    "S&P 500": ("^GSPC", "United States"),
    "Nasdaq": ("^IXIC", "United States"),
    "Dow Jones": ("^DJI", "United States"),
    "Nifty 50": ("^NSEI", "India"),
    "Sensex": ("^BSESN", "India"),
    "Bank Nifty": ("^NSEBANK", "India"),
    "FTSE 100": ("^FTSE", "Europe"),
    "DAX": ("^GDAXI", "Europe"),
    "CAC 40": ("^FCHI", "Europe"),
    "Euro Stoxx 50": ("^STOXX50E", "Europe"),
    "Nikkei 225": ("^N225", "Japan"),
    "Hang Seng": ("^HSI", "China"),
    "Shanghai": ("000001.SS", "China"),
    "KOSPI": ("^KS11", "Asia"),
    "ASX 200": ("^AXJO", "Asia"),
    "Tadawul (Saudi)": ("^TASI.SR", "Middle East"),
}
COMMODITIES_FX = {
    "Brent crude": ("BZ=F", "Global"),
    "WTI crude": ("CL=F", "Global"),
    "Gold": ("GC=F", "Global"),
    "Silver": ("SI=F", "Global"),
    "US 10Y yield": ("^TNX", "United States"),
    "USD/INR": ("INR=X", "India"),
    "EUR/USD": ("EURUSD=X", "Europe"),
    "USD/JPY": ("JPY=X", "Japan"),
    "USD/CNY": ("CNY=X", "China"),
    "Bitcoin": ("BTC-USD", "Global"),
}
# Main index used for the "market reaction" chart of each region
REGION_MAIN_INDEX = {
    "United States": "S&P 500", "India": "Nifty 50", "Europe": "Euro Stoxx 50", "Japan": "Nikkei 225",
    "China": "Hang Seng", "Asia": "KOSPI", "Middle East": "Tadawul (Saudi)", "Global": "S&P 500",
}

# ---------------------------------------------------------------------------
# WHICH COUNTRY / REGION IS A STORY ABOUT?  (keyword → region)
# ---------------------------------------------------------------------------
REGION_KEYWORDS = {
    "United States": ["u.s.", "america", "american", "washington", "wall street", "fed ", "federal reserve",
                      "powell", "white house", "trump", "treasury", "nasdaq", "s&p", "dow"],
    "India": ["india", "indian", "rbi", "sensex", "nifty", "modi", "delhi", "mumbai", "rupee", "sebi", "nse", "bse"],
    "China": ["china", "chinese", "beijing", "yuan", "pboc", "xi jinping", "hong kong", "shanghai", "hang seng"],
    "Japan": ["japan", "japanese", "tokyo", "boj", "bank of japan", "yen", "nikkei"],
    "Europe": ["europe", "european", "ecb", "eurozone", "euro ", "germany", "german", "france", "french", "uk ",
               "britain", "british", "bank of england", "london", "brussels", "eu ", "italy", "spain"],
    "Asia": ["korea", "seoul", "taiwan", "singapore", "australia", "indonesia", "vietnam", "asia", "asean"],
    "Middle East": ["iran", "israel", "saudi", "gulf", "uae", "dubai", "qatar", "iraq", "gaza", "red sea", "yemen"],
}

# ---------------------------------------------------------------------------
# NEWS → SECTOR RULES
# Each rule: keywords that trigger it, and the likely effect on sectors.
# effect: +1 = usually positive, -1 = usually negative, 0 = watch (depends on direction)
# These are simple, transparent rules – "indicative", not advice.
# ---------------------------------------------------------------------------
SECTOR_RULES = [
    {"name": "Oil price", "keywords": ["crude", "oil price", "brent", "opec", "oil supply", "barrel"],
     "sectors": {"Energy / Oil & Gas": 0, "Airlines": 0, "Paints & Chemicals": 0, "Oil marketing (OMCs)": 0}},
    {"name": "Interest rates", "keywords": ["rate cut", "rate hike", "interest rate", "repo rate", "fed ", "federal reserve",
                                             "ecb", "rbi", "bank of japan", "monetary policy", "inflation"],
     "sectors": {"Banks & Financials": 0, "Real estate": 0, "Auto": 0, "Bonds": 0}},
    {"name": "Trade & tariffs", "keywords": ["tariff", "trade war", "export", "import duty", "trade deal", "sanction"],
     "sectors": {"Exporters / IT": 0, "Auto": 0, "Metals": 0, "Manufacturing": 0}},
    {"name": "Geopolitical risk", "keywords": ["war", "attack", "missile", "conflict", "invasion", "strike on", "military", "ceasefire"],
     "sectors": {"Defence": 1, "Gold": 1, "Energy / Oil & Gas": 1, "Airlines": -1, "Broad market": -1}},
    {"name": "Tech & AI", "keywords": ["ai ", "artificial intelligence", "chip", "semiconductor", "nvidia", "data center", "software"],
     "sectors": {"Technology": 0, "Semiconductors": 0, "IT services": 0}},
    {"name": "Currency", "keywords": ["rupee", "dollar index", "yen", "yuan", "euro ", "currency", "forex"],
     "sectors": {"Exporters / IT": 0, "Importers": 0}},
    {"name": "Gold & metals", "keywords": ["gold", "silver", "copper", "steel", "aluminium", "aluminum", "metal"],
     "sectors": {"Metals & Mining": 0, "Gold": 0, "Jewellery": 0}},
    {"name": "Earnings", "keywords": ["earnings", "quarterly results", "profit", "revenue", "q1 ", "q2 ", "q3 ", "q4 ", "guidance"],
     "sectors": {"Company-specific": 0}},
    {"name": "Banking & credit", "keywords": ["banks", "bank stocks", "bank shares", "banking sector", "lender", "npa", "bad loan", "credit growth", "loan growth", "default", "bond yield"],
     "sectors": {"Banks & Financials": 0}},
    {"name": "Auto & EV", "keywords": ["car sales", "auto sales", "electric vehicle", " ev ", "tesla", "automaker"],
     "sectors": {"Auto": 0, "Batteries": 0}},
    {"name": "Pharma & health", "keywords": ["pharma", "drug", "fda", "vaccine", "usfda", "healthcare"],
     "sectors": {"Pharma": 0}},
    {"name": "Government & budget", "keywords": ["budget", "fiscal", "gst", "tax cut", "stimulus", "subsidy", "election"],
     "sectors": {"Broad market": 0, "Infrastructure": 0}},
    {"name": "Crypto", "keywords": ["bitcoin", "crypto", "ethereum", "stablecoin"],
     "sectors": {"Crypto": 0}},
]

# Words that tilt the direction of a story (for equity markets)
POSITIVE_WORDS = ["surge", "soar", "rally", "jump", "gain", "record high", "beat", "rate cut", "cuts rates", "stimulus",
                  "deal", "ceasefire", "upgrade", "boost", "rebound", "strong", "growth", "eases", "cools", "approval"]
NEGATIVE_WORDS = ["plunge", "slump", "crash", "fall", "drop", "tumble", "miss", "rate hike", "raises rates", "recession",
                  "war", "attack", "sanction", "tariff", "downgrade", "default", "layoff", "inflation rises", "selloff",
                  "sell-off", "fear", "crisis", "probe", "ban", "weak", "slowdown", "shutdown"]

# Rule-specific direction hints: if the story says X, the sector move is Y.
DIRECTION_HINTS = {
    "Oil price": {"up": ["rise", "jump", "surge", "spike", "cut output", "supply cut"],
                  "down": ["fall", "drop", "slump", "slip", "output hike", "boost supplies", "ease"],
                  "if_up": {"Energy / Oil & Gas": 1, "Airlines": -1, "Paints & Chemicals": -1, "Oil marketing (OMCs)": -1},
                  "if_down": {"Energy / Oil & Gas": -1, "Airlines": 1, "Paints & Chemicals": 1, "Oil marketing (OMCs)": 1}},
    "Interest rates": {"up": ["hike", "raise", "hawkish", "inflation rises", "higher for longer"],
                       "down": ["cut", "dovish", "ease", "inflation cools", "inflation eases"],
                       "if_up": {"Banks & Financials": 0, "Real estate": -1, "Auto": -1, "Bonds": -1},
                       "if_down": {"Banks & Financials": 1, "Real estate": 1, "Auto": 1, "Bonds": 1}},
    "Trade & tariffs": {"up": ["tariff", "trade war", "sanction", "ban"], "down": ["trade deal", "lift", "ease", "truce"],
                        "if_up": {"Exporters / IT": -1, "Auto": -1, "Metals": -1, "Manufacturing": -1},
                        "if_down": {"Exporters / IT": 1, "Auto": 1, "Metals": 1, "Manufacturing": 1}},
}

# Themes that also point at a specific asset in the "How the market reacted" chart
THEME_ASSETS = {
    "Oil price": ["Brent crude"],
    "Gold & metals": ["Gold"],
    "Currency": ["USD/INR"],
    "Crypto": ["Bitcoin"],
    "Interest rates": ["US 10Y yield"],
}

# Who gains / who loses for each theme. "up"/"down" = direction of the theme (oil up, rates up, tariffs up…);
# "pos"/"neg" = direction of the story's tone when the theme has no up/down hint. "any" = when unclear.
THEME_IMPACT = {
    "Oil price": {
        "up":   (["Oil & gas producers", "Oil-exporting countries"],
                 ["Airlines", "Paints & chemicals", "Oil marketing companies", "Oil importers like India (inflation, rupee)"]),
        "down": (["Airlines", "Paints & chemicals", "Oil marketing companies", "Oil importers like India"],
                 ["Oil & gas producers", "Oil-exporting countries"]),
        "any":  (["Oil producers if prices rise; airlines & paints if they fall"],
                 ["Airlines & paints if prices rise; oil producers if they fall"])},
    "Interest rates": {
        "down": (["Banks & lenders (loan growth)", "Real estate", "Auto (cheaper loans)", "Bond prices"],
                 ["Savers / fixed-deposit returns", "Home currency may weaken"]),
        "up":   (["Bank margins", "Savers / fixed-deposit returns"],
                 ["Real estate", "Auto", "Growth & tech stocks", "Bond prices"]),
        "any":  (["Banks, real estate & auto if rates fall"], ["Real estate, auto & growth stocks if rates rise"])},
    "Trade & tariffs": {
        "up":   (["Domestic producers protected by tariffs"],
                 ["Exporters", "Auto & components", "Metals", "Global trade-linked stocks"]),
        "down": (["Exporters", "Auto & components", "Metals", "Manufacturing"], ["Safe havens like gold"]),
        "any":  (["Exporters if trade eases"], ["Exporters, auto & metals if tariffs rise"])},
    "Geopolitical risk": {
        "calm": (["Airlines & travel", "Broad equity markets", "Oil-importing economies"], ["Defence", "Gold", "Oil producers"]),
        "any":  (["Defence", "Gold", "Oil producers"], ["Airlines & travel", "Broad equity markets", "Oil-importing economies"])},
    "Tech & AI": {
        "pos":  (["Technology & semiconductors", "IT services", "Data-centre & power suppliers"], ["Businesses disrupted by AI"]),
        "neg":  (["Defensive sectors (FMCG, pharma)"], ["Technology & semiconductors", "IT services"]),
        "any":  (["Chip & tech suppliers if demand holds"], ["Tech stocks if spending slows"])},
    "Currency": {
        "any":  (["Exporters & IT services (if home currency weakens)"], ["Importers: oil, electronics (if home currency weakens)"])},
    "Gold & metals": {
        "pos":  (["Metals & mining", "Gold holders"], ["Jewellery makers", "Auto & construction (input costs)"]),
        "neg":  (["Jewellery, auto & construction (cheaper inputs)"], ["Metals & mining"]),
        "any":  (["Metals & mining if prices rise"], ["Jewellery & auto if metal prices rise"])},
    "Earnings": {
        "pos":  (["The company and its sector peers"], []),
        "neg":  ([], ["The company and its sector peers"]),
        "any":  (["The company if results beat estimates"], ["The company if results miss estimates"])},
    "Banking & credit": {
        "pos":  (["Banks & financials"], []),
        "neg":  ([], ["Banks & financials", "Credit-sensitive sectors"]),
        "any":  (["Banks if credit quality holds"], ["Banks if bad loans rise"])},
    "Auto & EV": {
        "pos":  (["Auto makers", "Battery & component suppliers"], []),
        "neg":  ([], ["Auto makers", "Battery & component suppliers"]),
        "any":  (["Auto makers if demand improves"], ["Auto makers if sales weaken"])},
    "Pharma & health": {
        "pos":  (["Pharma & healthcare"], []),
        "neg":  ([], ["Pharma & healthcare"]),
        "any":  (["Pharma if approvals come through"], ["Pharma if regulators push back"])},
    "Government & budget": {
        "pos":  (["Infrastructure & capital goods", "Broad market sentiment"], []),
        "neg":  ([], ["Bond yields / fiscal deficit", "Broad market sentiment"]),
        "any":  (["Sectors that get spending or tax relief"], ["Sectors that lose subsidies or face higher tax"])},
    "Crypto": {
        "pos":  (["Crypto assets & exchanges"], []),
        "neg":  ([], ["Crypto assets & exchanges"]),
        "any":  (["Crypto if sentiment improves"], ["Crypto if regulation tightens"])},
}

# =============================================================================
# NEWS FETCHING
# =============================================================================
UA = "Mozilla/5.0 (compatible; WorldPulse/1.0; personal news reader)"
STOP = set("""a an the of to in on for and or with at by from as is are be was were has have had will would can could
after before over into about amid says say said new more than its it this that what why how who live latest update""".split())


def _clean(text: str) -> str:
    text = html.unescape(re.sub(r"<[^>]+>", " ", text or ""))
    return re.sub(r"\s+", " ", text).strip()


def _when(entry) -> datetime | None:
    for k in ("published_parsed", "updated_parsed"):
        t = entry.get(k)
        if t:
            return datetime(*t[:6], tzinfo=timezone.utc)
    for k in ("published", "updated"):
        if entry.get(k):
            try:
                d = parsedate_to_datetime(entry[k])
                return d.astimezone(timezone.utc) if d.tzinfo else d.replace(tzinfo=timezone.utc)
            except Exception:
                pass
    return None


def story_key(title: str) -> str:
    """Words that identify a story, so the same news from 5 outlets merges into one."""
    words = [w for w in re.findall(r"[a-z0-9]+", title.lower()) if w not in STOP and len(w) > 2]
    return " ".join(sorted(set(words))[:8])


def parse_feed(raw: bytes | str, source: dict) -> list[dict]:
    """Turn one RSS/Atom document into clean article dicts."""
    feed = feedparser.parse(raw)
    out = []
    for e in feed.entries:
        title = _clean(e.get("title", ""))
        link = e.get("link", "")
        if not title or not link:
            continue
        publisher = source["name"]
        # Google News titles look like "Headline - Publisher"
        if "news.google.com" in source["url"] and " - " in title:
            title, publisher = title.rsplit(" - ", 1)
            publisher = f"{publisher.strip()} (via Google News)"
        summary = _clean(e.get("summary", "") or e.get("description", ""))
        if summary.startswith(title[:40]):  # Google News repeats the title in the summary
            summary = ""
        out.append({
            "id": hashlib.sha1(link.encode()).hexdigest()[:16],
            "title": title.strip(),
            "link": link,
            "publisher": publisher,
            "feed": source["name"],
            "summary": summary[:600],
            "published": (_when(e) or datetime.now(timezone.utc)).isoformat(),
            "region": source.get("region", "Global"),
            "category": source.get("category", "World"),
        })
    return out


def fetch_source(source: dict, timeout: int = 12) -> tuple[list[dict], str | None]:
    try:
        r = requests.get(source["url"], headers={"User-Agent": UA}, timeout=timeout)
        r.raise_for_status()
        items = parse_feed(r.content, source)
        return items, None if items else "No stories in feed"
    except Exception as exc:  # network errors, 404s, bad XML …
        return [], f"{type(exc).__name__}: {str(exc)[:120]}"


def fetch_all(sources: list[dict], workers: int = 16, fetcher=fetch_source) -> tuple[list[dict], dict]:
    """Fetch every enabled source in parallel. Returns (articles, health per source)."""
    articles, health = [], {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(fetcher, s): s for s in sources}
        for f in as_completed(futs):
            s = futs[f]
            items, err = f.result()
            health[s["name"]] = {"ok": err is None, "count": len(items), "error": err, "url": s["url"]}
            articles.extend(items)
    return merge_duplicates(articles), health


def merge_duplicates(articles: list[dict]) -> list[dict]:
    """Same story from several outlets → one item with 'also_in' list (more outlets = bigger story)."""
    groups: dict[str, dict] = {}
    by_id: set[str] = set()
    for a in sorted(articles, key=lambda x: x["published"]):
        if a["id"] in by_id:
            continue
        by_id.add(a["id"])
        k = story_key(a["title"])
        if not k:
            k = a["id"]
        if k in groups:
            g = groups[k]
            if a["publisher"] not in g["also_in"] and a["publisher"] != g["publisher"]:
                g["also_in"].append(a["publisher"])
            if not g["summary"] and a["summary"]:
                g["summary"] = a["summary"]
        else:
            a = dict(a)
            a["also_in"] = []
            groups[k] = a
    return sorted(groups.values(), key=lambda x: x["published"], reverse=True)

# =============================================================================
# MARKET IMPACT (keyword rules – indicative, not advice)
# =============================================================================
def _has(text: str, kw: str) -> bool:
    kw = kw.strip().lower()
    tail = r"(?:s|es|ed|d|ing)?" if kw[-1:].isalpha() else ""   # jump → jumps/jumped/jumping
    return re.search(r"(?<![a-z0-9])" + re.escape(kw) + tail + r"(?![a-z0-9])", text) is not None


def _count(text: str, words: list[str]) -> int:
    return sum(1 for w in words if _has(text, w))


def analyze_rules(article: dict) -> dict:
    text = f" {article.get('title', '')} . {article.get('summary', '')} ".lower()
    title = f" {article.get('title', '')} ".lower()

    # where is it about?
    regions = [r for r, kws in REGION_KEYWORDS.items() if any(_has(text, k) for k in kws)]
    if not regions:
        regions = [article.get("region", "Global")]

    # tone for equities
    pos, neg = _count(text, POSITIVE_WORDS), _count(text, NEGATIVE_WORDS)
    tone = 1 if pos > neg else -1 if neg > pos else 0

    # themes and sectors
    themes, sectors = [], {}
    for rule in SECTOR_RULES:
        if not any(_has(text, k) for k in rule["keywords"]):
            continue
        themes.append(rule["name"])
        eff = dict(rule["sectors"])
        hint = DIRECTION_HINTS.get(rule["name"])
        if hint:
            up, down = _count(text, hint["up"]), _count(text, hint["down"])
            if up > down:
                eff.update(hint["if_up"])
            elif down > up:
                eff.update(hint["if_down"])
        for s, v in eff.items():
            if v == 0 and tone and rule["name"] not in DIRECTION_HINTS:
                v = tone
            sectors[s] = v if s not in sectors else (sectors[s] or v)

    markets = []
    for r in regions:
        idx = REGION_MAIN_INDEX.get(r)
        if idx and idx not in markets:
            markets.append(idx)

    for th in themes:  # commodity / currency themes also point at that asset
        for asset in THEME_ASSETS.get(th, []):
            if asset not in markets:
                markets.append(asset)

    # importance: themes hit + how many outlets carry it + market words in the headline
    score = len(themes) * 2 + min(len(article.get("also_in", [])), 6) + 2 * _count(title, [
        "stocks", "shares", "market", "sensex", "nifty", "dow", "nasdaq", "s&p", "index", "oil", "gold", "rupee",
        "fed", "rbi", "ecb", "rate", "inflation", "gdp", "tariff"])
    level = "High" if score >= 8 else "Medium" if score >= 4 else "Low"

    positive, negative = [], []
    for th in themes:
        t = THEME_IMPACT.get(th)
        if not t:
            continue
        hint = DIRECTION_HINTS.get(th)
        key = "any"
        if hint:
            up, down = _count(text, hint["up"]), _count(text, hint["down"])
            key = "up" if up > down else "down" if down > up else "any"
        elif th == "Geopolitical risk":
            calm = _count(text, ["ceasefire", "peace", "truce", "de-escalat", "peace talks", "agreement"])
            hot = _count(text, ["attack", "strike", "missile", "invasion", "escalat", "bomb", "war"])
            key = "calm" if calm > hot else "any"
        elif tone and (("pos" if tone > 0 else "neg") in t):
            key = "pos" if tone > 0 else "neg"
        p, n = t.get(key, t["any"])
        positive += [x for x in p if x not in positive]
        negative += [x for x in n if x not in negative]

    return {
        "crux": crux_of(article),
        "positive": positive[:4],
        "negative": negative[:4],
        "source": "rules",
        "regions": regions,
        "markets": markets,
        "themes": themes,
        "sectors": [{"name": s, "effect": v} for s, v in sectors.items()],
        "tone": tone,
        "score": score,
        "level": level,
        "summary": _explain(themes, tone, regions),
    }


def crux_of(article: dict) -> str:
    """The point of the story in one line: the summary's first sentence if it adds something, else the headline."""
    title = (article.get("title") or "").strip()
    summ = re.sub(r"\s+", " ", article.get("summary") or "").strip()
    if summ and summ.lower()[:60] != title.lower()[:60]:
        first = re.split(r"(?<=[.!?])\s+", summ)[0]
        if 40 <= len(first) <= 260:
            return first
        if len(first) > 260:
            return first[:250].rsplit(" ", 1)[0] + "…"
    return title


def _explain(themes, tone, regions) -> str:
    if not themes:
        return "No direct market theme detected – general news."
    t = {1: "positive", -1: "negative", 0: "mixed / unclear"}[tone]
    return f"Touches {', '.join(themes)} in {', '.join(regions)}. Overall tone for stocks looks {t}."



# =============================================================================
# PRICES
# =============================================================================
def _history(ticker: str, period: str, interval: str):
    import yfinance as yf
    df = yf.Ticker(ticker).history(period=period, interval=interval, auto_adjust=False)
    if df is None or df.empty:
        return None
    s = df["Close"].dropna()
    if s.index.tz is None:              # keep the exchange's own time zone so "today" means the market's today
        s.index = s.index.tz_localize("UTC")
    return s


def price_block(name: str, ticker: str, region: str, group: str, history=_history) -> dict | None:
    try:
        daily = history(ticker, "1mo", "1d")
        if daily is None or len(daily) == 0:
            return None
        intra = history(ticker, "5d", "15m")
        closes = [float(x) for x in daily.tolist()]
        has_intra = intra is not None and len(intra) > 0
        last = float(intra.iloc[-1]) if has_intra else closes[-1]
        last_day = (intra.index[-1] if has_intra else daily.index[-1]).date()
        # previous close = last daily close from a day BEFORE the latest price
        earlier = [float(v) for t, v in daily.items() if t.date() < last_day]
        prev = earlier[-1] if earlier else closes[0]
        out = {"name": name, "ticker": ticker, "region": region, "group": group, "last": last, "prev": prev,
               "change_pct": (last / prev - 1) * 100 if prev else 0.0, "spark": closes[-22:],
               "as_of": (intra.index[-1] if has_intra else daily.index[-1]).tz_convert("UTC").isoformat()}
        if intra is not None and len(intra):
            out["intraday"] = [[t.tz_convert("UTC").isoformat(), round(float(v), 4)] for t, v in intra.items()]
        return out
    except Exception as exc:
        print(f"  price failed {name} ({ticker}): {exc}", file=sys.stderr)
        return None


def fetch_prices(history=_history) -> dict:
    jobs = [(n, t, r, "Index") for n, (t, r) in INDICES.items()] + [(n, t, r, "Macro") for n, (t, r) in COMMODITIES_FX.items()]
    quotes = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        for res in pool.map(lambda j: price_block(*j, history=history), jobs):
            if res:
                quotes[res["name"]] = res
    order = [j[0] for j in jobs]
    return {"quotes": [quotes[n] for n in order if n in quotes]}




# =============================================================================
# TOP PERFORMERS: stocks, ETFs, commodities (Yahoo Finance) and Indian mutual funds (mfapi.in)
# =============================================================================
STOCKS = {
    "India": {
        "RELIANCE.NS": "Reliance Industries", "TCS.NS": "TCS", "HDFCBANK.NS": "HDFC Bank", "ICICIBANK.NS": "ICICI Bank",
        "INFY.NS": "Infosys", "BHARTIARTL.NS": "Bharti Airtel", "SBIN.NS": "State Bank of India", "ITC.NS": "ITC",
        "LT.NS": "Larsen & Toubro", "HINDUNILVR.NS": "Hindustan Unilever", "KOTAKBANK.NS": "Kotak Mahindra Bank",
        "AXISBANK.NS": "Axis Bank", "BAJFINANCE.NS": "Bajaj Finance", "MARUTI.NS": "Maruti Suzuki", "SUNPHARMA.NS": "Sun Pharma",
        "HCLTECH.NS": "HCL Technologies", "M&M.NS": "Mahindra & Mahindra", "TITAN.NS": "Titan", "ULTRACEMCO.NS": "UltraTech Cement",
        "NTPC.NS": "NTPC", "POWERGRID.NS": "Power Grid", "ONGC.NS": "ONGC", "ADANIENT.NS": "Adani Enterprises",
        "ADANIPORTS.NS": "Adani Ports", "ASIANPAINT.NS": "Asian Paints", "WIPRO.NS": "Wipro", "NESTLEIND.NS": "Nestle India",
        "TATASTEEL.NS": "Tata Steel", "JSWSTEEL.NS": "JSW Steel", "COALINDIA.NS": "Coal India", "BAJAJFINSV.NS": "Bajaj Finserv",
        "TECHM.NS": "Tech Mahindra", "HINDALCO.NS": "Hindalco", "GRASIM.NS": "Grasim", "CIPLA.NS": "Cipla",
        "DRREDDY.NS": "Dr Reddy's", "EICHERMOT.NS": "Eicher Motors", "HEROMOTOCO.NS": "Hero MotoCorp", "BRITANNIA.NS": "Britannia",
        "APOLLOHOSP.NS": "Apollo Hospitals", "INDUSINDBK.NS": "IndusInd Bank", "TRENT.NS": "Trent", "BEL.NS": "Bharat Electronics",
        "HAL.NS": "Hindustan Aeronautics", "ETERNAL.NS": "Eternal (Zomato)", "JIOFIN.NS": "Jio Financial", "DMART.NS": "Avenue Supermarts (DMart)",
        "IRCTC.NS": "IRCTC", "TATAPOWER.NS": "Tata Power", "VEDL.NS": "Vedanta",
    },
    "US": {
        "AAPL": "Apple", "MSFT": "Microsoft", "NVDA": "Nvidia", "AMZN": "Amazon", "GOOGL": "Alphabet (Google)", "META": "Meta",
        "TSLA": "Tesla", "AVGO": "Broadcom", "BRK-B": "Berkshire Hathaway", "JPM": "JPMorgan Chase", "V": "Visa",
        "MA": "Mastercard", "LLY": "Eli Lilly", "UNH": "UnitedHealth", "XOM": "Exxon Mobil", "WMT": "Walmart", "COST": "Costco",
        "NFLX": "Netflix", "AMD": "AMD", "ORCL": "Oracle", "CRM": "Salesforce", "ADBE": "Adobe", "KO": "Coca-Cola",
        "PEP": "PepsiCo", "DIS": "Disney", "BAC": "Bank of America", "INTC": "Intel", "PLTR": "Palantir",
    },
    "Global": {
        "TSM": "TSMC (Taiwan)", "ASML": "ASML (Netherlands)", "NVO": "Novo Nordisk (Denmark)", "SAP": "SAP (Germany)",
        "TM": "Toyota (Japan)", "BABA": "Alibaba (China)", "SHEL": "Shell (UK)", "HSBC": "HSBC (UK)", "SONY": "Sony (Japan)",
    },
}
ETFS = {
    "NIFTYBEES.NS": ("Nippon India Nifty 50 BeES", "India"), "JUNIORBEES.NS": ("Nippon India Nifty Next 50 BeES", "India"),
    "BANKBEES.NS": ("Nippon India Bank BeES", "India"), "ITBEES.NS": ("Nippon India IT BeES", "India"),
    "GOLDBEES.NS": ("Nippon India Gold BeES", "India"), "SILVERBEES.NS": ("Nippon India Silver BeES", "India"),
    "MON100.NS": ("Motilal Oswal Nasdaq 100 ETF", "India"), "CPSEETF.NS": ("CPSE ETF", "India"),
    "SPY": ("SPDR S&P 500", "US"), "QQQ": ("Invesco Nasdaq 100", "US"), "VTI": ("Vanguard Total US Market", "US"),
    "VEA": ("Vanguard Developed Markets", "US"), "EEM": ("iShares Emerging Markets", "US"), "INDA": ("iShares MSCI India", "US"),
    "GLD": ("SPDR Gold", "US"), "SLV": ("iShares Silver", "US"), "TLT": ("iShares 20+ Yr Treasury", "US"),
    "XLE": ("Energy Select Sector", "US"), "XLF": ("Financial Select Sector", "US"), "SMH": ("VanEck Semiconductors", "US"),
}
COMMODITIES = {
    "GC=F": "Gold", "SI=F": "Silver", "PL=F": "Platinum", "HG=F": "Copper", "ALI=F": "Aluminium",
    "BZ=F": "Brent crude", "CL=F": "WTI crude", "NG=F": "Natural gas", "ZW=F": "Wheat", "ZC=F": "Corn",
    "ZS=F": "Soybeans", "KC=F": "Coffee", "SB=F": "Sugar", "CC=F": "Cocoa", "CT=F": "Cotton",
}
# Popular Indian mutual funds (Direct plan, Growth option), found by name on mfapi.in once a day.
# (label, category, search queries to try, words the scheme name must contain)
MUTUAL_FUNDS = [
    ("Parag Parikh Flexi Cap", "Flexi cap", ["Parag Parikh Flexi Cap"], ["parag", "flexi"]),
    ("HDFC Flexi Cap", "Flexi cap", ["HDFC Flexi Cap"], ["hdfc", "flexi"]),
    ("Kotak Flexicap", "Flexi cap", ["Kotak Flexicap", "Kotak Flexi Cap", "Kotak"], ["kotak", "flexi"]),
    ("ICICI Prudential Large Cap", "Large cap", ["ICICI Prudential Large Cap", "ICICI Prudential Bluechip"], ["icici", "large cap"]),
    ("Nippon India Large Cap", "Large cap", ["Nippon India Large Cap"], ["nippon", "large cap"]),
    ("SBI Large Cap", "Large cap", ["SBI Large Cap", "SBI Bluechip", "SBI Blue Chip"], ["sbi", "large cap|bluechip|blue chip"]),
    ("Motilal Oswal Midcap", "Mid cap", ["Motilal Oswal Midcap", "Motilal Oswal Mid Cap"], ["motilal", "midcap|mid cap"]),
    ("HDFC Mid Cap", "Mid cap", ["HDFC Mid Cap", "HDFC Mid-Cap"], ["hdfc", "mid cap|mid-cap|midcap"]),
    ("Kotak Midcap", "Mid cap", ["Kotak Midcap", "Kotak Emerging Equity"], ["kotak", "midcap|mid cap|emerging equity"]),
    ("Nippon India Small Cap", "Small cap", ["Nippon India Small Cap"], ["nippon", "small cap"]),
    ("SBI Small Cap", "Small cap", ["SBI Small Cap"], ["sbi", "small cap"]),
    ("Quant Small Cap", "Small cap", ["Quant Small Cap"], ["quant", "small cap"]),
    ("Axis Small Cap", "Small cap", ["Axis Small Cap"], ["axis", "small cap"]),
    ("UTI Nifty 50 Index", "Index", ["UTI Nifty 50 Index"], ["uti", "nifty 50 index"]),
    ("Navi Nifty 50 Index", "Index", ["Navi Nifty 50 Index"], ["navi", "nifty 50 index"]),
    ("Mirae Asset ELSS Tax Saver", "ELSS (tax saver)", ["Mirae Asset ELSS Tax Saver", "Mirae Asset Tax Saver"], ["mirae", "elss|tax saver"]),
    ("Quant ELSS Tax Saver", "ELSS (tax saver)", ["Quant ELSS Tax Saver", "Quant Tax Plan"], ["quant", "elss|tax"]),
    ("ICICI Prudential Equity & Debt", "Hybrid", ["ICICI Prudential Equity & Debt", "ICICI Prudential Equity and Debt", "Equity & Debt"], ["icici", "equity", "debt"]),
    ("HDFC Balanced Advantage", "Hybrid", ["HDFC Balanced Advantage"], ["hdfc", "balanced advantage"]),
    ("ICICI Prudential Technology", "Sectoral", ["ICICI Prudential Technology"], ["icici", "technology"]),
    ("SBI Healthcare Opportunities", "Sectoral", ["SBI Healthcare Opportunities"], ["sbi", "healthcare"]),
    ("Motilal Oswal Nasdaq 100 FoF", "International", ["Motilal Oswal Nasdaq 100", "Nasdaq 100 Fund of Fund", "Nasdaq"], ["motilal", "nasdaq"]),
    ("SBI Gold", "Gold", ["SBI Gold"], ["sbi", "gold"]),
    ("HDFC Corporate Bond", "Debt", ["HDFC Corporate Bond"], ["hdfc", "corporate bond"]),
    ("ICICI Prudential Liquid", "Debt", ["ICICI Prudential Liquid", "Liquid Fund"], ["icici", "liquid"]),
]
MF_REFRESH_HOURS = 12


def _closes(tickers: list[str]):
    """Daily closes for the last year, one column per ticker."""
    import yfinance as yf
    df = yf.download(tickers, period="1y", interval="1d", auto_adjust=True, progress=False, threads=True, group_by="column")
    if df is None or df.empty:
        return None
    c = df["Close"] if "Close" in df.columns.get_level_values(0) else df
    if not hasattr(c, "columns"):
        c = c.to_frame(tickers[0])
    return c


def _perf(series) -> dict | None:
    s = series.dropna()
    if len(s) < 5:
        return None
    last = float(s.iloc[-1])
    def back(n):
        return float(s.iloc[-1 - n]) if len(s) > n else float(s.iloc[0])
    return {"last": last, "d1": (last / back(1) - 1) * 100, "w1": (last / back(5) - 1) * 100,
            "m1": (last / back(21) - 1) * 100, "m6": (last / back(126) - 1) * 100, "y1": (last / float(s.iloc[0]) - 1) * 100,
            "as_of": s.index[-1].strftime("%Y-%m-%d"), "spark": [round(float(x), 4) for x in s.iloc[-60::3]]}


def build_performers(closes=_closes) -> dict:
    groups = []
    for market, book in STOCKS.items():
        groups.append(("stocks", market, {t: (n, market) for t, n in book.items()}))
    groups.append(("etfs", "ETF", ETFS))
    groups.append(("commodities", "Commodity", {t: (n, "Global") for t, n in COMMODITIES.items()}))
    out = {"stocks": [], "etfs": [], "commodities": []}
    for kind, _, book in groups:
        try:
            df = closes(list(book))
        except Exception as exc:
            print(f"  performers failed for {kind}: {exc}", file=sys.stderr)
            continue
        if df is None:
            continue
        for t, (name, market) in book.items():
            if t not in df.columns:
                continue
            p = _perf(df[t])
            if p:
                out[kind].append({"ticker": t, "name": name, "market": market,
                                  "currency": "INR" if t.endswith(".NS") else "USD", **{k: (round(v, 3) if isinstance(v, float) else v) for k, v in p.items()}})
    return out


def _mf_get(url):
    r = requests.get(url, headers={"User-Agent": UA}, timeout=20)
    r.raise_for_status()
    return r.json()


def _mf_returns(rows: list[dict]) -> dict | None:
    pts = []
    for r in rows:
        try:
            d, m, y = r["date"].split("-")
            pts.append((datetime(int(y), int(m), int(d)), float(r["nav"])))
        except Exception:
            continue
    if len(pts) < 30:
        return None
    pts.sort()
    last_d, last_v = pts[-1]
    def at(days):
        target = last_d - timedelta(days=days)
        older = [v for d, v in pts if d <= target]
        return older[-1] if older else None
    def cagr(days, years):
        v = at(days)
        return ((last_v / v) ** (1 / years) - 1) * 100 if v else None
    def simple(days):
        v = at(days)
        return (last_v / v - 1) * 100 if v else None
    res = {"nav": last_v, "nav_date": last_d.strftime("%Y-%m-%d"), "m1": simple(30), "m6": simple(182), "y1": simple(365),
           "y3": cagr(365 * 3, 3), "y5": cagr(365 * 5, 5)}
    res = {k: (round(v, 3) if isinstance(v, float) else v) for k, v in res.items()}
    # weekly NAVs for the last 5 years (for the chart)
    start = last_d - timedelta(days=365 * 5)
    hist, nxt = [], start
    for d, v in pts:
        if d >= nxt:
            hist.append([d.strftime("%Y-%m-%d"), round(v, 3)])
            nxt = d + timedelta(days=7)
    if hist and hist[-1][0] != res["nav_date"]:
        hist.append([res["nav_date"], round(last_v, 3)])
    res["hist"] = hist
    return res


def _pick_scheme(hits: list[dict], label: str, must: list[str]) -> dict | None:
    """Choose the Direct-plan Growth scheme whose name has all required words (shortest name wins)."""
    def norm(x):
        return re.sub(r"[^a-z0-9& ]+", " ", x.lower()).replace("  ", " ")
    allow_index = any(w in label.lower() for w in ("index", "nasdaq", "fof", "fund of fund"))
    ok = []
    for h in hits:
        n = norm(h.get("schemeName", ""))
        if "direct" not in n or "growth" not in n or any(w in n for w in ("idcw", "dividend", "bonus", "segregated")):
            continue
        if not all(any(alt in n for alt in w.split("|")) for w in must):
            continue
        if not allow_index and any(w in n for w in (" index", " etf", "fund of fund", " fof")):
            continue
        if "large" not in label.lower() and ("large and mid" in n or "large & mid" in n):
            continue
        ok.append(h)
    return min(ok, key=lambda h: len(h["schemeName"])) if ok else None


FUNDS_VERSION = 4


def build_funds(get=_mf_get) -> dict | None:
    old = _load(os.path.join(OUT_DIR, "funds.json")) or {}
    same = old.get("v") == FUNDS_VERSION
    if same and old.get("updated") and old.get("funds"):
        age = datetime.now(timezone.utc) - datetime.fromisoformat(old["updated"])
        if age < timedelta(hours=MF_REFRESH_HOURS):
            return None                      # NAVs change once a day – keep the cached file
    codes = {f["query"]: f["code"] for f in old.get("funds", [])} if same else {}
    funds = []
    for label, category, queries, must in MUTUAL_FUNDS:
        try:
            code = codes.get(label)
            if not code:
                for q in queries:
                    pick = _pick_scheme(get("https://api.mfapi.in/mf/search?q=" + requests.utils.quote(q)), label, must)
                    if pick:
                        code = pick["schemeCode"]
                        break
            if not code:
                print(f"  fund not found: {label}", file=sys.stderr)
                continue
            data = get(f"https://api.mfapi.in/mf/{code}")
            ret = _mf_returns(data.get("data", []))
            if not ret:
                continue
            meta = data.get("meta", {})
            funds.append({"query": label, "code": code, "name": meta.get("scheme_name", label), "house": meta.get("fund_house", ""),
                          "category": category, **ret})
        except Exception as exc:
            print(f"  fund failed {label}: {exc}", file=sys.stderr)
    if not funds:
        return None
    return {"v": FUNDS_VERSION, "updated": datetime.now(timezone.utc).isoformat(), "source": "AMFI NAVs via mfapi.in", "funds": funds}



# =============================================================================
# MOST ACTIVE TODAY (intraday volume) – the ~1,200 biggest Indian and US companies
# =============================================================================
def _intraday(tickers):
    import yfinance as yf
    return yf.download(tickers, period="1d", interval="15m", auto_adjust=False, progress=False, threads=True, group_by="column")


def build_active(intraday=_intraday) -> dict | None:
    uni = _load(os.path.join(OUT_DIR, "universe.json"))
    if not uni or not uni.get("rows"):
        return None
    rows = uni["rows"]
    pick = {"India": [r for r in rows if r[2] in ("NSE", "BSE") and len(r) > 11 and r[11] in ("Large cap", "Mid cap", "Small cap")],
            "US": [r for r in rows if r[2] not in ("NSE", "BSE") and len(r) > 11 and r[11] in ("Mega cap", "Large cap")]}
    out = {}
    for market, rs in pick.items():
        res = []
        for i in range(0, len(rs), 200):
            chunk = rs[i:i + 200]
            try:
                df = intraday([r[0] for r in chunk])
            except Exception as exc:
                print(f"  intraday failed ({market}): {exc}", file=sys.stderr)
                continue
            if df is None or df.empty:
                continue
            lv0 = df.columns.get_level_values(0)
            if "Volume" not in lv0 or "Close" not in lv0:
                continue
            V, Cl = df["Volume"], df["Close"]
            for r in chunk:
                t = r[0]
                if t not in V.columns or t not in Cl.columns:
                    continue
                c = Cl[t].dropna()
                if not len(c):
                    continue
                day = c.index[-1].date()
                v = V[t].dropna()
                v = v[[d.date() == day for d in v.index]]
                vol = float(v.sum())
                if vol <= 0:
                    continue
                last = float(c.iloc[-1])
                value = float((c.reindex(v.index).ffill().fillna(last) * v).sum())
                prev = r[3] if r[9] < day.isoformat() else (r[3] / (1 + r[4] / 100) if r[4] is not None else r[3])
                res.append({"t": t, "n": r[1], "x": r[2], "sector": r[10], "size": r[11], "price": round(last, 2),
                            "chg": round((last / prev - 1) * 100, 2) if prev else None, "vol": int(vol), "value": round(value),
                            "surge": round(vol / r[15], 2) if len(r) > 15 and r[15] else None, "day": day.isoformat(),
                            "as_of": c.index[-1].tz_convert("UTC").isoformat() if c.index[-1].tzinfo else c.index[-1].isoformat()})
            time.sleep(0.5)
        keep = {}
        for lst in (sorted(res, key=lambda x: -x["vol"])[:100], sorted(res, key=lambda x: -x["value"])[:100],
                    sorted([x for x in res if x["surge"]], key=lambda x: -x["surge"])[:100]):
            for x in lst:
                keep[x["t"]] = x
        out[market] = sorted(keep.values(), key=lambda x: -x["vol"])
    if not any(out.values()):
        return None
    return {"updated": datetime.now(timezone.utc).isoformat(), **out}

# =============================================================================
# OPTIONAL: CLAUDE WRITES THE CRUX + IMPACT (only if the ANTHROPIC_API_KEY secret is set)
# =============================================================================
AI_MODEL = os.environ.get("WORLDPULSE_MODEL", "claude-haiku-4-5-20251001")
AI_MAX_PER_RUN = 200       # new stories analysed per run (keeps cost low)
AI_PROMPT = """You are a markets analyst writing for busy retail investors in India and worldwide.
For EACH news item below, return:
- "crux": the core of the story in ONE plain sentence (max 30 words), facts only.
- "positive": up to 3 short phrases naming who could GAIN in markets (sectors, assets, countries, or named companies) and why, e.g. "Oil producers – higher realisations".
- "negative": up to 3 short phrases naming who could LOSE and why.
If an item has no real market relevance, use empty lists.
Return ONLY a JSON array: [{"id": "...", "crux": "...", "positive": ["..."], "negative": ["..."]}]

ITEMS:
"""


def ai_enrich(articles: list[dict], previous: dict) -> int:
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    for a in articles:                       # reuse earlier AI notes
        if a["id"] in previous:
            a["ai"] = previous[a["id"]]
    if not key:
        return 0
    todo = [a for a in articles if "ai" not in a]
    todo.sort(key=lambda a: (a["impact"]["score"], a["published"]), reverse=True)
    todo = todo[:AI_MAX_PER_RUN]
    done = 0
    for i in range(0, len(todo), 20):
        batch = todo[i:i + 20]
        items = "\n".join(json.dumps({"id": a["id"], "title": a["title"], "summary": (a.get("summary") or "")[:400]},
                                      ensure_ascii=False) for a in batch)
        try:
            r = requests.post("https://api.anthropic.com/v1/messages", timeout=90, headers={
                "x-api-key": key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                json={"model": AI_MODEL, "max_tokens": 4000, "messages": [{"role": "user", "content": AI_PROMPT + items}]})
            r.raise_for_status()
            text = "".join(c.get("text", "") for c in r.json().get("content", []))
            m = re.search(r"\[.*\]", text, re.S)
            rows = json.loads(m.group(0)) if m else []
        except Exception as exc:
            print(f"  AI batch failed: {exc}", file=sys.stderr)
            continue
        by = {a["id"]: a for a in batch}
        for row in rows:
            a = by.get(str(row.get("id")))
            if a and row.get("crux"):
                a["ai"] = {"crux": str(row["crux"])[:300],
                           "positive": [str(x)[:120] for x in (row.get("positive") or [])][:3],
                           "negative": [str(x)[:120] for x in (row.get("negative") or [])][:3]}
                done += 1
    return done

# =============================================================================
# MAIN
# =============================================================================
def _load(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _write(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, path)


def build_news(fetcher=fetch_source) -> dict:
    fresh, health = fetch_all(SOURCES, fetcher=fetcher)
    now = datetime.now(timezone.utc)
    old = _load(os.path.join(OUT_DIR, "news.json")) or {}
    seen = {a["id"]: a.get("first_seen") for a in old.get("articles", [])}
    keys = {story_key(a["title"]) for a in fresh}
    # keep older stories (still within KEEP_HOURS) that have dropped out of the feeds
    carried = [a for a in old.get("articles", []) if story_key(a["title"]) not in keys]
    cutoff = (now - timedelta(hours=KEEP_HOURS)).isoformat()
    arts = []
    for a in fresh + carried:
        if a["published"] < cutoff:
            continue
        a["first_seen"] = seen.get(a["id"]) or a.get("first_seen") or now.isoformat()
        a["impact"] = analyze_rules(a)
        arts.append(a)
    arts.sort(key=lambda x: x["published"], reverse=True)
    arts = arts[:MAX_STORIES]
    n_ai = ai_enrich(arts, {a["id"]: a["ai"] for a in old.get("articles", []) if a.get("ai")})
    if n_ai:
        print(f"  Claude wrote crux + impact for {n_ai} new stories")
    return {"updated": now.isoformat(), "sources": [{"name": s["name"], "region": s["region"], "category": s["category"],
            **{k: v for k, v in health.get(s["name"], {}).items() if k != "url"}} for s in SOURCES],
            "regions": REGIONS, "categories": CATEGORIES, "region_main_index": REGION_MAIN_INDEX,
            "articles": arts}


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    print("Fetching news…")
    news = build_news()
    ok = sum(1 for s in news["sources"] if s.get("ok"))
    print(f"  {len(news['articles'])} stories, {ok}/{len(SOURCES)} sources ok")
    if ok == 0:
        print("No source responded – keeping the previous news.json", file=sys.stderr)
    else:
        _write(os.path.join(OUT_DIR, "news.json"), news)
    print("Fetching prices…")
    prices = fetch_prices()
    if prices["quotes"]:
        prices["updated"] = datetime.now(timezone.utc).isoformat()
        _write(os.path.join(OUT_DIR, "prices.json"), prices)
    print(f"  {len(prices['quotes'])} prices")
    print("Fetching top performers…")
    perf = build_performers()
    if any(perf.values()):
        perf["updated"] = datetime.now(timezone.utc).isoformat()
        _write(os.path.join(OUT_DIR, "performers.json"), perf)
    print(f"  {len(perf['stocks'])} stocks, {len(perf['etfs'])} ETFs, {len(perf['commodities'])} commodities")
    print("Most active (intraday volume)…")
    try:
        act = build_active()
    except Exception as exc:
        act = None
        print(f"  most-active failed: {exc}", file=sys.stderr)
    if act:
        _write(os.path.join(OUT_DIR, "active.json"), act)
        print(f"  India {len(act.get('India', []))}, US {len(act.get('US', []))}")
    print("Mutual funds…")
    funds = build_funds()
    if funds:
        _write(os.path.join(OUT_DIR, "funds.json"), funds)
        print(f"  {len(funds['funds'])} funds refreshed")
    else:
        print("  using cached fund data")


if __name__ == "__main__":
    main()
