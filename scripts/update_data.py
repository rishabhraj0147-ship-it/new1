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
    {"name": "Banking & credit", "keywords": ["bank", "loan", "npa", "credit", "default", "bond yield", "debt"],
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

    return {
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
    s.index = s.index.tz_convert("UTC") if s.index.tz is not None else s.index.tz_localize("UTC")
    return s


def price_block(name: str, ticker: str, region: str, group: str, history=_history) -> dict | None:
    try:
        daily = history(ticker, "1mo", "1d")
        if daily is None or len(daily) == 0:
            return None
        intra = history(ticker, "5d", "15m")
        closes = [float(x) for x in daily.tolist()]
        last = float(intra.iloc[-1]) if intra is not None and len(intra) else closes[-1]
        same_day = intra is not None and len(intra) and daily.index[-1].date() == intra.index[-1].date()
        prev = closes[-2] if len(closes) >= 2 and (same_day or intra is None or not len(intra)) else closes[-1]
        out = {"name": name, "ticker": ticker, "region": region, "group": group, "last": last, "prev": prev,
               "change_pct": (last / prev - 1) * 100 if prev else 0.0, "spark": closes[-22:],
               "as_of": (intra.index[-1] if intra is not None and len(intra) else daily.index[-1]).isoformat()}
        if intra is not None and len(intra):
            out["intraday"] = [[t.isoformat(), round(float(v), 4)] for t, v in intra.items()]
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
    return {"updated": now.isoformat(), "sources": [{"name": s["name"], "region": s["region"], "category": s["category"],
            **{k: v for k, v in health.get(s["name"], {}).items() if k != "url"}} for s in SOURCES],
            "regions": REGIONS, "categories": CATEGORIES, "region_main_index": REGION_MAIN_INDEX,
            "articles": arts[:MAX_STORIES]}


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


if __name__ == "__main__":
    main()
