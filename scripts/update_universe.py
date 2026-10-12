#!/usr/bin/env python3
"""
WorldPulse – full stock universe (runs twice a day from .github/workflows/update-universe.yml).

Downloads the official lists of every listed company on:
  • NSE (India)            – nsearchives.nseindia.com EQUITY_L.csv
  • BSE (India)            – BSE's public scrip list (companies not also on NSE)
  • NYSE / NASDAQ / NYSE American (US) – nasdaqtrader.com symbol directories
then fetches one year of daily prices for each from Yahoo Finance and writes data/universe.json
with 1-day, 1-week, 1-month, 6-month and 1-year returns.

For education only – not investment advice.
"""
from __future__ import annotations

import csv
import io
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

OUT_DIR = os.environ.get("WORLDPULSE_OUT", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data"))
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
CHUNK = 150
SKIP_WORDS = ("warrant", "right", " unit", "units", "preferred", "preference", "notes due", "debenture", "%", "when issued",
              "acquisition corp", "subordinated", "trust preferred")


def _get(url, **kw):
    h = {"User-Agent": UA, "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9"}
    h.update(kw.pop("headers", {}))
    r = requests.get(url, headers=h, timeout=40, **kw)
    r.raise_for_status()
    return r


# ----------------------------------------------------------------------------- lists
def nse_list() -> list[dict]:
    for url in ("https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
                "https://archives.nseindia.com/content/equities/EQUITY_L.csv"):
        try:
            rows = list(csv.DictReader(io.StringIO(_get(url, headers={"Referer": "https://www.nseindia.com/"}).text)))
            out = []
            for r in rows:
                r = {k.strip(): (v or "").strip() for k, v in r.items()}
                if r.get("SERIES") not in ("EQ", "BE", "BZ", "SM", "ST"):
                    continue
                out.append({"t": r["SYMBOL"] + ".NS", "n": r.get("NAME OF COMPANY", r["SYMBOL"]).title(), "x": "NSE",
                            "isin": r.get("ISIN NUMBER", "")})
            if out:
                return out
        except Exception as exc:
            print(f"  NSE list failed ({url}): {exc}", file=sys.stderr)
    return []


def bse_list(skip_isins: set[str]) -> list[dict]:
    url = ("https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w?Group=&Scripcode=&industry=&segment=Equity&status=Active")
    try:
        data = _get(url, headers={"Referer": "https://www.bseindia.com/", "Origin": "https://www.bseindia.com"}).json()
    except Exception as exc:
        print(f"  BSE list failed: {exc}", file=sys.stderr)
        return []
    out = []
    for r in data if isinstance(data, list) else data.get("Table", []):
        isin = (r.get("ISIN_NUMBER") or "").strip()
        code = str(r.get("SCRIP_CD") or "").strip()
        if not code or (isin and isin in skip_isins):
            continue
        name = (r.get("Issuer_Name") or r.get("Scrip_Name") or code).strip()
        out.append({"t": code + ".BO", "n": name.title(), "x": "BSE", "isin": isin})
    return out


def us_list() -> list[dict]:
    out = []
    try:
        txt = _get("https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt").text
        for r in csv.DictReader(io.StringIO(txt), delimiter="|"):
            if not r.get("Symbol") or r.get("Test Issue") == "Y" or r.get("ETF") == "Y" or r["Symbol"].startswith("File Creation"):
                continue
            out.append({"t": r["Symbol"], "n": r["Security Name"], "x": "NASDAQ"})
    except Exception as exc:
        print(f"  NASDAQ list failed: {exc}", file=sys.stderr)
    try:
        txt = _get("https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt").text
        exch = {"N": "NYSE", "A": "NYSE American"}
        for r in csv.DictReader(io.StringIO(txt), delimiter="|"):
            if r.get("Exchange") not in exch or r.get("Test Issue") == "Y" or r.get("ETF") == "Y":
                continue
            sym = (r.get("ACT Symbol") or "").strip()
            if not sym or "$" in sym:
                continue
            out.append({"t": sym.replace(".", "-"), "n": r["Security Name"], "x": exch[r["Exchange"]]})
    except Exception as exc:
        print(f"  NYSE list failed: {exc}", file=sys.stderr)
    clean = []
    for r in out:
        low = r["n"].lower()
        if any(w in low for w in SKIP_WORDS):
            continue
        r["n"] = r["n"].split(" - ")[0].replace(" Common Stock", "").replace(" Class A Ordinary Shares", " (Class A)").strip()
        clean.append(r)
    return clean


# ----------------------------------------------------------------------------- sector, size, country
NSE_INDEX = "https://nsearchives.nseindia.com/content/indices/"
NSE_SIZE_LISTS = [("ind_nifty100list.csv", "Large cap"), ("ind_niftymidcap150list.csv", "Mid cap"),
                  ("ind_niftysmallcap250list.csv", "Small cap"), ("ind_niftymicrocap250_list.csv", "Micro cap")]


def _nse_csv(name):
    rows = list(csv.DictReader(io.StringIO(_get(NSE_INDEX + name, headers={"Referer": "https://www.nseindia.com/"}).text)))
    return [{k.strip(): (v or "").strip() for k, v in r.items()} for r in rows]


def india_meta() -> dict:
    """symbol.NS -> {sector, size}. Size follows SEBI's ranks: top 100 large, next 150 mid, next 250 small."""
    meta = {}
    for name, size in NSE_SIZE_LISTS + [("ind_niftytotalmarket_list.csv", None)]:
        try:
            for r in _nse_csv(name):
                t = r.get("Symbol", "") + ".NS"
                m = meta.setdefault(t, {})
                if r.get("Industry"):
                    m["sector"] = r["Industry"]
                if size and "size" not in m:
                    m["size"] = size
        except Exception as exc:
            print(f"  NSE index list failed ({name}): {exc}", file=sys.stderr)
    return meta


def _cap_bucket(cap: float) -> str:
    if cap >= 200e9: return "Mega cap"
    if cap >= 10e9: return "Large cap"
    if cap >= 2e9: return "Mid cap"
    if cap >= 300e6: return "Small cap"
    return "Micro cap"


def us_meta() -> dict:
    """ticker -> {sector, industry, size, country} from the Nasdaq stock screener (covers NYSE, NASDAQ, AMEX)."""
    meta = {}
    try:
        j = _get("https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&offset=0&download=true",
                 headers={"Accept": "application/json, text/plain, */*", "Origin": "https://www.nasdaq.com",
                          "Referer": "https://www.nasdaq.com/"}).json()
        for r in (j.get("data") or {}).get("rows") or []:
            t = (r.get("symbol") or "").strip().replace("/", "-").replace(".", "-")
            try:
                cap = float(str(r.get("marketCap") or "0").replace(",", "") or 0)
            except ValueError:
                cap = 0
            meta[t] = {"sector": (r.get("sector") or "").strip(), "industry": (r.get("industry") or "").strip(),
                       "country": (r.get("country") or "").strip(), "size": _cap_bucket(cap) if cap else ""}
    except Exception as exc:
        print(f"  US screener failed: {exc}", file=sys.stderr)
    return meta


# ----------------------------------------------------------------------------- prices
def _download(tickers):
    import yfinance as yf
    df = yf.download(tickers, period="1y", interval="1d", auto_adjust=True, progress=False, threads=True, group_by="column")
    if df is None or df.empty:
        return None
    c = df["Close"] if "Close" in df.columns.get_level_values(0) else df
    if not hasattr(c, "columns"):
        c = c.to_frame(tickers[0])
    return c


def perf(s):
    s = s.dropna()
    if len(s) < 3:
        return None
    last = float(s.iloc[-1])
    if last <= 0:
        return None
    def back(n):
        return float(s.iloc[-1 - n]) if len(s) > n else float(s.iloc[0])
    r = lambda b: round((last / b - 1) * 100, 2) if b else None
    return {"p": round(last, 2), "d1": r(back(1)), "w1": r(back(5)), "m1": r(back(21)), "m6": r(back(126)),
            "y1": r(float(s.iloc[0])) if len(s) > 200 else None, "as_of": s.index[-1].strftime("%Y-%m-%d")}


N_SHARDS = 32


def shard_of(ticker: str) -> int:
    """Same rule as index.html: sum of character codes, modulo 32."""
    return sum(ord(c) for c in ticker) % N_SHARDS


def _series(s) -> list:
    s = s.dropna()
    vals = [round(float(v), 2) if abs(v) >= 1 else round(float(v), 4) for v in s.tolist()]
    return [s.index[0].strftime("%Y-%m-%d"), s.index[-1].strftime("%Y-%m-%d"), vals]


def build(listings: list[dict], download=_download, pause=1.0, meta=None, shards=None) -> list[list]:
    rows, ok = [], 0
    meta = meta or {}
    for i in range(0, len(listings), CHUNK):
        chunk = listings[i:i + CHUNK]
        try:
            df = download([x["t"] for x in chunk])
        except Exception as exc:
            print(f"  chunk {i // CHUNK} failed: {exc}", file=sys.stderr)
            df = None
        if df is not None:
            for x in chunk:
                if x["t"] in df.columns:
                    p = perf(df[x["t"]])
                    if p:
                        m = meta.get(x["t"], {})
                        rows.append([x["t"], x["n"][:70], x["x"], p["p"], p["d1"], p["w1"], p["m1"], p["m6"], p["y1"], p["as_of"],
                                     m.get("sector") or "", m.get("size") or ("Below top 750" if x["x"] == "NSE" else ""), m.get("country") or ("India" if x["x"] in ("NSE", "BSE") else ""),
                                     m.get("industry") or ""])
                        if shards is not None:
                            shards.setdefault(shard_of(x["t"]), {})[x["t"]] = _series(df[x["t"]])
                        ok += 1
        if (i // CHUNK) % 10 == 0:
            print(f"  {i + len(chunk)}/{len(listings)} checked, {ok} with prices")
        time.sleep(pause)
    return rows


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    print("Downloading company lists…")
    nse = nse_list()
    bse = bse_list({x["isin"] for x in nse if x.get("isin")})
    us = us_list()
    print(f"  NSE {len(nse)}, BSE-only {len(bse)}, US {len(us)}")
    listings = nse + bse + us
    if not listings:
        print("No lists could be downloaded – keeping the previous universe.json", file=sys.stderr)
        return
    print("Sector / size data…")
    meta = {**india_meta(), **us_meta()}
    print(f"  {len(meta)} companies with sector or size")
    shards = {}
    rows = build(listings, meta=meta, shards=shards)
    if len(rows) < 100:
        print("Too few prices – keeping the previous universe.json", file=sys.stderr)
        return
    counts = {}
    for r in rows:
        counts[r[2]] = counts.get(r[2], 0) + 1
    data = {"updated": datetime.now(timezone.utc).isoformat(), "count": len(rows), "by_exchange": counts,
            "cols": ["ticker", "name", "exchange", "price", "d1", "w1", "m1", "m6", "y1", "as_of", "sector", "size", "country", "industry"],
            "shards": N_SHARDS, "rows": rows}
    tmp = os.path.join(OUT_DIR, "universe.json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, os.path.join(OUT_DIR, "universe.json"))
    cdir = os.path.join(OUT_DIR, "charts")
    os.makedirs(cdir, exist_ok=True)
    for k, v in shards.items():
        with open(os.path.join(cdir, f"{k}.json"), "w", encoding="utf-8") as f:
            json.dump(v, f, separators=(",", ":"))
    print(f"Saved {len(rows)} stocks: {counts}; {sum(1 for r in rows if r[10])} with sector")


if __name__ == "__main__":
    main()
