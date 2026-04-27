#!/usr/bin/env python3
"""
HBMS Trading Screener v6.3 – SP500/DAX/HSI Edition
Fast Polygon OHLC/snapshot data, yfinance fallbacks for unsupported symbols.
"""

import argparse, base64, datetime, io, json, os, re, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote
import numpy as np
import pandas as pd
import yfinance as yf
import requests
from pathlib import Path

requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)


def load_dotenv(path=".env"):
    env_path = Path(path)
    if not env_path.is_absolute():
        env_path = Path(__file__).resolve().parent / env_path
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "").strip()
EODHD_API_KEY = os.getenv("EODHD_API_KEY", "").strip()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
GITHUB_REPO  = "Henryblmb/HBMS-Trading"
GITHUB_FILE  = "data.json"
MAX_WORKERS = int(os.getenv("SCREENER_WORKERS", "16"))

HY_BREADTH_SYMBOLS = [
    "HYG", "JNK", "SJNK", "SHYG", "USHY", "HYLB", "SPHY", "ANGL",
    "FALN", "HYLS", "PHB", "HYGH", "HYHG", "BKLN", "SRLN", "FTSL",
    "LQD", "VCIT", "VCSH", "IGSB", "IGIB", "EMB",
]

RETURN_HISTOGRAM_SYMBOLS = [
    {"t": "SPY", "n": "S&P 500 ETF", "g": "Index ETF"},
    {"t": "QQQ", "n": "Nasdaq 100 ETF", "g": "Index ETF"},
    {"t": "DIA", "n": "Dow Jones ETF", "g": "Index ETF"},
    {"t": "IWM", "n": "Russell 2000 ETF", "g": "Index ETF"},
    {"t": "XLK", "n": "Technology Select Sector", "g": "Sector ETF"},
    {"t": "XLE", "n": "Energy Select Sector", "g": "Sector ETF"},
    {"t": "XLF", "n": "Financials Select Sector", "g": "Sector ETF"},
    {"t": "XLI", "n": "Industrials Select Sector", "g": "Sector ETF"},
    {"t": "XLV", "n": "Health Care Select Sector", "g": "Sector ETF"},
    {"t": "XLY", "n": "Consumer Discretionary", "g": "Sector ETF"},
    {"t": "XLP", "n": "Consumer Staples", "g": "Sector ETF"},
    {"t": "XLU", "n": "Utilities Select Sector", "g": "Sector ETF"},
    {"t": "XLB", "n": "Materials Select Sector", "g": "Sector ETF"},
    {"t": "XLRE", "n": "Real Estate Select Sector", "g": "Sector ETF"},
    {"t": "XLC", "n": "Communication Services", "g": "Sector ETF"},
    {"t": "SOXX", "n": "iShares Semiconductor ETF", "g": "Semiconductors"},
    {"t": "SMH", "n": "VanEck Semiconductor ETF", "g": "Semiconductors"},
    {"t": "^SOX", "n": "PHLX Semiconductor Index", "g": "Semiconductors"},
    {"t": "IGV", "n": "Expanded Tech-Software ETF", "g": "Industry ETF"},
    {"t": "XBI", "n": "Biotech ETF", "g": "Industry ETF"},
    {"t": "KRE", "n": "Regional Banking ETF", "g": "Industry ETF"},
    {"t": "IYR", "n": "US Real Estate ETF", "g": "Industry ETF"},
    {"t": "XME", "n": "Metals & Mining ETF", "g": "Industry ETF"},
    {"t": "OIH", "n": "Oil Services ETF", "g": "Industry ETF"},
    {"t": "XRT", "n": "Retail ETF", "g": "Industry ETF"},
]

OPTIONS_PUT_IV_SYMBOLS = [
    {"t": "DIA", "n": "Dow Jones ETF", "g": "Index ETF"},
    {"t": "IWM", "n": "Russell 2000 ETF", "g": "Index ETF"},
    {"t": "QQQ", "n": "Nasdaq 100 ETF", "g": "Index ETF"},
    {"t": "SPY", "n": "S&P 500 ETF", "g": "Index ETF"},
    {"t": "IGV", "n": "Expanded Tech-Software ETF", "g": "Industry ETF"},
    {"t": "IYR", "n": "US Real Estate ETF", "g": "Industry ETF"},
    {"t": "KRE", "n": "Regional Banking ETF", "g": "Industry ETF"},
    {"t": "OIH", "n": "Oil Services ETF", "g": "Industry ETF"},
    {"t": "XBI", "n": "Biotech ETF", "g": "Industry ETF"},
    {"t": "XME", "n": "Metals & Mining ETF", "g": "Industry ETF"},
    {"t": "XRT", "n": "Retail ETF", "g": "Industry ETF"},
    {"t": "XLB", "n": "Materials Select Sector", "g": "Sector ETF"},
    {"t": "XLC", "n": "Communication Services", "g": "Sector ETF"},
    {"t": "XLE", "n": "Energy Select Sector", "g": "Sector ETF"},
    {"t": "XLF", "n": "Financials Select Sector", "g": "Sector ETF"},
    {"t": "XLI", "n": "Industrials Select Sector", "g": "Sector ETF"},
    {"t": "XLK", "n": "Technology Select Sector", "g": "Sector ETF"},
    {"t": "XLP", "n": "Consumer Staples", "g": "Sector ETF"},
    {"t": "XLU", "n": "Utilities Select Sector", "g": "Sector ETF"},
    {"t": "XLV", "n": "Health Care Select Sector", "g": "Sector ETF"},
    {"t": "XLY", "n": "Consumer Discretionary", "g": "Sector ETF"},
    {"t": "SOXX", "n": "iShares Semiconductor ETF", "g": "Semiconductors"},
]

TOP_100_US_OPTIONS_SYMBOLS = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "GOOG", "AMZN", "META", "BRK.B", "AVGO", "TSLA",
    "LLY", "JPM", "WMT", "V", "ORCL", "MA", "XOM", "NFLX", "COST", "JNJ",
    "PG", "HD", "ABBV", "BAC", "KO", "PLTR", "PM", "UNH", "GE", "CSCO",
    "IBM", "WFC", "CRM", "ABT", "LIN", "MCD", "MRK", "AMD", "MS", "T",
    "AXP", "GS", "DIS", "RTX", "PEP", "UBER", "NOW", "INTU", "BX", "VZ",
    "CAT", "QCOM", "BKNG", "TMO", "ISRG", "SCHW", "BA", "SPGI", "TXN", "AMGN",
    "NEE", "PGR", "BLK", "HON", "C", "PFE", "SYK", "DHR", "UNP", "LOW",
    "AMAT", "DE", "ADBE", "TJX", "GILD", "PANW", "ETN", "BSX", "CMCSA", "ADP",
    "COP", "VRTX", "LMT", "MU", "ADI", "KLAC", "CB", "MDT", "MMC", "SBUX",
    "NKE", "UPS", "BMY", "SO", "MO", "ELV", "ICE", "CME", "APH", "MCO",
]


class PolygonClient:
    BASE = "https://api.polygon.io"

    def __init__(self, api_key):
        self.api_key = api_key
        self.session = requests.Session()

    @property
    def enabled(self):
        return bool(self.api_key)

    def get(self, path, params=None, retries=3, timeout=25):
        if not self.enabled:
            return None
        params = dict(params or {})
        params["apiKey"] = self.api_key
        url = self.BASE + path
        for attempt in range(retries):
            try:
                r = self.session.get(url, params=params, timeout=timeout)
                if r.status_code == 429 and attempt < retries - 1:
                    time.sleep(1.5 + attempt * 2)
                    continue
                if r.status_code >= 400:
                    if attempt < retries - 1 and r.status_code in (408, 429, 500, 502, 503, 504):
                        time.sleep(1 + attempt * 2)
                        continue
                    return None
                return r.json()
            except Exception:
                if attempt < retries - 1:
                    time.sleep(1 + attempt * 2)
        return None

    def get_url(self, url, retries=3, timeout=30):
        if not self.enabled:
            return None
        sep = "&" if "?" in url else "?"
        if "apiKey=" not in url:
            url = f"{url}{sep}apiKey={self.api_key}"
        for attempt in range(retries):
            try:
                r = self.session.get(url, timeout=timeout)
                if r.status_code == 429 and attempt < retries - 1:
                    time.sleep(1.5 + attempt * 2)
                    continue
                if r.status_code >= 400:
                    if attempt < retries - 1 and r.status_code in (408, 429, 500, 502, 503, 504):
                        time.sleep(1 + attempt * 2)
                        continue
                    return None
                return r.json()
            except Exception:
                if attempt < retries - 1:
                    time.sleep(1 + attempt * 2)
        return None

    def aggs(self, ticker, years=2, multiplier=1, timespan="day"):
        end = datetime.date.today()
        start = end - datetime.timedelta(days=int(years * 370) + 10)
        path = f"/v2/aggs/ticker/{quote(ticker, safe=':')}/range/{multiplier}/{timespan}/{start}/{end}"
        data = self.get(path, {"adjusted": "true", "sort": "asc", "limit": 50000})
        rows = (data or {}).get("results") or []
        if not rows:
            return None
        df = pd.DataFrame(rows)
        df["Date"] = pd.to_datetime(df["t"], unit="ms").dt.tz_localize(None)
        df = df.rename(columns={"o": "Open", "h": "High", "l": "Low", "c": "Close", "v": "Volume"})
        if "Volume" not in df.columns:
            df["Volume"] = 0
        return df.set_index("Date")[["Open", "High", "Low", "Close", "Volume"]].sort_index()

    def dividends(self, ticker, start_date, end_date):
        if not self.enabled:
            return []
        params = {
            "ticker": ticker,
            "ex_dividend_date.gte": str(start_date),
            "ex_dividend_date.lte": str(end_date),
            "sort": "ex_dividend_date",
            "order": "asc",
            "limit": 1000,
        }
        data = self.get("/v3/reference/dividends", params, timeout=30)
        rows = list((data or {}).get("results") or [])
        next_url = (data or {}).get("next_url")
        while next_url:
            data = self.get_url(next_url, timeout=30)
            rows.extend((data or {}).get("results") or [])
            next_url = (data or {}).get("next_url")
        return rows

    def reference_tickers(self, market="stocks", active=True, ticker_type="CS", limit=1000):
        if not self.enabled:
            return []
        params = {
            "market": market,
            "active": str(active).lower(),
            "limit": limit,
            "sort": "ticker",
            "order": "asc",
        }
        if ticker_type:
            params["type"] = ticker_type
        data = self.get("/v3/reference/tickers", params, timeout=30)
        rows = list((data or {}).get("results") or [])
        next_url = (data or {}).get("next_url")
        while next_url:
            data = self.get_url(next_url, timeout=30)
            rows.extend((data or {}).get("results") or [])
            next_url = (data or {}).get("next_url")
        return rows

    def full_stock_snapshots(self, tickers):
        snapshots = {}
        clean = [polygon_stock_symbol(t) for t in tickers if polygon_stock_symbol(t)]
        if not clean:
            data = self.get(
                "/v2/snapshot/locale/us/markets/stocks/tickers",
                {"include_otc": "false"},
                timeout=45,
            )
            for row in (data or {}).get("tickers") or []:
                if row.get("ticker"):
                    snapshots[row["ticker"]] = row
            return snapshots
        for i in range(0, len(clean), 250):
            chunk = clean[i:i + 250]
            data = self.get(
                "/v2/snapshot/locale/us/markets/stocks/tickers",
                {"tickers": ",".join(chunk), "include_otc": "false"},
                timeout=30,
            )
            for row in (data or {}).get("tickers") or []:
                if row.get("ticker"):
                    snapshots[row["ticker"]] = row
        return snapshots


POLYGON = PolygonClient(POLYGON_API_KEY)
STOCK_SNAPSHOTS = {}


class EODHDClient:
    BASE = "https://eodhd.com/api"

    def __init__(self, api_key):
        self.api_key = api_key
        self.session = requests.Session()

    @property
    def enabled(self):
        return bool(self.api_key)

    def eod_history(self, symbols, years=5, period="d", start_date=None, end_date=None):
        if not self.enabled:
            return None, "none"
        end = end_date or datetime.date.today()
        start = start_date or (end - datetime.timedelta(days=int(years * 370) + 10))
        for symbol in symbols:
            try:
                url = f"{self.BASE}/eod/{quote(symbol)}"
                params = {
                    "api_token": self.api_key,
                    "fmt": "json",
                    "period": period,
                    "order": "a",
                    "from": str(start),
                    "to": str(end),
                }
                r = self.session.get(url, params=params, timeout=30)
                if r.status_code >= 400:
                    continue
                data = r.json()
                if not isinstance(data, list) or not data:
                    continue
                df = pd.DataFrame(data)
                if "date" not in df.columns or "close" not in df.columns:
                    continue
                close_col = "adjusted_close" if "adjusted_close" in df.columns else "close"
                df["Date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
                df["Close"] = pd.to_numeric(df[close_col], errors="coerce")
                if "open" in df.columns:
                    df["Open"] = pd.to_numeric(df["open"], errors="coerce")
                else:
                    df["Open"] = df["Close"]
                if "high" in df.columns:
                    df["High"] = pd.to_numeric(df["high"], errors="coerce")
                else:
                    df["High"] = df["Close"]
                if "low" in df.columns:
                    df["Low"] = pd.to_numeric(df["low"], errors="coerce")
                else:
                    df["Low"] = df["Close"]
                df["Volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0)
                df = df.set_index("Date")[["Open", "High", "Low", "Close", "Volume"]].dropna(subset=["Close"]).sort_index()
                if len(df) >= 200:
                    return df, f"eodhd:{symbol}"
            except Exception:
                continue
        return None, "none"

    def intraday_history(self, symbols, years=20, interval="1h", start_date=None, end_date=None):
        if not self.enabled:
            return None, "none"
        end = end_date or datetime.date.today()
        max_days = 7600 if interval == "1h" else (600 if interval == "5m" else 120)
        lookback_days = min(int(years * 365.25) + 10, max_days)
        start = start_date or (end - datetime.timedelta(days=lookback_days))
        start_ts = int(datetime.datetime.combine(start, datetime.time.min, tzinfo=datetime.timezone.utc).timestamp())
        end_ts = int(datetime.datetime.combine(end + datetime.timedelta(days=1), datetime.time.min, tzinfo=datetime.timezone.utc).timestamp())
        for symbol in symbols:
            try:
                url = f"{self.BASE}/intraday/{quote(symbol)}"
                params = {
                    "api_token": self.api_key,
                    "fmt": "json",
                    "interval": interval,
                    "from": start_ts,
                    "to": end_ts,
                }
                r = self.session.get(url, params=params, timeout=45)
                if r.status_code >= 400:
                    continue
                data = r.json()
                if not isinstance(data, list) or not data:
                    continue
                df = pd.DataFrame(data)
                if "datetime" not in df.columns:
                    continue
                df["Date"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce").dt.tz_convert(None)
                for src, dst in (("open", "Open"), ("high", "High"), ("low", "Low"), ("close", "Close")):
                    df[dst] = pd.to_numeric(df.get(src), errors="coerce")
                df["Volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0)
                df = df.set_index("Date")[["Open", "High", "Low", "Close", "Volume"]].dropna(subset=["Close"]).sort_index()
                if len(df) >= 200:
                    return df, f"eodhd_intraday_{interval}:{symbol}"
            except Exception:
                continue
        return None, "none"

    def commodity_history(self, code, years=10, interval="daily", start_date=None, end_date=None):
        if not self.enabled:
            return None, "none"
        end = end_date or datetime.date.today()
        start = start_date or (end - datetime.timedelta(days=int(years * 370) + 10))
        try:
            url = f"{self.BASE}/commodities/historical/{quote(code)}"
            params = {
                "api_token": self.api_key,
                "fmt": "json",
                "interval": interval,
                "from": str(start),
                "to": str(end),
            }
            r = self.session.get(url, params=params, timeout=30)
            if r.status_code >= 400:
                return None, "none"
            payload = r.json()
            data = payload.get("data") if isinstance(payload, dict) else payload
            if not isinstance(data, list) or not data:
                return None, "none"
            df = pd.DataFrame(data)
            if "date" not in df.columns or "value" not in df.columns:
                return None, "none"
            df["Date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
            df["Close"] = pd.to_numeric(df["value"], errors="coerce")
            df = df.set_index("Date")[["Close"]].dropna().sort_index()
            return df, f"eodhd_commodity:{code}"
        except Exception:
            return None, "none"

    def options_eod(self, symbol, start_date, end_date, exp_from=None, exp_to=None, limit=1000, offset=0, compact=True, option_type="put"):
        if not self.enabled:
            return []
        try:
            url = f"{self.BASE}/mp/unicornbay/options/eod"
            params = {
                "api_token": self.api_key,
                "filter[underlying_symbol]": symbol,
                "filter[tradetime_from]": str(start_date),
                "filter[tradetime_to]": str(end_date),
                "page[limit]": str(limit),
                "page[offset]": str(offset),
            }
            if option_type:
                params["filter[type]"] = option_type
            if compact:
                params["compact"] = "1"
            if exp_from:
                params["filter[exp_date_from]"] = str(exp_from)
            if exp_to:
                params["filter[exp_date_to]"] = str(exp_to)
            r = self.session.get(url, params=params, timeout=45)
            if r.status_code >= 400:
                return []
            payload = r.json()
            rows = payload.get("data") if isinstance(payload, dict) else payload
            if not isinstance(rows, list):
                return []
            fields = payload.get("meta", {}).get("fields", []) if isinstance(payload, dict) else []
            if compact and fields:
                parsed = []
                for row in rows:
                    if isinstance(row, list):
                        parsed.append({fields[i]: row[i] for i in range(min(len(fields), len(row)))})
                    elif isinstance(row, dict):
                        parsed.append(row.get("attributes", row))
                return parsed
            return [row.get("attributes", row) for row in rows if isinstance(row, dict)]
        except Exception:
            return []


EODHD = EODHDClient(EODHD_API_KEY)


def yf_history(ticker_obj, period, interval, retries=3):
    for attempt in range(retries):
        try:
            df = ticker_obj.history(period=period, interval=interval, auto_adjust=True)
            if df is not None and not df.empty and len(df) > 0:
                return df
            if attempt < retries - 1:
                time.sleep(2 + attempt * 3)
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 + attempt * 3)
            else:
                raise e
    return None


INDEX_MAP = {
    "^GSPC": "I:SPX",
    "^NDX": "I:NDX",
    "^DJI": "I:DJI",
    "^RUT": "I:RUT",
}


def polygon_stock_symbol(ticker):
    if any(x in ticker for x in ("^", "=", ".DE", ".PA", ".SS")):
        return None
    return ticker.replace("-", ".")


def polygon_symbol(ticker):
    if ticker in INDEX_MAP:
        return INDEX_MAP[ticker]
    if ticker.endswith("=X") and len(ticker) == 8:
        return "C:" + ticker.replace("=X", "")
    return polygon_stock_symbol(ticker)


def yf_df(symbol, period="2y", interval="1d"):
    df = yf_history(yf.Ticker(symbol), period, interval)
    if df is None or df.empty:
        return None
    keep = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in df.columns]
    return df[keep].dropna(subset=["Close"]).copy()


def history_df(symbol, years=2, period="2y"):
    pt = polygon_symbol(symbol)
    if pt:
        df = POLYGON.aggs(pt, years=years)
        if df is not None and len(df) >= 20:
            return df, "polygon"
    df = yf_df(symbol, period=period)
    if df is not None and len(df) >= 20:
        return df, "yfinance"
    return None, "none"


def snapshot_price(symbol, fallback_price):
    pt = polygon_stock_symbol(symbol)
    snap = STOCK_SNAPSHOTS.get(pt or "")
    if not snap:
        return fallback_price, None
    price = None
    for block in ("min", "day", "lastTrade"):
        b = snap.get(block) or {}
        price = b.get("c") or b.get("p")
        if price:
            break
    prev = (snap.get("prevDay") or {}).get("c")
    return float(price or fallback_price), float(prev) if prev else None


def fetch_current_sp500():
    csv_urls = [
        "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv",
        "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv",
    ]
    for url in csv_urls:
        try:
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=25, verify=False)
            r.raise_for_status()
            df = pd.read_csv(io.StringIO(r.text))
            rows = []
            for _, row in df.iterrows():
                ticker = str(row.get("Symbol", "")).strip().replace(".", "-")
                name = str(row.get("Security", row.get("Name", ticker))).strip()
                if ticker:
                    rows.append({"t": ticker, "n": name, "g": "SP500"})
            if len(rows) >= 450:
                print(f"  Current S&P 500 loaded from CSV: {len(rows)} symbols")
                return rows
        except Exception as e:
            print(f"  S&P 500 CSV source failed: {e}")
    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        df = tables[0]
        rows = []
        for _, row in df.iterrows():
            ticker = str(row.get("Symbol", "")).strip().replace(".", "-")
            name = str(row.get("Security", ticker)).strip()
            if ticker:
                rows.append({"t": ticker, "n": name, "g": "SP500"})
        if len(rows) >= 450:
            print(f"  Current S&P 500 loaded: {len(rows)} symbols")
            return rows
        print(f"  S&P 500 dynamic list too small ({len(rows)}) -> fallback")
    except Exception as e:
        print(f"  S&P 500 dynamic list failed -> fallback: {e}")
    return get_sp500()


def get_sp500():
    return [
        {"t":"AAPL","n":"Apple","g":"SP500"},{"t":"MSFT","n":"Microsoft","g":"SP500"},
        {"t":"NVDA","n":"Nvidia","g":"SP500"},{"t":"AMZN","n":"Amazon","g":"SP500"},
        {"t":"META","n":"Meta","g":"SP500"},{"t":"GOOGL","n":"Alphabet","g":"SP500"},
        {"t":"TSLA","n":"Tesla","g":"SP500"},{"t":"JPM","n":"JPMorgan","g":"SP500"},
        {"t":"GS","n":"Goldman Sachs","g":"SP500"},{"t":"WFC","n":"Wells Fargo","g":"SP500"},
        {"t":"BAC","n":"Bank of America","g":"SP500"},{"t":"MS","n":"Morgan Stanley","g":"SP500"},
        {"t":"ADBE","n":"Adobe","g":"SP500"},{"t":"AMD","n":"AMD","g":"SP500"},
        {"t":"INTC","n":"Intel","g":"SP500"},{"t":"XOM","n":"Exxon Mobil","g":"SP500"},
        {"t":"PFE","n":"Pfizer","g":"SP500"},{"t":"NFLX","n":"Netflix","g":"SP500"},
        {"t":"COIN","n":"Coinbase","g":"SP500"},{"t":"SNOW","n":"Snowflake","g":"SP500"},
        {"t":"TTD","n":"Trade Desk","g":"SP500"},{"t":"DIS","n":"Disney","g":"SP500"},
        {"t":"UBER","n":"Uber","g":"SP500"},{"t":"CRM","n":"Salesforce","g":"SP500"},
        {"t":"INTU","n":"Intuit","g":"SP500"},{"t":"KO","n":"Coca-Cola","g":"SP500"},
        {"t":"PEP","n":"PepsiCo","g":"SP500"},{"t":"MCD","n":"McDonald's","g":"SP500"},
        {"t":"NKE","n":"Nike","g":"SP500"},{"t":"SBUX","n":"Starbucks","g":"SP500"},
        {"t":"V","n":"Visa","g":"SP500"},{"t":"MA","n":"Mastercard","g":"SP500"},
        {"t":"PYPL","n":"PayPal","g":"SP500"},{"t":"SHOP","n":"Shopify","g":"SP500"},
        {"t":"DUOL","n":"Duolingo","g":"SP500"},{"t":"UNH","n":"UnitedHealth","g":"SP500"},
        {"t":"LLY","n":"Eli Lilly","g":"SP500"},{"t":"ABBV","n":"AbbVie","g":"SP500"},
        {"t":"MRK","n":"Merck","g":"SP500"},{"t":"CVX","n":"Chevron","g":"SP500"},
        {"t":"OXY","n":"Occidental","g":"SP500"},{"t":"CAT","n":"Caterpillar","g":"SP500"},
        {"t":"DE","n":"John Deere","g":"SP500"},{"t":"BA","n":"Boeing","g":"SP500"},
        {"t":"RTX","n":"Raytheon","g":"SP500"},{"t":"LMT","n":"Lockheed Martin","g":"SP500"},
        {"t":"AMAT","n":"Applied Materials","g":"SP500"},{"t":"AVGO","n":"Broadcom","g":"SP500"},
        {"t":"QCOM","n":"Qualcomm","g":"SP500"},{"t":"PLTR","n":"Palantir","g":"SP500"},
        {"t":"DDOG","n":"Datadog","g":"SP500"},{"t":"NET","n":"Cloudflare","g":"SP500"},
        {"t":"ARM","n":"ARM Holdings","g":"SP500"},{"t":"MU","n":"Micron","g":"SP500"},
        {"t":"CRWD","n":"CrowdStrike","g":"SP500"},{"t":"PANW","n":"Palo Alto","g":"SP500"},
        {"t":"NOW","n":"ServiceNow","g":"SP500"},{"t":"WDAY","n":"Workday","g":"SP500"},
        {"t":"MMM","n":"3M","g":"SP500"},{"t":"ABT","n":"Abbott","g":"SP500"},
        {"t":"ACN","n":"Accenture","g":"SP500"},{"t":"AXP","n":"American Express","g":"SP500"},
        {"t":"BLK","n":"BlackRock","g":"SP500"},{"t":"BX","n":"Blackstone","g":"SP500"},
        {"t":"C","n":"Citigroup","g":"SP500"},{"t":"COF","n":"Capital One","g":"SP500"},
        {"t":"COST","n":"Costco","g":"SP500"},{"t":"DHR","n":"Danaher","g":"SP500"},
        {"t":"EMR","n":"Emerson Electric","g":"SP500"},{"t":"F","n":"Ford","g":"SP500"},
        {"t":"GM","n":"General Motors","g":"SP500"},{"t":"HD","n":"Home Depot","g":"SP500"},
        {"t":"HON","n":"Honeywell","g":"SP500"},{"t":"IBM","n":"IBM","g":"SP500"},
        {"t":"JNJ","n":"Johnson & Johnson","g":"SP500"},{"t":"KKR","n":"KKR","g":"SP500"},
        {"t":"LOW","n":"Lowe's","g":"SP500"},{"t":"LULU","n":"Lululemon","g":"SP500"},
        {"t":"MDT","n":"Medtronic","g":"SP500"},{"t":"MRNA","n":"Moderna","g":"SP500"},
        {"t":"MSCI","n":"MSCI Inc","g":"SP500"},{"t":"NEE","n":"NextEra Energy","g":"SP500"},
        {"t":"ORCL","n":"Oracle","g":"SP500"},{"t":"PG","n":"Procter & Gamble","g":"SP500"},
        {"t":"REGN","n":"Regeneron","g":"SP500"},{"t":"SPGI","n":"S&P Global","g":"SP500"},
        {"t":"SYK","n":"Stryker","g":"SP500"},{"t":"TGT","n":"Target","g":"SP500"},
        {"t":"TJX","n":"TJX Companies","g":"SP500"},{"t":"TMO","n":"Thermo Fisher","g":"SP500"},
        {"t":"TXN","n":"Texas Instruments","g":"SP500"},{"t":"UNP","n":"Union Pacific","g":"SP500"},
        {"t":"UPS","n":"UPS","g":"SP500"},{"t":"VRTX","n":"Vertex Pharma","g":"SP500"},
        {"t":"VZ","n":"Verizon","g":"SP500"},{"t":"WMT","n":"Walmart","g":"SP500"},
        {"t":"ZTS","n":"Zoetis","g":"SP500"},{"t":"BRK-B","n":"Berkshire Hathaway","g":"SP500"},
        {"t":"AOS","n":"A.O. Smith","g":"SP500"},{"t":"AES","n":"AES Corp","g":"SP500"},
        {"t":"AFL","n":"Aflac","g":"SP500"},{"t":"A","n":"Agilent","g":"SP500"},
        {"t":"APD","n":"Air Products","g":"SP500"},{"t":"ABNB","n":"Airbnb","g":"SP500"},
        {"t":"ALB","n":"Albemarle","g":"SP500"},{"t":"ARE","n":"Alexandria RE","g":"SP500"},
        {"t":"ALGN","n":"Align Tech","g":"SP500"},{"t":"ALL","n":"Allstate","g":"SP500"},
        {"t":"MO","n":"Altria","g":"SP500"},{"t":"AMCR","n":"Amcor","g":"SP500"},
        {"t":"AEE","n":"Ameren","g":"SP500"},{"t":"AAL","n":"American Airlines","g":"SP500"},
        {"t":"AEP","n":"American Electric","g":"SP500"},{"t":"AIG","n":"American Intl Group","g":"SP500"},
        {"t":"AMT","n":"American Tower","g":"SP500"},{"t":"AWK","n":"American Water","g":"SP500"},
        {"t":"AMP","n":"Ameriprise","g":"SP500"},{"t":"AME","n":"AMETEK","g":"SP500"},
        {"t":"AMGN","n":"Amgen","g":"SP500"},{"t":"APH","n":"Amphenol","g":"SP500"},
        {"t":"ADI","n":"Analog Devices","g":"SP500"},{"t":"ANSS","n":"Ansys","g":"SP500"},
        {"t":"AON","n":"Aon","g":"SP500"},{"t":"APA","n":"APA Corp","g":"SP500"},
        {"t":"APTV","n":"Aptiv","g":"SP500"},{"t":"ACGL","n":"Arch Capital","g":"SP500"},
        {"t":"ADM","n":"Archer-Daniels","g":"SP500"},{"t":"ANET","n":"Arista Networks","g":"SP500"},
        {"t":"AJG","n":"Arthur J Gallagher","g":"SP500"},{"t":"AIZ","n":"Assurant","g":"SP500"},
        {"t":"T","n":"AT&T","g":"SP500"},{"t":"ATO","n":"Atmos Energy","g":"SP500"},
        {"t":"ADSK","n":"Autodesk","g":"SP500"},{"t":"ADP","n":"ADP","g":"SP500"},
        {"t":"AZO","n":"AutoZone","g":"SP500"},{"t":"AVB","n":"AvalonBay","g":"SP500"},
        {"t":"AVY","n":"Avery Dennison","g":"SP500"},{"t":"AXON","n":"Axon Enterprise","g":"SP500"},
        {"t":"BKR","n":"Baker Hughes","g":"SP500"},{"t":"BALL","n":"Ball Corp","g":"SP500"},
        {"t":"BK","n":"BNY Mellon","g":"SP500"},{"t":"BAX","n":"Baxter Intl","g":"SP500"},
        {"t":"BDX","n":"Becton Dickinson","g":"SP500"},{"t":"BBY","n":"Best Buy","g":"SP500"},
        {"t":"BIIB","n":"Biogen","g":"SP500"},{"t":"BSX","n":"Boston Scientific","g":"SP500"},
        {"t":"BMY","n":"Bristol-Myers","g":"SP500"},{"t":"BR","n":"Broadridge","g":"SP500"},
        {"t":"BRO","n":"Brown & Brown","g":"SP500"},{"t":"BLDR","n":"Builders FirstSource","g":"SP500"},
        {"t":"BG","n":"Bunge Global","g":"SP500"},{"t":"CDNS","n":"Cadence Design","g":"SP500"},
        {"t":"CPT","n":"Camden Property","g":"SP500"},{"t":"CPB","n":"Campbell Soup","g":"SP500"},
        {"t":"CAH","n":"Cardinal Health","g":"SP500"},{"t":"KMX","n":"CarMax","g":"SP500"},
        {"t":"CCL","n":"Carnival","g":"SP500"},{"t":"CARR","n":"Carrier Global","g":"SP500"},
        {"t":"CBOE","n":"Cboe Global","g":"SP500"},{"t":"CBRE","n":"CBRE Group","g":"SP500"},
        {"t":"CDW","n":"CDW Corp","g":"SP500"},{"t":"CNC","n":"Centene","g":"SP500"},
        {"t":"CNP","n":"CenterPoint","g":"SP500"},{"t":"CF","n":"CF Industries","g":"SP500"},
        {"t":"SCHW","n":"Charles Schwab","g":"SP500"},{"t":"CHTR","n":"Charter Comm","g":"SP500"},
        {"t":"CMG","n":"Chipotle","g":"SP500"},{"t":"CB","n":"Chubb","g":"SP500"},
        {"t":"CHD","n":"Church & Dwight","g":"SP500"},{"t":"CI","n":"Cigna","g":"SP500"},
        {"t":"CINF","n":"Cincinnati Financial","g":"SP500"},{"t":"CTAS","n":"Cintas","g":"SP500"},
        {"t":"CSCO","n":"Cisco","g":"SP500"},{"t":"CFG","n":"Citizens Financial","g":"SP500"},
        {"t":"CLX","n":"Clorox","g":"SP500"},{"t":"CME","n":"CME Group","g":"SP500"},
        {"t":"CMS","n":"CMS Energy","g":"SP500"},{"t":"CTSH","n":"Cognizant","g":"SP500"},
        {"t":"CL","n":"Colgate-Palmolive","g":"SP500"},{"t":"CMCSA","n":"Comcast","g":"SP500"},
        {"t":"COP","n":"ConocoPhillips","g":"SP500"},{"t":"ED","n":"Consolidated Edison","g":"SP500"},
        {"t":"STZ","n":"Constellation Brands","g":"SP500"},{"t":"CEG","n":"Constellation Energy","g":"SP500"},
        {"t":"CPRT","n":"Copart","g":"SP500"},{"t":"GLW","n":"Corning","g":"SP500"},
        {"t":"CTVA","n":"Corteva","g":"SP500"},{"t":"CSGP","n":"CoStar Group","g":"SP500"},
        {"t":"CTRA","n":"Coterra Energy","g":"SP500"},{"t":"CCI","n":"Crown Castle","g":"SP500"},
        {"t":"CSX","n":"CSX","g":"SP500"},{"t":"CMI","n":"Cummins","g":"SP500"},
        {"t":"CVS","n":"CVS Health","g":"SP500"},{"t":"DECK","n":"Deckers Outdoor","g":"SP500"},
        {"t":"DAL","n":"Delta Air Lines","g":"SP500"},{"t":"DVN","n":"Devon Energy","g":"SP500"},
        {"t":"DXCM","n":"Dexcom","g":"SP500"},{"t":"FANG","n":"Diamondback Energy","g":"SP500"},
        {"t":"DLR","n":"Digital Realty","g":"SP500"},{"t":"DFS","n":"Discover Financial","g":"SP500"},
        {"t":"DG","n":"Dollar General","g":"SP500"},{"t":"DLTR","n":"Dollar Tree","g":"SP500"},
        {"t":"D","n":"Dominion Energy","g":"SP500"},{"t":"DPZ","n":"Domino's","g":"SP500"},
        {"t":"DOV","n":"Dover","g":"SP500"},{"t":"DHI","n":"D.R. Horton","g":"SP500"},
        {"t":"DTE","n":"DTE Energy","g":"SP500"},{"t":"DUK","n":"Duke Energy","g":"SP500"},
        {"t":"DD","n":"DuPont","g":"SP500"},{"t":"ETN","n":"Eaton","g":"SP500"},
        {"t":"EBAY","n":"eBay","g":"SP500"},{"t":"ECL","n":"Ecolab","g":"SP500"},
        {"t":"EIX","n":"Edison Intl","g":"SP500"},{"t":"EW","n":"Edwards Lifesciences","g":"SP500"},
        {"t":"EA","n":"Electronic Arts","g":"SP500"},{"t":"ELV","n":"Elevance Health","g":"SP500"},
        {"t":"EMN","n":"Eastman Chemical","g":"SP500"},{"t":"ENPH","n":"Enphase Energy","g":"SP500"},
        {"t":"ETR","n":"Entergy","g":"SP500"},{"t":"EOG","n":"EOG Resources","g":"SP500"},
        {"t":"EQT","n":"EQT Corp","g":"SP500"},{"t":"EFX","n":"Equifax","g":"SP500"},
        {"t":"EQIX","n":"Equinix","g":"SP500"},{"t":"EQR","n":"Equity Residential","g":"SP500"},
        {"t":"ESS","n":"Essex Property","g":"SP500"},{"t":"EL","n":"Estee Lauder","g":"SP500"},
        {"t":"ETSY","n":"Etsy","g":"SP500"},{"t":"EXC","n":"Exelon","g":"SP500"},
        {"t":"EXPE","n":"Expedia","g":"SP500"},{"t":"EXR","n":"Extra Space Storage","g":"SP500"},
        {"t":"FFIV","n":"F5","g":"SP500"},{"t":"FICO","n":"Fair Isaac","g":"SP500"},
        {"t":"FAST","n":"Fastenal","g":"SP500"},{"t":"FRT","n":"Federal Realty","g":"SP500"},
        {"t":"FDX","n":"FedEx","g":"SP500"},{"t":"FIS","n":"Fidelity Natl Info","g":"SP500"},
        {"t":"FITB","n":"Fifth Third","g":"SP500"},{"t":"FSLR","n":"First Solar","g":"SP500"},
        {"t":"FE","n":"FirstEnergy","g":"SP500"},{"t":"FI","n":"Fiserv","g":"SP500"},
        {"t":"FTNT","n":"Fortinet","g":"SP500"},{"t":"FTV","n":"Fortive","g":"SP500"},
        {"t":"FCX","n":"Freeport-McMoRan","g":"SP500"},{"t":"GRMN","n":"Garmin","g":"SP500"},
        {"t":"IT","n":"Gartner","g":"SP500"},{"t":"GE","n":"GE Aerospace","g":"SP500"},
        {"t":"GEHC","n":"GE HealthCare","g":"SP500"},{"t":"GEV","n":"GE Vernova","g":"SP500"},
        {"t":"GNRC","n":"Generac","g":"SP500"},{"t":"GD","n":"General Dynamics","g":"SP500"},
        {"t":"GIS","n":"General Mills","g":"SP500"},{"t":"GPC","n":"Genuine Parts","g":"SP500"},
        {"t":"GILD","n":"Gilead Sciences","g":"SP500"},{"t":"GPN","n":"Global Payments","g":"SP500"},
        {"t":"HAL","n":"Halliburton","g":"SP500"},{"t":"HIG","n":"Hartford Financial","g":"SP500"},
        {"t":"HAS","n":"Hasbro","g":"SP500"},{"t":"HCA","n":"HCA Healthcare","g":"SP500"},
        {"t":"HSY","n":"Hershey","g":"SP500"},{"t":"HES","n":"Hess","g":"SP500"},
        {"t":"HPE","n":"HP Enterprise","g":"SP500"},{"t":"HLT","n":"Hilton","g":"SP500"},
        {"t":"HOLX","n":"Hologic","g":"SP500"},{"t":"HRL","n":"Hormel Foods","g":"SP500"},
        {"t":"HST","n":"Host Hotels","g":"SP500"},{"t":"HWM","n":"Howmet Aerospace","g":"SP500"},
        {"t":"HPQ","n":"HP Inc","g":"SP500"},{"t":"HUBB","n":"Hubbell","g":"SP500"},
        {"t":"HUM","n":"Humana","g":"SP500"},{"t":"HBAN","n":"Huntington Bancshares","g":"SP500"},
        {"t":"HII","n":"Huntington Ingalls","g":"SP500"},{"t":"IEX","n":"IDEX Corp","g":"SP500"},
        {"t":"IDXX","n":"IDEXX Labs","g":"SP500"},{"t":"ITW","n":"Illinois Tool Works","g":"SP500"},
        {"t":"INCY","n":"Incyte","g":"SP500"},{"t":"IR","n":"Ingersoll Rand","g":"SP500"},
        {"t":"PODD","n":"Insulet","g":"SP500"},{"t":"ICE","n":"Intercontinental Exch","g":"SP500"},
        {"t":"IFF","n":"Intl Flavors","g":"SP500"},{"t":"IP","n":"Intl Paper","g":"SP500"},
        {"t":"ISRG","n":"Intuitive Surgical","g":"SP500"},{"t":"IVZ","n":"Invesco","g":"SP500"},
        {"t":"INVH","n":"Invitation Homes","g":"SP500"},{"t":"IQV","n":"IQVIA","g":"SP500"},
        {"t":"IRM","n":"Iron Mountain","g":"SP500"},{"t":"JKHY","n":"Jack Henry","g":"SP500"},
        {"t":"J","n":"Jacobs Solutions","g":"SP500"},{"t":"JCI","n":"Johnson Controls","g":"SP500"},
        {"t":"K","n":"Kellanova","g":"SP500"},{"t":"KVUE","n":"Kenvue","g":"SP500"},
        {"t":"KDP","n":"Keurig Dr Pepper","g":"SP500"},{"t":"KEY","n":"KeyCorp","g":"SP500"},
        {"t":"KEYS","n":"Keysight","g":"SP500"},{"t":"KMB","n":"Kimberly-Clark","g":"SP500"},
        {"t":"KIM","n":"Kimco Realty","g":"SP500"},{"t":"KMI","n":"Kinder Morgan","g":"SP500"},
        {"t":"KLAC","n":"KLA Corp","g":"SP500"},{"t":"KHC","n":"Kraft Heinz","g":"SP500"},
        {"t":"KR","n":"Kroger","g":"SP500"},{"t":"LHX","n":"L3Harris","g":"SP500"},
        {"t":"LH","n":"Labcorp","g":"SP500"},{"t":"LRCX","n":"Lam Research","g":"SP500"},
        {"t":"LW","n":"Lamb Weston","g":"SP500"},{"t":"LVS","n":"Las Vegas Sands","g":"SP500"},
        {"t":"LDOS","n":"Leidos","g":"SP500"},{"t":"LEN","n":"Lennar","g":"SP500"},
        {"t":"LIN","n":"Linde","g":"SP500"},{"t":"LYV","n":"Live Nation","g":"SP500"},
        {"t":"L","n":"Loews","g":"SP500"},{"t":"LYB","n":"LyondellBasell","g":"SP500"},
        {"t":"MTB","n":"M&T Bank","g":"SP500"},{"t":"MRO","n":"Marathon Oil","g":"SP500"},
        {"t":"MPC","n":"Marathon Petroleum","g":"SP500"},{"t":"MAR","n":"Marriott Intl","g":"SP500"},
        {"t":"MMC","n":"Marsh McLennan","g":"SP500"},{"t":"MLM","n":"Martin Marietta","g":"SP500"},
        {"t":"MAS","n":"Masco","g":"SP500"},{"t":"MTCH","n":"Match Group","g":"SP500"},
        {"t":"MKC","n":"McCormick","g":"SP500"},{"t":"MCK","n":"McKesson","g":"SP500"},
        {"t":"MET","n":"MetLife","g":"SP500"},{"t":"MTD","n":"Mettler-Toledo","g":"SP500"},
        {"t":"MGM","n":"MGM Resorts","g":"SP500"},{"t":"MCHP","n":"Microchip Tech","g":"SP500"},
        {"t":"MAA","n":"Mid-America Apt","g":"SP500"},{"t":"MHK","n":"Mohawk Industries","g":"SP500"},
        {"t":"MOH","n":"Molina Healthcare","g":"SP500"},{"t":"TAP","n":"Molson Coors","g":"SP500"},
        {"t":"MDLZ","n":"Mondelez Intl","g":"SP500"},{"t":"MPWR","n":"Monolithic Power","g":"SP500"},
        {"t":"MNST","n":"Monster Beverage","g":"SP500"},{"t":"MCO","n":"Moody's","g":"SP500"},
        {"t":"MOS","n":"Mosaic","g":"SP500"},{"t":"MSI","n":"Motorola Solutions","g":"SP500"},
        {"t":"NDAQ","n":"Nasdaq Inc","g":"SP500"},{"t":"NTAP","n":"NetApp","g":"SP500"},
        {"t":"NEM","n":"Newmont","g":"SP500"},{"t":"NI","n":"NiSource","g":"SP500"},
        {"t":"NOC","n":"Northrop Grumman","g":"SP500"},{"t":"NCLH","n":"Norwegian Cruise","g":"SP500"},
        {"t":"NRG","n":"NRG Energy","g":"SP500"},{"t":"NUE","n":"Nucor","g":"SP500"},
        {"t":"NVR","n":"NVR Inc","g":"SP500"},{"t":"NXPI","n":"NXP Semiconductors","g":"SP500"},
        {"t":"ORLY","n":"O'Reilly Auto","g":"SP500"},{"t":"ODFL","n":"Old Dominion Freight","g":"SP500"},
        {"t":"OMC","n":"Omnicom","g":"SP500"},{"t":"ON","n":"ON Semiconductor","g":"SP500"},
        {"t":"OKE","n":"ONEOK","g":"SP500"},{"t":"OTIS","n":"Otis Worldwide","g":"SP500"},
        {"t":"PCAR","n":"PACCAR","g":"SP500"},{"t":"PKG","n":"Packaging Corp","g":"SP500"},
        {"t":"PARA","n":"Paramount Global","g":"SP500"},{"t":"PH","n":"Parker Hannifin","g":"SP500"},
        {"t":"PAYX","n":"Paychex","g":"SP500"},{"t":"PAYC","n":"Paycom Software","g":"SP500"},
        {"t":"PNR","n":"Pentair","g":"SP500"},{"t":"PCG","n":"PG&E","g":"SP500"},
        {"t":"PM","n":"Philip Morris","g":"SP500"},{"t":"PSX","n":"Phillips 66","g":"SP500"},
        {"t":"PNW","n":"Pinnacle West","g":"SP500"},{"t":"PNC","n":"PNC Financial","g":"SP500"},
        {"t":"POOL","n":"Pool Corp","g":"SP500"},{"t":"PPG","n":"PPG Industries","g":"SP500"},
        {"t":"PPL","n":"PPL Corp","g":"SP500"},{"t":"PFG","n":"Principal Financial","g":"SP500"},
        {"t":"PGR","n":"Progressive","g":"SP500"},{"t":"PLD","n":"Prologis","g":"SP500"},
        {"t":"PRU","n":"Prudential Financial","g":"SP500"},{"t":"PEG","n":"Public Service Ent","g":"SP500"},
        {"t":"PTC","n":"PTC Inc","g":"SP500"},{"t":"PSA","n":"Public Storage","g":"SP500"},
        {"t":"PHM","n":"PulteGroup","g":"SP500"},{"t":"QRVO","n":"Qorvo","g":"SP500"},
        {"t":"PWR","n":"Quanta Services","g":"SP500"},{"t":"DGX","n":"Quest Diagnostics","g":"SP500"},
        {"t":"RL","n":"Ralph Lauren","g":"SP500"},{"t":"RJF","n":"Raymond James","g":"SP500"},
        {"t":"O","n":"Realty Income","g":"SP500"},{"t":"REG","n":"Regency Centers","g":"SP500"},
        {"t":"RF","n":"Regions Financial","g":"SP500"},{"t":"RSG","n":"Republic Services","g":"SP500"},
        {"t":"RMD","n":"ResMed","g":"SP500"},{"t":"ROK","n":"Rockwell Automation","g":"SP500"},
        {"t":"ROL","n":"Rollins","g":"SP500"},{"t":"ROP","n":"Roper Technologies","g":"SP500"},
        {"t":"ROST","n":"Ross Stores","g":"SP500"},{"t":"RCL","n":"Royal Caribbean","g":"SP500"},
        {"t":"SBAC","n":"SBA Communications","g":"SP500"},{"t":"SLB","n":"Schlumberger","g":"SP500"},
        {"t":"STX","n":"Seagate Tech","g":"SP500"},{"t":"SRE","n":"Sempra","g":"SP500"},
        {"t":"SHW","n":"Sherwin-Williams","g":"SP500"},{"t":"SJM","n":"Smucker","g":"SP500"},
        {"t":"SNPS","n":"Synopsys","g":"SP500"},{"t":"SO","n":"Southern Company","g":"SP500"},
        {"t":"SPG","n":"Simon Property","g":"SP500"},{"t":"SWKS","n":"Skyworks","g":"SP500"},
        {"t":"SNA","n":"Snap-on","g":"SP500"},{"t":"STT","n":"State Street","g":"SP500"},
        {"t":"STLD","n":"Steel Dynamics","g":"SP500"},{"t":"STE","n":"Steris","g":"SP500"},
        {"t":"SWK","n":"Stanley Black Decker","g":"SP500"},{"t":"SYF","n":"Synchrony Financial","g":"SP500"},
        {"t":"SYY","n":"Sysco","g":"SP500"},{"t":"TMUS","n":"T-Mobile","g":"SP500"},
        {"t":"TEL","n":"TE Connectivity","g":"SP500"},{"t":"TDY","n":"Teledyne Tech","g":"SP500"},
        {"t":"TER","n":"Teradyne","g":"SP500"},{"t":"TXT","n":"Textron","g":"SP500"},
        {"t":"TSCO","n":"Tractor Supply","g":"SP500"},{"t":"TT","n":"Trane Technologies","g":"SP500"},
        {"t":"TDG","n":"TransDigm","g":"SP500"},{"t":"TRV","n":"Travelers","g":"SP500"},
        {"t":"TRGP","n":"Targa Resources","g":"SP500"},{"t":"TFC","n":"Truist Financial","g":"SP500"},
        {"t":"TYL","n":"Tyler Technologies","g":"SP500"},{"t":"TSN","n":"Tyson Foods","g":"SP500"},
        {"t":"USB","n":"US Bancorp","g":"SP500"},{"t":"UDR","n":"UDR Inc","g":"SP500"},
        {"t":"ULTA","n":"Ulta Beauty","g":"SP500"},{"t":"UAL","n":"United Airlines","g":"SP500"},
        {"t":"URI","n":"United Rentals","g":"SP500"},{"t":"UHS","n":"Universal Health","g":"SP500"},
        {"t":"VLO","n":"Valero Energy","g":"SP500"},{"t":"VTR","n":"Ventas","g":"SP500"},
        {"t":"VRSN","n":"VeriSign","g":"SP500"},{"t":"VRSK","n":"Verisk Analytics","g":"SP500"},
        {"t":"VTRS","n":"Viatris","g":"SP500"},{"t":"VST","n":"Vistra","g":"SP500"},
        {"t":"VMC","n":"Vulcan Materials","g":"SP500"},{"t":"GWW","n":"W.W. Grainger","g":"SP500"},
        {"t":"WAB","n":"Wabtec","g":"SP500"},{"t":"WBA","n":"Walgreens Boots","g":"SP500"},
        {"t":"WBD","n":"Warner Bros Discovery","g":"SP500"},{"t":"WEC","n":"WEC Energy","g":"SP500"},
        {"t":"WELL","n":"Welltower","g":"SP500"},{"t":"WST","n":"West Pharmaceutical","g":"SP500"},
        {"t":"WDC","n":"Western Digital","g":"SP500"},{"t":"WY","n":"Weyerhaeuser","g":"SP500"},
        {"t":"WMB","n":"Williams Companies","g":"SP500"},{"t":"WTW","n":"Willis Towers Watson","g":"SP500"},
        {"t":"WYNN","n":"Wynn Resorts","g":"SP500"},{"t":"XEL","n":"Xcel Energy","g":"SP500"},
        {"t":"XYL","n":"Xylem","g":"SP500"},{"t":"YUM","n":"Yum! Brands","g":"SP500"},
        {"t":"ZBRA","n":"Zebra Technologies","g":"SP500"},{"t":"ZBH","n":"Zimmer Biomet","g":"SP500"},
    ]

def get_dax():
    return [
        {"t":"ADS.DE","n":"Adidas","g":"DAX"},{"t":"AIR.DE","n":"Airbus","g":"DAX"},
        {"t":"ALV.DE","n":"Allianz","g":"DAX"},{"t":"BAS.DE","n":"BASF","g":"DAX"},
        {"t":"BAYN.DE","n":"Bayer","g":"DAX"},{"t":"BEI.DE","n":"Beiersdorf","g":"DAX"},
        {"t":"BMW.DE","n":"BMW","g":"DAX"},{"t":"BNR.DE","n":"Brenntag","g":"DAX"},
        {"t":"CBK.DE","n":"Commerzbank","g":"DAX"},{"t":"CON.DE","n":"Continental","g":"DAX"},
        {"t":"1COV.DE","n":"Covestro","g":"DAX"},{"t":"DTG.DE","n":"Daimler Truck","g":"DAX"},
        {"t":"DBK.DE","n":"Deutsche Bank","g":"DAX"},{"t":"DB1.DE","n":"Deutsche Boerse","g":"DAX"},
        {"t":"DHL.DE","n":"DHL Group","g":"DAX"},{"t":"DTE.DE","n":"Deutsche Telekom","g":"DAX"},
        {"t":"EOAN.DE","n":"E.ON","g":"DAX"},{"t":"FRE.DE","n":"Fresenius","g":"DAX"},
        {"t":"HNR1.DE","n":"Hannover Rueck","g":"DAX"},{"t":"HEI.DE","n":"Heidelberg Materials","g":"DAX"},
        {"t":"HEN3.DE","n":"Henkel","g":"DAX"},{"t":"IFX.DE","n":"Infineon","g":"DAX"},
        {"t":"MBG.DE","n":"Mercedes-Benz","g":"DAX"},{"t":"MRK.DE","n":"Merck KGaA","g":"DAX"},
        {"t":"MTX.DE","n":"MTU Aero Engines","g":"DAX"},{"t":"MUV2.DE","n":"Munich Re","g":"DAX"},
        {"t":"P911.DE","n":"Porsche AG","g":"DAX"},{"t":"PAH3.DE","n":"Porsche SE","g":"DAX"},
        {"t":"QIA.DE","n":"Qiagen","g":"DAX"},{"t":"RHM.DE","n":"Rheinmetall","g":"DAX"},
        {"t":"RWE.DE","n":"RWE","g":"DAX"},{"t":"SAP.DE","n":"SAP","g":"DAX"},
        {"t":"SRT3.DE","n":"Sartorius","g":"DAX"},{"t":"SIE.DE","n":"Siemens","g":"DAX"},
        {"t":"ENR.DE","n":"Siemens Energy","g":"DAX"},{"t":"SHL.DE","n":"Siemens Healthineers","g":"DAX"},
        {"t":"SY1.DE","n":"Symrise","g":"DAX"},{"t":"VOW3.DE","n":"Volkswagen","g":"DAX"},
        {"t":"VNA.DE","n":"Vonovia","g":"DAX"},{"t":"ZAL.DE","n":"Zalando","g":"DAX"},
    ]

def get_hsi():
    def yahoo_hk(code):
        return f"{int(code):04d}.HK"

    fallback = [
        "00001","00002","00003","00005","00006","00011","00012","00016","00017","00027","00066","00101",
        "00175","00241","00267","00288","00291","00316","00322","00386","00388","00669","00688","00700",
        "00762","00823","00836","00857","00868","00881","00939","00941","00960","00968","00981","00992",
        "01024","01038","01044","01088","01093","01099","01109","01113","01177","01209","01211","01299",
        "01378","01398","01810","01818","01876","01928","01929","01997","02015","02020","02057","02269",
        "02313","02318","02319","02331","02333","02382","02388","02628","02688","02899","03328","03690",
        "03692","03968","03988","06030","06098","06618","06690","06862","09618","09626","09633","09863",
        "09868","09888","09961","09988","09999",
    ]
    try:
        url = "https://www.aastocks.com/en/stocks/market/index/hk-index-con.aspx?index=HSI"
        txt = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=20).text
        codes = sorted(set(__import__("re").findall(r"\\b(\\d{5})\\.HK\\b", txt)))
        if len(codes) >= 70:
            print(f"  Hang Seng constituents loaded: {len(codes)} symbols")
            return [{"t": yahoo_hk(c), "n": f"{c}.HK", "g": "HSI"} for c in codes]
    except Exception as e:
        print(f"  HSI dynamic list failed -> fallback: {e}")
    print(f"  Hang Seng fallback list: {len(fallback)} symbols")
    return [{"t": yahoo_hk(c), "n": f"{c}.HK", "g": "HSI"} for c in fallback]

def get_trending_assets():
    return [
        {"t":"^GSPC","n":"S&P 500","g":"Index"},{"t":"^NDX","n":"Nasdaq 100","g":"Index"},
        {"t":"^DJI","n":"Dow Jones","g":"Index"},{"t":"^RUT","n":"Russell 2000","g":"Index"},
        {"t":"^GDAXI","n":"DAX 40","g":"Index"},{"t":"^FTSE","n":"FTSE 100","g":"Index"},
        {"t":"^FCHI","n":"CAC 40","g":"Index"},{"t":"^STOXX50E","n":"Euro Stoxx 50","g":"Index"},
        {"t":"^N225","n":"Nikkei 225","g":"Index"},{"t":"^HSI","n":"Hang Seng","g":"Index"},
        {"t":"000300.SS","n":"CSI 300","g":"Index"},{"t":"^AXJO","n":"ASX 200","g":"Index"},
        {"t":"^BSESN","n":"Sensex India","g":"Index"},{"t":"^KS11","n":"Kospi Korea","g":"Index"},
        {"t":"EURUSD=X","n":"EUR/USD","g":"Forex"},{"t":"GBPUSD=X","n":"GBP/USD","g":"Forex"},
        {"t":"USDJPY=X","n":"USD/JPY","g":"Forex"},{"t":"AUDUSD=X","n":"AUD/USD","g":"Forex"},
        {"t":"USDCHF=X","n":"USD/CHF","g":"Forex"},{"t":"USDCAD=X","n":"USD/CAD","g":"Forex"},
        {"t":"NZDUSD=X","n":"NZD/USD","g":"Forex"},{"t":"EURGBP=X","n":"EUR/GBP","g":"Forex"},
        {"t":"EURJPY=X","n":"EUR/JPY","g":"Forex"},{"t":"GBPJPY=X","n":"GBP/JPY","g":"Forex"},
        {"t":"AUDJPY=X","n":"AUD/JPY","g":"Forex"},{"t":"USDSGD=X","n":"USD/SGD","g":"Forex"},
        {"t":"GC=F","n":"Gold","g":"Metall"},{"t":"SI=F","n":"Silver","g":"Metall"},
        {"t":"PL=F","n":"Platinum","g":"Metall"},{"t":"PA=F","n":"Palladium","g":"Metall"},
        {"t":"CL=F","n":"WTI Oil","g":"Rohstoff"},{"t":"BZ=F","n":"Brent Oil","g":"Rohstoff"},
        {"t":"NG=F","n":"Natural Gas","g":"Rohstoff"},{"t":"HG=F","n":"Copper","g":"Rohstoff"},
    ]

def get_sectors():
    return [
        {"t":"XLK",  "n":"Technology",      "s":"Tech"},
        {"t":"XLV",  "n":"Healthcare",       "s":"Healthcare"},
        {"t":"XLRE", "n":"Real Estate",      "s":"Real Estate"},
        {"t":"XLE",  "n":"Energy",           "s":"Energy"},
        {"t":"XLP",  "n":"Consumer Staples", "s":"Consumer Staples"},
        {"t":"GLD",  "n":"Gold",             "s":"Gold"},
        {"t":"XLF",  "n":"Financials",       "s":"Finance"},
        {"t":"IGV",  "n":"Software",         "s":"Software"},
        {"t":"XLI",  "n":"Industrials",      "s":"Industrial"},
        {"t":"SPY",  "n":"S&P 500",          "s":"Benchmark"},
    ]

# ─── CALCULATIONS ────────────────────────────────────────────────
def calc_rsi(closes, period=14):
    if len(closes) < period + 2:
        return None
    closes = np.array(closes, dtype=float)
    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    ag = np.mean(gains[:period])
    al = np.mean(losses[:period])
    for i in range(period, len(gains)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
    if al == 0:
        return 100.0
    return round(100 - 100 / (1 + ag / al), 1)


def calc_rsi_series(closes, period=14):
    closes = np.array(closes, dtype=float)
    out = [None] * len(closes)
    if len(closes) < period + 2:
        return out
    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    ag = float(np.mean(gains[:period]))
    al = float(np.mean(losses[:period]))
    out[period] = 100.0 if al == 0 else round(100 - 100 / (1 + ag / al), 2)
    for i in range(period, len(gains)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
        out[i + 1] = 100.0 if al == 0 else round(100 - 100 / (1 + ag / al), 2)
    return out

def calc_ma_score(hist_daily):
    if hist_daily is None or len(hist_daily) < 20:
        return None, None
    closes = pd.Series(hist_daily["Close"].values, dtype=float)
    price  = float(closes.iloc[-1])
    n      = len(closes)
    emas = {
        10:  float(closes.ewm(span=10,  adjust=False).mean().iloc[-1]) if n >= 10  else None,
        20:  float(closes.ewm(span=20,  adjust=False).mean().iloc[-1]) if n >= 20  else None,
        50:  float(closes.ewm(span=50,  adjust=False).mean().iloc[-1]) if n >= 50  else None,
        100: float(closes.ewm(span=100, adjust=False).mean().iloc[-1]) if n >= 100 else None,
        200: float(closes.ewm(span=200, adjust=False).mean().iloc[-1]) if n >= 200 else None,
    }
    above = sum(1 for e in emas.values() if e is not None and price > e)
    below = sum(1 for e in emas.values() if e is not None and price < e)
    if above + below == 0:
        return 0, "neutral"
    score = max(-5, min(5, above - below))
    return score, "bullish" if score > 0 else ("bearish" if score < 0 else "neutral")

def is_in_golden_pocket(price, high, low):
    if high <= low or price <= 0:
        return False, 0
    fib_pos = (high - price) / (high - low) * 100
    return (61.8 <= fib_pos <= 65.0), round(fib_pos, 2)

def pct_change(closes, periods):
    if len(closes) <= periods:
        return None
    return round(float((closes.iloc[-1] / closes.iloc[-periods - 1] - 1) * 100), 2)


def last_ema(closes, span):
    return float(closes.ewm(span=span, adjust=False).mean().iloc[-1]) if len(closes) >= span else None


def process_stock(stock):
    t = stock["t"]; n = stock["n"]; g = stock["g"]
    try:
        hist_d, source = history_df(t, years=2, period="2y")
        if hist_d is None or len(hist_d) < 20:
            return None
        hist_d = hist_d.dropna(subset=["Close"])
        close_d = hist_d["Close"].astype(float)
        if len(close_d) < 2:
            return None
        price, prev_close = snapshot_price(t, float(close_d.iloc[-1]))
        if price <= 0:
            return None
        chg_pct = round(float((price / (prev_close or close_d.iloc[-2]) - 1) * 100), 2)
        hist_52w = hist_d.tail(252) if len(hist_d) >= 252 else hist_d
        hi52 = round(float(max(hist_52w["High"].max(), price)), 4)
        lo52 = round(float(hist_52w["Low"].min()), 4)
        year_start = datetime.datetime(datetime.datetime.now().year, 1, 1)
        ytd = hist_d[hist_d.index >= str(year_start.date())]
        hi_ytd = round(float(max(ytd["High"].max(), price)), 4) if not ytd.empty else hi52
        lo_ytd = round(float(ytd["Low"].min()), 4) if not ytd.empty else lo52
        d_closes = close_d.tolist()
        rsi_d = calc_rsi(d_closes[-80:]) if len(d_closes) >= 16 else None
        weekly = close_d.resample("W-FRI").last().dropna()
        rsi_w = calc_rsi(weekly.tolist()[-80:]) if len(weekly) >= 16 else None
        in_gp_52w, fib_52w = is_in_golden_pocket(price, hi52, lo52)
        in_gp_ytd, fib_ytd = is_in_golden_pocket(price, hi_ytd, lo_ytd)
        e10, e20, e50 = last_ema(close_d, 10), last_ema(close_d, 20), last_ema(close_d, 50)
        e100, e200 = last_ema(close_d, 100), last_ema(close_d, 200)
        return {
            "ticker": t, "name": n, "group": g,
            "price": round(price, 4), "chg": chg_pct,
            "chg_1w": pct_change(close_d, 5), "chg_1m": pct_change(close_d, 21),
            "chg_3m": pct_change(close_d, 63),
            "hi52": hi52, "lo52": lo52, "hi_ytd": hi_ytd, "lo_ytd": lo_ytd,
            "fib_52w": fib_52w, "fib_ytd": fib_ytd,
            "in_gp_52w": in_gp_52w, "in_gp_ytd": in_gp_ytd,
            "rsi_weekly": rsi_w, "rsi_daily": rsi_d,
            "dist_from_hi": round((hi52 - price) / hi52 * 100, 1) if hi52 > 0 else None,
            "above_10":  bool(e10 and price > e10),
            "above_20":  bool(e20 and price > e20),
            "above_50":  bool(e50 and price > e50),
            "above_100": bool(e100 and price > e100),
            "above_200": bool(e200 and price > e200),
            "source": source,
        }
    except Exception as e:
        print(f"  ERROR {t}: {e}")
        return None


def process_trending(asset):
    t = asset["t"]; n = asset["n"]; g = asset["g"]
    try:
        hist_d, source = history_df(t, years=2, period="2y")
        if hist_d is None or len(hist_d) < 20:
            return None
        close_d = hist_d["Close"].dropna().astype(float)
        if len(close_d) < 2:
            return None
        price, prev_close = snapshot_price(t, float(close_d.iloc[-1]))
        if price <= 0:
            return None
        chg_pct = round(float((price / (prev_close or close_d.iloc[-2]) - 1) * 100), 2)
        hist_52w = hist_d.tail(252) if len(hist_d) >= 252 else hist_d
        score, trend = calc_ma_score(hist_d)
        return {
            "ticker": t, "name": n, "group": g,
            "price": round(price, 4), "chg": chg_pct,
            "chg_1w": pct_change(close_d, 5), "chg_1m": pct_change(close_d, 21),
            "chg_3m": pct_change(close_d, 63),
            "hi52": round(float(max(hist_52w["High"].max(), price)), 4),
            "lo52": round(float(hist_52w["Low"].min()), 4),
            "score": score, "trend": trend,
            "ema10": round(last_ema(close_d, 10), 4) if last_ema(close_d, 10) else None,
            "ema20": round(last_ema(close_d, 20), 4) if last_ema(close_d, 20) else None,
            "ema50": round(last_ema(close_d, 50), 4) if last_ema(close_d, 50) else None,
            "ema100": round(last_ema(close_d, 100), 4) if last_ema(close_d, 100) else None,
            "ema200": round(last_ema(close_d, 200), 4) if last_ema(close_d, 200) else None,
            "source": source,
        }
    except Exception as e:
        print(f"  ERROR Trending {t}: {e}")
        return None


def process_sector(stock):
    t = stock["t"]; n = stock["n"]; s = stock["s"]
    try:
        hist, source = history_df(t, years=2, period="2y")
        if hist is None or len(hist) < 20:
            return None
        closes = hist["Close"].dropna().astype(float)
        if len(closes) < 20:
            return None
        price, prev_close = snapshot_price(t, float(closes.iloc[-1]))
        year_start = datetime.datetime(datetime.datetime.now().year, 1, 1)
        ytd = hist[hist.index >= str(year_start.date())]
        chg_ytd = round(float((price / ytd["Close"].iloc[0] - 1) * 100), 2) if not ytd.empty else None
        dates = []
        if s == "Benchmark":
            idx_list = hist.index.strftime("%Y-%m-%d").tolist()
            dates = idx_list[-252:] if len(idx_list) >= 252 else idx_list
        return {
            "ticker": t, "name": n, "sector": s, "price": round(price, 4),
            "chg_1d": round(float((price / (prev_close or closes.iloc[-2]) - 1) * 100), 2),
            "chg_1w": pct_change(closes, 5), "chg_1m": pct_change(closes, 21),
            "chg_3m": pct_change(closes, 63), "chg_6m": pct_change(closes, 126),
            "chg_ytd": chg_ytd,
            "closes": closes.tolist(),
            "dates": dates,
            "source": source,
        }
    except Exception as e:
        print(f"  ERROR Sector {t}: {e}")
        return None

# ─── DUAL RRG WITH TAILS ─────────────────────────────────────────
def calc_rrg_series(sc, spy_c, window, sma_period, tail_len=10):
    mn = min(len(sc), len(spy_c))
    if mn < sma_period * 3:
        return None, None, "unknown", []
    sc_w = sc[-window:] if len(sc) >= window else sc
    spy_w = spy_c[-window:] if len(spy_c) >= window else spy_c
    mn2 = min(len(sc_w), len(spy_w))
    if mn2 < sma_period * 2:
        return None, None, "unknown", []
    sc_w = sc_w[-mn2:]
    spy_w = spy_w[-mn2:]
    rs = sc_w / spy_w * 100
    rs_sma = pd.Series(rs).rolling(sma_period).mean().values
    valid = rs_sma != 0
    rs_r = np.where(valid, rs / rs_sma * 100, np.nan)
    rs_rsma = pd.Series(rs_r).rolling(sma_period).mean().values
    valid2 = rs_rsma != 0
    rs_m = np.where(valid2, rs_r / rs_rsma * 100, np.nan)
    ok = ~(np.isnan(rs_r) | np.isnan(rs_m))
    if ok.sum() == 0:
        return None, None, "unknown", []
    valid_r = rs_r[ok]
    valid_m = rs_m[ok]
    n_valid = len(valid_r)
    tail_indices = list(range(max(0, n_valid - tail_len), n_valid))
    tail = [[round(float(valid_r[i]), 2), round(float(valid_m[i]), 2)] for i in tail_indices]
    r = round(float(valid_r[-1]), 2)
    m = round(float(valid_m[-1]), 2)
    if r >= 100 and m >= 100:   q = "leading"
    elif r >= 100 and m < 100:  q = "weakening"
    elif r < 100 and m < 100:   q = "lagging"
    else:                       q = "improving"
    return r, m, q, tail

def calc_rrg(sectors):
    spy = next((s for s in sectors if s["ticker"] == "SPY"), None)
    if not spy or not spy.get("closes"):
        return sectors
    spy_c = np.array(spy["closes"], dtype=float)
    for sec in sectors:
        if sec["ticker"] == "SPY":
            sec["rrg_12m"] = {"rs_ratio": 100.0, "rs_momentum": 100.0, "quadrant": "benchmark", "tail": []}
            sec["rrg_3m"]  = {"rs_ratio": 100.0, "rs_momentum": 100.0, "quadrant": "benchmark", "tail": []}
            sec["quadrant"] = "benchmark"; sec["agreement"] = True
            sec["diverging"] = False; sec["q_12m"] = "benchmark"; sec["q_3m"] = "benchmark"
            continue
        if not sec.get("closes"):
            sec["rrg_12m"] = {}; sec["rrg_3m"] = {}
            sec["quadrant"] = "unknown"; sec["agreement"] = False
            sec["diverging"] = False; sec["q_12m"] = "unknown"; sec["q_3m"] = "unknown"
            continue
        sc = np.array(sec["closes"], dtype=float)
        r12, m12, q12, tail12 = calc_rrg_series(sc, spy_c, 252, 10, tail_len=20)
        r3,  m3,  q3,  tail3  = calc_rrg_series(sc, spy_c, 66,  5,  tail_len=10)
        sec["rrg_12m"] = {"rs_ratio": r12, "rs_momentum": m12, "quadrant": q12, "tail": tail12}
        sec["rrg_3m"]  = {"rs_ratio": r3,  "rs_momentum": m3,  "quadrant": q3,  "tail": tail3}
        agreement = (q12 == q3) and q12 not in ("unknown", "benchmark")
        diverging = not agreement and q12 not in ("unknown", "benchmark") and q3 not in ("unknown", "benchmark")
        sec["quadrant"]  = q12 if q12 not in ("unknown", "benchmark") else q3
        sec["agreement"] = agreement; sec["diverging"] = diverging
        sec["q_12m"] = q12; sec["q_3m"] = q3
    for sec in sectors:
        sec.pop("closes", None)
    return sectors

# ─── BREADTH ─────────────────────────────────────────────────────
def calc_breadth(stock_results):
    sp    = [s for s in stock_results if s.get("group") == "SP500"]
    total = len(sp)
    if total == 0:
        return {}
    advancing = declining = new_highs = new_lows = 0
    above_10 = above_20 = above_50 = above_100 = above_200 = 0
    for s in sp:
        chg   = s.get("chg",   0)
        hi52  = s.get("hi52",  0)
        lo52  = s.get("lo52",  0)
        price = s.get("price", 0)
        if chg > 0:   advancing += 1
        elif chg < 0: declining += 1
        if hi52 > 0 and price >= hi52 * 0.985: new_highs += 1
        if lo52 > 0 and price <= lo52 * 1.015: new_lows  += 1
        if s.get("above_10"):  above_10  += 1
        if s.get("above_20"):  above_20  += 1
        if s.get("above_50"):  above_50  += 1
        if s.get("above_100"): above_100 += 1
        if s.get("above_200"): above_200 += 1
    def pct(n): return round(n / total * 100, 1) if total > 0 else 0
    ad_ratio = advancing / (advancing + declining) if (advancing + declining) > 0 else 0.5
    nh_pct   = round(new_highs / total * 100, 2)
    nl_pct   = round(new_lows  / total * 100, 2)
    hindenburg_active = (nh_pct > 2.2 and nl_pct > 2.2)
    result = {
        "pct_above_10":  pct(above_10),  "pct_above_20":  pct(above_20),
        "pct_above_50":  pct(above_50),  "pct_above_100": pct(above_100),
        "pct_above_200": pct(above_200),
        "new_highs": new_highs, "new_lows": new_lows,
        "nh_pct": nh_pct, "nl_pct": nl_pct,
        "advancing": advancing, "declining": declining,
        "ad_ratio": round(ad_ratio, 4),
        "mclellan": 0,
        "zweig_ratio":  round(ad_ratio, 4),
        "zweig_active": ad_ratio > 0.615,
        "zweig_setup":  ad_ratio < 0.40,
        "hindenburg_active": hindenburg_active,
        "hindenburg_nh_pct": nh_pct, "hindenburg_nl_pct": nl_pct,
        "universe_size": total,
        "calculated_from": "SP500 universe (EMAs)",
    }
    print(f"  Breadth: {advancing}↑ {declining}↓ | "
          f"200D:{pct(above_200)}% 50D:{pct(above_50)}% 20D:{pct(above_20)}% | "
          f"NH:{new_highs} NL:{new_lows} | Zweig:{round(ad_ratio,3)}")
    return result

# ─── INSIDER TRADING ─────────────────────────────────────────────
def fetch_insider_trades(sp500_list, lookback_days=14, min_value=50000):
    import csv, io
    trades  = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "text/csv,text/plain,*/*",
    }
    sp_tickers = {s["t"] for s in sp500_list if "." not in s["t"]}
    endpoints = [
        (f"http://openinsider.com/screener?s=&o=&pl=50&ph=&ll=&lh="
         f"&fd={lookback_days}&fdr=&td=0&tdr=&fdlyl=&fdlyh=&daysago="
         f"&xp=1&vl=&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999"
         f"&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h="
         f"&sortcol=0&cnt=300&action=1&format=csv", "BUY"),
        (f"http://openinsider.com/screener?s=&o=&pl=50&ph=&ll=&lh="
         f"&fd={lookback_days}&fdr=&td=0&tdr=&fdlyl=&fdlyh=&daysago="
         f"&xs=1&vl=&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999"
         f"&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h="
         f"&sortcol=0&cnt=300&action=1&format=csv", "SELL"),
    ]
    def parse_num(s):
        if not s: return 0.0
        s = s.strip().replace("$","").replace(",","").replace("+","")
        if not s or s == "-": return 0.0
        try:
            mult = 1
            if s.endswith("K"): mult = 1_000;     s = s[:-1]
            if s.endswith("M"): mult = 1_000_000; s = s[:-1]
            return float(s) * mult
        except: return 0.0
    for url, txn_type in endpoints:
        try:
            r = requests.get(url, headers=headers, timeout=20)
            print(f"  OpenInsider {txn_type}: HTTP {r.status_code}, {len(r.content)} bytes")
            if r.status_code != 200: continue
            content = r.text
            reader = csv.reader(io.StringIO(content))
            rows   = list(reader)
            print(f"  Parsed {len(rows)} CSV rows")
            if len(rows) < 2: continue
            count_before = len(trades)
            for row in rows[1:]:
                try:
                    if len(row) < 12: continue
                    if row[0].strip().lower() in ('x', '', 'filing date'): continue
                    if len(row) >= 13 and row[0].strip() in ('X', ''):
                        off = 1
                    else:
                        off = 0
                    filed   = row[off + 0].strip() if len(row) > off+0 else ""
                    traded  = row[off + 1].strip() if len(row) > off+1 else ""
                    ticker  = row[off + 2].strip().upper() if len(row) > off+2 else ""
                    company = row[off + 3].strip() if len(row) > off+3 else ""
                    insider = row[off + 4].strip() if len(row) > off+4 else ""
                    title   = row[off + 5].strip() if len(row) > off+5 else ""
                    price   = parse_num(row[off + 7]) if len(row) > off+7 else 0
                    qty     = int(parse_num(row[off + 8])) if len(row) > off+8 else 0
                    owned   = int(parse_num(row[off + 9])) if len(row) > off+9 else 0
                    val     = int(parse_num(row[off + 11])) if len(row) > off+11 else int(price*qty)
                    if not ticker or len(ticker) > 6: continue
                    if not insider: continue
                    if abs(val) < min_value: continue
                    if ticker not in sp_tickers: continue
                    trades.append({
                        "ticker": ticker, "company": company[:60],
                        "date": traded[:10], "filed": filed[:10],
                        "insider": insider[:50], "role": title[:40],
                        "type": txn_type, "shares": abs(qty),
                        "price": round(price, 2), "value": abs(val),
                        "owned_after": abs(owned), "cik": 0,
                    })
                except Exception: continue
            added = len(trades) - count_before
            print(f"  → {added} SP500 {txn_type} trades added")
            time.sleep(2.0)
        except Exception as e:
            print(f"  OpenInsider {txn_type} error: {e}")
    trades.sort(key=lambda x: x["value"], reverse=True)
    buys  = sum(1 for t in trades if t["type"] == "BUY")
    sells = sum(1 for t in trades if t["type"] == "SELL")
    print(f"  Insider total: {len(trades)} — {buys} buys / {sells} sells")
    return trades[:150]

# ─── GITHUB UPLOAD ───────────────────────────────────────────────
def upload_to_github(data_json):
    if not GITHUB_TOKEN:
        print("  GitHub upload skipped: set GITHUB_TOKEN env var")
        return False
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    r = requests.get(url, headers=headers, timeout=25)
    sha = r.json().get("sha") if r.status_code == 200 else None
    payload = {
        "message": f"Update {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "content": base64.b64encode(data_json.encode()).decode(),
    }
    if sha: payload["sha"] = sha
    r2 = requests.put(url, headers=headers, json=payload, timeout=30)
    if r2.status_code in [200, 201]:
        print("  GitHub upload OK!"); return True
    print(f"  GitHub error: {r2.status_code}"); return False

# ─── MARKET INDICATORS ───────────────────────────────────────────
def chart_history(symbol, years=1, period="1y", limit=260):
    hist, source = history_df(symbol, years=years, period=period)
    if hist is None or hist.empty or "Close" not in hist.columns:
        return []
    rows = []
    hist = hist.sort_index()
    if limit:
        hist = hist.tail(limit)
    for idx, row in hist.iterrows():
        close = row.get("Close")
        if pd.isna(close):
            continue
        rows.append({
            "date": str(pd.Timestamp(idx).date()),
            "close": round(float(close), 4),
            "source": source,
        })
    return rows


def rows_from_history_df(hist, source, limit=None):
    if hist is None or hist.empty or "Close" not in hist.columns:
        return []
    rows = []
    hist = hist.sort_index()
    if limit:
        hist = hist.tail(limit)
    for idx, row in hist.iterrows():
        close = row.get("Close")
        if pd.isna(close):
            continue
        item = {
            "date": str(pd.Timestamp(idx).date()),
            "close": round(float(close), 4),
            "source": source,
        }
        for src_col, out_key in (("Open", "open"), ("High", "high"), ("Low", "low")):
            val = row.get(src_col)
            if val is not None and not pd.isna(val):
                item[out_key] = round(float(val), 4)
        rows.append(item)
    return rows


def return_histogram_payload(symbols=None, years=25):
    payload = {}
    for meta in symbols or RETURN_HISTOGRAM_SYMBOLS:
        symbol = meta["t"]
        try:
            hist = yf_df(symbol, period="max", interval="1d")
            source = "yfinance:max" if hist is not None and not hist.empty else "none"
            if hist is None or hist.empty or len(hist) < 120:
                hist, source = history_df(symbol, years=years, period="max")
            if hist is None or hist.empty or "Close" not in hist.columns:
                continue
            close = hist["Close"].astype(float).dropna().sort_index()
            if close.empty:
                continue
            monthly = close.resample("ME").last().dropna()
            returns = monthly.pct_change().dropna() * 100
            if len(returns) < 24:
                continue
            rows = []
            for idx, val in returns.items():
                rows.append({
                    "date": str(pd.Timestamp(idx).date()),
                    "ret": round(float(val), 4),
                })
            vals = returns.astype(float)
            mean = float(vals.mean())
            sigma = float(vals.std(ddof=1)) if len(vals) > 1 else 0.0
            last_price = float(close.iloc[-1])
            prev_close = float(close.iloc[-2]) if len(close) > 1 else last_price
            payload[symbol] = {
                "ticker": symbol,
                "name": meta.get("n", symbol),
                "group": meta.get("g", "ETF"),
                "source": source,
                "frequency": "monthly",
                "start": str(pd.Timestamp(returns.index[0]).date()),
                "end": str(pd.Timestamp(returns.index[-1]).date()),
                "last_price": round(last_price, 4),
                "last_day_chg": round((last_price / prev_close - 1) * 100, 2) if prev_close else None,
                "mean": round(mean, 4),
                "sigma": round(sigma, 4),
                "plus_2sigma": round(mean + 2 * sigma, 4),
                "minus_2sigma": round(mean - 2 * sigma, 4),
                "observations": int(len(rows)),
                "returns": rows,
            }
        except Exception as e:
            print(f"  Return histogram {symbol}: {e}")
    return payload


def _safe_float(value):
    try:
        if value is None:
            return None
        val = float(value)
        if not np.isfinite(val):
            return None
        return val
    except Exception:
        return None


def _add_put_iv_rows_to_buckets(rows, buckets):
    for row in rows:
        if str(row.get("type", "")).lower() != "put":
            continue
        date = str(row.get("tradetime", ""))[:10]
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
            continue
        dte = _safe_float(row.get("dte"))
        moneyness = _safe_float(row.get("moneyness"))
        vol = _safe_float(row.get("volatility"))
        if vol is None or vol <= 0:
            continue
        if dte is not None and not (14 <= dte <= 120):
            continue
        if moneyness is not None and not (-0.35 <= moneyness <= 0.10):
            continue
        iv_pct = vol * 100 if vol <= 3 else vol
        if not (1 <= iv_pct <= 300):
            continue
        oi = _safe_float(row.get("open_interest")) or 0
        volume = _safe_float(row.get("volume")) or 0
        weight = max(1.0, oi, volume)
        bucket = buckets.setdefault(date, {"w": 0.0, "ivw": 0.0, "contracts": 0})
        bucket["w"] += weight
        bucket["ivw"] += iv_pct * weight
        bucket["contracts"] += 1


def _put_iv_rows_from_buckets(buckets, rolling_window=63):
    daily = []
    for date, bucket in sorted(buckets.items()):
        if bucket["w"] <= 0:
            continue
        daily.append({
            "date": date,
            "iv": bucket["ivw"] / bucket["w"],
            "contracts": int(bucket["contracts"]),
        })
    if len(daily) < 5:
        return [], {"status": "insufficient_data", "observations": len(daily)}

    ivs = pd.Series([r["iv"] for r in daily], dtype="float64")
    min_periods = min(20, rolling_window)
    means = ivs.rolling(rolling_window, min_periods=min_periods).mean()
    sigmas = ivs.rolling(rolling_window, min_periods=min_periods).std(ddof=1)
    out = []
    for i, row in enumerate(daily):
        mean = means.iloc[i]
        sigma = sigmas.iloc[i]
        z = None
        if pd.notna(mean) and pd.notna(sigma) and sigma > 0:
            z = (row["iv"] - float(mean)) / float(sigma)
        abs_z = abs(z) if z is not None else 0
        sd_level = 4 if abs_z >= 4 else 3 if abs_z >= 3 else 2 if abs_z >= 2 else 0
        out.append({
            "date": row["date"],
            "iv": round(float(row["iv"]), 4),
            "mean": round(float(mean), 4) if pd.notna(mean) else None,
            "upper2": round(float(mean + 2 * sigma), 4) if pd.notna(mean) and pd.notna(sigma) else None,
            "lower2": round(float(mean - 2 * sigma), 4) if pd.notna(mean) and pd.notna(sigma) else None,
            "upper3": round(float(mean + 3 * sigma), 4) if pd.notna(mean) and pd.notna(sigma) else None,
            "lower3": round(float(mean - 3 * sigma), 4) if pd.notna(mean) and pd.notna(sigma) else None,
            "upper4": round(float(mean + 4 * sigma), 4) if pd.notna(mean) and pd.notna(sigma) else None,
            "lower4": round(float(mean - 4 * sigma), 4) if pd.notna(mean) and pd.notna(sigma) else None,
            "z": round(float(z), 4) if z is not None else None,
            "sd": sd_level,
            "contracts": row["contracts"],
        })
    current = next((r for r in reversed(out) if r.get("z") is not None), out[-1] if out else {})
    return out, {
        "status": "ok",
        "observations": len(out),
        "rolling_window": rolling_window,
        "current": current,
    }


def put_iv_rows_for_symbol(symbol, years=2, existing_rows=None, force_backfill=False, rolling_window=63):
    if not EODHD.enabled:
        return [], {"status": "missing_api_key"}
    end = datetime.date.today()
    existing_rows = [r for r in (existing_rows or []) if r.get("date") and NumberLike(r.get("iv"))]
    buckets = {}
    for row in existing_rows:
        buckets[row["date"]] = {"w": 1.0, "ivw": float(row["iv"]), "contracts": int(row.get("contracts") or 1)}

    have_dates = sorted(buckets)
    has_backfill = bool(have_dates and have_dates[0] <= str(end - datetime.timedelta(days=365 * years - 30)))
    last_date = datetime.date.fromisoformat(have_dates[-1]) if have_dates else None
    if force_backfill or not has_backfill:
        start = end - datetime.timedelta(days=365 * years)
        windows = []
        cur = start.replace(day=1)
        while cur <= end:
            nxt = (cur.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
            win_start = max(start, cur)
            win_end = min(end, nxt - datetime.timedelta(days=1))
            windows.append((win_start, win_end, (0,)))
            cur = nxt
    else:
        start = max(end - datetime.timedelta(days=21), (last_date or end) - datetime.timedelta(days=3))
        windows = [(start, end, (0, 1000, 2000))]

    fetched = 0
    for win_start, win_end, offsets in windows:
        for offset in offsets:
            rows = EODHD.options_eod(
                symbol,
                win_start,
                win_end,
                exp_from=win_start + datetime.timedelta(days=14),
                exp_to=win_end + datetime.timedelta(days=120),
                limit=1000,
                offset=offset,
                compact=True,
            )
            fetched += len(rows)
            _add_put_iv_rows_to_buckets(rows, buckets)
        time.sleep(0.02)

    out, status = _put_iv_rows_from_buckets(buckets, rolling_window=rolling_window)
    status["fetched_contract_rows"] = fetched
    status["history_mode"] = "backfill_2y" if (force_backfill or not has_backfill) else "incremental"
    if out:
        status["start"] = out[0]["date"]
        status["end"] = out[-1]["date"]
    return out, status


def NumberLike(value):
    try:
        float(value)
        return True
    except Exception:
        return False


def existing_options_put_iv_payload():
    try:
        path = Path(__file__).resolve().parent / "data.json"
        if not path.exists():
            return {}
        data = json.loads(path.read_text(encoding="utf-8"))
        return data.get("market", {}).get("options_put_iv", {}) or {}
    except Exception:
        return {}


def options_put_iv_payload(symbols=None):
    payload = {}
    existing = existing_options_put_iv_payload()
    metas = list(symbols or OPTIONS_PUT_IV_SYMBOLS)

    def build_item(meta):
        symbol = meta["t"]
        try:
            rows, status = put_iv_rows_for_symbol(symbol, existing_rows=existing.get(symbol, {}).get("rows", []))
            item = {
                "ticker": symbol,
                "name": meta.get("n", symbol),
                "group": meta.get("g", "ETF"),
                "source": "eodhd:options_eod",
                "rows": rows,
                "status": status,
            }
            print(f"  Put IV {symbol}: {len(rows)} days | status: {status.get('status')}")
            return symbol, item
        except Exception as e:
            item = {
                "ticker": symbol,
                "name": meta.get("n", symbol),
                "group": meta.get("g", "ETF"),
                "source": "eodhd:options_eod",
                "rows": [],
                "status": {"status": "error", "message": str(e)[:120]},
            }
            print(f"  Put IV {symbol}: {e}")
            return symbol, item

    workers = max(1, int(os.getenv("OPTIONS_PUT_IV_WORKERS", "4")))
    if workers == 1 or len(metas) <= 1:
        for meta in metas:
            symbol, item = build_item(meta)
            payload[symbol] = item
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(build_item, meta) for meta in metas]
            for future in as_completed(futures):
                symbol, item = future.result()
                payload[symbol] = item
    return payload


def _dte_bucket(dte):
    if dte is None:
        return "na"
    if dte <= 7:
        return "0-7"
    if dte <= 30:
        return "8-30"
    if dte <= 90:
        return "31-90"
    if dte <= 180:
        return "91-180"
    return "180+"


def _moneyness_bucket(moneyness):
    if moneyness is None:
        return "na"
    if moneyness <= -0.20:
        return "<-20%"
    if moneyness <= -0.05:
        return "-20/-5%"
    if moneyness <= 0.05:
        return "atm"
    if moneyness <= 0.20:
        return "+5/+20%"
    return ">20%"


def unusual_options_for_symbol(symbol, lookback_days=45):
    if not EODHD.enabled:
        return []
    end = datetime.date.today()
    start = end - datetime.timedelta(days=lookback_days)
    short_rows = []
    for offset in (0, 3000):
        short_rows.extend(EODHD.options_eod(
            symbol,
            start,
            end,
            exp_from=start,
            exp_to=end + datetime.timedelta(days=90),
            limit=1000,
            offset=offset,
            compact=True,
            option_type=None,
        ))
    long_rows = EODHD.options_eod(
        symbol,
        start,
        end,
        exp_from=end + datetime.timedelta(days=91),
        exp_to=end + datetime.timedelta(days=365),
        limit=1000,
        compact=True,
        option_type=None,
    )
    rows = short_rows + long_rows
    parsed = []
    for row in rows:
        date = str(row.get("tradetime", ""))[:10]
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
            continue
        typ = str(row.get("type", "")).lower()
        if typ not in ("call", "put"):
            continue
        vol = _safe_float(row.get("volume")) or 0
        oi = _safe_float(row.get("open_interest")) or 0
        dte = _safe_float(row.get("dte"))
        if dte is not None and (dte < 0 or dte > 365):
            continue
        midpoint = _safe_float(row.get("midpoint"))
        last = _safe_float(row.get("last"))
        bid = _safe_float(row.get("bid"))
        ask = _safe_float(row.get("ask"))
        if midpoint is None and bid is not None and ask is not None and ask >= bid:
            midpoint = (bid + ask) / 2
        price = midpoint if midpoint and midpoint > 0 else (last if last and last > 0 else None)
        premium = vol * price * 100 if price else 0
        parsed.append({
            "date": date,
            "symbol": symbol,
            "contract": row.get("contract"),
            "type": typ,
            "strike": _safe_float(row.get("strike")),
            "exp_date": row.get("exp_date"),
            "dte": dte,
            "moneyness": _safe_float(row.get("moneyness")),
            "volume": vol,
            "open_interest": oi,
            "vol_oi": (vol / oi) if oi > 0 else (vol if vol >= 1000 else None),
            "price": price,
            "premium": premium,
            "iv": (_safe_float(row.get("volatility")) or 0) * 100,
            "delta": _safe_float(row.get("delta")),
            "volume_pctchange": _safe_float(row.get("volume_pctchange")),
        })
    if not parsed:
        return []
    latest = max(r["date"] for r in parsed)
    buckets = {}
    for r in parsed:
        key = (r["type"], _dte_bucket(r["dte"]), _moneyness_bucket(r["moneyness"]))
        buckets.setdefault(key, []).append(r["volume"])
    out = []
    for r in parsed:
        if r["date"] != latest:
            continue
        dte = r["dte"]
        moneyness = r["moneyness"]
        abs_money = abs(moneyness) if moneyness is not None else 0
        short_dated = dte is not None and dte <= 45
        very_short = dte is not None and dte <= 14
        far_otm = abs_money >= 0.15
        otm = abs_money >= 0.05
        lotto = short_dated and otm and r["price"] is not None and r["price"] <= 2.5
        if r["volume"] < 500:
            continue
        key = (r["type"], _dte_bucket(r["dte"]), _moneyness_bucket(r["moneyness"]))
        base = [v for v in buckets.get(key, []) if v is not None and v >= 0]
        mean = float(np.mean(base)) if base else 0.0
        sigma = float(np.std(base, ddof=1)) if len(base) > 1 else 0.0
        z = (r["volume"] - mean) / sigma if sigma > 0 else None
        vol_oi = r["vol_oi"] or 0
        pct = r["volume_pctchange"] or 0
        truly_unusual = (
            (z is not None and z >= 2.5 and r["volume"] >= 750)
            or vol_oi >= 2.0
            or pct >= 500
            or (short_dated and otm and r["volume"] >= 1200 and r["premium"] >= 100000)
            or (far_otm and r["volume"] >= 1500 and vol_oi >= 0.75)
        )
        if not truly_unusual:
            continue
        score = 0
        if z is not None:
            score += min(35, max(0, z) * 7)
        score += min(22, max(0, np.log10(max(r["premium"], 1) / 150000)) * 12)
        score += min(30, vol_oi * 8)
        score += min(14, max(0, np.log10(max(r["volume"], 1) / 500)) * 10)
        if pct >= 500:
            score += 12
        if very_short:
            score += 18
        elif short_dated:
            score += 14
        elif dte is not None and dte <= 90:
            score += 8
        elif dte is not None and dte > 180:
            score -= 18
        if far_otm:
            score += 16 if r["premium"] >= 100000 else 10
        elif otm:
            score += 8
        if short_dated and otm:
            score += 16
        if lotto and r["volume"] >= 1500:
            score += 10
        if r["volume"] >= 5000:
            score += 8
        if r["premium"] < 150000 and not (short_dated and otm and r["volume"] >= 1500):
            continue
        if score < 55:
            continue
        tags = []
        if very_short:
            tags.append("very short")
        elif short_dated:
            tags.append("short dated")
        elif dte is not None and dte > 180:
            tags.append("LEAPS")
        if far_otm:
            tags.append("far OTM")
        elif otm:
            tags.append("OTM")
        if vol_oi >= 2:
            tags.append("Vol/OI")
        if z is not None and z >= 3:
            tags.append("volume z")
        if lotto:
            tags.append("lotto")
        setup = " / ".join(tags[:3]) if tags else "premium flow"
        out.append({
            "date": r["date"],
            "symbol": symbol,
            "contract": r["contract"],
            "type": r["type"],
            "strike": round(r["strike"], 2) if r["strike"] is not None else None,
            "exp_date": r["exp_date"],
            "dte": int(r["dte"]) if r["dte"] is not None else None,
            "moneyness": round(r["moneyness"], 4) if r["moneyness"] is not None else None,
            "volume": int(r["volume"]),
            "open_interest": int(r["open_interest"]),
            "vol_oi": round(vol_oi, 2) if vol_oi else None,
            "price": round(r["price"], 2) if r["price"] is not None else None,
            "premium": int(r["premium"]),
            "iv": round(r["iv"], 2) if r["iv"] else None,
            "delta": round(r["delta"], 3) if r["delta"] is not None else None,
            "volume_z": round(z, 2) if z is not None else None,
            "volume_pctchange": round(pct, 1) if pct else None,
            "setup": setup,
            "score": round(score, 1),
        })
    return out


def unusual_options_trades_payload(symbols=None, top_n=100):
    symbols = list(symbols or TOP_100_US_OPTIONS_SYMBOLS)
    rows = []
    workers = max(1, int(os.getenv("UNUSUAL_OPTIONS_WORKERS", "10")))

    def fetch(symbol):
        try:
            found = unusual_options_for_symbol(symbol)
            print(f"  Unusual options {symbol}: {len(found)}")
            return found
        except Exception as e:
            print(f"  Unusual options {symbol}: {e}")
            return []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(fetch, symbol) for symbol in symbols]
        for future in as_completed(futures):
            rows.extend(future.result())
    rows.sort(key=lambda r: (r.get("score") or 0, r.get("premium") or 0, r.get("volume") or 0), reverse=True)
    top_rows = []
    leaps_count = 0
    for row in rows:
        is_leap = (row.get("dte") or 0) > 180
        if is_leap and leaps_count >= max(8, top_n // 4):
            continue
        top_rows.append(row)
        if is_leap:
            leaps_count += 1
        if len(top_rows) >= top_n:
            break
    latest = max((r.get("date") for r in rows if r.get("date")), default=None)
    return {
        "source": "eodhd:options_eod",
        "universe": symbols,
        "lookback_days": 45,
        "latest_date": latest,
        "rows": top_rows,
        "status": {
            "status": "ok" if rows else "empty",
            "symbols": len(symbols),
            "matches": len(rows),
            "returned": len(top_rows),
        },
    }


def vix_sd_payload(years=20):
    hist = None
    source = "none"
    use_high = False
    if EODHD.enabled:
        hist, source = EODHD.intraday_history(["VIX.INDX"], years=years, interval="1h")
        if hist is not None and not hist.empty:
            use_high = True
            source = f"{source}_daily_high"
        else:
            hist, source = EODHD.eod_history(["VIX.INDX"], years=years)
    if hist is None or hist.empty:
        hist = yf_df("^VIX", period=f"{years}y", interval="1d")
        source = "yfinance:^VIX" if hist is not None and not hist.empty else "none"
    if hist is None or hist.empty or "Close" not in hist.columns:
        return [], {
            "current_vix": None,
            "current_sd": None,
            "current_sd_clamped": None,
            "signal": False,
            "threshold_vix": 35,
            "source": source,
        }

    rows = []
    hist = hist.sort_index()
    if use_high and "High" in hist.columns:
        vix_series = hist["High"].dropna().astype(float)
        vix_series.index = pd.to_datetime(vix_series.index).normalize()
        vix_series = vix_series.groupby(vix_series.index).max()
    else:
        vix_series = hist["Close"].dropna().astype(float)
    for idx, vix in vix_series.items():
        if not np.isfinite(vix) or vix <= 0:
            continue
        sd = (float(vix) - 20.0) / 8.0
        sd_clamped = max(-2.0, min(7.0, sd))
        rows.append({
            "date": str(pd.Timestamp(idx).date()),
            "vix": round(float(vix), 2),
            "sd": round(sd, 2),
            "sd_clamped": round(sd_clamped, 2),
            "signal": bool(vix >= 35.0),
            "source": source,
            "interval": "1h" if use_high else "1d",
            "method": "daily_intraday_high" if use_high else "daily_close",
        })

    current = rows[-1] if rows else {}
    return rows, {
        "current_vix": current.get("vix"),
        "current_sd": current.get("sd"),
        "current_sd_clamped": current.get("sd_clamped"),
        "signal": bool(current.get("signal")),
        "threshold_vix": 35,
        "source": source,
        "interval": current.get("interval"),
        "method": current.get("method"),
    }


def vix_realized_vol_payload(sp500_hist, vix_rows, rv_window=20):
    if sp500_hist is None or sp500_hist.empty or not vix_rows:
        return [], {
            "current_vix": None,
            "current_rv20": None,
            "current_premium": None,
            "current_ratio": None,
            "signal": False,
            "rv_window": rv_window,
        }

    closes = sp500_hist.sort_index()["Close"].dropna().astype(float)
    returns = np.log(closes / closes.shift(1))
    rv = returns.rolling(rv_window).std() * np.sqrt(252) * 100
    rv_df = pd.DataFrame({
        "spx": closes,
        "rv20": rv,
    })
    rv_df.index = pd.to_datetime(rv_df.index).normalize()
    rv_df = rv_df[~rv_df.index.duplicated(keep="last")]

    vix_df = pd.DataFrame(vix_rows)
    if vix_df.empty or "date" not in vix_df.columns or "vix" not in vix_df.columns:
        return [], {}
    vix_df["Date"] = pd.to_datetime(vix_df["date"], errors="coerce")
    vix_df["vix"] = pd.to_numeric(vix_df["vix"], errors="coerce")
    vix_df = vix_df.dropna(subset=["Date", "vix"]).set_index("Date").sort_index()
    vix_df.index = pd.to_datetime(vix_df.index).normalize()
    vix_df = vix_df[~vix_df.index.duplicated(keep="last")]

    merged = pd.concat([rv_df, vix_df[["vix"]]], axis=1, join="inner").dropna()
    rows = []
    for idx, row in merged.iterrows():
        rv20 = float(row["rv20"])
        vix = float(row["vix"])
        if not np.isfinite(rv20) or not np.isfinite(vix) or rv20 <= 0 or vix <= 0:
            continue
        premium = vix - rv20
        ratio = vix / rv20
        rows.append({
            "date": str(pd.Timestamp(idx).date()),
            "spx": round(float(row["spx"]), 4),
            "vix": round(vix, 2),
            "rv20": round(rv20, 2),
            "premium": round(premium, 2),
            "ratio": round(ratio, 3),
            "signal": bool(premium >= 10 and ratio >= 1.25),
        })

    current = rows[-1] if rows else {}
    return rows, {
        "current_vix": current.get("vix"),
        "current_rv20": current.get("rv20"),
        "current_premium": current.get("premium"),
        "current_ratio": current.get("ratio"),
        "signal": bool(current.get("signal")),
        "rv_window": rv_window,
        "signal_rule": "VIX - RV20 >= 10 and VIX/RV20 >= 1.25",
    }


def vix_spike_payload(years=20):
    hist = None
    source = "none"
    if EODHD.enabled:
        hist, source = EODHD.eod_history(["VIX.INDX"], years=years)
    if hist is None or hist.empty:
        hist = yf_df("^VIX", period=f"{years}y", interval="1d")
        source = "yfinance:^VIX" if hist is not None and not hist.empty else "none"
    if hist is None or hist.empty or "Close" not in hist.columns:
        return [], {"source": source, "status": "unavailable"}

    close = hist.sort_index()["Close"].dropna().astype(float)
    close.index = pd.to_datetime(close.index).normalize()
    close = close[~close.index.duplicated(keep="last")]
    pct = close.pct_change() * 100.0
    abs_change = close.diff()
    df = pd.DataFrame({"vix": close, "chg_pct": pct, "chg_abs": abs_change}).dropna()
    rows = []
    for idx, row in df.iterrows():
        chg = float(row["chg_pct"])
        rows.append({
            "date": str(pd.Timestamp(idx).date()),
            "vix": round(float(row["vix"]), 2),
            "chg_pct": round(chg, 3),
            "chg_abs": round(float(row["chg_abs"]), 3),
        })

    vals = np.array([r["chg_pct"] for r in rows], dtype=float)
    current = rows[-1] if rows else {}
    return rows, {
        "source": source,
        "status": "available" if rows else "unavailable",
        "current_date": current.get("date"),
        "current_vix": current.get("vix"),
        "current_chg_pct": current.get("chg_pct"),
        "current_chg_abs": current.get("chg_abs"),
        "upper_95": round(float(np.nanpercentile(vals, 95)), 3) if len(vals) else None,
        "lower_5": round(float(np.nanpercentile(vals, 5)), 3) if len(vals) else None,
        "observations": len(rows),
        "years": years,
    }


def eodhd_sp500_history(years=10, display_start="2016-01-01"):
    # Fetch pre-display warmup so visible weekly calculations start with real values.
    display_dt = datetime.datetime.strptime(display_start, "%Y-%m-%d").date()
    warmup_start = display_dt - datetime.timedelta(days=220)
    hist, source = EODHD.eod_history(["GSPC.INDX", "SPY.US"], years=years, start_date=warmup_start)
    if hist is not None and not hist.empty:
        visible = hist[hist.index >= pd.Timestamp(display_start)]
        return rows_from_history_df(visible, source), hist, source
    fallback_rows = chart_history("SPY", years=years, period=f"{years}y", limit=None)
    if fallback_rows:
        df = pd.DataFrame(fallback_rows)
        df["Date"] = pd.to_datetime(df["date"])
        df["Close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.set_index("Date")[["Close"]].dropna()
        visible_rows = [r for r in fallback_rows if r.get("date", "") >= display_start]
        return visible_rows, df, "polygon_fallback"
    return [], None, "none"


def weekly_rows_from_history_df(hist, source, display_start="2006-01-01"):
    if hist is None or hist.empty or "Close" not in hist.columns:
        return []
    closes = hist["Close"].astype(float).dropna().sort_index()
    weekly = closes.resample("W-FRI").last().dropna()
    rows = []
    for idx, close in weekly.items():
        date_s = str(pd.Timestamp(idx).date())
        if date_s >= display_start:
            rows.append({"date": date_s, "close": round(float(close), 4), "source": source})
    return rows


def sp500_weekly_rsi_payload(hist, source, display_start="2016-01-01"):
    if hist is None or hist.empty or "Close" not in hist.columns:
        return [], [], {"status": "unavailable", "source": source}
    closes = hist["Close"].astype(float).dropna().sort_index()
    weekly = closes.resample("W-FRI").last().dropna()
    rsi_values = calc_rsi_series(weekly.tolist(), 14)
    weekly_rows = []
    for idx, close, rsi in zip(weekly.index, weekly.values, rsi_values):
        weekly_rows.append({
            "date": str(pd.Timestamp(idx).date()),
            "close": round(float(close), 4),
            "rsi": None if rsi is None else round(float(rsi), 2),
        })
    weekly_rows = [row for row in weekly_rows if row["date"] >= display_start]

    signals = []
    start = None
    for i, row in enumerate(weekly_rows):
        rsi = row["rsi"]
        if rsi is None:
            continue
        if start is None and rsi < 40:
            start = i
        if start is not None and rsi >= 60:
            start_row = weekly_rows[start]
            signals.append({
                "start_index": start,
                "confirm_index": i,
                "start_date": start_row["date"],
                "confirm_date": row["date"],
                "start_rsi": start_row["rsi"],
                "confirm_rsi": row["rsi"],
                "start_close": start_row["close"],
                "confirm_close": row["close"],
            })
            start = None

    last_week = next((r for r in reversed(weekly_rows) if r["rsi"] is not None), None)
    last_signal = signals[-1] if signals else None
    if last_week is None:
        status = "unavailable"
    elif start is not None:
        status = "setup"
    elif last_signal and last_signal["confirm_date"] == last_week["date"]:
        status = "confirmed"
    elif last_week["rsi"] >= 60:
        status = "bullish"
    elif last_week["rsi"] < 40:
        status = "oversold"
    else:
        status = "neutral"

    return weekly_rows, signals, {
        "status": status,
        "source": source,
        "weekly_rsi": None if last_week is None else last_week["rsi"],
        "weekly_date": None if last_week is None else last_week["date"],
        "last_signal": last_signal,
        "active_setup_start": None if start is None else weekly_rows[start],
    }


def bt20_breadth_payload(years=10, display_start="2016-01-01"):
    hist, source = EODHD.eod_history(["S5TW.INDX"], years=years, start_date=datetime.date(2016, 1, 1))
    if hist is None or hist.empty or "Close" not in hist.columns:
        return [], [], {"status": "unavailable", "source": source}

    closes = hist["Close"].astype(float).dropna().sort_index()
    rows = []
    for idx, val in closes.items():
        date_s = str(pd.Timestamp(idx).date())
        if date_s < display_start:
            continue
        rows.append({"date": date_s, "value": round(float(val), 2), "source": source})

    signals = []
    setup_idx = None
    for i, row in enumerate(rows):
        val = row["value"]
        if setup_idx is None:
            if val < 15:
                setup_idx = i
            continue
        if val < rows[setup_idx]["value"]:
            setup_idx = i
        if i - setup_idx > 15:
            setup_idx = i if val < 15 else None
            continue
        if val >= 80:
            start = rows[setup_idx]
            signals.append({
                "start_index": setup_idx,
                "confirm_index": i,
                "start_date": start["date"],
                "confirm_date": row["date"],
                "start_value": start["value"],
                "confirm_value": row["value"],
            })
            setup_idx = None

    last = rows[-1] if rows else None
    last_signal = signals[-1] if signals else None
    if last is None:
        status = "unavailable"
    elif setup_idx is not None:
        status = "setup"
    elif last_signal and last_signal["confirm_date"] == last["date"]:
        status = "confirmed"
    elif last["value"] >= 80:
        status = "thrust_zone"
    elif last["value"] <= 15:
        status = "oversold"
    else:
        status = "neutral"

    return rows, signals, {
        "status": status,
        "source": source,
        "current": None if last is None else last["value"],
        "current_date": None if last is None else last["date"],
        "last_signal": last_signal,
        "active_setup_start": None if setup_idx is None else rows[setup_idx],
    }


def bt50_weekly_breadth_payload(years=20, display_start="2006-01-01"):
    hist, source = EODHD.eod_history(["S5FI.INDX"], years=years, start_date=datetime.date(2006, 1, 1))
    if hist is None or hist.empty or "Close" not in hist.columns:
        return [], [], {"status": "unavailable", "source": source}

    closes = hist["Close"].astype(float).dropna().sort_index()
    weekly = closes.resample("W-FRI").last().dropna()
    rows = []
    for idx, val in weekly.items():
        date_s = str(pd.Timestamp(idx).date())
        if date_s >= display_start:
            rows.append({"date": date_s, "value": round(float(val), 2), "source": source})

    signals = []
    setup_idx = None
    for i, row in enumerate(rows):
        val = row["value"]
        if setup_idx is None:
            if val < 10:
                setup_idx = i
            continue
        if val < rows[setup_idx]["value"]:
            setup_idx = i
        if i - setup_idx > 12:
            setup_idx = i if val < 10 else None
            continue
        if val >= 80:
            start = rows[setup_idx]
            signals.append({
                "start_index": setup_idx,
                "confirm_index": i,
                "start_date": start["date"],
                "confirm_date": row["date"],
                "start_value": start["value"],
                "confirm_value": row["value"],
            })
            setup_idx = None

    last = rows[-1] if rows else None
    last_signal = signals[-1] if signals else None
    if last is None:
        status = "unavailable"
    elif setup_idx is not None:
        status = "setup"
    elif last_signal and last_signal["confirm_date"] == last["date"]:
        status = "confirmed"
    elif last["value"] >= 80:
        status = "thrust_zone"
    elif last["value"] <= 10:
        status = "oversold"
    else:
        status = "neutral"

    return rows, signals, {
        "status": status,
        "source": source,
        "current": None if last is None else last["value"],
        "current_date": None if last is None else last["date"],
        "last_signal": last_signal,
        "active_setup_start": None if setup_idx is None else rows[setup_idx],
    }


def oil_market_payload(sp500_hist=None, years=10, display_start="2016-01-01"):
    wti = brent = None
    wti_source = brent_source = "none"
    try:
        fred_url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DCOILWTICO,DCOILBRENTEU"
        fred = pd.read_csv(fred_url)
        fred["Date"] = pd.to_datetime(fred["observation_date"]).dt.tz_localize(None)
        fred = fred.replace(".", np.nan)
        fred["DCOILWTICO"] = pd.to_numeric(fred["DCOILWTICO"], errors="coerce")
        fred["DCOILBRENTEU"] = pd.to_numeric(fred["DCOILBRENTEU"], errors="coerce")
        fred = fred.set_index("Date").sort_index()
        wti = fred[["DCOILWTICO"]].rename(columns={"DCOILWTICO": "Close"}).dropna()
        brent = fred[["DCOILBRENTEU"]].rename(columns={"DCOILBRENTEU": "Close"}).dropna()
        wti_source = "fred:DCOILWTICO"
        brent_source = "fred:DCOILBRENTEU"
    except Exception:
        pass
    if wti is None or wti.empty:
        wti, wti_source = EODHD.commodity_history("WTI", years=years, interval="daily", start_date=datetime.date(2016, 1, 1))
    if brent is None or brent.empty:
        brent, brent_source = EODHD.commodity_history("BRENT", years=years, interval="daily", start_date=datetime.date(2016, 1, 1))
    if wti is None or wti.empty:
        fallback = yf_df("CL=F", period="10y")
        if fallback is not None and not fallback.empty:
            wti = fallback[["Close"]].copy()
            wti_source = "yfinance:CL=F"
    if brent is None or brent.empty:
        fallback = yf_df("BZ=F", period="10y")
        if fallback is not None and not fallback.empty:
            brent = fallback[["Close"]].copy()
            brent_source = "yfinance:BZ=F"

    cl_curve_rows, cl_curve_status = cl1_cl2_curve_payload(sp500_hist=sp500_hist)
    rows = []
    if wti is not None and not wti.empty:
        w = wti["Close"].astype(float).dropna().sort_index()
        s = None
        if sp500_hist is not None and not sp500_hist.empty and "Close" in sp500_hist.columns:
            s = sp500_hist["Close"].astype(float).dropna().sort_index()
        merged = pd.DataFrame({"wti": w})
        if brent is not None and not brent.empty:
            merged["brent"] = brent["Close"].astype(float).dropna().sort_index()
        if s is not None and not s.empty:
            merged["sp500"] = s
        merged = merged.ffill().dropna(subset=["wti"])
        merged = merged[merged.index >= pd.Timestamp(display_start)]
        for idx, r in merged.iterrows():
            item = {
                "date": str(pd.Timestamp(idx).date()),
                "wti": round(float(r["wti"]), 4),
                "source": wti_source,
            }
            if pd.notna(r.get("brent")):
                item["brent"] = round(float(r["brent"]), 4)
                item["wti_brent_spread"] = round(float(r["wti"] - r["brent"]), 4)
            if pd.notna(r.get("sp500")):
                item["sp500"] = round(float(r["sp500"]), 4)
            rows.append(item)

    if cl_curve_rows:
        return cl_curve_rows, cl_curve_status

    latest = rows[-1] if rows else {}
    return rows, {
        "source": wti_source,
        "brent_source": brent_source,
        "current_wti": latest.get("wti"),
        "current_brent": latest.get("brent"),
        "current_spread": latest.get("wti_brent_spread"),
        "current_date": latest.get("date"),
        "spread_label": "WTI - Brent",
    }


def gold_silver_ratio_payload(display_start="2000-01-01", rolling_window=156):
    gold = yf_df("GC=F", period="max", interval="1d")
    silver = yf_df("SI=F", period="max", interval="1d")
    source = "yfinance:GC=F/SI=F"
    if gold is None or gold.empty or silver is None or silver.empty:
        return [], {"source": source, "status": "unavailable"}

    gold = gold.copy()
    silver = silver.copy()
    gold.index = pd.to_datetime(gold.index).tz_localize(None)
    silver.index = pd.to_datetime(silver.index).tz_localize(None)
    merged = pd.DataFrame({
        "gold": gold["Close"].astype(float).dropna().sort_index(),
        "silver": silver["Close"].astype(float).dropna().sort_index(),
    }).ffill().dropna()
    merged = merged[merged["silver"] > 0]
    if merged.empty:
        return [], {"source": source, "status": "unavailable"}

    weekly = merged.resample("W-FRI").last().dropna()
    weekly["ratio"] = weekly["gold"] / weekly["silver"]
    weekly = weekly[weekly.index >= pd.Timestamp(display_start)]
    static_mean = float(weekly["ratio"].mean()) if not weekly.empty else None
    static_sigma = float(weekly["ratio"].std(ddof=1)) if len(weekly) > 1 else None
    weekly["rolling_mean"] = weekly["ratio"].rolling(rolling_window, min_periods=max(26, rolling_window // 3)).mean()
    weekly["rolling_sigma"] = weekly["ratio"].rolling(rolling_window, min_periods=max(26, rolling_window // 3)).std(ddof=1)

    def sigma_pack(ratio, mean, sigma):
        if mean is None or sigma is None or not sigma or pd.isna(mean) or pd.isna(sigma):
            return {"mean": None, "sigma": None, "z": None, "signal": None, "status": "unavailable"}
        z = (ratio - mean) / sigma
        if z >= 2:
            status, signal = "plus2_extreme", "plus2"
        elif z >= 1:
            status, signal = "plus1_high", None
        elif z <= -2:
            status, signal = "minus2_extreme", "minus2"
        elif z <= -1:
            status, signal = "minus1_low", None
        else:
            status, signal = "mean_zone", None
        return {"mean": float(mean), "sigma": float(sigma), "z": float(z), "signal": signal, "status": status}

    rows = []
    for idx, row in weekly.iterrows():
        ratio = float(row["ratio"])
        rolling = sigma_pack(ratio, row.get("rolling_mean"), row.get("rolling_sigma"))
        static = sigma_pack(ratio, static_mean, static_sigma)
        item = {
            "date": str(pd.Timestamp(idx).date()),
            "ratio": round(ratio, 4),
            "gold": round(float(row["gold"]), 4),
            "silver": round(float(row["silver"]), 4),
            "rolling_mean": round(rolling["mean"], 4) if rolling["mean"] is not None else None,
            "rolling_sigma": round(rolling["sigma"], 4) if rolling["sigma"] is not None else None,
            "rolling_z": round(rolling["z"], 4) if rolling["z"] is not None else None,
            "rolling_signal": rolling["signal"],
            "static_z": round(static["z"], 4) if static["z"] is not None else None,
            "static_signal": static["signal"],
        }
        rows.append(item)

    latest = rows[-1] if rows else {}
    rolling_latest = sigma_pack(
        latest.get("ratio"),
        latest.get("rolling_mean"),
        latest.get("rolling_sigma"),
    ) if latest else {"status": "unavailable", "z": None}
    static_latest = sigma_pack(latest.get("ratio"), static_mean, static_sigma) if latest else {"status": "unavailable", "z": None}
    return rows, {
        "source": source,
        "status": rolling_latest["status"],
        "current_ratio": latest.get("ratio"),
        "current_date": latest.get("date"),
        "rolling_window": rolling_window,
        "rolling_mean": latest.get("rolling_mean"),
        "rolling_sigma": latest.get("rolling_sigma"),
        "rolling_z_score": round(rolling_latest["z"], 4) if rolling_latest["z"] is not None else None,
        "rolling_status": rolling_latest["status"],
        "static_mean": round(static_mean, 4) if static_mean is not None else None,
        "static_sigma": round(static_sigma, 4) if static_sigma is not None else None,
        "static_z_score": round(static_latest["z"], 4) if static_latest["z"] is not None else None,
        "static_status": static_latest["status"],
    }


FORWARD_PE_SEED_TEXT = """
Apr 2026 21.54
Mar 2026 20.85
Feb 2026 21.97
Jan 2026 22.17
Dec 2025 22.70
Nov 2025 22.72
Oct 2025 22.69
Sep 2025 22.89
Aug 2025 22.11
Jul 2025 21.70
Jun 2025 22.15
May 2025 21.11
Apr 2025 19.88
Mar 2025 20.95
Feb 2025 22.23
Jan 2025 22.55
Dec 2024 22.88
Nov 2024 23.47
Oct 2024 22.19
Sep 2024 23.61
Aug 2024 23.14
Jul 2024 22.62
Jun 2024 22.91
May 2024 22.14
Apr 2024 21.12
Mar 2024 22.31
Feb 2024 21.64
Jan 2024 20.58
Dec 2023 20.91
Nov 2023 20.03
Oct 2023 18.39
Sep 2023 19.39
Aug 2023 20.38
Jul 2023 20.75
Jun 2023 20.45
May 2023 19.21
Apr 2023 19.16
Mar 2023 19.07
Feb 2023 18.43
Jan 2023 18.92
Dec 2022 18.12
Nov 2022 19.25
Oct 2022 18.27
Sep 2022 17.08
Aug 2022 18.83
Jul 2022 19.67
Jun 2022 18.74
May 2022 20.46
Apr 2022 20.46
Mar 2022 22.80
Feb 2022 22.01
Jan 2022 22.72
Dec 2021 23.23
Nov 2021 22.26
Oct 2021 22.45
Sep 2021 20.83
Aug 2021 21.87
Jul 2021 21.25
Jun 2021 20.26
May 2021 19.82
Apr 2021 19.72
Mar 2021 18.91
Feb 2021 18.14
Jan 2021 17.68
Dec 2020 19.63
Nov 2020 18.92
Oct 2020 17.09
Sep 2020 18.99
Aug 2020 19.76
Jul 2020 18.47
Jun 2020 20.44
May 2020 20.08
Apr 2020 19.21
Mar 2020 20.93
Feb 2020 23.92
Jan 2020 26.12
Dec 2019 25.95
Nov 2019 25.23
Oct 2019 24.40
Sep 2019 23.55
Aug 2019 23.15
Jul 2019 23.58
Jun 2019 21.03
May 2019 19.67
Apr 2019 21.06
Mar 2019 17.88
Feb 2019 17.56
Jan 2019 17.06
Dec 2018 16.24
Nov 2018 17.88
Oct 2018 17.57
Sep 2018 18.69
Aug 2018 18.61
Jul 2018 18.06
Jun 2018 17.60
May 2018 17.52
Apr 2018 17.15
Mar 2018 17.26
Feb 2018 17.74
Jan 2018 18.46
Dec 2017 17.61
Nov 2017 17.44
Oct 2017 16.97
Sep 2017 17.79
Aug 2017 17.45
Jul 2017 17.44
Jun 2017 18.16
May 2017 18.08
Apr 2017 17.87
Mar 2017 18.81
Feb 2017 18.81
Jan 2017 18.14
Dec 2016 18.71
Nov 2016 18.38
Oct 2016 17.77
Sep 2016 18.54
Aug 2016 18.56
Jul 2016 18.58
Jun 2016 18.72
May 2016 18.70
Apr 2016 18.42
Mar 2016 19.21
Feb 2016 18.02
Jan 2016 18.10
Dec 2015 19.97
Nov 2015 20.33
Oct 2015 20.32
Sep 2015 19.38
Aug 2015 19.91
Jul 2015 21.24
Jun 2015 20.73
May 2015 21.18
Apr 2015 20.96
Mar 2015 20.40
Feb 2015 20.76
Jan 2015 19.68
Dec 2014 19.59
Nov 2014 19.68
Oct 2014 19.20
Sep 2014 18.05
Aug 2014 18.33
Jul 2014 17.67
Jun 2014 17.42
May 2014 17.10
Apr 2014 16.74
Mar 2014 16.42
Feb 2014 16.31
Jan 2014 15.63
Dec 2013 16.00
Nov 2013 15.63
Oct 2013 15.20
Sep 2013 14.90
Aug 2013 14.47
Jul 2013 14.94
Jun 2013 14.62
May 2013 14.85
Apr 2013 14.54
Mar 2013 14.49
Feb 2013 13.99
Jan 2013 13.84
Dec 2012 13.83
Nov 2012 13.73
Oct 2012 13.69
Sep 2012 14.38
Aug 2012 14.04
Jul 2012 13.77
Jun 2012 13.73
May 2012 13.20
Apr 2012 14.09
Mar 2012 14.42
Feb 2012 13.98
Jan 2012 13.43
Dec 2011 12.80
Nov 2011 12.69
Oct 2011 12.75
Sep 2011 11.36
Aug 2011 12.24
Jul 2011 12.98
Jun 2011 13.34
May 2011 13.59
Apr 2011 13.77
Mar 2011 13.62
Feb 2011 13.64
Jan 2011 13.22
Dec 2010 13.17
Nov 2010 12.36
Oct 2010 12.39
Sep 2010 12.44
Aug 2010 11.44
Jul 2010 12.01
Jun 2010 11.75
May 2010 12.42
Apr 2010 13.53
Mar 2010 13.83
Feb 2010 13.07
Jan 2010 12.70
Dec 2009 13.99
Nov 2009 13.74
Oct 2009 13.00
Sep 2009 14.31
Aug 2009 13.81
Jul 2009 13.37
Jun 2009 13.78
May 2009 13.77
"""


def parse_forward_pe_rows(text):
    month_map = {m: i for i, m in enumerate(["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}
    rows = {}
    for mon, year, val in re.findall(r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})\s+(-?\d+(?:\.\d+)?)\b", text or ""):
        date = datetime.date(int(year), month_map[mon], 1)
        pe = float(val)
        if 0 < pe < 80:
            rows[str(date)] = {"date": str(date), "pe": round(pe, 2)}
    return [rows[k] for k in sorted(rows)]


def fetch_trendonify_forward_pe():
    url = "https://trendonify.com/united-states/stock-market/forward-pe-ratio"
    try:
        r = requests.get(url, timeout=25, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code >= 400 or "Just a moment" in r.text:
            return []
        return parse_forward_pe_rows(r.text)
    except Exception:
        return []


def fetch_stockmarket_forward_pe_current():
    try:
        r = requests.get("https://www.stockmarketperatio.com/", timeout=25, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code >= 400:
            return None
        m_val = re.search(r'id=["\']forwardPE["\'][^>]*>\s*([0-9.]+)\s*<', r.text)
        m_date = re.search(r'Data as of\s+(\d{4}-\d{2}-\d{2})', r.text)
        if not (m_val and m_date):
            return None
        dt = pd.Timestamp(m_date.group(1)).date().replace(day=1)
        val = float(m_val.group(1))
        if 0 < val < 80:
            return {"date": str(dt), "pe": round(val, 2), "source": "stockmarketperatio"}
    except Exception:
        return None
    return None


def forward_pe_payload():
    rows = fetch_trendonify_forward_pe()
    source = "Trendonify"
    if len(rows) < 100:
        rows = parse_forward_pe_rows(FORWARD_PE_SEED_TEXT)
        source = "Trendonify seed + StockMarketPERatio current fallback"

    current = fetch_stockmarket_forward_pe_current()
    if current:
        by_date = {r["date"]: dict(r) for r in rows}
        latest_seed_date = max(by_date) if by_date else ""
        # Keep the Trendonify seed for overlapping months; use the free current feed only to extend.
        if current["date"] > latest_seed_date:
            by_date[current["date"]] = current
            rows = [by_date[k] for k in sorted(by_date)]

    if not rows:
        return [], {"source": source, "status": "unavailable"}

    dates = pd.to_datetime([r["date"] for r in rows])
    vals = np.array([float(r["pe"]) for r in rows], dtype=float)
    latest = rows[-1]
    latest_dt = pd.Timestamp(latest["date"])

    def period_stats(years):
        start = latest_dt - pd.DateOffset(years=years)
        sample = vals[dates >= start]
        if len(sample) == 0:
            return {"avg": None, "median": None, "min": None, "max": None, "obs": 0}
        rank = float((sample <= float(latest["pe"])).sum() / len(sample) * 100)
        return {
            "avg": round(float(np.mean(sample)), 2),
            "median": round(float(np.median(sample)), 2),
            "min": round(float(np.min(sample)), 2),
            "max": round(float(np.max(sample)), 2),
            "percentile": round(rank, 1),
            "obs": int(len(sample)),
        }

    avg20 = period_stats(20)
    pe = float(latest["pe"])
    ref = avg20.get("avg") or float(np.mean(vals))
    status = "expensive" if pe >= ref * 1.18 else "above_average" if pe >= ref * 1.06 else "cheap" if pe <= ref * 0.88 else "fair_value"
    return rows, {
        "source": source,
        "status": status,
        "current_pe": round(pe, 2),
        "current_date": latest["date"],
        "start_date": rows[0]["date"],
        "observations": len(rows),
        "avg_5y": period_stats(5),
        "avg_10y": period_stats(10),
        "avg_20y": avg20,
        "avg_all": round(float(np.mean(vals)), 2),
        "median_all": round(float(np.median(vals)), 2),
    }


def cl_contract_symbol(year, month):
    codes = {1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M", 7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z"}
    return f"CL{codes[month]}{str(year)[-2:]}.NYM"


def add_months_date(date_obj, months):
    y = date_obj.year + (date_obj.month - 1 + months) // 12
    m = (date_obj.month - 1 + months) % 12 + 1
    return datetime.date(y, m, 1)


def cl1_cl2_curve_payload(sp500_hist=None):
    today = datetime.date.today()
    candidates = []
    for offset in range(0, 10):
        d = add_months_date(today, offset)
        candidates.append(cl_contract_symbol(d.year, d.month))

    usable = []
    for sym in candidates:
        try:
            hist = yf_df(sym, period="18mo", interval="1d")
            if hist is None or hist.empty or "Close" not in hist.columns or len(hist) < 120:
                continue
            last_date = pd.Timestamp(hist.index[-1]).date()
            if (today - last_date).days > 14:
                continue
            usable.append((sym, hist[["Close"]].copy()))
            if len(usable) >= 2:
                break
        except Exception:
            continue

    if len(usable) < 2:
        return [], {}

    (cl1_sym, cl1), (cl2_sym, cl2) = usable[0], usable[1]
    cl1.index = pd.to_datetime(cl1.index).tz_localize(None).normalize()
    cl2.index = pd.to_datetime(cl2.index).tz_localize(None).normalize()
    merged = pd.DataFrame({
        "cl1": cl1["Close"].astype(float).dropna().sort_index(),
        "cl2": cl2["Close"].astype(float).dropna().sort_index(),
    })
    if sp500_hist is not None and not sp500_hist.empty and "Close" in sp500_hist.columns:
        merged["sp500"] = sp500_hist["Close"].astype(float).dropna().sort_index()
    merged = merged.ffill().dropna(subset=["cl1", "cl2"])
    start = pd.Timestamp(today - datetime.timedelta(days=370))
    merged = merged[merged.index >= start]
    # Use CL2 - CL1 so negative values mean backwardation/front-month premium.
    merged["cl1_cl2_spread"] = merged["cl2"] - merged["cl1"]

    rows = []
    for idx, r in merged.iterrows():
        item = {
            "date": str(pd.Timestamp(idx).date()),
            "cl1": round(float(r["cl1"]), 4),
            "cl2": round(float(r["cl2"]), 4),
            "cl1_cl2_spread": round(float(r["cl1_cl2_spread"]), 4),
            "cl1_symbol": cl1_sym,
            "cl2_symbol": cl2_sym,
            "source": f"yfinance:{cl1_sym}/{cl2_sym}",
        }
        if pd.notna(r.get("sp500")):
            item["sp500"] = round(float(r["sp500"]), 4)
        rows.append(item)

    latest = rows[-1] if rows else {}
    return rows, {
        "source": f"yfinance:{cl1_sym}/{cl2_sym}",
        "current_cl1": latest.get("cl1"),
        "current_cl2": latest.get("cl2"),
        "current_spread": latest.get("cl1_cl2_spread"),
        "current_date": latest.get("date"),
        "spread_label": "CL2 - CL1",
        "cl1_symbol": cl1_sym,
        "cl2_symbol": cl2_sym,
    }


def dividend_adjusted_history(symbol, years=25, period="max", limit=260):
    hist, source = history_df(symbol, years=years, period=period)
    if hist is None or hist.empty or "Close" not in hist.columns:
        return []
    hist = hist.sort_index().dropna(subset=["Close"])
    if len(hist) < 2:
        return []

    start_date = pd.Timestamp(hist.index[0]).date()
    end_date = pd.Timestamp(hist.index[-1]).date()
    div_by_date = {}

    pt = polygon_stock_symbol(symbol)
    if pt and POLYGON.enabled:
        for row in POLYGON.dividends(pt, start_date, end_date):
            ex_date = row.get("ex_dividend_date")
            cash = row.get("cash_amount")
            if not ex_date or cash is None:
                continue
            key = str(ex_date)[:10]
            div_by_date[key] = div_by_date.get(key, 0.0) + float(cash)

    if not div_by_date:
        try:
            divs = yf.Ticker(symbol).dividends
            for idx, val in divs.items():
                d = pd.Timestamp(idx).tz_localize(None).date()
                if start_date <= d <= end_date:
                    key = str(d)
                    div_by_date[key] = div_by_date.get(key, 0.0) + float(val)
            if div_by_date:
                source = f"{source}+yfinance_dividends"
        except Exception:
            pass
    elif source == "polygon":
        source = "polygon_total_return"

    closes = hist["Close"].astype(float)
    wealth = []
    prev_close = float(closes.iloc[0])
    tr_value = prev_close
    for i, (idx, close) in enumerate(closes.items()):
        close = float(close)
        if i == 0:
            wealth.append(tr_value)
            prev_close = close
            continue
        div = div_by_date.get(str(pd.Timestamp(idx).date()), 0.0)
        if prev_close > 0:
            tr_value *= (close + div) / prev_close
        else:
            tr_value = close
        wealth.append(tr_value)
        prev_close = close

    out = hist.copy()
    out["Close"] = wealth
    rows = []
    for idx, row in out.tail(limit).iterrows():
        close = row.get("Close")
        if pd.isna(close):
            continue
        rows.append({
            "date": str(pd.Timestamp(idx).date()),
            "close": round(float(close), 4),
            "source": source,
            "dividend_adjusted": bool(div_by_date),
        })
    return rows


def high_yield_nhnl_history(limit=260):
    series = []
    for symbol in HY_BREADTH_SYMBOLS:
        hist, _source = history_df(symbol, years=2, period="2y")
        if hist is None or hist.empty or "Close" not in hist.columns or len(hist) < 120:
            continue
        close = hist["Close"].astype(float).dropna()
        if len(close) < 120:
            continue
        close.index = pd.to_datetime(close.index).normalize()
        close = close[~close.index.duplicated(keep="last")].sort_index()
        roll_hi = close.rolling(252, min_periods=80).max()
        roll_lo = close.rolling(252, min_periods=80).min()
        df = pd.DataFrame({
            f"{symbol}_nh": close >= roll_hi * 0.995,
            f"{symbol}_nl": close <= roll_lo * 1.005,
            f"{symbol}_ok": roll_hi.notna() & roll_lo.notna(),
        }, index=close.index)
        series.append(df)

    if not series:
        return [], {"raw": None, "scaled": None, "new_highs": 0, "new_lows": 0, "universe": 0}

    merged = pd.concat(series, axis=1, join="outer", sort=False).sort_index().ffill()
    rows = []
    def is_true_value(value):
        return pd.notna(value) and bool(value) is True

    for idx, row in merged.iterrows():
        ok_cols = [c for c in merged.columns if c.endswith("_ok")]
        ok = [c[:-3] for c in ok_cols if is_true_value(row.get(c, False))]
        if not ok:
            continue
        nh = sum(1 for base in ok if is_true_value(row.get(base + "_nh", False)))
        nl = sum(1 for base in ok if is_true_value(row.get(base + "_nl", False)))
        raw = nh - nl
        universe = len(ok)
        scaled = round(raw / universe * 500, 1) if universe else 0
        rows.append({
            "date": str(pd.Timestamp(idx).date()),
            "raw": int(raw),
            "scaled": scaled,
            "new_highs": int(nh),
            "new_lows": int(nl),
            "universe": int(universe),
        })

    rows = rows[-limit:]
    current = rows[-1] if rows else {"raw": None, "scaled": None, "new_highs": 0, "new_lows": 0, "universe": 0}
    return rows, current


def fetch_market_indicators():
    result = {
        "vix": None, "vix_sma20": None,
        "vix3m": None, "vix9d": None,
        "vix_spread": None, "pc_ratio": None,
        "vix_history": [],
        "vix_sd_history": [],
        "vix_sd_current": {},
        "vix_realized_vol_history": [],
        "vix_realized_vol_current": {},
        "vix_spike_history": [],
        "vix_spike_status": {},
        "spy_history": [],
        "sp500_history": [],
        "sp500_history_20y": [],
        "sp500_ema_weekly_history": [],
        "sp500_rsi_weekly": [],
        "sp500_rsi_signals": [],
        "sp500_rsi_status": {},
        "bt20_history": [],
        "bt20_signals": [],
        "bt20_status": {},
        "bt50_sp500_history": [],
        "bt50_history": [],
        "bt50_signals": [],
        "bt50_status": {},
        "hyg_history": [],
        "hyg_price": None,
        "hyg_nhnl_history": [],
        "hyg_nhnl_current": {},
        "oil_history": [],
        "oil_status": {},
        "gold_silver_ratio_history": [],
        "gold_silver_ratio_status": {},
        "forward_pe_history": [],
        "forward_pe_status": {},
        "return_histograms": {},
        "options_put_iv": {},
        "unusual_options_trades": {},
        "pc_total_history": [],
        "pc_equity_history": [],
    }
    vix_hist = None
    v3m_hist = None

    sp500_rows, sp500_hist, sp500_source = eodhd_sp500_history(years=10)
    sp500_ema_rows, sp500_ema_hist, sp500_ema_source = eodhd_sp500_history(years=20, display_start="2006-01-01")
    sp500_20y_rows, sp500_20y_hist, sp500_20y_source = eodhd_sp500_history(years=20, display_start="2006-01-01")
    weekly_rsi, rsi_signals, rsi_status = sp500_weekly_rsi_payload(sp500_20y_hist, sp500_20y_source, display_start="2006-01-01")
    bt20_rows, bt20_signals, bt20_status = bt20_breadth_payload(years=10)
    bt50_rows, bt50_signals, bt50_status = bt50_weekly_breadth_payload(years=20)
    result["spy_history"] = sp500_rows
    result["sp500_history"] = sp500_rows
    result["sp500_history_20y"] = sp500_20y_rows
    result["sp500_ema_weekly_history"] = weekly_rows_from_history_df(sp500_ema_hist, sp500_ema_source, display_start="2006-01-01")
    result["bt50_sp500_history"] = weekly_rows_from_history_df(sp500_20y_hist, sp500_20y_source, display_start="2006-01-01")
    result["sp500_rsi_weekly"] = weekly_rsi
    result["sp500_rsi_signals"] = rsi_signals
    result["sp500_rsi_status"] = rsi_status
    result["bt20_history"] = bt20_rows
    result["bt20_signals"] = bt20_signals
    result["bt20_status"] = bt20_status
    result["bt50_history"] = bt50_rows
    result["bt50_signals"] = bt50_signals
    result["bt50_status"] = bt50_status
    result["vix_sd_history"], result["vix_sd_current"] = vix_sd_payload(years=20)
    result["vix_realized_vol_history"], result["vix_realized_vol_current"] = vix_realized_vol_payload(
        sp500_20y_hist,
        result["vix_sd_history"],
        rv_window=20,
    )
    result["vix_spike_history"], result["vix_spike_status"] = vix_spike_payload(years=20)
    result["oil_history"], result["oil_status"] = oil_market_payload(sp500_hist, years=10)
    result["gold_silver_ratio_history"], result["gold_silver_ratio_status"] = gold_silver_ratio_payload()
    result["forward_pe_history"], result["forward_pe_status"] = forward_pe_payload()
    result["hyg_history"] = dividend_adjusted_history("HYG", years=25, period="max")
    hyg_price_history = chart_history("HYG", years=1, period="1y")
    if hyg_price_history:
        result["hyg_price"] = hyg_price_history[-1]["close"]
    result["hyg_nhnl_history"], result["hyg_nhnl_current"] = high_yield_nhnl_history()
    result["return_histograms"] = return_histogram_payload()
    result["options_put_iv"] = options_put_iv_payload()
    result["unusual_options_trades"] = unusual_options_trades_payload()
    print(f"  S&P/EODHD history: {len(result['sp500_history'])} days ({sp500_source})")
    print(f"  S&P weekly EMA history: {len(result['sp500_ema_weekly_history'])} weeks ({sp500_ema_source})")
    print(f"  S&P 20Y weekly history: {len(result['bt50_sp500_history'])} weeks ({sp500_20y_source})")
    print(f"  S&P weekly RSI: {len(result['sp500_rsi_weekly'])} weeks | signals: {len(result['sp500_rsi_signals'])}")
    print(f"  S&P 20D breadth: {len(result['bt20_history'])} days | signals: {len(result['bt20_signals'])}")
    print(f"  S&P 50D weekly breadth: {len(result['bt50_history'])} weeks | signals: {len(result['bt50_signals'])}")
    print(f"  VIX SD history: {len(result['vix_sd_history'])} days | SD: {result['vix_sd_current'].get('current_sd')}")
    print(f"  VIX vs RV20 history: {len(result['vix_realized_vol_history'])} days | premium: {result['vix_realized_vol_current'].get('current_premium')}")
    print(f"  VIX spike history: {len(result['vix_spike_history'])} days | change: {result['vix_spike_status'].get('current_chg_pct')}%")
    print(f"  Oil history: {len(result['oil_history'])} days | spread: {result['oil_status'].get('current_spread')}")
    print(f"  Gold/Silver ratio: {len(result['gold_silver_ratio_history'])} weeks | ratio: {result['gold_silver_ratio_status'].get('current_ratio')}")
    print(f"  Forward P/E history: {len(result['forward_pe_history'])} months | PE: {result['forward_pe_status'].get('current_pe')}")
    print(f"  HYG total-return history: {len(result['hyg_history'])} days")
    print(f"  HYG NH-NL history: {len(result['hyg_nhnl_history'])} days")
    print(f"  Return histograms: {len(result['return_histograms'])} symbols")
    print(f"  Options put IV: {len(result['options_put_iv'])} symbols")
    print(f"  Unusual options trades: {len(result['unusual_options_trades'].get('rows', []))} rows")

    # ── VIX 30D ──────────────────────────────────────────────────
    try:
        tk = yf.Ticker("^VIX")
        vix_hist = yf_history(tk, "15mo", "1d")
        if vix_hist is not None and not vix_hist.empty:
            result["vix"] = round(float(vix_hist["Close"].iloc[-1]), 2)
            if len(vix_hist) >= 20:
                result["vix_sma20"] = round(float(vix_hist["Close"].tail(20).mean()), 2)
            print(f"  VIX: {result['vix']} ({len(vix_hist)} days)")
    except Exception as e:
        print(f"  VIX error: {e}")

    # ── VIX3M ────────────────────────────────────────────────────
    for sym in ["^VIX3M", "VXMT", "^VXV"]:
        try:
            tk3 = yf.Ticker(sym)
            v3m_hist = yf_history(tk3, "15mo", "1d")
            if v3m_hist is not None and not v3m_hist.empty:
                result["vix3m"] = round(float(v3m_hist["Close"].iloc[-1]), 2)
                print(f"  VIX3M ({sym}): {result['vix3m']}")
                break
            print(f"  VIX3M {sym}: no data")
        except Exception as e:
            print(f"  VIX3M {sym}: {e}")

    # ── VIX9D ────────────────────────────────────────────────────
    for sym in ["^VIX9D", "^VXST"]:
        try:
            tk9 = yf.Ticker(sym)
            h9  = yf_history(tk9, "1mo", "1d")
            if h9 is not None and not h9.empty:
                result["vix9d"] = round(float(h9["Close"].iloc[-1]), 2)
                print(f"  VIX9D ({sym}): {result['vix9d']}")
                break
            print(f"  VIX9D {sym}: no data")
        except Exception as e:
            print(f"  VIX9D {sym}: {e}")

    # ── Spread ───────────────────────────────────────────────────
    if result["vix"] and result["vix3m"]:
        result["vix_spread"] = round(result["vix3m"] - result["vix"], 2)

    # ── VIX History ──────────────────────────────────────────────
    if vix_hist is not None and not vix_hist.empty:
        vix_s = vix_hist["Close"].copy()
        vix_s.index = pd.to_datetime(vix_s.index).normalize()
        vix_s = vix_s.rename("vix")
        if v3m_hist is not None and not v3m_hist.empty:
            v3m_s = v3m_hist["Close"].copy()
            v3m_s.index = pd.to_datetime(v3m_s.index).normalize()
            v3m_s = v3m_s.rename("vix3m")
            merged = pd.concat([vix_s, v3m_s], axis=1, join="inner", sort=False).dropna()
            if len(merged) < 10:
                merged = pd.concat([vix_s, v3m_s], axis=1, join="outer", sort=False).ffill().dropna()
        else:
            df = vix_s.to_frame()
            df["vix3m"] = df["vix"].rolling(20).mean() + df["vix"].rolling(20).std()
            merged = df.dropna()
        merged = merged.tail(252)
        merged["spread"] = merged["vix3m"] - merged["vix"]
        result["vix_history"] = [
            {"date": str(i.date()), "vix": round(float(r["vix"]),2),
             "vix3m": round(float(r["vix3m"]),2), "spread": round(float(r["spread"]),2)}
            for i, r in merged.iterrows()
        ]
        print(f"  VIX history: {len(result['vix_history'])} days | Spread: {result['vix_spread']}")

    # ── EODHD VIX override: yfinance can occasionally return stale/flat index history.
    if EODHD.enabled:
        try:
            e_vix, e_vix_src = EODHD.eod_history(["VIX.INDX"], years=4)
            e_v3m, e_v3m_src = EODHD.eod_history(["VIX3M.INDX"], years=4)
            e_v9d, e_v9d_src = EODHD.eod_history(["VIX9D.INDX"], years=1)
            if e_vix is not None and not e_vix.empty:
                vix_hist = e_vix
                result["vix"] = round(float(e_vix["Close"].iloc[-1]), 2)
                if len(e_vix) >= 20:
                    result["vix_sma20"] = round(float(e_vix["Close"].tail(20).mean()), 2)
                print(f"  VIX EODHD: {result['vix']} ({len(e_vix)} days)")
            if e_v3m is not None and not e_v3m.empty:
                v3m_hist = e_v3m
                result["vix3m"] = round(float(e_v3m["Close"].iloc[-1]), 2)
                print(f"  VIX3M EODHD: {result['vix3m']} ({len(e_v3m)} days)")
            if e_v9d is not None and not e_v9d.empty:
                result["vix9d"] = round(float(e_v9d["Close"].iloc[-1]), 2)
                print(f"  VIX9D EODHD: {result['vix9d']} ({len(e_v9d)} days)")
            if result["vix"] and result["vix3m"]:
                result["vix_spread"] = round(result["vix3m"] - result["vix"], 2)
            if vix_hist is not None and not vix_hist.empty and v3m_hist is not None and not v3m_hist.empty:
                vix_s = vix_hist["Close"].copy()
                vix_s.index = pd.to_datetime(vix_s.index).normalize()
                vix_s = vix_s.rename("vix")
                v3m_s = v3m_hist["Close"].copy()
                v3m_s.index = pd.to_datetime(v3m_s.index).normalize()
                v3m_s = v3m_s.rename("vix3m")
                merged = pd.concat([vix_s, v3m_s], axis=1, join="inner", sort=False).dropna().tail(756)
                merged["spread"] = merged["vix3m"] - merged["vix"]
                result["vix_history"] = [
                    {"date": str(i.date()), "vix": round(float(r["vix"]), 2),
                     "vix3m": round(float(r["vix3m"]), 2), "spread": round(float(r["spread"]), 2)}
                    for i, r in merged.iterrows()
                ]
                print(f"  VIX history EODHD: {len(result['vix_history'])} days | Spread: {result['vix_spread']}")
        except Exception as e:
            print(f"  VIX EODHD error: {e}")

    # ── Put/Call Ratio — CBOE direct API (primary) ───────────────
    import csv, io

    def _cboe_pc(url, label):
        """Fetch P/C history directly from CBOE JSON API."""
        try:
            r = requests.get(url, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://www.cboe.com/",
            }, timeout=15)
            print(f"  CBOE {label}: HTTP {r.status_code}")
            if r.status_code != 200:
                return []
            data = r.json()
            # CBOE format: {"data": {"timestamp": [...], "close": [...]}}
            ts  = data.get("data", {}).get("timestamp", [])
            cls = data.get("data", {}).get("close", [])
            if not ts or not cls:
                # Try alternate key structures
                rows = data.get("data", [])
                if isinstance(rows, list) and rows:
                    hist = []
                    for row in rows:
                        try:
                            d = str(row.get("date","") or row.get("timestamp",""))[:10]
                            v = float(row.get("close", 0) or row.get("value", 0))
                            if d and 0.05 < v < 10.0:
                                hist.append({"date": d, "val": round(v,3)})
                        except: continue
                    hist.sort(key=lambda x: x["date"])
                    return hist[-130:]
                return []
            hist = []
            for d, v in zip(ts, cls):
                try:
                    date_s = str(d)[:10]
                    fv = float(v)
                    if date_s and 0.05 < fv < 10.0:
                        hist.append({"date": date_s, "val": round(fv, 3)})
                except: continue
            hist.sort(key=lambda x: x["date"])
            print(f"  CBOE {label}: {len(hist)} points, last={hist[-1]['val'] if hist else '—'}")
            return hist[-130:]
        except Exception as e:
            print(f"  CBOE {label} error: {e}")
            return []

    cboe_pairs = [
        ("https://cdn.cboe.com/api/global/us_indices/daily_prices/PCR_TOTAL_DATA.json",  "pc_total_history",  True),
        ("https://cdn.cboe.com/api/global/us_indices/daily_prices/PCR_EQUITY_DATA.json", "pc_equity_history", False),
    ]
    for url, hist_key, is_total in cboe_pairs:
        label = "Total" if is_total else "Equity"
        hist = _cboe_pc(url, f"P/C {label}")
        if hist:
            result[hist_key] = hist
            if is_total:
                result["pc_ratio"] = hist[-1]["val"]
        time.sleep(0.5)

    # ── stooq + yfinance fallback (only if CBOE failed) ─────────
    if not result["pc_total_history"]:
        print("  P/C: CBOE failed → trying stooq/yfinance fallback")
        d_end   = datetime.datetime.now().strftime("%Y%m%d")
        d_start = (datetime.datetime.now() - datetime.timedelta(days=200)).strftime("%Y%m%d")
        stooq_headers_fb = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "text/csv,text/plain,*/*",
        }
        stooq_pairs = [
            (f"https://stooq.com/q/d/l/?s=%5Epcr&d1={d_start}&d2={d_end}&i=d",  "pc_total_history",  True),
            (f"https://stooq.com/q/d/l/?s=%5Epcre&d1={d_start}&d2={d_end}&i=d", "pc_equity_history", False),
        ]
        for url, hist_key, is_total in stooq_pairs:
            label = "Total" if is_total else "Equity"
            try:
                r = requests.get(url, headers=stooq_headers_fb, timeout=20)
                print(f"  stooq P/C {label}: HTTP {r.status_code}, {len(r.text)} chars")
                if r.status_code != 200:
                    continue
                preview = r.text[:200].replace('\n',' | ')
                print(f"  Preview: {preview!r}")
                # Parse - try DictReader then positional
                lines = r.text.strip().splitlines()
                history = []
                if len(lines) >= 2:
                    # Positional: stooq CSV is Date,Open,High,Low,Close,Volume
                    for line in lines[1:]:
                        parts = line.split(",")
                        if len(parts) < 5: continue
                        try:
                            dv = parts[0].strip()
                            cv = parts[4].strip()
                            if len(dv) < 8 or cv.lower() in ("","null","n/d"): continue
                            val = round(float(cv), 3)
                            if 0.05 < val < 10.0:
                                history.append({"date": dv[:10], "val": val})
                        except: continue
                history.sort(key=lambda x: x["date"])
                history = history[-130:]
                if history:
                    result[hist_key] = history
                    if is_total: result["pc_ratio"] = history[-1]["val"]
                    print(f"  stooq P/C {label}: {history[-1]['val']} ({len(history)} days) ✓")
                else:
                    print(f"  stooq P/C {label}: 0 valid rows")
                time.sleep(0.8)
            except Exception as e:
                print(f"  stooq P/C {label} error: {e}")

        # yfinance last resort
        if not result["pc_total_history"]:
            print("  P/C: stooq failed → skipping noisy Yahoo symbols (^PCALL/^PCEALL are unavailable)")
        if False and not result["pc_total_history"]:
            for sym, hist_key, is_total in [("^PCALL","pc_total_history",True),("^PCEALL","pc_equity_history",False)]:
                try:
                    tk  = yf.Ticker(sym)
                    hdf = yf_history(tk, "7mo", "1d")
                    if hdf is not None and not hdf.empty:
                        closes = hdf["Close"].dropna()
                        hist = [{"date": str(pd.Timestamp(d).date()), "val": round(float(v),3)}
                                for d, v in zip(closes.index, closes.values) if 0.05 < float(v) < 10.0]
                        hist = hist[-130:]
                        if hist:
                            result[hist_key] = hist
                            if is_total: result["pc_ratio"] = hist[-1]["val"]
                            print(f"  yfinance {sym}: {hist[-1]['val']} ({len(hist)} days) ✓")
                        else:
                            print(f"  yfinance {sym}: no valid values")
                    else:
                        print(f"  yfinance {sym}: no data")
                except Exception as e:
                    print(f"  yfinance {sym}: {e}")

    if not result["pc_total_history"]:
        print("  P/C: all sources failed — charts will be empty")
    else:
        print(f"  P/C final: Total={len(result['pc_total_history'])} pts, "
              f"Equity={len(result['pc_equity_history'])} pts, "
              f"Current={result.get('pc_ratio','—')}")

    return result

def clean_nans(obj):
    import math
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: clean_nans(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_nans(v) for v in obj]
    return obj


def run_parallel(label, items, fn, item_name):
    results = []
    done = 0
    total = len(items)
    print(f"\n{label} ({total} items, workers={MAX_WORKERS})...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fn, item): item for item in items}
        for fut in as_completed(futures):
            item = futures[fut]
            done += 1
            try:
                row = fut.result()
            except Exception as e:
                row = None
                print(f"  ERROR {item.get('t', item_name)}: {e}")
            if row:
                results.append(row)
            if done == total or done % 25 == 0:
                print(f"  {done:>4}/{total:<4} done · {len(results)} OK")
    return results


def refresh_snapshots(universe, sectors, trending):
    global STOCK_SNAPSHOTS
    tickers = [x["t"] for x in universe] + [x["t"] for x in sectors] + [x["t"] for x in trending]
    if not POLYGON.enabled:
        print("  Polygon disabled: set POLYGON_API_KEY env var")
        STOCK_SNAPSHOTS = {}
        return
    print("\n[0/5] Polygon snapshots (15m delayed if plan uses delayed data)...")
    STOCK_SNAPSHOTS = POLYGON.full_stock_snapshots(tickers)
    print(f"  Snapshots loaded: {len(STOCK_SNAPSHOTS)}")


def run_once(upload=True, include_insider=True):
    print("=" * 55)
    print(f"  HBMS Screener v6.3 SP500/DAX/HSI – {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("  Current S&P 500 + DAX 40 + Hang Seng constituents")
    print("=" * 55)

    secs = get_sectors()
    assets = get_trending_assets()
    sp500 = fetch_current_sp500()
    dax = get_dax()
    hsi = get_hsi()
    universe = sp500 + dax + hsi
    refresh_snapshots(universe, secs, assets)
    print(f"  Universe: {len(sp500)} S&P 500 + {len(dax)} DAX + {len(hsi)} HSI = {len(universe)}")

    sector_raw = run_parallel("[1/5] Sectors (Dual RRG 12M + 3M)", secs, process_sector, "sector")
    sectors = calc_rrg(sector_raw)
    agree = [s for s in sectors if s.get("agreement") and s["ticker"] != "SPY"]
    diverge = [s for s in sectors if s.get("diverging")]
    print(f"  RRG agreement: {len(agree)} · diverging: {len(diverge)}")

    trending_results = run_parallel("[2/5] Trending Assets", assets, process_trending, "asset")
    stock_results = run_parallel("[3/5] Stocks (S&P 500, DAX, HSI)", universe, process_stock, "stock")

    print("\n[4/5] Market Breadth...")
    breadth = calc_breadth(stock_results)

    print("\n[4b/5] Market Indicators (VIX + P/C)...")
    market_indicators = fetch_market_indicators()

    insider_trades = []
    if include_insider:
        print("\n[5/5] Insider Trades (OpenInsider.com)...")
        insider_trades = fetch_insider_trades(sp500)
    else:
        print("\n[5/5] Insider Trades skipped")

    in_gp = [r for r in stock_results if r["in_gp_52w"] or r["in_gp_ytd"]]
    os_w = [r for r in stock_results if r["rsi_weekly"] and r["rsi_weekly"] <= 35]
    sources = {}
    for row in stock_results + trending_results + sectors:
        src = row.get("source", "unknown")
        sources[src] = sources.get(src, 0) + 1

    print("\n" + "=" * 55)
    print(f"  Stocks:        {len(stock_results):>4}")
    print(f"  Insider Trades:{len(insider_trades):>4}")
    print(f"  Trending:      {len(trending_results):>4}")
    print(f"  Golden Pocket: {len(in_gp):>4}")
    print(f"  RSI W<35:      {len(os_w):>4}")
    print(f"  RRG Agree:     {len(agree):>4}")
    print(f"  RRG Diverge:   {len(diverge):>4}")
    print(f"  Sources:       {sources}")
    print("=" * 55)

    output = {
        "generated": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "total": len(stock_results),
        "stocks": stock_results,
        "trending": trending_results,
        "sectors": sectors,
        "breadth": breadth,
        "market": market_indicators,
        "insider": insider_trades,
        "meta": {
            "sp500_count": len(sp500),
            "dax_count": len(dax),
            "hsi_count": len(hsi),
        },
    }

    output = clean_nans(output)
    data_json = json.dumps(output, ensure_ascii=False, indent=2)
    Path("data.json").write_text(data_json, encoding="utf-8")
    print(f"\nSaved: data.json ({len(data_json) // 1024} KB)")
    if upload:
        upload_to_github(data_json)
    else:
        print("  GitHub upload skipped by --no-upload")
    print("Done!")


# ─── MAIN ────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--loop", action="store_true", help="run forever and refresh every --interval seconds")
    parser.add_argument("--interval", type=int, default=900, help="loop refresh interval in seconds")
    parser.add_argument("--no-upload", action="store_true", help="write local data.json but do not upload")
    parser.add_argument("--skip-insider", action="store_true", help="skip slower OpenInsider scrape")
    args = parser.parse_args()

    while True:
        run_once(upload=not args.no_upload, include_insider=not args.skip_insider)
        if not args.loop:
            break
        print(f"\nSleeping {args.interval}s until next refresh...")
        time.sleep(args.interval)

if __name__ == "__main__":
    main()
