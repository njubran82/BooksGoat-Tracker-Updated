#!/usr/bin/env python3
"""
BooksGoat tracker v2
- Google Sheet primary + CSV fallback
- Supports BOTH:
    1) structured supplier sheet format:
       Title, ISBN-13, ISBN-10, 5 Qty, 10 Qty, 25 Qty, List Price, Amazon Price, Amazon Rank
    2) simple backup format:
       Enabled, ISBN, Label, Price, URL
- Tries to use BooksGoat product URL for cleaner title + live supplier price
- Uses stronger eBay query fallbacks to reduce "No comps"
- Saves CSV/XLSX results
- Tracks state/history and can send alerts
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import os
import re
import smtplib
import statistics
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import unquote

import pandas as pd
import requests

# Temporary hardcoded App ID for testing in GitHub
# Move this to GitHub Secrets later.
os.environ["EBAY_APP_ID"] = "JubranIn-ProfitSc-PRD-4bf497123-06cdelb7"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:148.0) "
    "Gecko/20100101 Firefox/148.0"
)
REQUEST_TIMEOUT = 30
EBAY_FINDING_URL = "https://svcs.ebay.com/services/search/FindingService/v1"

BASE_DIR = Path(__file__).resolve().parent
GOOGLE_SHEET_CSV_URL = os.getenv("GOOGLE_SHEET_CSV_URL", "")
BACKUP_INPUT_FILE = BASE_DIR / "booksgoat_supplier_backup.csv"
ALLOW_CSV_FALLBACK = True
AUTO_REFRESH_BACKUP_CSV = True
STRICT_REQUIRE_ANY_INPUT = True

STATE_FILE = BASE_DIR / "booksgoat_ebay_tracker_state.json"
HISTORY_FILE = BASE_DIR / "booksgoat_ebay_tracker_history.csv"
ERROR_LOG_FILE = BASE_DIR / "booksgoat_ebay_tracker_errors.log"
RESULTS_CSV = BASE_DIR / "booksgoat_ebay_scan_results.csv"
RESULTS_XLSX = BASE_DIR / "booksgoat_ebay_scan_results.xlsx"

DEFAULT_MIN_SOLD_PRICE = 12.0
DEFAULT_EBAY_FEE_RATE = 0.13
DEFAULT_PAYMENT_FEE_RATE = 0.00
DEFAULT_SHIPPING_COST = 4.50
DEFAULT_PACKAGING_COST = 0.50
DEFAULT_BUFFER_COST = 0.50
DEFAULT_PAUSE_SECONDS = 0.20
DEFAULT_ALERT_TOP_N = 15
DEFAULT_MIN_PROFIT_ALERT = 4.0
DEFAULT_MIN_ROI_ALERT = 0.15

EMAIL_ENABLED = os.getenv("EMAIL_ENABLED", "true").strip().lower() in {"1", "true", "yes", "y", "on"}
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
TO_EMAIL = os.getenv("TO_EMAIL", "")
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))


STOPWORDS = {
    "the", "and", "for", "with", "from", "edition", "revised", "updated",
    "hardcover", "paperback", "spiralbound", "spiral", "softcover",
    "by", "a", "an", "of", "to", "in", "on", "student", "guide",
    "manual", "textbook", "book", "volume", "vol", "set"
}


@dataclass
class SupplierBook:
    title: str
    isbn13: str
    isbn10: str
    price_5: Optional[float]
    price_10: Optional[float]
    price_25: Optional[float]
    list_price: Optional[float]
    amazon_price: Optional[float]
    amazon_rank: Optional[int]
    source_price: Optional[float]
    product_url: str
    enabled: bool
    source_label: str


@dataclass
class ProductPageInfo:
    fetched: bool
    page_title: str
    extracted_title: str
    extracted_price: Optional[float]
    in_stock: Optional[bool]
    notes: str


@dataclass
class EbaySummary:
    query_used: str
    sold_count: int
    active_count: int
    sold_prices: List[float]
    active_prices: List[float]
    sold_median: Optional[float]
    sold_mean: Optional[float]
    sold_max: Optional[float]
    active_median: Optional[float]
    active_min: Optional[float]
    notes: str


@dataclass
class TierEvaluation:
    tier_name: str
    unit_cost: Optional[float]
    estimated_sale_price: Optional[float]
    fees: Optional[float]
    total_cost: Optional[float]
    estimated_profit: Optional[float]
    roi: Optional[float]
    qualifies: bool
    reason: str


@dataclass
class ScanResult:
    title: str
    isbn13: str
    isbn10: str
    product_url: str
    supplier_page_fetched: bool
    supplier_page_title: str
    supplier_live_price: Optional[float]
    source_price: Optional[float]
    amazon_price: Optional[float]
    amazon_rank: Optional[int]
    list_price: Optional[float]
    stock_status: str
    ebay_query_used: str
    ebay_sold_count: int
    ebay_active_count: int
    sold_median: Optional[float]
    sold_mean: Optional[float]
    sold_max: Optional[float]
    active_median: Optional[float]
    active_min: Optional[float]
    selected_tier: str
    selected_unit_cost: Optional[float]
    estimated_sale_price: Optional[float]
    estimated_fees: Optional[float]
    estimated_total_cost: Optional[float]
    estimated_profit: Optional[float]
    estimated_roi: Optional[float]
    tier_5_profit: Optional[float]
    tier_5_roi: Optional[float]
    tier_10_profit: Optional[float]
    tier_10_roi: Optional[float]
    tier_25_profit: Optional[float]
    tier_25_roi: Optional[float]
    quick_decision: str
    notes: str


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def local_now_string() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log_error(message: str) -> None:
    line = f"[{local_now_string()}] {message}\n"
    with ERROR_LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(line)
    print(message, file=sys.stderr)


def clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def clean_isbn(value: Any) -> str:
    return re.sub(r"\D", "", str(value or ""))


def parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = clean_text(value)
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    text = text.replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def parse_int(value: Any) -> Optional[int]:
    num = parse_float(value)
    return int(num) if num is not None else None


def format_currency(value: Optional[float]) -> str:
    return "Unknown" if value is None else f"${value:,.2f}"


def fetch_sheet_csv_text(csv_url: str) -> str:
    resp = requests.get(csv_url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.text


def refresh_backup_csv(csv_text: str, backup_path: Path = BACKUP_INPUT_FILE) -> None:
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", newline="", delete=False, dir=str(backup_path.parent)) as tmp:
        tmp.write(csv_text)
        temp_path = Path(tmp.name)
    temp_path.replace(backup_path)


def safe_get(d: Any, *path: Any, default: Any = None) -> Any:
    cur = d
    for key in path:
        if isinstance(cur, list):
            if not cur:
                return default
            try:
                cur = cur[key]
            except Exception:
                return default
        elif isinstance(cur, dict):
            cur = cur.get(key, default)
        else:
            return default
    return cur


def normalize_supplier_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    original_columns = list(df.columns)
    normalized_input = {str(col).strip().lower(): col for col in original_columns}

    # Support simple format: Enabled, ISBN, Label, Price, URL
    if "label" in normalized_input and "isbn" in normalized_input:
        rename_map: Dict[str, str] = {
            normalized_input["label"]: "Title",
            normalized_input["isbn"]: "ISBN-13",
        }
        if "enabled" in normalized_input:
            rename_map[normalized_input["enabled"]] = "Enabled"
        if "price" in normalized_input:
            rename_map[normalized_input["price"]] = "Source Price"
        if "url" in normalized_input:
            rename_map[normalized_input["url"]] = "URL"
        df = df.rename(columns=rename_map)

    return df


def dataframe_to_supplier_books(df: pd.DataFrame) -> List[SupplierBook]:
    df = normalize_supplier_dataframe(df)
    normalized = {str(col).strip().lower(): col for col in df.columns}

    def has_col(name: str) -> bool:
        return name.lower() in normalized

    def get_col(name: str) -> Optional[str]:
        return normalized.get(name.lower())

    if not has_col("Title"):
        raise KeyError("Missing required column: Title")

    books: List[SupplierBook] = []
    seen: set[str] = set()

    for _, row in df.iterrows():
        enabled = True
        if has_col("Enabled"):
            enabled_val = clean_text(row.get(get_col("Enabled"), "")).lower()
            enabled = enabled_val in {"", "y", "yes", "true", "1"}
        if not enabled:
            continue

        title = clean_text(row.get(get_col("Title"), ""))
        if not title:
            continue

        isbn13 = clean_isbn(row.get(get_col("ISBN-13"), "")) if has_col("ISBN-13") else ""
        isbn10 = clean_isbn(row.get(get_col("ISBN-10"), "")) if has_col("ISBN-10") else ""

        dedupe_key = isbn13 or isbn10 or title.lower()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        source_price = parse_float(row.get(get_col("Source Price"))) if has_col("Source Price") else None
        list_price = parse_float(row.get(get_col("List Price"))) if has_col("List Price") else None

        # If only simple CSV format exists, also use Source Price as a fallback tier price
        price_5 = parse_float(row.get(get_col("5 Qty"))) if has_col("5 Qty") else source_price
        price_10 = parse_float(row.get(get_col("10 Qty"))) if has_col("10 Qty") else source_price
        price_25 = parse_float(row.get(get_col("25 Qty"))) if has_col("25 Qty") else source_price

        books.append(
            SupplierBook(
                title=title,
                isbn13=isbn13,
                isbn10=isbn10,
                price_5=price_5,
                price_10=price_10,
                price_25=price_25,
                list_price=list_price,
                amazon_price=parse_float(row.get(get_col("Amazon Price"))) if has_col("Amazon Price") else None,
                amazon_rank=parse_int(row.get(get_col("Amazon Rank"))) if has_col("Amazon Rank") else None,
                source_price=source_price,
                product_url=clean_text(row.get(get_col("URL"), "")) if has_col("URL") else "",
                enabled=enabled,
                source_label=title,
            )
        )

    return books


def load_supplier_csv(path: Path) -> List[SupplierBook]:
    df = pd.read_csv(path)
    books = dataframe_to_supplier_books(df)
    if not books:
        raise RuntimeError(f"No valid books were loaded from backup CSV: {path}")
    return books


def load_supplier_google_sheet(url: str) -> List[SupplierBook]:
    csv_text = fetch_sheet_csv_text(url)
    from io import StringIO
    df = pd.read_csv(StringIO(csv_text))
    books = dataframe_to_supplier_books(df)
    if not books:
        raise RuntimeError("No valid books were loaded from the Google Sheet.")
    if AUTO_REFRESH_BACKUP_CSV:
        refresh_backup_csv(csv_text, BACKUP_INPUT_FILE)
    return books


def load_books(args: argparse.Namespace) -> Tuple[List[SupplierBook], str]:
    google_url = clean_text(args.google_sheet_url or GOOGLE_SHEET_CSV_URL)
    if google_url:
        try:
            return load_supplier_google_sheet(google_url), "google_sheet"
        except Exception as exc:
            log_error(f"Google Sheet load failed: {exc}")
            if not ALLOW_CSV_FALLBACK:
                raise

    backup_path = Path(args.input)
    if backup_path.exists():
        try:
            log_error(f"Falling back to local CSV: {backup_path}")
            return load_supplier_csv(backup_path), "csv_fallback"
        except Exception as exc:
            if STRICT_REQUIRE_ANY_INPUT:
                raise RuntimeError(f"CSV fallback failed: {exc}")
            log_error(f"CSV fallback failed: {exc}")
            return [], "none"

    raise RuntimeError("Could not load input from Google Sheet or CSV fallback.")


def extract_booksgoat_title_from_html(html_text: str, fallback_title: str = "") -> str:
    patterns = [
        r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+name=["\']twitter:title["\'][^>]+content=["\']([^"\']+)["\']',
        r'<title>(.*?)</title>',
        r'<h1[^>]*>(.*?)</h1>',
    ]
    for pattern in patterns:
        match = re.search(pattern, html_text, re.IGNORECASE | re.DOTALL)
        if match:
            title = html.unescape(re.sub(r"<[^>]+>", " ", match.group(1)))
            title = clean_text(title)
            if title:
                return title
    return fallback_title


def extract_price_from_html(html_text: str) -> Optional[float]:
    patterns = [
        r'["\']price["\']\s*:\s*["\']?([0-9]+(?:\.[0-9]{1,2})?)',
        r'content=["\']([0-9]+(?:\.[0-9]{1,2})?)["\'][^>]*itemprop=["\']price["\']',
        r'itemprop=["\']price["\'][^>]*content=["\']([0-9]+(?:\.[0-9]{1,2})?)["\']',
        r'[$]([0-9]+(?:\.[0-9]{1,2})?)',
    ]
    candidates: List[float] = []
    for pattern in patterns:
        for match in re.finditer(pattern, html_text, re.IGNORECASE):
            value = parse_float(match.group(1))
            if value is not None and 1 <= value <= 5000:
                candidates.append(value)
    if not candidates:
        return None
    # Prefer the smallest reasonable supplier-side price if multiple are present
    return round(sorted(candidates)[0], 2)


def extract_stock_from_html(html_text: str) -> Optional[bool]:
    text = html_text.lower()
    if "out of stock" in text:
        return False
    if "in stock" in text or "availability" in text:
        return True
    return None


def fetch_product_page_info(book: SupplierBook) -> ProductPageInfo:
    if not book.product_url:
        return ProductPageInfo(
            fetched=False,
            page_title="",
            extracted_title="",
            extracted_price=None,
            in_stock=None,
            notes="No product URL provided.",
        )

    try:
        resp = requests.get(book.product_url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        html_text = resp.text

        page_title = extract_booksgoat_title_from_html(html_text, book.title)
        extracted_price = extract_price_from_html(html_text)
        in_stock = extract_stock_from_html(html_text)

        return ProductPageInfo(
            fetched=True,
            page_title=page_title,
            extracted_title=page_title,
            extracted_price=extracted_price,
            in_stock=in_stock,
            notes="Fetched supplier product page successfully.",
        )
    except Exception as exc:
        return ProductPageInfo(
            fetched=False,
            page_title="",
            extracted_title="",
            extracted_price=None,
            in_stock=None,
            notes=f"Supplier page fetch failed: {exc}",
        )


def clean_book_title_for_query(title: str) -> str:
    text = html.unescape(unquote(title))
    text = re.sub(r"\(isbn[^)]*\)", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\{[^}]*\}", " ", text)
    text = re.sub(r"\[[^\]]*\]", " ", text)
    text = re.sub(r"[*_~|]+", " ", text)
    text = re.sub(r"[^A-Za-z0-9 ]+", " ", text)
    text = clean_text(text)

    tokens = []
    for tok in text.split():
        low = tok.lower()
        if low in STOPWORDS:
            continue
        if len(tok) == 1:
            continue
        tokens.append(tok)

    return clean_text(" ".join(tokens))


def build_title_variants(title: str) -> List[str]:
    cleaned = clean_book_title_for_query(title)
    tokens = cleaned.split()

    variants: List[str] = []
    if cleaned:
        variants.append(cleaned)
        variants.append(f'"{cleaned}"')

    if len(tokens) >= 8:
        variants.append(" ".join(tokens[:8]))
    if len(tokens) >= 6:
        variants.append(" ".join(tokens[:6]))
    if len(tokens) >= 4:
        variants.append(" ".join(tokens[:4]))

    # Remove duplicates while preserving order
    out: List[str] = []
    seen: set[str] = set()
    for v in variants:
        key = v.lower()
        if key and key not in seen:
            seen.add(key)
            out.append(v)
    return out


def build_queries(book: SupplierBook, page_info: ProductPageInfo) -> List[str]:
    queries: List[str] = []

    if book.isbn13:
        queries.append(book.isbn13)
    if book.isbn10:
        queries.append(book.isbn10)

    primary_title = page_info.extracted_title or book.title
    title_variants = build_title_variants(primary_title)

    for q in title_variants:
        queries.append(q)

    # Backup from raw source label
    if book.source_label and book.source_label != primary_title:
        for q in build_title_variants(book.source_label):
            queries.append(q)

    out: List[str] = []
    seen: set[str] = set()
    for q in queries:
        key = q.lower().strip()
        if key and key not in seen:
            seen.add(key)
            out.append(q)

    return out


def ebay_headers(app_id: str) -> Dict[str, str]:
    return {
        "X-EBAY-SOA-OPERATION-NAME": "findItemsAdvanced",
        "X-EBAY-SOA-SERVICE-VERSION": "1.13.0",
        "X-EBAY-SOA-SECURITY-APPNAME": app_id,
        "X-EBAY-SOA-RESPONSE-DATA-FORMAT": "JSON",
        "X-EBAY-SOA-GLOBAL-ID": "EBAY-US",
        "User-Agent": USER_AGENT,
    }


def extract_prices_from_finding_items(items: Iterable[Dict[str, Any]]) -> List[float]:
    prices: List[float] = []
    for item in items:
        price_text = safe_get(item, "sellingStatus", 0, "currentPrice", 0, "__value__")
        price = parse_float(price_text)
        if price is not None and 1 <= price <= 5000:
            prices.append(price)
    return prices


def call_ebay_finding_api(app_id: str, keywords: str, sold_only: bool, entries_per_page: int = 20) -> Tuple[List[float], int]:
    params: List[Tuple[str, str]] = [
        ("keywords", keywords),
        ("paginationInput.entriesPerPage", str(entries_per_page)),
        ("sortOrder", "BestMatch" if not sold_only else "EndTimeSoonest"),
        ("outputSelector(0)", "SellerInfo"),
        ("itemFilter(1).name", "LocatedIn"),
        ("itemFilter(1).value", "US"),
    ]
    if sold_only:
        params.extend([
            ("itemFilter(0).name", "SoldItemsOnly"),
            ("itemFilter(0).value", "true"),
        ])
    else:
        params.extend([
            ("itemFilter(0).name", "HideDuplicateItems"),
            ("itemFilter(0).value", "true"),
        ])

    response = requests.get(
        EBAY_FINDING_URL,
        headers=ebay_headers(app_id),
        params=params,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    data = response.json()

    ack = safe_get(data, "findItemsAdvancedResponse", 0, "ack", 0)
    if ack != "Success":
        errors = safe_get(data, "findItemsAdvancedResponse", 0, "errorMessage", 0, "error", default=[])
        raise RuntimeError(f"eBay API error for '{keywords}': {errors}")

    search_result = safe_get(data, "findItemsAdvancedResponse", 0, "searchResult", 0, default={})
    count = int(search_result.get("@count", 0) or 0)
    items = search_result.get("item", []) if count else []
    prices = extract_prices_from_finding_items(items)
    return prices, count


def summarize_prices(prices: List[float]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    if not prices:
        return None, None, None, None
    return statistics.median(prices), statistics.mean(prices), max(prices), min(prices)


def get_ebay_summary(app_id: str, book: SupplierBook, page_info: ProductPageInfo, pause_seconds: float = DEFAULT_PAUSE_SECONDS) -> EbaySummary:
    notes: List[str] = []
    queries = build_queries(book, page_info)

    for idx, query in enumerate(queries, start=1):
        try:
            sold_prices, sold_count = call_ebay_finding_api(app_id, query, sold_only=True)
            time.sleep(pause_seconds)
            active_prices, active_count = call_ebay_finding_api(app_id, query, sold_only=False)

            sold_median, sold_mean, sold_max, _ = summarize_prices(sold_prices)
            active_median, _, _, active_min = summarize_prices(active_prices)

            notes.append(f"Query {idx}/{len(queries)}: {query} -> sold {sold_count}, active {active_count}")

            if sold_count > 0 or active_count > 0:
                return EbaySummary(
                    query_used=query,
                    sold_count=sold_count,
                    active_count=active_count,
                    sold_prices=sold_prices,
                    active_prices=active_prices,
                    sold_median=sold_median,
                    sold_mean=sold_mean,
                    sold_max=sold_max,
                    active_median=active_median,
                    active_min=active_min,
                    notes=" | ".join(notes),
                )
        except Exception as exc:
            notes.append(f"Query '{query}' failed: {exc}")

    return EbaySummary(
        query_used=queries[0] if queries else (book.isbn13 or book.title),
        sold_count=0,
        active_count=0,
        sold_prices=[],
        active_prices=[],
        sold_median=None,
        sold_mean=None,
        sold_max=None,
        active_median=None,
        active_min=None,
        notes=" | ".join(notes) if notes else "No eBay matches found.",
    )


def choose_supplier_cost(book: SupplierBook, page_info: ProductPageInfo) -> Tuple[Optional[float], str]:
    # Prefer live product page price
    if page_info.extracted_price is not None:
        return page_info.extracted_price, "supplier_live_price"
    if book.source_price is not None:
        return book.source_price, "source_price"
    if book.price_5 is not None:
        return book.price_5, "price_5"
    if book.price_10 is not None:
        return book.price_10, "price_10"
    if book.price_25 is not None:
        return book.price_25, "price_25"
    return None, "none"


def choose_estimated_sale_price(summary: EbaySummary, book: SupplierBook, supplier_cost: Optional[float]) -> Optional[float]:
    candidates: List[float] = []

    if summary.sold_median is not None:
        candidates.append(summary.sold_median)
    if summary.sold_mean is not None:
        candidates.append(summary.sold_mean)
    if summary.active_min is not None and summary.active_min >= DEFAULT_MIN_SOLD_PRICE:
        candidates.append(summary.active_min * 0.97)
    if summary.active_median is not None and summary.active_median >= DEFAULT_MIN_SOLD_PRICE:
        candidates.append(summary.active_median * 0.95)
    if book.amazon_price is not None and book.amazon_price >= DEFAULT_MIN_SOLD_PRICE:
        candidates.append(book.amazon_price * 0.92)
    if book.list_price is not None and book.list_price >= DEFAULT_MIN_SOLD_PRICE:
        candidates.append(book.list_price * 0.55)

    filtered = [round(x, 2) for x in candidates if x is not None and x >= DEFAULT_MIN_SOLD_PRICE]
    if not filtered:
        return None

    # Conservative estimate: use lower-middle of reasonable options
    filtered = sorted(filtered)

    if supplier_cost is not None:
        viable = [x for x in filtered if x > supplier_cost * 1.15]
        if viable:
            filtered = viable

    return round(filtered[max(0, len(filtered) // 3)], 2)


def evaluate_tier(
    tier_name: str,
    unit_cost: Optional[float],
    estimated_sale_price: Optional[float],
    sold_count: int,
    ebay_fee_rate: float,
    payment_fee_rate: float,
    shipping_cost: float,
    packaging_cost: float,
    buffer_cost: float,
) -> TierEvaluation:
    if unit_cost is None:
        return TierEvaluation(tier_name, None, estimated_sale_price, None, None, None, None, False, "No supplier price")
    if estimated_sale_price is None:
        return TierEvaluation(tier_name, unit_cost, None, None, None, None, None, False, "No resale price estimate")

    fees = estimated_sale_price * (ebay_fee_rate + payment_fee_rate)
    total_cost = unit_cost + fees + shipping_cost + packaging_cost + buffer_cost
    profit = estimated_sale_price - total_cost
    roi = profit / unit_cost if unit_cost > 0 else None

    qualifies = False
    if tier_name == "25 Qty":
        qualifies = sold_count >= 8 and profit >= 8 and (roi or -999) >= 0.25
    elif tier_name == "10 Qty":
        qualifies = sold_count >= 4 and profit >= 6 and (roi or -999) >= 0.20
    elif tier_name == "5 Qty":
        qualifies = sold_count >= 1 and profit >= 4 and (roi or -999) >= 0.15

    return TierEvaluation(
        tier_name=tier_name,
        unit_cost=round(unit_cost, 2),
        estimated_sale_price=round(estimated_sale_price, 2),
        fees=round(fees, 2),
        total_cost=round(total_cost, 2),
        estimated_profit=round(profit, 2),
        roi=round(roi, 4) if roi is not None else None,
        qualifies=qualifies,
        reason="",
    )


def select_best_tier(evals: List[TierEvaluation]) -> TierEvaluation:
    qualified = [e for e in evals if e.qualifies and e.estimated_profit is not None]
    if qualified:
        tier_priority = {"25 Qty": 3, "10 Qty": 2, "5 Qty": 1}
        qualified.sort(key=lambda e: (tier_priority.get(e.tier_name, 0), e.estimated_profit or -999), reverse=True)
        return qualified[0]

    valid = [e for e in evals if e.estimated_profit is not None]
    if not valid:
        return evals[0]
    valid.sort(key=lambda e: e.estimated_profit if e.estimated_profit is not None else -999, reverse=True)
    return valid[0]


def quick_decision(profit: Optional[float], roi: Optional[float], sold_count: int) -> str:
    if profit is None or roi is None:
        return "No comps"
    if sold_count >= 8 and profit >= 10 and roi >= 0.30:
        return "Strong buy"
    if sold_count >= 3 and profit >= 6 and roi >= 0.20:
        return "Buy"
    if sold_count >= 1 and profit >= 3 and roi >= 0.10:
        return "Borderline"
    return "Pass"


def scan_book(
    book: SupplierBook,
    app_id: str,
    ebay_fee_rate: float,
    payment_fee_rate: float,
    shipping_cost: float,
    packaging_cost: float,
    buffer_cost: float,
    pause_seconds: float,
) -> ScanResult:
    page_info = fetch_product_page_info(book)
    summary = get_ebay_summary(app_id, book, page_info, pause_seconds=pause_seconds)

    supplier_cost, supplier_cost_source = choose_supplier_cost(book, page_info)
    sale_price = choose_estimated_sale_price(summary, book, supplier_cost)

    tier_5_cost = book.price_5 if book.price_5 is not None else supplier_cost
    tier_10_cost = book.price_10 if book.price_10 is not None else supplier_cost
    tier_25_cost = book.price_25 if book.price_25 is not None else supplier_cost

    tier_5 = evaluate_tier("5 Qty", tier_5_cost, sale_price, summary.sold_count, ebay_fee_rate, payment_fee_rate, shipping_cost, packaging_cost, buffer_cost)
    tier_10 = evaluate_tier("10 Qty", tier_10_cost, sale_price, summary.sold_count, ebay_fee_rate, payment_fee_rate, shipping_cost, packaging_cost, buffer_cost)
    tier_25 = evaluate_tier("25 Qty", tier_25_cost, sale_price, summary.sold_count, ebay_fee_rate, payment_fee_rate, shipping_cost, packaging_cost, buffer_cost)

    selected = select_best_tier([tier_5, tier_10, tier_25])

    stock_status = "Unknown"
    if page_info.in_stock is True:
        stock_status = "In Stock"
    elif page_info.in_stock is False:
        stock_status = "Out of Stock"

    notes_parts = [
        page_info.notes,
        f"Supplier cost source: {supplier_cost_source}",
        summary.notes,
    ]
    if selected.qualifies:
        notes_parts.append(f"Selected {selected.tier_name} via smart tiering")
    else:
        notes_parts.append(f"No tier fully qualified; best fallback was {selected.tier_name}")

    effective_title = page_info.extracted_title or book.title

    return ScanResult(
        title=effective_title,
        isbn13=book.isbn13,
        isbn10=book.isbn10,
        product_url=book.product_url,
        supplier_page_fetched=page_info.fetched,
        supplier_page_title=page_info.page_title,
        supplier_live_price=page_info.extracted_price,
        source_price=book.source_price,
        amazon_price=book.amazon_price,
        amazon_rank=book.amazon_rank,
        list_price=book.list_price,
        stock_status=stock_status,
        ebay_query_used=summary.query_used,
        ebay_sold_count=summary.sold_count,
        ebay_active_count=summary.active_count,
        sold_median=summary.sold_median,
        sold_mean=summary.sold_mean,
        sold_max=summary.sold_max,
        active_median=summary.active_median,
        active_min=summary.active_min,
        selected_tier=selected.tier_name,
        selected_unit_cost=selected.unit_cost,
        estimated_sale_price=selected.estimated_sale_price,
        estimated_fees=selected.fees,
        estimated_total_cost=selected.total_cost,
        estimated_profit=selected.estimated_profit,
        estimated_roi=selected.roi,
        tier_5_profit=tier_5.estimated_profit,
        tier_5_roi=tier_5.roi,
        tier_10_profit=tier_10.estimated_profit,
        tier_10_roi=tier_10.roi,
        tier_25_profit=tier_25.estimated_profit,
        tier_25_roi=tier_25.roi,
        quick_decision=quick_decision(selected.estimated_profit, selected.roi, summary.sold_count),
        notes=" | ".join([x for x in notes_parts if x]),
    )


def results_to_dataframe(results: List[ScanResult]) -> pd.DataFrame:
    df = pd.DataFrame([asdict(r) for r in results])
    if not df.empty:
        df["estimated_roi_pct"] = df["estimated_roi"].apply(lambda x: round(x * 100, 2) if pd.notna(x) else None)
        df["decision_sort"] = df["quick_decision"].map({
            "Strong buy": 0,
            "Buy": 1,
            "Borderline": 2,
            "Pass": 3,
            "No comps": 4,
            "Error": 5,
        }).fillna(9)
        df.sort_values(
            by=["decision_sort", "estimated_profit", "estimated_roi_pct", "ebay_sold_count"],
            ascending=[True, False, False, False],
            inplace=True,
        )
        df.drop(columns=["decision_sort"], inplace=True)
    return df


def save_outputs(df: pd.DataFrame, output_csv: Path, output_xlsx: Optional[Path]) -> None:
    df.to_csv(output_csv, index=False)
    if output_xlsx is not None:
        try:
            with pd.ExcelWriter(output_xlsx, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Results")
        except Exception as exc:
            log_error(f"Could not write XLSX output: {exc}")


def load_state() -> Dict[str, Any]:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        log_error(f"Could not read state file: {exc}")
        return {}


def save_state(state: Dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def result_key(result: ScanResult) -> str:
    return result.isbn13 or result.isbn10 or result.title


def append_history(result: ScanResult, event_summary: str, source_used: str) -> None:
    file_exists = HISTORY_FILE.exists()
    with HISTORY_FILE.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "checked_at_utc",
                "source_used",
                "title",
                "isbn13",
                "isbn10",
                "selected_tier",
                "selected_unit_cost",
                "estimated_sale_price",
                "estimated_profit",
                "estimated_roi",
                "ebay_sold_count",
                "ebay_active_count",
                "quick_decision",
                "notes",
                "event_summary",
            ])
        writer.writerow([
            utc_now_iso(),
            source_used,
            result.title,
            result.isbn13,
            result.isbn10,
            result.selected_tier,
            result.selected_unit_cost,
            result.estimated_sale_price,
            result.estimated_profit,
            result.estimated_roi,
            result.ebay_sold_count,
            result.ebay_active_count,
            result.quick_decision,
            result.notes,
            event_summary,
        ])


def diff_result(previous: Optional[Dict[str, Any]], current: ScanResult, min_profit_alert: float, min_roi_alert: float) -> List[str]:
    events: List[str] = []
    if previous is None:
        if (current.estimated_profit or 0) >= min_profit_alert and (current.estimated_roi or 0) >= min_roi_alert:
            events.append(
                f"New opportunity: {current.quick_decision}, profit {format_currency(current.estimated_profit)}, ROI {round((current.estimated_roi or 0) * 100, 2)}%."
            )
        return events

    def changed_num(field: str, label: str, pct: bool = False) -> None:
        prev = previous.get(field)
        curr = getattr(current, field)
        if prev != curr:
            if pct:
                prev_s = "Unknown" if prev is None else f"{prev * 100:.2f}%"
                curr_s = "Unknown" if curr is None else f"{curr * 100:.2f}%"
            else:
                prev_s = format_currency(prev) if isinstance(curr, (int, float)) or isinstance(prev, (int, float)) else str(prev)
                curr_s = format_currency(curr) if isinstance(curr, (int, float)) or isinstance(prev, (int, float)) else str(curr)
            events.append(f"{label} changed: {prev_s} -> {curr_s}.")

    important_fields = [
        ("estimated_sale_price", "Estimated sale price", False),
        ("selected_unit_cost", "Selected unit cost", False),
        ("estimated_profit", "Estimated profit", False),
        ("estimated_roi", "Estimated ROI", True),
    ]
    for field, label, pct in important_fields:
        changed_num(field, label, pct)

    if previous.get("quick_decision") != current.quick_decision:
        events.append(f"Decision changed: {previous.get('quick_decision')} -> {current.quick_decision}.")
    if previous.get("ebay_sold_count") != current.ebay_sold_count:
        events.append(f"Sold comp count changed: {previous.get('ebay_sold_count')} -> {current.ebay_sold_count}.")
    if previous.get("stock_status") != current.stock_status:
        events.append(f"Stock status changed: {previous.get('stock_status')} -> {current.stock_status}.")

    significant = []
    for e in events:
        if "Estimated profit" in e or "Estimated ROI" in e or "Decision changed" in e or "New opportunity" in e:
            significant.append(e)
    return significant or events[:1]


def build_html_message(items: List[Dict[str, Any]], source_used: str) -> str:
    rows = []
    for item in items:
        result: ScanResult = item["result"]
        event_lines = "<br>".join(f"• {e}" for e in item["events"])
        roi_pct = "Unknown" if result.estimated_roi is None else f"{result.estimated_roi * 100:.2f}%"
        rows.append(
            f"""
            <tr>
              <td style="padding:8px;border:1px solid #ddd;vertical-align:top;">{result.title}</td>
              <td style="padding:8px;border:1px solid #ddd;vertical-align:top;">{result.isbn13 or result.isbn10}</td>
              <td style="padding:8px;border:1px solid #ddd;vertical-align:top;">{result.stock_status}</td>
              <td style="padding:8px;border:1px solid #ddd;vertical-align:top;">{result.selected_tier}</td>
              <td style="padding:8px;border:1px solid #ddd;vertical-align:top;">{format_currency(result.selected_unit_cost)}</td>
              <td style="padding:8px;border:1px solid #ddd;vertical-align:top;">{format_currency(result.estimated_sale_price)}</td>
              <td style="padding:8px;border:1px solid #ddd;vertical-align:top;">{format_currency(result.estimated_profit)}</td>
              <td style="padding:8px;border:1px solid #ddd;vertical-align:top;">{roi_pct}</td>
              <td style="padding:8px;border:1px solid #ddd;vertical-align:top;">{result.ebay_sold_count}</td>
              <td style="padding:8px;border:1px solid #ddd;vertical-align:top;">{result.quick_decision}</td>
              <td style="padding:8px;border:1px solid #ddd;vertical-align:top;">{event_lines}</td>
            </tr>
            """
        )
    return f"""
    <html>
      <body>
        <h2>BooksGoat eBay Tracker Alert</h2>
        <p>Checked at: {local_now_string()}</p>
        <p>Input source used: {source_used}</p>
        <table style="border-collapse:collapse;width:100%;font-family:Arial,sans-serif;font-size:14px;">
          <thead>
            <tr>
              <th style="padding:8px;border:1px solid #ddd;text-align:left;">Title</th>
              <th style="padding:8px;border:1px solid #ddd;text-align:left;">ISBN</th>
              <th style="padding:8px;border:1px solid #ddd;text-align:left;">Stock</th>
              <th style="padding:8px;border:1px solid #ddd;text-align:left;">Tier</th>
              <th style="padding:8px;border:1px solid #ddd;text-align:left;">Unit Cost</th>
              <th style="padding:8px;border:1px solid #ddd;text-align:left;">Sale Price</th>
              <th style="padding:8px;border:1px solid #ddd;text-align:left;">Profit</th>
              <th style="padding:8px;border:1px solid #ddd;text-align:left;">ROI</th>
              <th style="padding:8px;border:1px solid #ddd;text-align:left;">Sold Count</th>
              <th style="padding:8px;border:1px solid #ddd;text-align:left;">Decision</th>
              <th style="padding:8px;border:1px solid #ddd;text-align:left;">Changes</th>
            </tr>
          </thead>
          <tbody>
            {''.join(rows)}
          </tbody>
        </table>
      </body>
    </html>
    """


def build_text_message(items: List[Dict[str, Any]], source_used: str) -> str:
    lines = [f"BooksGoat eBay Tracker Alert | Checked at {local_now_string()} | Input source: {source_used}"]
    for item in items:
        result: ScanResult = item["result"]
        roi_pct = "Unknown" if result.estimated_roi is None else f"{result.estimated_roi * 100:.2f}%"
        lines.extend([
            "",
            f"Title: {result.title}",
            f"ISBN: {result.isbn13 or result.isbn10}",
            f"Stock: {result.stock_status}",
            f"Tier: {result.selected_tier}",
            f"Unit cost: {format_currency(result.selected_unit_cost)}",
            f"Sale price: {format_currency(result.estimated_sale_price)}",
            f"Profit: {format_currency(result.estimated_profit)}",
            f"ROI: {roi_pct}",
            f"Sold comps: {result.ebay_sold_count}",
            f"Decision: {result.quick_decision}",
            "Changes:",
            *[f"- {e}" for e in item["events"]],
        ])
    return "\n".join(lines)


def send_email(subject: str, text_body: str, html_body: Optional[str] = None) -> None:
    if not EMAIL_ENABLED:
        return
    if not (EMAIL_ADDRESS and EMAIL_PASSWORD and TO_EMAIL):
        raise RuntimeError("Email is enabled but EMAIL_ADDRESS, EMAIL_PASSWORD, or TO_EMAIL is missing.")
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = EMAIL_ADDRESS
    msg["To"] = TO_EMAIL
    msg.attach(MIMEText(text_body, "plain", "utf-8"))
    if html_body:
        msg.attach(MIMEText(html_body, "html", "utf-8"))
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        server.sendmail(EMAIL_ADDRESS, [TO_EMAIL], msg.as_string())


def send_alerts(items: List[Dict[str, Any]], source_used: str) -> None:
    subject = f"BooksGoat eBay Alert: {len(items)} item(s) changed"
    send_email(subject, build_text_message(items, source_used), build_html_message(items, source_used))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Track BooksGoat supplier books against eBay sold/listed prices.")
    parser.add_argument("--input", type=str, default=str(BACKUP_INPUT_FILE), help="Path to local CSV fallback")
    parser.add_argument("--google-sheet-url", type=str, default="", help="Google Sheets CSV export URL")
    parser.add_argument("--output-csv", type=str, default=str(RESULTS_CSV), help="Output CSV filename")
    parser.add_argument("--output-xlsx", type=str, default=str(RESULTS_XLSX), help="Output XLSX filename")
    parser.add_argument("--ebay-app-id", type=str, default=os.getenv("EBAY_APP_ID", ""), help="eBay App ID")
    parser.add_argument("--ebay-fee-rate", type=float, default=DEFAULT_EBAY_FEE_RATE)
    parser.add_argument("--payment-fee-rate", type=float, default=DEFAULT_PAYMENT_FEE_RATE)
    parser.add_argument("--shipping-cost", type=float, default=DEFAULT_SHIPPING_COST)
    parser.add_argument("--packaging-cost", type=float, default=DEFAULT_PACKAGING_COST)
    parser.add_argument("--buffer-cost", type=float, default=DEFAULT_BUFFER_COST)
    parser.add_argument("--pause-seconds", type=float, default=DEFAULT_PAUSE_SECONDS)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--top-n-alert", type=int, default=DEFAULT_ALERT_TOP_N)
    parser.add_argument("--min-profit-alert", type=float, default=DEFAULT_MIN_PROFIT_ALERT)
    parser.add_argument("--min-roi-alert", type=float, default=DEFAULT_MIN_ROI_ALERT)
    parser.add_argument("--send-baseline-email", action="store_true", help="Send alerts on first baseline if items qualify")
    return parser.parse_args()


def run_once(args: argparse.Namespace) -> int:
    app_id = clean_text(args.ebay_app_id)
    if not app_id:
        print("Missing eBay App ID. Set EBAY_APP_ID or pass --ebay-app-id.", file=sys.stderr)
        return 1

    books, source_used = load_books(args)
    if args.limit and args.limit > 0:
        books = books[:args.limit]
    print(f"[{local_now_string()}] Loaded {len(books)} supplier book(s) using {source_used}.")

    results: List[ScanResult] = []
    total = len(books)
    for idx, book in enumerate(books, start=1):
        print(f"[{idx}/{total}] Scanning: {book.title[:90]}")
        try:
            result = scan_book(
                book=book,
                app_id=app_id,
                ebay_fee_rate=args.ebay_fee_rate,
                payment_fee_rate=args.payment_fee_rate,
                shipping_cost=args.shipping_cost,
                packaging_cost=args.packaging_cost,
                buffer_cost=args.buffer_cost,
                pause_seconds=args.pause_seconds,
            )
        except Exception as exc:
            result = ScanResult(
                title=book.title,
                isbn13=book.isbn13,
                isbn10=book.isbn10,
                product_url=book.product_url,
                supplier_page_fetched=False,
                supplier_page_title="",
                supplier_live_price=None,
                source_price=book.source_price,
                amazon_price=book.amazon_price,
                amazon_rank=book.amazon_rank,
                list_price=book.list_price,
                stock_status="Unknown",
                ebay_query_used=book.isbn13 or book.isbn10 or book.title,
                ebay_sold_count=0,
                ebay_active_count=0,
                sold_median=None,
                sold_mean=None,
                sold_max=None,
                active_median=None,
                active_min=None,
                selected_tier="5 Qty",
                selected_unit_cost=book.price_5 or book.source_price,
                estimated_sale_price=None,
                estimated_fees=None,
                estimated_total_cost=None,
                estimated_profit=None,
                estimated_roi=None,
                tier_5_profit=None,
                tier_5_roi=None,
                tier_10_profit=None,
                tier_10_roi=None,
                tier_25_profit=None,
                tier_25_roi=None,
                quick_decision="Error",
                notes=str(exc),
            )
        results.append(result)

    df = results_to_dataframe(results)
    save_outputs(df, Path(args.output_csv), Path(args.output_xlsx) if args.output_xlsx else None)

    state = load_state()
    changed_items: List[Dict[str, Any]] = []
    top_df = df.head(args.top_n_alert) if not df.empty else df
    top_keys = set()
    if not top_df.empty:
        for _, row in top_df.iterrows():
            top_keys.add(clean_text(row.get("isbn13") or row.get("isbn10") or row.get("title")))

    for result in results:
        key = result_key(result)
        previous = state.get(key)
        events = diff_result(previous, result, args.min_profit_alert, args.min_roi_alert)
        append_history(result, " | ".join(events) if events else "No significant change", source_used)

        should_alert = False
        if previous is None:
            should_alert = args.send_baseline_email and bool(events)
        elif events:
            should_alert = True

        key_match = clean_text(key) in top_keys
        qualifies_now = (result.estimated_profit or 0) >= args.min_profit_alert and (result.estimated_roi or 0) >= args.min_roi_alert
        if should_alert and (key_match or qualifies_now):
            changed_items.append({"result": result, "events": events})

        state[key] = asdict(result)

    save_state(state)

    if changed_items:
        send_alerts(changed_items, source_used)
        print(f"Sent alert for {len(changed_items)} item(s).")
    else:
        print("No alert-worthy changes detected.")

    if not df.empty:
        preview_cols = [
            "title",
            "supplier_live_price",
            "source_price",
            "ebay_query_used",
            "ebay_sold_count",
            "estimated_profit",
            "estimated_roi_pct",
            "quick_decision",
        ]
        print("\nTop opportunities:")
        print(df[preview_cols].head(15).to_string(index=False))

    print(f"Saved CSV: {Path(args.output_csv).resolve()}")
    if args.output_xlsx:
        print(f"Saved XLSX: {Path(args.output_xlsx).resolve()}")
    return 0


def main() -> int:
    args = parse_args()
    try:
        return run_once(args)
    except Exception as exc:
        log_error(f"Unexpected error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
