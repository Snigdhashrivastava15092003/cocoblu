"""
Myntra Product Scraper
======================
Scrapes product data from Myntra product pages using Playwright.
Architecture mirrors flipkart_scraper.py for integration consistency.

Primary data source: window.__myx JavaScript object (contains full product state)
Fallback: CSS selectors, JSON-LD, Open Graph meta tags

Version: 1.0.0
"""

import json
import re
import os
import logging
import random
import gc
import hashlib
import time
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urlparse

from bs4 import BeautifulSoup

# Shared size normalization layer
from size_mappings import (
    normalize_size,
    get_size_equivalents,
    is_valid_size,
    get_size_info,
    size_similarity,
)

# =============================================================================
# LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Playwright imports
try:
    from playwright.sync_api import (
        sync_playwright,
        TimeoutError as PlaywrightTimeout,
        Error as PlaywrightError,
    )
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    PlaywrightTimeout = Exception
    PlaywrightError = Exception

# =============================================================================
# CONFIGURATION
# =============================================================================
MAX_URL_LENGTH = 2048
PLAYWRIGHT_TIMEOUT = 60000
MAX_HTML_SIZE = 15 * 1024 * 1024

MAX_TITLE_LENGTH = 1000
MAX_DESCRIPTION_LENGTH = 5000
MAX_SPEC_KEY_LENGTH = 200
MAX_SPEC_VALUE_LENGTH = 1000
MAX_BRAND_LENGTH = 200
MAX_IMAGES = 15
MAX_SPECS = 50
MAX_SIZES = 50


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def generate_url_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def _safe(text: str, max_length: int) -> str:
    if not text or not isinstance(text, str):
        return ""
    return text.strip()[:max_length]


def _norm(text: str) -> str:
    if not text or not isinstance(text, str):
        return ""
    return " ".join(text.split()).strip()


def _parse_price(text: str) -> Tuple[Optional[float], Optional[str]]:
    """Parse price from text like '₹799' or 'Rs. 1,899'."""
    if not text or len(text) > 200:
        return None, None
    try:
        t = text.replace(",", "").strip()
        m = re.search(r"[₹Rs.]*\s*(\d+(?:\.\d{1,2})?)", t)
        if m:
            price = float(m.group(1))
            if 0 < price < 10_000_000:
                return price, "INR"
    except (ValueError, AttributeError):
        pass
    return None, None


def _parse_int(text: str) -> Optional[int]:
    if not text or len(text) > 50:
        return None
    try:
        m = re.search(r"(\d{1,10}(?:,\d{3})*)", text)
        if m:
            return int(m.group(1).replace(",", ""))
    except (ValueError, AttributeError):
        pass
    return None


# =============================================================================
# URL VALIDATION
# =============================================================================

def validate_url(url: str) -> Tuple[bool, str]:
    """Validate that the URL is a Myntra product page."""
    if not url or not isinstance(url, str):
        return False, "Invalid URL format"
    if len(url) > MAX_URL_LENGTH:
        return False, "URL too long"
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("https", "http"):
            return False, "Only HTTP/HTTPS allowed"
        if "myntra.com" not in parsed.netloc.lower():
            return False, "Only Myntra URLs allowed"
        return True, "OK"
    except Exception:
        return False, "URL validation error"


def extract_product_id_from_url(url: str) -> Optional[str]:
    """Extract numeric product ID from Myntra URL.
    
    Pattern: https://www.myntra.com/{category}/{brand}/{slug}/{productId}/buy
    """
    try:
        m = re.search(r"/(\d{5,12})/buy", url)
        if m:
            return m.group(1)
    except Exception:
        pass
    return None


# =============================================================================
# EXTRACTION LAYER 1: window.__myx (PRIMARY)
# =============================================================================

def _extract_myx_data(html: str) -> Dict[str, Any]:
    """
    Extract product data from window.__myx JavaScript object.
    
    This is the most reliable data source — contains the entire product
    state including all sizes, variants, inventory, and specifications.
    """
    result = {
        "title": None, "brand": None, "price": None, "mrp": None,
        "discount": None, "currency": "INR", "rating": None,
        "review_count": None, "images": [], "sizes": [],
        "specifications": {}, "description": None, "category": None,
        "seller": None, "product_id": None,
    }

    if not html:
        return result

    try:
        # Find window.__myx = {...}; using brace-counting for robust extraction
        marker = "window.__myx"
        idx = html.find(marker)
        if idx == -1:
            logger.debug("window.__myx not found in HTML")
            return result

        # Find the opening brace after the marker
        eq_idx = html.find("=", idx + len(marker))
        if eq_idx == -1:
            return result
        brace_start = html.find("{", eq_idx)
        if brace_start == -1:
            return result

        # Count braces to find matching closing brace
        depth = 0
        in_string = False
        escape = False
        brace_end = brace_start
        for i in range(brace_start, min(brace_start + 5_000_000, len(html))):
            ch = html[i]
            if escape:
                escape = False
                continue
            if ch == '\\' and in_string:
                escape = True
                continue
            if ch == '"' and not escape:
                in_string = not in_string
                continue
            if in_string:
                continue
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    brace_end = i
                    break

        if depth != 0:
            logger.warning("Failed to find matching closing brace for window.__myx")
            return result

        raw_json = html[brace_start:brace_end + 1]
        myx = json.loads(raw_json)
    except (json.JSONDecodeError, AttributeError) as e:
        logger.warning(f"Failed to parse window.__myx: {e}")
        return result

    # Navigate into pdpData
    pdp = myx.get("pdpData", {})
    if not pdp:
        # Sometimes data is at top level
        pdp = myx

    # --- Basic Info ---
    result["product_id"] = str(pdp.get("id", "")) or str(pdp.get("productId", ""))
    result["title"] = _safe(
        pdp.get("name") or pdp.get("productName", ""),
        MAX_TITLE_LENGTH,
    )
    
    # Brand
    brand_info = pdp.get("brand", {})
    if isinstance(brand_info, dict):
        result["brand"] = _safe(brand_info.get("name", ""), MAX_BRAND_LENGTH)
    elif isinstance(brand_info, str):
        result["brand"] = _safe(brand_info, MAX_BRAND_LENGTH)
    if not result["brand"]:
        result["brand"] = _safe(pdp.get("brandName", ""), MAX_BRAND_LENGTH)

    # --- Pricing ---
    price_info = pdp.get("price", {})
    if isinstance(price_info, dict):
        result["price"] = price_info.get("discounted") or price_info.get("sellingPrice")
        result["mrp"] = price_info.get("mrp") or price_info.get("marked")
        discount_val = price_info.get("discount") or price_info.get("discountPercent")
        if discount_val:
            result["discount"] = f"{discount_val}%"
    elif isinstance(price_info, (int, float)):
        result["price"] = price_info

    # Fallback pricing from sizes/styles
    if not result["price"]:
        sizes_data = pdp.get("sizes", [])
        if sizes_data and isinstance(sizes_data, list):
            for sz in sizes_data:
                if isinstance(sz, dict):
                    p = sz.get("price") or sz.get("discountedPrice")
                    if isinstance(p, (int, float)) and p > 0:
                        result["price"] = p
                        m = sz.get("mrp") or sz.get("strikedPrice")
                        if isinstance(m, (int, float)):
                            result["mrp"] = m
                        break

    # --- Rating ---
    ratings = pdp.get("ratings", {})
    if isinstance(ratings, dict):
        result["rating"] = ratings.get("averageRating") or ratings.get("average")
        result["review_count"] = (
            ratings.get("totalCount")
            or ratings.get("ratingCount")
            or ratings.get("reviewCount")
        )
    
    # --- Category ---
    breadcrumbs = pdp.get("breadcrumbs", [])
    if breadcrumbs and isinstance(breadcrumbs, list):
        # Last breadcrumb is usually the most specific category
        last_bc = breadcrumbs[-1]
        if isinstance(last_bc, dict):
            result["category"] = last_bc.get("name", "")
        elif isinstance(last_bc, str):
            result["category"] = last_bc
    if not result["category"]:
        result["category"] = pdp.get("articleType", {}).get("typeName", "") if isinstance(pdp.get("articleType"), dict) else ""
    if not result["category"]:
        result["category"] = pdp.get("category", "")

    # --- Sizes ---
    sizes_list = pdp.get("sizes", [])
    if isinstance(sizes_list, list):
        for sz in sizes_list[:MAX_SIZES]:
            if isinstance(sz, dict):
                size_label = sz.get("label") or sz.get("size") or sz.get("value") or ""
                if size_label:
                    available = sz.get("available", True)
                    inventory = sz.get("inventory", -1)
                    sku_id = sz.get("skuId", "")
                    price_for_size = sz.get("price") or sz.get("discountedPrice")
                    mrp_for_size = sz.get("mrp") or sz.get("strikedPrice")
                    result["sizes"].append({
                        "label": str(size_label).strip(),
                        "available": bool(available),
                        "inventory": inventory,
                        "skuId": sku_id,
                        "price": price_for_size,
                        "mrp": mrp_for_size,
                    })

    # --- Images ---
    # Try media/images array
    media = pdp.get("media", {})
    if isinstance(media, dict):
        albums = media.get("albums", {})
        if isinstance(albums, dict):
            for album_key in ["default", "front", "back", "left", "right"]:
                album_imgs = albums.get(album_key, [])
                if isinstance(album_imgs, list):
                    for img_obj in album_imgs:
                        if isinstance(img_obj, dict):
                            img_url = img_obj.get("imageURL") or img_obj.get("src") or img_obj.get("url")
                            if img_url and len(result["images"]) < MAX_IMAGES:
                                result["images"].append(img_url)

    # Fallback: images array directly
    if not result["images"]:
        direct_imgs = pdp.get("images", [])
        if isinstance(direct_imgs, list):
            for img in direct_imgs[:MAX_IMAGES]:
                if isinstance(img, dict):
                    url = img.get("src") or img.get("imageURL") or img.get("url")
                    if url:
                        result["images"].append(url)
                elif isinstance(img, str) and img.startswith("http"):
                    result["images"].append(img)

    # Fallback: search style images
    if not result["images"]:
        style_images = pdp.get("styleImages", {})
        if isinstance(style_images, dict):
            for key, img_obj in style_images.items():
                if isinstance(img_obj, dict):
                    url = img_obj.get("imageURL") or img_obj.get("secureSrc") or img_obj.get("src")
                    if url and len(result["images"]) < MAX_IMAGES:
                        result["images"].append(url)

    # --- Specifications ---
    # Try productDetails array
    product_details = pdp.get("productDetails", [])
    if isinstance(product_details, list):
        for detail_group in product_details:
            if isinstance(detail_group, dict):
                title = detail_group.get("title", "")
                content = detail_group.get("description", "")
                if title and content:
                    result["specifications"][_safe(title, MAX_SPEC_KEY_LENGTH)] = _safe(
                        content, MAX_SPEC_VALUE_LENGTH
                    )
                # Also check nested specifications
                specs_list = detail_group.get("specifications", [])
                if isinstance(specs_list, list):
                    for spec in specs_list:
                        if isinstance(spec, dict):
                            k = spec.get("key", "")
                            v = spec.get("value", "")
                            if k and v:
                                result["specifications"][
                                    _safe(k, MAX_SPEC_KEY_LENGTH)
                                ] = _safe(v, MAX_SPEC_VALUE_LENGTH)

    # Try sharedData or articleAttributes
    attrs = pdp.get("articleAttributes", {})
    if isinstance(attrs, dict):
        for k, v in attrs.items():
            if k and v and k not in result["specifications"]:
                result["specifications"][_safe(k, MAX_SPEC_KEY_LENGTH)] = _safe(
                    str(v), MAX_SPEC_VALUE_LENGTH
                )

    # --- Description ---
    desc = pdp.get("descriptors", [])
    if isinstance(desc, list):
        for d in desc:
            if isinstance(d, dict):
                desc_text = d.get("description", "")
                if desc_text and not result["description"]:
                    # Strip HTML tags from description
                    clean_desc = re.sub(r"<[^>]+>", " ", desc_text)
                    result["description"] = _safe(_norm(clean_desc), MAX_DESCRIPTION_LENGTH)

    # --- Seller ---
    seller_info = pdp.get("sellers", [])
    if isinstance(seller_info, list) and seller_info:
        first_seller = seller_info[0]
        if isinstance(first_seller, dict):
            result["seller"] = first_seller.get("sellerName") or first_seller.get("name", "")

    logger.info(f"📦 __myx extraction: title='{result['title'][:50]}...', "
                f"price={result['price']}, mrp={result['mrp']}, "
                f"sizes={len(result['sizes'])}, images={len(result['images'])}, "
                f"specs={len(result['specifications'])}")
    
    return result


# =============================================================================
# EXTRACTION LAYER 2: CSS SELECTORS (FALLBACK)
# =============================================================================

def _extract_from_dom(soup: BeautifulSoup) -> Dict[str, Any]:
    """
    Extract product data using CSS selectors.
    Acts as fallback when window.__myx is not available or incomplete.
    """
    result = {
        "title": None, "brand": None, "price": None, "mrp": None,
        "discount": None, "rating": None, "review_count": None,
        "images": [], "sizes": [], "specifications": {}, "description": None,
    }

    if not soup:
        return result

    try:
        # Brand
        brand_el = soup.select_one(".pdp-title")
        if brand_el:
            result["brand"] = _safe(brand_el.get_text(strip=True), MAX_BRAND_LENGTH)

        # Product Name
        name_el = soup.select_one(".pdp-name")
        if name_el:
            brand_text = result["brand"] or ""
            name_text = name_el.get_text(strip=True)
            result["title"] = _safe(f"{brand_text} {name_text}".strip(), MAX_TITLE_LENGTH)

        # Selling Price
        price_el = soup.select_one(".pdp-price strong") or soup.select_one(".pdp-price")
        if price_el:
            price_text = price_el.get_text(strip=True)
            price_val, _ = _parse_price(price_text)
            if price_val:
                result["price"] = price_val

        # MRP
        mrp_el = soup.select_one(".pdp-mrp s") or soup.select_one(".pdp-mrp")
        if mrp_el:
            mrp_text = mrp_el.get_text(strip=True)
            mrp_val, _ = _parse_price(mrp_text)
            if mrp_val:
                result["mrp"] = mrp_val

        # Discount
        disc_el = soup.select_one(".pdp-discount")
        if disc_el:
            result["discount"] = _safe(disc_el.get_text(strip=True), 50)

        # Rating
        rating_container = soup.select_one(".pdp-ratings-container")
        if rating_container:
            rating_text = rating_container.get_text(strip=True)
            m = re.search(r"(\d+\.?\d*)", rating_text)
            if m:
                try:
                    r = float(m.group(1))
                    if 0 < r <= 5:
                        result["rating"] = r
                except ValueError:
                    pass
            # Review/rating count
            count_match = re.search(r"(\d[\d,]*)\s*(?:Ratings?|Reviews?)", rating_text, re.IGNORECASE)
            if count_match:
                result["review_count"] = _parse_int(count_match.group(1))

        # Sizes
        size_buttons = soup.select(".size-buttons-size-button")
        for btn in size_buttons[:MAX_SIZES]:
            # Extract size label (first text content, usually a <p> tag)
            label_el = btn.select_one("p") or btn
            size_text = label_el.get_text(strip=True)
            if size_text and is_valid_size(size_text):
                # Check if disabled
                classes = " ".join(btn.get("class", []))
                is_disabled = "disabled" in classes.lower()
                result["sizes"].append({
                    "label": size_text,
                    "available": not is_disabled,
                })

        # Images
        image_els = soup.select(".image-grid-image img, .pdp-image-container img, .image-grid-container img")
        for img in image_els[:MAX_IMAGES]:
            src = img.get("src") or img.get("data-src") or ""
            if src and "myntassets.com" in src:
                # Upgrade to high-res
                src = re.sub(r"h_\d+", "h_960", src)
                src = re.sub(r"w_\d+", "w_720", src)
                src = re.sub(r"q_\d+", "q_95", src)
                result["images"].append(src)

        # Specifications
        spec_table = soup.select(".index-tableContainer .index-row, .pdp-productDescriptorsContainer .index-row")
        for row in spec_table[:MAX_SPECS]:
            cells = row.select(".index-rowKey, .index-rowValue")
            if len(cells) == 2:
                key = _safe(cells[0].get_text(strip=True), MAX_SPEC_KEY_LENGTH)
                val = _safe(cells[1].get_text(strip=True), MAX_SPEC_VALUE_LENGTH)
                if key and val:
                    result["specifications"][key] = val

        # Description
        desc_container = soup.select_one(".pdp-productDescriptorsContainer")
        if desc_container:
            desc_text = desc_container.get_text(separator=" ", strip=True)
            result["description"] = _safe(_norm(desc_text), MAX_DESCRIPTION_LENGTH)

    except Exception as e:
        logger.warning(f"DOM extraction error: {e}")

    return result


# =============================================================================
# EXTRACTION LAYER 3: JSON-LD & OpenGraph META
# =============================================================================

def _extract_json_ld(soup: BeautifulSoup) -> Dict[str, Any]:
    """Extract product data from JSON-LD structured data."""
    result = {
        "title": None, "brand": None, "price": None, "mrp": None,
        "images": [], "description": None, "rating": None,
    }
    try:
        scripts = soup.find_all("script", type="application/ld+json")
        for script in scripts:
            if not script.string:
                continue
            data = json.loads(script.string)
            if isinstance(data, dict) and data.get("@type") == "Product":
                result["title"] = _safe(data.get("name", ""), MAX_TITLE_LENGTH)
                
                brand = data.get("brand", {})
                if isinstance(brand, dict):
                    result["brand"] = _safe(brand.get("name", ""), MAX_BRAND_LENGTH)
                elif isinstance(brand, str):
                    result["brand"] = _safe(brand, MAX_BRAND_LENGTH)

                result["description"] = _safe(data.get("description", ""), MAX_DESCRIPTION_LENGTH)

                img = data.get("image")
                if isinstance(img, str):
                    result["images"] = [img]
                elif isinstance(img, list):
                    result["images"] = img[:MAX_IMAGES]

                # Offers
                offers = data.get("offers", {})
                if isinstance(offers, dict):
                    price_val = offers.get("price")
                    if price_val:
                        try:
                            result["price"] = float(price_val)
                        except (ValueError, TypeError):
                            pass

                # Aggregate rating
                agg = data.get("aggregateRating", {})
                if isinstance(agg, dict):
                    try:
                        result["rating"] = float(agg.get("ratingValue", 0))
                    except (ValueError, TypeError):
                        pass

                break  # Only process first Product
    except Exception as e:
        logger.debug(f"JSON-LD extraction error: {e}")

    return result


def _extract_og_meta(soup: BeautifulSoup) -> Dict[str, Any]:
    """Extract product data from Open Graph meta tags."""
    result = {
        "title": None, "price": None, "image": None,
    }
    try:
        og_title = soup.find("meta", property="og:title")
        if og_title:
            result["title"] = _safe(og_title.get("content", ""), MAX_TITLE_LENGTH)

        og_price = soup.find("meta", property="og:price:amount") or soup.find("meta", property="product:price:amount")
        if og_price:
            try:
                result["price"] = float(og_price.get("content", "0"))
            except (ValueError, TypeError):
                pass

        og_image = soup.find("meta", property="og:image")
        if og_image:
            result["image"] = og_image.get("content", "")

    except Exception as e:
        logger.debug(f"OG meta extraction error: {e}")

    return result


# =============================================================================
# MULTI-SOURCE RECONCILIATION
# =============================================================================

def _reconcile_fields(
    myx_data: Dict[str, Any],
    dom_data: Dict[str, Any],
    jsonld_data: Dict[str, Any],
    og_data: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Merge data from all extraction layers.
    Priority: __myx > DOM > JSON-LD > OG tags
    """
    result = {}

    # Helper: pick first non-empty value from sources in priority order
    def _pick(*values):
        for v in values:
            if v is not None and v != "" and v != [] and v != {}:
                return v
        return None

    result["title"] = _pick(myx_data.get("title"), dom_data.get("title"),
                            jsonld_data.get("title"), og_data.get("title"))
    result["brand"] = _pick(myx_data.get("brand"), dom_data.get("brand"),
                            jsonld_data.get("brand"))
    result["price"] = _pick(myx_data.get("price"), dom_data.get("price"),
                            jsonld_data.get("price"), og_data.get("price"))
    result["mrp"] = _pick(myx_data.get("mrp"), dom_data.get("mrp"),
                          jsonld_data.get("mrp"))
    result["discount"] = _pick(myx_data.get("discount"), dom_data.get("discount"))
    result["currency"] = "INR"
    result["rating"] = _pick(myx_data.get("rating"), dom_data.get("rating"),
                             jsonld_data.get("rating"))
    result["review_count"] = _pick(myx_data.get("review_count"), dom_data.get("review_count"))
    result["category"] = _pick(myx_data.get("category"))
    result["seller"] = _pick(myx_data.get("seller"))
    result["product_id"] = _pick(myx_data.get("product_id"))

    # Images: merge from all sources, deduplicate
    all_images = []
    seen = set()
    for source in [myx_data, dom_data, jsonld_data]:
        for img in source.get("images", []):
            if img and img not in seen:
                all_images.append(img)
                seen.add(img)
    if og_data.get("image") and og_data["image"] not in seen:
        all_images.append(og_data["image"])
    result["images"] = all_images[:MAX_IMAGES]

    # Sizes: __myx has the richest data (with availability + price per size)
    result["sizes"] = myx_data.get("sizes") or dom_data.get("sizes") or []

    # Specifications: merge, __myx takes priority
    specs = {}
    for source in [dom_data, jsonld_data, myx_data]:  # later overrides earlier
        specs.update(source.get("specifications", {}))
    result["specifications"] = specs

    # Description
    result["description"] = _pick(
        myx_data.get("description"), dom_data.get("description"),
        jsonld_data.get("description"),
    )

    return result


# =============================================================================
# PLAYWRIGHT: FETCH & INTERACT
# =============================================================================

def human_mouse_move(page):
    """Simulate human-like mouse movement."""
    try:
        width = page.viewport_size["width"]
        height = page.viewport_size["height"]
        start_x = random.randint(0, width)
        start_y = random.randint(0, height)
        end_x = random.randint(0, width)
        end_y = random.randint(0, height)

        steps = random.randint(15, 35)
        for i in range(steps):
            t = i / steps
            x = start_x + (end_x - start_x) * t + random.randint(-10, 10)
            y = start_y + (end_y - start_y) * t + random.randint(-10, 10)
            page.mouse.move(x, y)
            time.sleep(random.uniform(0.01, 0.04))
    except Exception:
        pass


def fetch_with_playwright(url: str, target_size: str = None) -> Tuple[
    Optional[str],     # html
    List[str],         # available_sizes
    List[str],         # purchasable_sizes
    Optional[str],     # selected_size
    str,               # stock_status
    Dict[str, Any],    # captured_api_data
]:
    """
    Fetch Myntra product page using Playwright with anti-bot measures.
    Intercepts network API responses for auxiliary data (ratings, inventory, etc.).
    
    Returns:
        (html, available_sizes, purchasable_sizes, selected_size, stock_status, captured_api_data)
    """
    if not PLAYWRIGHT_AVAILABLE:
        logger.error("Playwright not available")
        return None, [], [], None, "Unknown", {}

    playwright_instance = None
    browser = None
    context = None
    page = None

    try:
        playwright_instance = sync_playwright().start()

        # Environment detection
        is_lambda = (
            os.environ.get("AWS_EXECUTION_ENV") is not None
            or os.environ.get("AWS_LAMBDA_FUNCTION_NAME") is not None
        )
        headless_mode = True if is_lambda else False

        # Randomized user agents
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        ]
        selected_ua = random.choice(user_agents)
        chrome_version = "131"
        m = re.search(r"Chrome/(\d+)", selected_ua)
        if m:
            chrome_version = m.group(1)

        viewports = [
            {"width": 1920, "height": 1080},
            {"width": 1536, "height": 864},
            {"width": 1440, "height": 900},
            {"width": 1366, "height": 768},
        ]
        selected_vp = random.choice(viewports)

        browser = playwright_instance.chromium.launch(
            headless=headless_mode,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
                "--disable-infobars",
                "--window-position=0,0",
                f'--window-size={selected_vp["width"]},{selected_vp["height"]}',
            ],
            timeout=PLAYWRIGHT_TIMEOUT,
        )

        # Session support
        session_file = "myntra_session.json"
        ctx_kwargs = dict(
            user_agent=selected_ua,
            viewport=selected_vp,
            java_script_enabled=True,
            locale="en-IN",
            timezone_id="Asia/Kolkata",
            permissions=["geolocation"],
            geolocation={"latitude": 28.4595, "longitude": 77.0266},
            extra_http_headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                          "image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Cache-Control": "max-age=0",
                "sec-ch-ua": f'"Not(A:Brand";v="99", "Google Chrome";v="{chrome_version}", '
                             f'"Chromium";v="{chrome_version}"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
            },
        )

        if os.path.exists(session_file):
            logger.info("🍪 Loading Myntra session")
            context = browser.new_context(storage_state=session_file, **ctx_kwargs)
        else:
            logger.info("🆕 Starting fresh Myntra session")
            context = browser.new_context(**ctx_kwargs)

        context.set_default_timeout(PLAYWRIGHT_TIMEOUT)
        page = context.new_page()

        # ---- Stealth JS injection ----
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5],
            });
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-IN', 'en-US', 'en'],
            });
            window.chrome = { runtime: {} };
        """)
        # ---- Network API interception ----
        captured_api_data = {}

        def _handle_response(response):
            """Capture relevant Myntra API responses."""
            try:
                url_str = response.url
                if response.status != 200:
                    return
                content_type = response.headers.get("content-type", "")
                if "application/json" not in content_type:
                    return

                # Capture product detail / rating / inventory APIs
                api_patterns = {
                    "product": r"/api/v\d+/product/\d+",
                    "ratings": r"/gateway/v\d+/reviews/",
                    "inventory": r"/api/v\d+/inventory/",
                    "crosssell": r"/api/v\d+/cross-sell/",
                    "price": r"/api/v\d+/price/",
                }
                for key, pattern in api_patterns.items():
                    if re.search(pattern, url_str):
                        try:
                            body = response.json()
                            captured_api_data[key] = body
                            logger.info(f"🔗 Captured API: {key} ({url_str[:80]}...)")
                        except Exception:
                            pass
                        break
            except Exception:
                pass

        page.on("response", _handle_response)

        # ---- Navigate ----
        logger.info(f"🌐 Navigating to: {url}")
        page.goto(url, wait_until="domcontentloaded", timeout=PLAYWRIGHT_TIMEOUT)

        # Random delay for hydration
        page.wait_for_timeout(random.randint(2000, 4000))

        # Simulate human activity
        human_mouse_move(page)
        page.wait_for_timeout(random.randint(500, 1500))

        # Scroll down a bit to trigger lazy loading
        page.evaluate("window.scrollBy(0, 300)")
        page.wait_for_timeout(random.randint(500, 1000))

        # Wait for price element to appear (page hydrated)
        try:
            page.wait_for_selector(".pdp-price", timeout=15000)
            logger.info("✅ Page hydrated — .pdp-price visible")
        except PlaywrightTimeout:
            logger.warning("⚠️ Timeout waiting for .pdp-price — continuing anyway")

        # ---- Close popups ----
        try:
            popup_selectors = [
                "//span[contains(text(),'✕')]",
                "button[class*='close']",
                "[aria-label='Close']",
                ".modal-close",
            ]
            for sel in popup_selectors:
                try:
                    if page.locator(sel).is_visible(timeout=1500):
                        page.locator(sel).click(timeout=1500)
                        page.wait_for_timeout(500)
                        break
                except Exception:
                    continue
        except Exception:
            pass

        # ---- Size extraction ----
        available_sizes = []
        purchasable_sizes = []

        # Myntra uses multiple selectors for size buttons:
        # - .size-buttons-size-button (standard)
        # - .size-buttons-sizeButtonAsLink > .size-buttons-size-button (link-wrapped)
        # - .size-buttons-big-size (often used for out of stock sizes like '11-12Y')
        # - .size-buttons-unified-size
        primary_selectors = [
            ".size-buttons-size-button",
            ".size-buttons-sizeButtonAsLink > .size-buttons-size-button",
            ".size-buttons-big-size",
            ".size-buttons-unified-size"
        ]
        size_buttons = page.locator(", ".join(primary_selectors))
        size_count = min(size_buttons.count(), MAX_SIZES)
        
        # Fallback: if no buttons found, try broader selector
        if size_count == 0:
            size_buttons = page.locator("[class*='size-buttons-size-button']")
            size_count = min(size_buttons.count(), MAX_SIZES)
        
        logger.info(f"🔍 Found {size_count} size buttons")

        def _extract_size_label(btn):
            """Extract clean size label from a size button, ignoring badge text and embedded prices."""
            try:
                # Helper to clean text
                def _clean(t):
                    if not t: return t
                    # Strip embedded price texts like "Rs. 441" or "₹ 441"
                    t = re.sub(r'(Rs\.?|₹)\s*\d+.*$', '', t, flags=re.IGNORECASE).strip()
                    # Strip known badge suffixes
                    t = re.sub(r'\d+\s*left$', '', t, flags=re.IGNORECASE).strip()
                    t = re.sub(r'few\s*left$', '', t, flags=re.IGNORECASE).strip()
                    # Strip "HideBody Measurem" or similar
                    t = re.sub(r'HideBody.*$', '', t, flags=re.IGNORECASE).strip()
                    t = re.sub(r'Body Measurement.*$', '', t, flags=re.IGNORECASE).strip()
                    return t
                
                # First try: get the first <p> tag's inner text (most reliable)
                p_tags = btn.locator("p")
                if p_tags.count() > 0:
                    raw = _clean(p_tags.first.inner_text().strip())
                    if raw and is_valid_size(raw):
                        return raw
                
                # Second try: full text content, cleaned
                full_text = btn.text_content().strip()
                if not full_text:
                    return None
                
                cleaned = _clean(full_text)
                if cleaned and is_valid_size(cleaned):
                    return cleaned
                
                # Third try: just take the first line/word group
                first_part = full_text.split('\n')[0].strip()
                first_part = _clean(first_part)
                if first_part and is_valid_size(first_part):
                    return first_part
                
            except Exception:
                pass
            return None
        for i in range(size_count):
            try:
                btn = size_buttons.nth(i)
                if btn.is_visible(timeout=2000):
                    sz_text = _extract_size_label(btn)

                    if sz_text:
                        available_sizes.append(sz_text)
            except Exception:
                continue
                
        # Deduplicate while preserving order
        available_sizes = list(dict.fromkeys(available_sizes))
        
        # ---- Determine Purchasable Sizes using __myx ----
        # The DOM representation of "out of stock" varies wildly (strikethrough SVGs, opacity, disabled classes).
        # We rely on window.__myx.pdpData.sizes as the ultimate source of truth for availability.
        try:
            myx_sizes = page.evaluate("""() => {
                try {
                    const myx = window.__myx;
                    if (!myx || !myx.pdpData) return [];
                    return (myx.pdpData.sizes || []).filter(s => s.label).map(s => ({
                        label: s.label.toUpperCase(),
                        available: s.available
                    }));
                } catch(e) { return []; }
            }""")
            
            if myx_sizes and isinstance(myx_sizes, list):
                # Create a lookup map for availability
                myx_avail_map = {sz["label"]: sz["available"] for sz in myx_sizes if "label" in sz}
                
                for sz in available_sizes:
                    sz_upper = sz.upper()
                    if myx_avail_map.get(sz_upper) is True:
                        purchasable_sizes.append(sz)
                    elif myx_avail_map.get(sz_upper) is False:
                        # Explicitly unavailable in __myx
                        pass
                    else:
                        # Fallback if size not found in __myx for some reason
                        purchasable_sizes.append(sz)
                        
                # Also, if myx found sizes we didn't see in DOM, add them
                for sz in myx_sizes:
                    lbl = sz.get("label")
                    if lbl and lbl not in [a.upper() for a in available_sizes]:
                        # A size that wasn't rendered as a button at all
                        available_sizes.append(lbl)
                        if sz.get("available"):
                            purchasable_sizes.append(lbl)
                
            else:
                # Absolute fallback if __myx evaluation fails: assume all visible sizes are purchasable
                purchasable_sizes = list(available_sizes)
                logger.warning("⚠️ Could not evaluate __myx sizes for availability; assuming all visible are purchasable")
                
        except Exception as e:
            logger.debug(f"Failed to extract __myx sizes for availability: {e}")
            purchasable_sizes = list(available_sizes)

        # Final deduplicate to be absolutely safe
        available_sizes = list(dict.fromkeys(available_sizes))
        purchasable_sizes = list(dict.fromkeys(purchasable_sizes))

        logger.info(f"📏 Available sizes: {available_sizes}")
        logger.info(f"🛒 Purchasable sizes: {purchasable_sizes}")

        # ---- Size selection ----
        selected_size = None
        if target_size and available_sizes:
            normalized_target = normalize_size(target_size)
            
            for i in range(size_count):
                try:
                    btn = size_buttons.nth(i)
                    sz_text = _extract_size_label(btn)

                    if not sz_text:
                        continue

                    # Check for exact or normalized match
                    if (sz_text.upper() == target_size.upper()
                            or normalize_size(sz_text) == normalized_target
                            or size_similarity(sz_text, target_size) == 1.0):
                        if btn.is_visible(timeout=2000):
                            btn.click(timeout=3000)
                            page.wait_for_timeout(2000)
                            selected_size = sz_text
                            logger.info(f"✅ Selected size: {selected_size}")
                            break
                except Exception:
                    continue

        # ---- Stock status ----
        stock_status = _evaluate_stock_status(page)

        # ---- Save session ----
        try:
            context.storage_state(path=session_file)
        except Exception:
            pass

        # ---- Capture HTML ----
        html = page.content()
        if len(html) > MAX_HTML_SIZE:
            html = html[:MAX_HTML_SIZE]

        logger.info(f"🔗 Captured {len(captured_api_data)} API responses: {list(captured_api_data.keys())}")
        return html, available_sizes, purchasable_sizes, selected_size, stock_status, captured_api_data

    except Exception as e:
        logger.error(f"❌ Playwright fetch failed: {e}")
        return None, [], [], None, "Unknown", {}

    finally:
        try:
            if page:
                page.close()
            if context:
                context.close()
            if browser:
                browser.close()
            if playwright_instance:
                playwright_instance.stop()
        except Exception:
            pass


def _evaluate_stock_status(page) -> str:
    """
    Determine stock status by checking for Add to Bag / Notify Me buttons.
    
    Myntra uses:
    - "ADD TO BAG" button → In Stock
    - "NOTIFY ME" / "SOLD OUT" → Out of Stock
    """
    try:
        page.wait_for_timeout(1000)

        # Check "ADD TO BAG"
        add_to_bag_selectors = [
            "button:has-text('ADD TO BAG')",
            "button:has-text('Add to Bag')",
            "div.pdp-add-to-bag",
            "[class*='add-to-bag']",
        ]
        for sel in add_to_bag_selectors:
            try:
                elements = page.locator(sel).all()
                for btn in elements:
                    if btn.is_visible(timeout=500):
                        text = btn.inner_text().strip().upper()
                        if "ADD TO BAG" in text:
                            is_disabled = btn.get_attribute("disabled") is not None
                            aria_disabled = btn.get_attribute("aria-disabled") == "true"
                            if not (is_disabled or aria_disabled):
                                logger.info("✅ 'ADD TO BAG' found & enabled → IN STOCK")
                                return "In Stock"
            except Exception:
                continue

        # Check "BUY NOW"
        try:
            buy_now = page.locator("button:has-text('BUY NOW'), button:has-text('Buy Now')")
            for i in range(buy_now.count()):
                btn = buy_now.nth(i)
                if btn.is_visible(timeout=500):
                    logger.info("✅ 'BUY NOW' found → IN STOCK")
                    return "In Stock"
        except Exception:
            pass

        # Check "NOTIFY ME" / "SOLD OUT"
        notify_selectors = [
            "button:has-text('NOTIFY ME')",
            "div:has-text('Sold Out')",
            "div:has-text('SOLD OUT')",
            "button:has-text('Notify Me')",
            "[class*='notify-me']",
        ]
        for sel in notify_selectors:
            try:
                elements = page.locator(sel).all()
                for btn in elements:
                    if btn.is_visible(timeout=500):
                        logger.info("❌ 'NOTIFY ME' / 'SOLD OUT' found → OUT OF STOCK")
                        return "Out of Stock"
            except Exception:
                continue

        logger.warning("⚠️ Could not determine stock status → Unknown")
        return "Unknown"

    except Exception as e:
        logger.error(f"Stock evaluation error: {e}")
        return "Unknown"


# =============================================================================
# MAIN SCRAPER
# =============================================================================

def scrape_single_url(url: str, target_size: str = None) -> Dict[str, Any]:
    """
    Scrape a single Myntra product page.
    
    Args:
        url: Myntra product URL
        target_size: Optional specific size to select
    
    Returns:
        Dict with product data and success flag
    """
    url_hash = generate_url_hash(url)

    # Validate URL
    is_valid, validation_msg = validate_url(url)
    if not is_valid:
        return {
            "url_hash": url_hash,
            "error": validation_msg,
            "scraped_at": datetime.now().isoformat(),
            "success": False,
        }

    html = None
    soup = None

    try:
        # ================================================================
        # STEP 1: Fetch page with Playwright
        # ================================================================
        html, available_sizes, purchasable_sizes, selected_size, stock_status, captured_api_data = \
            fetch_with_playwright(url, target_size)

        if not html:
            raise Exception("Failed to fetch Myntra page — possible anti-bot block")

        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception:
            soup = BeautifulSoup(html, "html.parser")

        # ================================================================
        # STEP 2: Extract data from all sources
        # ================================================================
        myx_data = _extract_myx_data(html)
        dom_data = _extract_from_dom(soup)
        jsonld_data = _extract_json_ld(soup)
        og_data = _extract_og_meta(soup)

        # Log source availability
        sources_available = []
        if myx_data.get("title"):
            sources_available.append("__myx")
        if dom_data.get("title"):
            sources_available.append("DOM")
        if jsonld_data.get("title"):
            sources_available.append("JSON-LD")
        if og_data.get("title"):
            sources_available.append("OG")
        logger.info(f"📊 Active data sources: {sources_available}")

        # ================================================================
        # STEP 3: Reconcile data from all sources
        # ================================================================
        product = _reconcile_fields(myx_data, dom_data, jsonld_data, og_data)

        # ================================================================
        # STEP 4: Add Playwright-derived data
        # ================================================================
        # Size data from Playwright (live DOM) takes priority for availability
        if available_sizes:
            # Merge size availability from Playwright with price data from __myx
            myx_size_map = {}
            for sz in myx_data.get("sizes", []):
                myx_size_map[sz.get("label", "").upper()] = sz

            merged_sizes = []
            for sz_label in available_sizes:
                myx_sz = myx_size_map.get(sz_label.upper(), {})
                merged_sizes.append({
                    "label": sz_label,
                    "available": sz_label in purchasable_sizes,
                    "price": myx_sz.get("price"),
                    "mrp": myx_sz.get("mrp"),
                    "skuId": myx_sz.get("skuId", ""),
                })

            product["sizes"] = merged_sizes

        # Flatten size lists for backward compatibility with flipkart_scraper output
        product["available_sizes"] = available_sizes
        product["purchasable_sizes"] = purchasable_sizes
        product["size"] = selected_size

        # Stock
        product["instock"] = stock_status
        product["stock_detection_method"] = "buy_box_authoritative"

        # Size selection status
        if selected_size:
            product["size_selection_status"] = "SUCCESS"
        elif target_size:
            product["size_selection_status"] = "FAILED"
        else:
            product["size_selection_status"] = "NO_TARGET"

        # ================================================================
        # STEP 4b: Enrich from captured API data
        # ================================================================
        if captured_api_data:
            # Ratings from API
            ratings_api = captured_api_data.get("ratings", {})
            if isinstance(ratings_api, dict):
                avg = ratings_api.get("averageRating") or ratings_api.get("avgRating")
                if avg and not product.get("rating"):
                    try:
                        product["rating"] = float(avg)
                    except (ValueError, TypeError):
                        pass
                count = ratings_api.get("totalCount") or ratings_api.get("count")
                if count and not product.get("review_count"):
                    product["review_count"] = count

            # Price from API (in case of size-dependent pricing)
            price_api = captured_api_data.get("price", {})
            if isinstance(price_api, dict):
                api_price = price_api.get("discounted") or price_api.get("price")
                api_mrp = price_api.get("mrp") or price_api.get("marked")
                if api_price and selected_size:
                    # API price after size selection is most accurate
                    product["price"] = api_price
                    if api_mrp:
                        product["mrp"] = api_mrp

        # ================================================================
        # STEP 5: Final metadata
        # ================================================================
        product["url"] = url
        product["url_hash"] = url_hash
        product["platform"] = "myntra"
        product["extraction_method"] = "playwright_combined"
        product["scraped_at"] = datetime.now().isoformat()
        product["target_size_requested"] = _safe(target_size, 100) if target_size else None
        product["success"] = True

        # Rename specs for consistency
        product["specs"] = product.pop("specifications", {})

        logger.info(f"✅ Scrape SUCCESS: {product.get('title', '')[:60]}")
        logger.info(f"   Price: ₹{product.get('price')} | MRP: ₹{product.get('mrp')} | "
                     f"Stock: {product.get('instock')} | Sizes: {len(available_sizes)}")

        return product

    except Exception as e:
        error_message = str(e)
        logger.error(f"❌ Scraping failed: {error_message}")
        return {
            "url_hash": url_hash,
            "url": url,
            "platform": "myntra",
            "error": "Scraping failed",
            "error_details": error_message,
            "scraped_at": datetime.now().isoformat(),
            "success": False,
        }
    finally:
        if soup:
            soup.decompose()
        del soup, html
        gc.collect()


# =============================================================================
# LAMBDA HANDLER
# =============================================================================

def lambda_handler(event, context):
    """AWS Lambda handler for Myntra scraper."""
    try:
        if isinstance(event.get("body"), str):
            try:
                body = json.loads(event["body"])
            except json.JSONDecodeError:
                return {
                    "statusCode": 400,
                    "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                    "body": json.dumps({"error": "Invalid JSON"}),
                }
        else:
            body = event.get("body", event)

        if event.get("httpMethod") == "OPTIONS":
            return {
                "statusCode": 200,
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "POST, OPTIONS",
                    "Access-Control-Allow-Headers": "Content-Type",
                },
                "body": "",
            }

        url = body.get("url")
        target_size = body.get("size") or body.get("target_size")

        if not url:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"error": "No URL provided"}),
            }

        product = scrape_single_url(url, target_size)

        response_data = {
            "success": product.get("success", False),
            "data": product,
        }

        return {
            "statusCode": 200 if response_data["success"] else 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps(response_data, ensure_ascii=False),
        }

    except Exception:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"success": False, "error": "Internal server error"}),
        }
    finally:
        gc.collect()


# =============================================================================
# LOCAL TESTING
# =============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("MYNTRA PRODUCT SCRAPER v1.0")
    print("=" * 80 + "\n")

    test_url = "https://www.myntra.com/trunk/vanheusen/van-heusen-boys-pack-of-2-allover-print-ultra-soft---trunks-ikibtr2sp1521056/24493532/buy"
    target_size = "9-10Y"  # <--- SET YOUR TARGET SIZE HERE (e.g., 'M', 'L', '40', '42')
    
    print(f"🔗 URL: {test_url}")
    print(f"📏 Target Size: {target_size}\n")
    result = scrape_single_url(test_url, target_size=target_size)

    print("\n" + "=" * 80)
    print("RESULTS")
    print("=" * 80 + "\n")

    if "error" in result and not result.get("success"):
        print("❌" * 40)
        print("🚨 SCRAPING FAILED 🚨")
        print("❌" * 40)
        print(f"\n⚠️  Error: {result.get('error', 'Unknown')}")
        if result.get("error_details"):
            print(f"   Details: {result['error_details'][:200]}")
    else:
        print(f"📊 PRODUCT INFORMATION:")
        print(f"   Title: {result.get('title', 'N/A')}")
        print(f"   Brand: {result.get('brand', 'N/A')}")
        print(f"   Category: {result.get('category', 'N/A')}")

        print(f"\n💰 PRICING:")
        print(f"   Price: ₹{result.get('price', 'N/A')}")
        print(f"   MRP: ₹{result.get('mrp', 'N/A')}")
        print(f"   Discount: {result.get('discount', 'N/A')}")
        print(f"   Currency: {result.get('currency', 'N/A')}")
        print(f"   In Stock: {result.get('instock', 'N/A')}")

        print(f"\n📏 SIZES:")
        print(f"   Available: {result.get('available_sizes', [])}")
        print(f"   Purchasable: {result.get('purchasable_sizes', [])}")
        print(f"   Selected: {result.get('size', 'None')}")

        print(f"\n🖼️ IMAGES:")
        images = result.get("images", [])
        print(f"   Total: {len(images)}")
        for i, img in enumerate(images[:3]):
            print(f"   [{i+1}] {img[:80]}...")

        print(f"\n📋 SPECIFICATIONS:")
        specs = result.get("specs", {})
        print(f"   Total: {len(specs)}")
        for key, value in list(specs.items())[:5]:
            print(f"   {key}: {value}")

        print(f"\n⭐ RATING: {result.get('rating', 'N/A')}")
        print(f"   Reviews: {result.get('review_count', 'N/A')}")

        print(f"\n📝 DESCRIPTION:")
        desc = result.get("description", "")
        print(f"   {desc[:200]}..." if desc else "   N/A")

    print(f"\n⏱️ Scraped at: {result.get('scraped_at', 'N/A')}")
    print(f"🔑 URL Hash: {result.get('url_hash', 'N/A')}")
    print(f"✅ Success: {result.get('success', False)}")
