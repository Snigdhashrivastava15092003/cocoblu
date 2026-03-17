import json
import re
import sys
import time
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# -------- Configuration --------

# Default Amazon domain - can be overridden in request
DEFAULT_AMAZON_DOMAIN = "amazon.in"

# Supported Amazon domains
AMAZON_DOMAINS = {
    "in": "amazon.in",
    "us": "amazon.com",
    "uk": "amazon.co.uk",
    "de": "amazon.de",
    "fr": "amazon.fr",
    "it": "amazon.it",
    "es": "amazon.es",
    "ca": "amazon.ca",
    "au": "amazon.com.au",
    "br": "amazon.com.br",
    "jp": "amazon.co.jp",
    "mx": "amazon.com.mx",
}

# -------- URL Construction --------

def construct_amazon_url(asin: str, domain: str = None) -> str:
    """
    Construct Amazon product URL from ASIN.
    
    Args:
        asin: Amazon Standard Identification Number (10 characters)
        domain: Country code (in, us, uk, etc.) or full domain (amazon.in)
        
    Returns:
        Full Amazon product URL
    """
    # Clean ASIN
    asin = asin.strip().upper()
    
    # Validate ASIN format (usually 10 alphanumeric characters)
    if not re.match(r'^[A-Z0-9]{10}$', asin):
        logger.warning(f"ASIN '{asin}' doesn't match standard format (10 alphanumeric chars)")
    
    # Determine domain
    if domain:
        domain = domain.lower().strip()
        # If short code provided (e.g., "in", "us")
        if domain in AMAZON_DOMAINS:
            domain = AMAZON_DOMAINS[domain]
        # If full domain provided without amazon. prefix
        elif not domain.startswith("amazon."):
            domain = f"amazon.{domain}"
    else:
        domain = DEFAULT_AMAZON_DOMAIN
    
    # Construct URL
    url = f"https://www.{domain}/dp/{asin}"
    logger.info(f"Constructed URL: {url}")
    return url

def extract_asin_from_url(url: str) -> Optional[str]:
    """
    Extract ASIN from Amazon URL.
    
    Args:
        url: Amazon product URL
        
    Returns:
        ASIN string or None
    """
    patterns = [
        r'/dp/([A-Z0-9]{10})',
        r'/gp/product/([A-Z0-9]{10})',
        r'/product/([A-Z0-9]{10})',
        r'[?&]asin=([A-Z0-9]{10})',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url, re.IGNORECASE)
        if match:
            return match.group(1).upper()
    
    return None

# -------- Utilities --------

def _norm(text: str) -> str:
    """Normalize text by removing extra whitespace and zero-width characters."""
    if not text:
        return ""
    # Strip zero-width markers and other non-printing characters common in Amazon UI
    text = text.replace("\u200e", "").replace("\u200f", "").replace("\u200b", "")
    return re.sub(r"\s+", " ", str(text)).strip()

def _parse_price(text: str) -> Tuple[Optional[float], Optional[str]]:
    """Parse price from text and return (price, currency)."""
    if not text:
        return None, None
    t = text.replace(",", "").strip()
    m = re.search(r"₹\s*([0-9]+(?:\.[0-9]+)?)", t)
    if m:
        return float(m.group(1)), "INR"
    m = re.search(r"Rs\.?\s*([0-9]+(?:\.[0-9]+)?)", t, re.I)
    if m:
        return float(m.group(1)), "INR"
    m = re.search(r"\$\s*([0-9]+(?:\.[0-9]+)?)", t)
    if m:
        return float(m.group(1)), "USD"
    m = re.search(r"€\s*([0-9]+(?:\.[0-9]+)?)", t)
    if m:
        return float(m.group(1)), "EUR"
    m = re.search(r"£\s*([0-9]+(?:\.[0-9]+)?)", t)
    if m:
        return float(m.group(1)), "GBP"
    m = re.search(r"\b([A-Za-z]{3})\s*([0-9]+(?:\.[0-9]+)?)", t)
    if m:
        return float(m.group(2)), m.group(1).upper()
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)", t)
    if m:
        return float(m.group(1)), None
    return None, None

def _parse_int(text: str) -> Optional[int]:
    """Parse integer from text."""
    if not text:
        return None
    m = re.search(r"([0-9][0-9,]*)", text)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except Exception:
        return None

def _extract_json_ld(soup: BeautifulSoup) -> Dict[str, Any]:
    """Extract JSON-LD structured data from page."""
    scripts = soup.find_all("script", {"type": "application/ld+json"})
    for s in scripts:
        txt = (s.string or s.get_text() or "").strip()
        if not txt:
            continue
        try:
            data = json.loads(txt)
            if isinstance(data, dict):
                return data
            if isinstance(data, list) and data:
                # Find the first product-like dict
                for item in data:
                    if isinstance(item, dict) and item.get("@type", "").lower() in ["product", "creativework", "itemlist"]:
                        return item
                return data[0]
        except Exception:
            m = re.search(r"(\{.*\})", txt, re.S)
            if m:
                try:
                    return json.loads(m.group(1))
                except Exception:
                    pass
    return {}

def _is_valid_image_url(url: str) -> bool:
    """Check if URL is a valid image URL."""
    if not url:
        return False
    if url.startswith("//"):
        url = "https:" + url
    if not url.startswith(("http://", "https://")):
        return False
    low = url.lower()
    if any(ext in low for ext in [".jpg", ".jpeg", ".png", ".webp"]):
        return True
    if any(k in low for k in ["images", "media", "cloudfront", "amazonaws", "ssl-images"]):
        return True
    return False

def _extract_images(soup: BeautifulSoup) -> List[str]:
    """Extract product images from page."""
    imgs: List[Tuple[str, int]] = []
    landing = soup.select_one("img#landingImage, img[data-old-hires]")
    if landing:
        dyn = landing.get("data-a-dynamic-image")
        if dyn:
            try:
                data = json.loads(dyn)
                for url, size in data.items():
                    if isinstance(size, list) and len(size) >= 2:
                        area = int(size[0]) * int(size[1])
                    else:
                        area = 0
                    if _is_valid_image_url(url):
                        imgs.append((url, area))
            except Exception:
                pass
        hires = landing.get("data-old-hires")
        if hires and _is_valid_image_url(hires):
            imgs.append((hires, 999999))
    for thumb in soup.select("#altImages img"):
        src = thumb.get("src") or thumb.get("data-src")
        if src and _is_valid_image_url(src):
            imgs.append((src, 100000))
    if not imgs:
        for img in soup.find_all("img"):
            src = img.get("data-src") or img.get("src")
            if src and _is_valid_image_url(src):
                # Filter out UI elements
                if any(p in (src or "").lower() for p in ["sprite", "nav-sprite", "logo", "icon", "banner", "ads", "placeholder"]):
                    continue
                imgs.append((src, 100))
    seen: set[str] = set()
    out: List[str] = []
    for url, score in sorted(imgs, key=lambda x: x[1], reverse=True):
        if url not in seen:
            seen.add(url)
            out.append(url)
    return out[:8]

def _extract_description(soup: BeautifulSoup) -> str:
    """
    Extract product description with strict prioritization.
    1. 'About this item' feature bullets (cleanest source)
    2. 'Product Description' prose section
    3. JSON-LD description (if legitimate)
    """
    # 1. Feature Bullets (About this item / Voyager highlights)
    bullet_selectors = [
        "#feature-bullets li span.a-list-item",
        "#feature-bullets ul li span", 
        "#feature-bullets li",
        "#productFactsDesktopExpander ul li span.a-list-item",
        "#topHighlight .a-fixed-left-grid-col.a-col-right span",
    ]
    
    # Keywords indicating technical metadata that should NOT be in the description
    forbidden_starts = [
         "asen", "asin", "product dimensions", "date first available", "manufacturer", 
         "packer", "importer", "item model number", "country of origin", "department",
         "net quantity", "generic name", "best sellers rank", "customer reviews",
         "style name", "closure type", "neck style"
    ]
    
    feats = []
    for sel in bullet_selectors:
        candidates = soup.select(sel)
        current_feats = []
        for el in candidates:
            text = _norm(el.get_text())
            text_lower = text.lower()
            
            # Smart Filtering:
            # 1. Must be decent length (> 5 chars)
            # 2. Must not be the "About this item" header
            # 3. Must not start with forbidden technical keywords
            if (text and len(text) > 5 
                and not text_lower.startswith("about this item")
                and not any(text_lower.startswith(k) for k in forbidden_starts)):
                current_feats.append(text)
        
        if current_feats:
            feats = current_feats
            break
            
    if feats:
        # Join with clear delimiters
        return " | ".join(feats)

    # 2. Product Description (Prose)
    # Often contained in a specific div
    desc_el = soup.select_one("#productDescription span") or soup.select_one("#productDescription p") or soup.select_one("#productDescription")
    if desc_el:
        text = _norm(desc_el.get_text())
        if len(text) > 50: # Only accept substantial descriptions
            return text

    # 3. Meta Description (Fallback)
    # Often cleaner than random page text
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc:
        content = meta_desc.get("content", "").strip()
        if content and len(content) > 20 and "Amazon" not in content:
            return content

    return ""

def _extract_specs(soup: BeautifulSoup) -> Dict[str, str]:
    """Extract technical specifications from product page."""
    specs: Dict[str, str] = {}
    for table_sel in [
        "#productDetails_techSpec_section_1",
        "#productDetails_techSpec_section_2",
        "#productDetails_detailBullets_sections1",
        "#technicalSpecifications_feature_div table",
        "#prodDetails",
        ".voyager-ns-desktop-table",
        ".prodDetTable",
    ]:
        tables = soup.select(table_sel)
        for table in tables:
            for tr in table.find_all("tr"):
                # Standard Amazon tables (Legacy)
                tds = tr.find_all(["th", "td"])
                if len(tds) >= 2:
                    k = _norm(tds[0].get_text())
                    v = _norm(tds[1].get_text())
                    if k and v and len(k) < 100:
                        specs[k] = v
                
                # Voyager NS labels / values (Modern)
                label = tr.select_one(".voyager-ns-desktop-table-label, .prodDetSectionEntry")
                value = tr.select_one(".voyager-ns-desktop-table-value, .prodDetAttrValue")
                if label and value:
                    k = _norm(label.get_text())
                    v = _norm(value.get_text())
                    if k and v:
                        specs[k] = v

    # Fallback for Detail Bullets (Detail Page)
    bullets = soup.select("#detailBullets_feature_div li, .product-facts-detail")
    for li in bullets:
        # detailedBullets pattern
        key_el = li.select_one("span.a-text-bold")
        val_el = li.select_one("span:not(.a-text-bold)")
        
        # Voyager grid pattern (label on left, value on right)
        if not key_el:
            key_el = li.select_one(".a-col-left")
            val_el = li.select_one(".a-col-right")

        if key_el and val_el:
            k = _norm(key_el.get_text()).rstrip(":").strip()
            v = _norm(val_el.get_text())
            
            # Clean up redundant key-in-value text (e.g., "ASIN: B0BT..." -> "B0BT...")
            if v.lower().startswith(k.lower()):
                v = v[len(k):].strip().lstrip(":").strip()
                
            if k and v and len(k) < 100:
                specs[k] = v
    return specs

def _extract_color(soup: BeautifulSoup, title: str) -> Optional[str]:
    """Extract product color from multiple sources with fallback to title patterns."""
    # 1. Variation selectors (Inline twisters)
    for sel in [
        "#variation_color_name .selection",
        "#inline-twister-expanded-dimension-text-color_name",
        "#twister-plus-variation-color-name",
        "[data-feature-name='color_name'] .selection",
    ]:
        el = soup.select_one(sel)
        if el:
            txt = _norm(el.get_text())
            if txt and len(txt) < 30:
                return txt

    # 2. Title patterns (e.g., "(Maroon, XXL)" or "(ASJKMJRGFV78651_Maroon")
    # Patterns for (Value), (Value1, Value2), _Value
    patterns = [
        r"\([^,)]+,\s*([^,)]+)\)",  # (Size, Color)
        r"\(([^,)]+),\s*[^,)]+\)",  # (Color, Size)
        r"\(([^_)]+)_([^_)]+)\)",   # (Model_Color)
        r"\(([^_)]+)_([^_)]+)$",    # (Model_Color (unclosed)
        r"\(([^_)]+)\)$",           # (Color) 
        r"\(\s*([^)]+)\s*$",        # Unclosed Paren (Maroon
        r"_\s*([^_]+)$",            # _Maroon
        r"-\s*([^-]+)$",            # - Maroon
    ]
    for p in patterns:
        m = re.search(p, title)
        if m:
            # If multiple groups, the last one is often the color
            val = _norm(m.group(m.lastindex)) if m.lastindex else _norm(m.group(1))
            
            # Sanity check: if it looks like a model number (all caps+digits), maybe it's not the color
            if val and len(val) > 4 and val.isupper() and any(c.isdigit() for c in val):
                # If we have multiple groups and the first was the model, maybe the second is color
                if m.lastindex and m.lastindex > 1:
                    val = _norm(m.group(2))
                else: continue # Skip if it looks like just a model number
                
            if val and len(val) < 25 and not any(x in val.lower() for x in ["size", "count", "pack"]):
                return val
    return None

def _extract_size(soup: BeautifulSoup, title: str) -> Optional[str]:
    """Extract product size/variant from multiple sources."""
    
    # Strategy 1: Check variation dropdown/selection
    size_selectors = [
        "#variation_size_name .selection",
        "#native_dropdown_selected_size_name",
        "#inline-twister-expanded-dimension-text-size_name",
        "select#native_dropdown_selected_size_name option[selected]",
        "span.a-dropdown-prompt",
        ".a-dropdown-prompt",
        "[data-feature-name='size_name'] .selection",
    ]
    
    for selector in size_selectors:
        elem = soup.select_one(selector)
        if elem:
            size_text = _norm(elem.get_text())
            # STRICT FILTERING:
            # 1. Reject if starts with Select/Choose (e.g. "Select Size")
            # 2. Reject if it looks like a list (e.g. "S M L XL")
            if size_text:
                lower_text = size_text.lower()
                is_prompt = any(lower_text.startswith(p) for p in ["select", "choose", "option"])
                is_size_list = len(size_text.split()) > 3  # Lists usually have many parts
                
                if not is_prompt and not is_size_list:
                    logger.info(f"Found size in dropdown: {size_text}")
                    return size_text
    
    # Strategy 2: Check product details table
    details_table = soup.select_one("#productDetails_techSpec_section_1, #productDetails_detailBullets_sections1")
    if details_table:
        for row in details_table.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                header = _norm(th.get_text()).lower()
                if any(keyword in header for keyword in ["size", "capacity", "volume", "weight", "dimension"]):
                    size_value = _norm(td.get_text())
                    if size_value:
                        logger.info(f"Found size in product details: {size_value}")
                        return size_value
    
    # Strategy 3: Check product description list
    for li in soup.select("#feature-bullets li, .a-unordered-list.a-vertical li"):
        text = _norm(li.get_text())
        if any(keyword in text.lower() for keyword in ["size:", "capacity:", "volume:", "weight:"]):
            match = re.search(r"(?:size|capacity|volume|weight)\s*:?\s*([^,\n]+)", text, re.I)
            if match:
                size_value = _norm(match.group(1))
                logger.info(f"Found size in feature bullets: {size_value}")
                return size_value
    
    # Strategy 4: Extract from title using patterns
    size_patterns = [
        r'\b(\d+\s*(?:ml|l|litre|liter|oz|fl\.?\s*oz))\b',  # Volume
        r'\b(\d+\s*(?:g|gm|gram|kg|kilogram|lb|pound))\b',  # Weight
        r'\b((?:X{0,3})?(?:Small|Medium|Large|S|M|L|XL|XXL|XXXL))\b',  # Clothing sizes
        r'\b(\d+(?:\.\d+)?\s*(?:inch|in|cm|mm|ft|meter|metre))\b',  # Dimensions
        r'\b(\d+\s*(?:Pack|Count|Pieces?))\b',  # Pack size
    ]
    
    for pattern in size_patterns:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            size_value = _norm(match.group(1))
            logger.info(f"Found size in title: {size_value}")
            return size_value
    
    logger.warning("No size information found")
    return None

def _extract_availability(soup: BeautifulSoup) -> str:
    """
    Extract product availability with intelligent parsing.
    Returns normalized availability status.
    FIXED VERSION: Prioritizes out-of-stock detection and uses targeted scanning.
    """
    availability_text = None
    
    # Strategy 1: Try to get the main availability div content (highest priority)
    avail_div = soup.select_one("#availability")
    if avail_div:
        availability_text = _norm(avail_div.get_text())
        logger.info(f"Found availability div: {availability_text}")
    
    # Strategy 2: Try specific span classes (ordered by specificity)
    if not availability_text:
        for sel in [
            "#availability span.a-color-state",        # Out of stock - red (CHECK FIRST)
            "#availability .a-color-state",
            "#availability span.a-color-success",      # In stock - green
            "#availability span.a-color-price",        # In stock variant
            "#availability .a-color-success",
            "#availability span",                       # Generic span in availability
            ".a-color-state",                           # Broader state search
            ".a-color-success",                         # Broader success search
        ]:
            el = soup.select_one(sel)
            if el and el.get_text(strip=True):
                availability_text = _norm(el.get_text())
                logger.info(f"Found availability in selector {sel}: {availability_text}")
                break
    
    # Strategy 3: Check buy box for stock info
    if not availability_text:
        buybox = soup.select_one("#buybox, #desktop_buybox")
        if buybox:
            buybox_text = _norm(buybox.get_text())
            if buybox_text:
                # Look for availability indicators in buy box
                if re.search(r"(out of stock|unavailable|sold out)", buybox_text, re.I):
                    availability_text = "Out of Stock"
                    logger.info("Found out of stock in buy box")
                elif re.search(r"(in stock|available|only \d+ left)", buybox_text, re.I):
                    availability_text = "Available"
                    logger.info("Found availability in buy box")
    
    # Parse the availability text intelligently
    if availability_text:
        avail_lower = availability_text.lower()
        
        # Check for OUT OF STOCK signals FIRST (higher priority)
        if any(x in avail_lower for x in ["out of stock", "sold out", "currently unavailable", "temporarily unavailable", "not available"]):
            return "Out of Stock"
        
        # Check for IN STOCK signals
        elif any(x in avail_lower for x in ["in stock", "available now", "available", "only", "left in stock", "hurry"]):
            return "In Stock"
        
        # Check for pre-order
        elif "pre-order" in avail_lower or "preorder" in avail_lower:
            return "Pre-Order"
        
        # Return the actual text if it's meaningful
        elif len(availability_text) < 100:
            return availability_text
    
    # Strategy 4: Check for "Add to Cart" button presence - strong signal of availability
    add_to_cart = soup.select_one("#add-to-cart-button, input#add-to-cart-button, button#add-to-cart-button")
    if add_to_cart and not add_to_cart.get("disabled"):
        logger.info("Found enabled 'Add to Cart' button - product likely in stock")
        return "In Stock"
    
    # Strategy 5: Check for "Currently unavailable" message (common pattern)
    unavailable_msg = soup.select_one("#availability .a-color-state, #availability .a-color-error")
    if unavailable_msg:
        msg_text = _norm(unavailable_msg.get_text()).lower()
        if any(x in msg_text for x in ["unavailable", "out of stock", "sold out"]):
            logger.info(f"Found unavailable message: {msg_text}")
            return "Out of Stock"
    
    # Strategy 6: Targeted page section analysis (last resort)
    # Only check specific sections, not entire page
    product_section = soup.select_one("#centerCol, #dp-container, #ppd")
    if product_section:
        section_text = product_section.get_text(" ", strip=True).lower()
        
        # Check for OUT OF STOCK first (more specific patterns)
        if re.search(r"(currently unavailable|out of stock|sold out|item is not available|this item cannot be shipped)", section_text):
            logger.info("Found out of stock in product section")
            return "Out of Stock"
        
        # Then check for IN STOCK (with stricter patterns)
        if re.search(r"(only \d+ left in stock|get it by|delivery by|add to cart)", section_text):
            logger.info("Found in stock indicators in product section")
            return "In Stock"
    
    logger.warning("Could not determine availability status")
    return "Unknown"

def _infer_currency_from_url(url: str) -> Optional[str]:
    """Infer currency from Amazon domain."""
    if not url:
        return None
    u = url.lower()
    if "amazon.in" in u: return "INR"
    if "amazon.com" in u: return "USD"
    if "amazon.co.uk" in u: return "GBP"
    if "amazon.de" in u: return "EUR"
    if "amazon.fr" in u: return "EUR"
    if "amazon.it" in u: return "EUR"
    if "amazon.es" in u: return "EUR"
    if "amazon.ca" in u: return "CAD"
    if "amazon.com.mx" in u: return "MXN"
    if "amazon.co.jp" in u: return "JPY"
    if "amazon.com.br" in u: return "BRL"
    return None

# -------- Core scraping --------

from playwright.sync_api import sync_playwright

def _extract_mrp(soup: BeautifulSoup, product: Dict[str, Any]) -> None:
    """Extract MRP from product page."""
    if product["mrp"] is None:
        for sel in [
            ".a-price.a-text-price .a-offscreen",
            ".a-price.a-text-price .a-price-whole",
            ".a-price-range .a-text-price .a-offscreen",
            "span.a-price.a-text-price span.a-offscreen",
            ".centralizedApexBasisPriceCSS .a-price.a-text-price .a-offscreen",
            ".apex-basisprice-value .a-offscreen",
        ]:
            el = soup.select_one(sel)
            txt = _norm(el.get_text()) if el else ""
            if txt:
                p, _ = _parse_price(txt)
                if p is not None and (product["price"] is None or p >= (product["price"] or 0)):
                    product["mrp"] = p
                    logger.info(f"Found MRP: {p}")
                    break
        
        # Check meta tags for MRP
        if product["mrp"] is None:
            for meta in soup.find_all("meta"):
                name = (meta.get("name", "") or "").lower()
                prop = (meta.get("property", "") or "").lower()
                content = meta.get("content", "")
                if content and any(k in name or k in prop for k in ["mrp", "listprice", "originalprice", "list_price"]):
                    p, _ = _parse_price(content)
                    if p is not None:
                        product["mrp"] = p
                        logger.info(f"Found MRP in meta: {p}")
                        break

def fetch_with_playwright(url: str) -> Optional[str]:
    """
    Fetch page content using Playwright (fallback for bot detection).
    Returns HTML string or None.
    """
    logger.info("⚠️ Falling back to Playwright for Amazon scraping...")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={'width': 1920, 'height': 1080}
            )
            page = context.new_page()
            
            # Navigate with generous timeout
            page.goto(url, timeout=60000, wait_until="domcontentloaded")
            
            # Try to handle CAPTCHA (simple wait)
            if "captcha" in page.url or "enter the characters" in page.content().lower():
                logger.warning("Playwright hit CAPTCHA - waiting 5s to see if it clears/redirects...")
                time.sleep(5)
            
            # Wait for key element (optional but helps)
            try:
                page.wait_for_selector("#productTitle", timeout=10000)
            except Exception:
                pass # Proceed anyway, maybe we got partial load
                
            html = page.content()
            browser.close()
            return html
    except Exception as e:
        logger.error(f"Playwright fallback failed: {str(e)}")
        return None

def get_amazon_product_details(url: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
    """
    Scrape Amazon product page with retry logic and Playwright fallback.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    html_content = None
    use_playwright = False
    
    # Attempt 1-N: Requests
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                time.sleep(min(2 ** attempt, 6))
            logger.info(f"Attempt {attempt + 1} fetching: {url}")
            resp = requests.get(url, headers=headers, timeout=25)
            
            if resp.status_code == 200 and resp.text:
                # Check for soft-block indicators
                soup_check = BeautifulSoup(resp.text, "html.parser")
                title_check = soup_check.title.string.strip() if soup_check.title else ""
                
                # Check 1: Critical element presence (Product Title)
                has_product_title = soup_check.select_one("#productTitle") or soup_check.select_one("#title")
                
                # Check 2: Explicit Block Indicators
                is_captcha = "captcha" in title_check.lower() or "robot check" in title_check.lower()
                
                # Logic: It's a soft block ONLY if we are missing the product title AND it looks suspicious
                # OR if it's definitely a captcha page
                is_soft_block = False
                
                if is_captcha:
                    is_soft_block = True
                elif not has_product_title:
                    # If no product title found, it's likely a block or broken page
                    # But double check it's not just a weird page
                    is_soft_block = True
                    logger.warning("Soft block suspected: No #productTitle element found")

                if is_soft_block:
                    logger.warning(f"Soft block detected (Title: '{title_check}')")
                    if attempt == max_retries - 1:
                        use_playwright = True
                    continue
                    
                html_content = resp.text
                break
                
            if resp.status_code != 200:
                logger.warning(f"HTTP {resp.status_code} on attempt {attempt + 1}")
                if attempt == max_retries - 1:
                    use_playwright = True
                continue
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
            if attempt == max_retries - 1:
                use_playwright = True

    # Fallback: Playwright
    if not html_content and use_playwright:
        html_content = fetch_with_playwright(url)

    if not html_content:
        logger.error("Failed to fetch page content via Requests or Playwright")
        return None

    soup = BeautifulSoup(html_content, "html.parser")
    product: Dict[str, Any] = {
        "url": url,
        "asin": extract_asin_from_url(url),
        "title": "",
        "description": "",
        "content": "",
        "mrp": None,
        "price": None,
        "currency": None,
        "brand": None,
        "category": None,
        "size": None,
        "instock": None,
        "specs": {},
        "images": [],
        "rating": None,
        "reviews": None,
        "scraped_at": datetime.now().isoformat(),
    }

    # JSON-LD enrichment
    jd = _extract_json_ld(soup)
    try:
        if jd:
            name = jd.get("name")
            if name and not product["title"]:
                product["title"] = _norm(name)
            offers = jd.get("offers") or {}
            if isinstance(offers, dict):
                p = offers.get("price")
                cur = offers.get("priceCurrency")
                if p:
                    try:
                        product["price"] = float(p)
                        product["currency"] = cur or product["currency"]
                    except Exception:
                        pass
                mrp = offers.get("highPrice") or offers.get("listPrice") or offers.get("mrp")
                if mrp:
                    try:
                        product["mrp"] = float(mrp)
                    except Exception:
                        pass
                # Check availability from JSON-LD
                availability = offers.get("availability", "")
                if availability and not product["instock"]:
                    if "instock" in availability.lower():
                        product["instock"] = "In Stock"
                    elif "outofstock" in availability.lower():
                        product["instock"] = "Out of Stock"
            brand = jd.get("brand")
            if isinstance(brand, dict):
                product["brand"] = brand.get("name")
            elif isinstance(brand, str):
                product["brand"] = brand
    except Exception as e:
        logger.warning(f"Error parsing JSON-LD: {e}")

    # Title fallback selectors
    if not product["title"]:
        for sel in [
            "span#productTitle",
            "h1#title span",
            "h1.a-size-large span",
            "[data-automation-id='title']",
            "h1.a-size-large.a-spacing-none",
            "h1 span.a-size-large",
        ]:
            el = soup.select_one(sel)
            if el and el.get_text(strip=True):
                product["title"] = _norm(el.get_text())
                break

    # Category (breadcrumbs + text heuristics)
    if not product["category"]:
        crumbs = [_norm(a.get_text()) for a in soup.select("#wayfinding-breadcrumbs_feature_div a") if a.get_text(strip=True)]
        if crumbs:
            product["category"] = crumbs[-1]
        if not product["category"]:
            page_text = soup.get_text(" ", strip=True).lower()
            if any(x in page_text for x in ["laptop", "notebook", "computer", "macbook", "mac"]):
                product["category"] = "Laptop"
            elif any(x in page_text for x in ["mobile", "phone", "smartphone", "iphone"]):
                product["category"] = "Mobile"
            elif any(x in page_text for x in ["headphone", "headset", "earphone", "earbud"]):
                product["category"] = "Audio"
            elif any(x in page_text for x in ["tv", "television", "monitor"]):
                product["category"] = "Electronics"
            elif any(x in page_text for x in ["shirt", "pant", "dress", "kurta", "saree"]):
                product["category"] = "Fashion"

    # Price (robust selectors + meta/text)
    if product["price"] is None:
        for sel in [
            "#priceblock_dealprice",
            "#priceblock_ourprice",
            "#priceblock_saleprice",
            "#corePrice_feature_div .a-price .a-offscreen",
            ".a-price .a-offscreen",
            ".a-price-whole",
            ".a-price-range .a-offscreen",
            "span.a-price-whole",
            ".a-section .a-price .a-offscreen",
        ]:
            el = soup.select_one(sel)
            txt = _norm(el.get_text()) if el else ""
            if txt:
                p, cur = _parse_price(txt)
                if p is not None:
                    product["price"] = p
                    product["currency"] = product["currency"] or cur
                    logger.info(f"Found price: {p} {cur}")
                    break
        
        # Check meta tags
        if product["price"] is None:
            for meta in soup.find_all("meta"):
                name = (meta.get("name", "") or "").lower()
                prop = (meta.get("property", "") or "").lower()
                content = meta.get("content", "")
                if content and any(k in name or k in prop for k in ["price", "amount"]):
                    p, cur = _parse_price(content)
                    if p is not None:
                        product["price"] = p
                        product["currency"] = product["currency"] or cur
                        logger.info(f"Found price in meta: {p} {cur}")
                        break
        
        # Page text fallback
        if product["price"] is None:
            page_text = soup.get_text(" ", strip=True)
            for pattern in [r"₹\s*([0-9,]+(?:\.[0-9]+)?)", r"Rs\.?\s*([0-9,]+(?:\.[0-9]+)?)", r"\$\s*([0-9,]+(?:\.[0-9]+)?)"]:
                m = re.search(pattern, page_text)
                if m:
                    try:
                        price = float(m.group(1).replace(",", ""))
                        if price > 1:
                            product["price"] = price
                            if "₹" in pattern or "Rs" in pattern:
                                product["currency"] = product["currency"] or "INR"
                            elif r"\$" in pattern or "$" in pattern:
                                product["currency"] = product["currency"] or "USD"
                            logger.info(f"Found price in page text: {price}")
                            break
                    except Exception:
                        pass

    # MRP
    _extract_mrp(soup, product)

    # Availability - FIXED VERSION
    product["instock"] = _extract_availability(soup)

    # Size extraction
    if not product["size"]:
        product["size"] = _extract_size(soup, product["title"])

    # Rating numeric
    if product["rating"] is None:
        rnode = soup.select_one("#acrPopover")
        info = (rnode.get("title") if rnode else None) or (rnode.get("aria-label") if rnode else None) or ""
        m = re.search(r"([0-9.]+)\s+out of\s+5", info)
        if m:
            try:
                product["rating"] = float(m.group(1))
                logger.info(f"Found rating: {product['rating']}")
            except Exception:
                pass

    # Reviews count
    if product["reviews"] is None:
        count_text = ""
        for sel in ["#acrCustomerReviewText", "[data-hook='total-review-count']"]:
            el = soup.select_one(sel)
            if el and el.get_text(strip=True):
                count_text = _norm(el.get_text())
                break
        product["reviews"] = _parse_int(count_text) if count_text else None
        if product["reviews"]:
            logger.info(f"Found reviews count: {product['reviews']}")

    # Description/content
    if not product["description"]:
        product["description"] = _extract_description(soup)
    if not product["content"]:
        product["content"] = product["description"] or ""

    # Brand (byline with regex patterns)
    if not product["brand"]:
        byline = soup.select_one("#bylineInfo, a#bylineInfo")
        if byline and byline.get_text(strip=True):
            txt = _norm(byline.get_text())
            m = re.search(r"Brand:\s*(.+)", txt, re.I)
            if m:
                product["brand"] = _norm(m.group(1))
            else:
                m = re.search(r"Visit the\s+(.+?)\s+Store", txt, re.I)
                if m:
                    product["brand"] = _norm(m.group(1))
            if product["brand"]:
                logger.info(f"Found brand: {product['brand']}")

    # Specs and images
    product["specs"] = _extract_specs(soup)
    product["images"] = _extract_images(soup)
    
    # 🎨 Color Fallback: Extract from widgets/title if missing in specs
    if "Color" not in product["specs"] and "Colour" not in product["specs"]:
        extracted_color = _extract_color(soup, product["title"] or "")
        if extracted_color:
            product["specs"]["Color"] = extracted_color
            logger.info(f"Fallback extracted Color: {extracted_color}")

    # 🏷️ Brand Fallback: If not found, try to grep from title start
    if not product["brand"] and product["title"]:
        first_word = product["title"].split()[0]
        if len(first_word) > 2:
            product["brand"] = first_word
            product["specs"]["Brand"] = first_word

    # 🧩 Specific Voyager mapping (Style Number -> specs)
    if "Style Number" not in product["specs"] and "Item model number" in product["specs"]:
        product["specs"]["Style Number"] = product["specs"]["Item model number"]

    logger.info(f"Extracted {len(product['specs'])} specs and {len(product['images'])} images")

    # Title fallback to <title> if still empty
    if not product["title"]:
        if soup.title and soup.title.string:
            product["title"] = _norm(soup.title.string)
        else:
            product["title"] = "Untitled Product"

    # Currency inference
    if not product["currency"]:
        inferred = _infer_currency_from_url(url)
        if inferred:
            product["currency"] = inferred

    # Content sanity
    if not product["content"] or len(product["content"]) < 20:
        if product["description"] and len(product["description"]) > 20:
            product["content"] = product["description"]
        elif product["specs"]:
            parts = []
            for k, v in product["specs"].items():
                if v and 5 < len(v) < 100:
                    parts.append(f"{k}: {v}")
            product["content"] = "; ".join(parts[:5]) if parts else (product["title"] or "Product info not available")
        else:
            product["content"] = product["title"] or "Product info not available"

    # Validation: Ensure price is reasonable
    if product["price"] is not None and product["mrp"] is not None:
        if product["price"] > product["mrp"]:
            # Swap them if price > MRP (likely extracted wrong)
            product["price"], product["mrp"] = product["mrp"], product["price"]
            logger.warning("Swapped price and MRP as price was greater than MRP")

    return product

# -------- Validation & Response --------

def validate_asin(asin: str) -> Tuple[bool, str]:
    """
    Validate ASIN format.
    
    Args:
        asin: Amazon Standard Identification Number
        
    Returns:
        Tuple of (is_valid, message)
    """
    if not asin:
        return False, "ASIN is empty"
    
    asin = asin.strip().upper()
    
    # Standard ASIN is 10 alphanumeric characters
    if not re.match(r'^[A-Z0-9]{10}$', asin):
        return False, f"ASIN '{asin}' must be 10 alphanumeric characters"
    
    return True, "Valid ASIN"

def validate_amazon_url(url: str) -> Tuple[bool, str]:
    """Validate if URL is a valid Amazon product URL."""
    if not url:
        return False, "URL is empty"
    domains = [
        "amazon.com","amazon.co.uk","amazon.de","amazon.fr","amazon.it",
        "amazon.es","amazon.ca","amazon.com.au","amazon.in","amazon.com.br",
        "amazon.co.jp","amazon.com.mx"
    ]
    if not any(d in url.lower() for d in domains):
        return False, "URL is not from Amazon"
    if "/dp/" not in url and "/gp/product/" not in url and "/product/" not in url:
        return False, "URL does not appear to be a product page"
    return True, "Valid URL"

def create_error_response(status_code: int, message: str) -> Dict[str, Any]:
    """Create standardized error response."""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
        },
        "body": json.dumps(
            {"success": False, "error": message, "timestamp": datetime.now().isoformat()},
            indent=2,
        ),
    }

# -------- Lambda Handler --------

def lambda_handler(event, context):
    """
    Lambda handler for Amazon product scraping.
    
    Accepts ASIN or full URL:
    
    Input formats:
    1. {"asin": "B08N5WRWNW", "domain": "in"}  # domain optional, defaults to .in
    2. {"url": "https://www.amazon.in/dp/B08N5WRWNW"}  # Full URL still supported
    3. Query params: ?asin=B08N5WRWNW&domain=in
    
    Domain can be:
    - Short code: "in", "us", "uk", "de", "fr", "it", "es", "ca", "au", "br", "jp", "mx"
    - Full domain: "amazon.in", "amazon.com", etc.
    - If omitted, defaults to "amazon.in"
    """
    try:
        logger.info(f"Received event: {json.dumps(event) if isinstance(event, dict) else str(event)}")
        
        asin = ""
        url = ""
        domain = None

        # API Gateway (HTTP API / REST) - POST body
        if isinstance(event, dict) and event.get("body"):
            try:
                body = json.loads(event["body"]) if isinstance(event["body"], str) else event["body"]
                if isinstance(body, dict):
                    asin = body.get("asin", "")
                    url = body.get("url", "")
                    domain = body.get("domain")
            except json.JSONDecodeError:
                return create_error_response(400, "Invalid JSON in request body")

        # API Gateway GET / query params
        if not asin and not url and isinstance(event, dict) and event.get("queryStringParameters"):
            params = event["queryStringParameters"]
            asin = params.get("asin", "")
            url = params.get("url", "")
            domain = params.get("domain")

        # Direct invocation
        if not asin and not url and isinstance(event, dict):
            asin = event.get("asin", "")
            url = event.get("url", "")
            domain = event.get("domain")

        # OPTIONS preflight
        if event.get("httpMethod", "").upper() == "OPTIONS":
            return {
                "statusCode": 200,
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                    "Access-Control-Allow-Headers": "Content-Type, Authorization",
                },
                "body": "",
            }

        # Process ASIN if provided (priority over URL)
        if asin:
            is_valid, msg = validate_asin(asin)
            if not is_valid:
                return create_error_response(400, msg)
            
            url = construct_amazon_url(asin, domain)
            logger.info(f"Constructed URL from ASIN: {url}")
        
        # If no ASIN, check for URL
        elif url:
            is_valid, msg = validate_amazon_url(url)
            if not is_valid:
                return create_error_response(400, msg)
        else:
            return create_error_response(400, "Either 'asin' or 'url' parameter is required")

        product = get_amazon_product_details(url)
        if not product or not product.get("title"):
            return create_error_response(500, "Could not extract product details (blocked or structure changed).")

        # Calculate extraction quality metrics
        fields_extracted = len([v for v in product.values() if v not in [None, "", [], {}]])
        total_fields = len(product)
        extraction_rate = round((fields_extracted / total_fields) * 100, 2)

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type, Authorization",
            },
            "body": json.dumps(
                {
                    "success": True, 
                    "data": product, 
                    "metadata": {
                        "input_asin": asin if asin else extract_asin_from_url(url),
                        "input_domain": domain or DEFAULT_AMAZON_DOMAIN,
                        "constructed_url": url,
                        "fields_extracted": fields_extracted,
                        "total_fields": total_fields,
                        "extraction_rate": f"{extraction_rate}%",
                        "size_extracted": product.get("size") is not None,
                        "price_extracted": product.get("price") is not None,
                        "availability_extracted": product.get("instock") not in [None, "Unknown"],
                    }
                },
                indent=2,
                ensure_ascii=False
            ),
        }

    except requests.exceptions.RequestException as e:
        logger.exception("Network error occurred")
        return create_error_response(500, f"Network error: {str(e)}")
    except json.JSONDecodeError as e:
        logger.exception("JSON decode error")
        return create_error_response(400, "Invalid JSON in request")
    except Exception as e:
        logger.exception("Unexpected error occurred")
        return create_error_response(500, f"An unexpected error occurred: {str(e)}")


# -------- Testing / Local Execution --------

if __name__ == "__main__":
    """
    For local testing. Run with:
    python az_scraper.py <ASIN_OR_URL>
    """
    
    # Check for CLI arguments
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        print("="*80)
        print(f"TESTING WITH INPUT: {arg}")
        print("="*80)
        
        # Determine if it's a URL or ASIN
        if "amazon" in arg.lower() and (arg.startswith("http") or "www" in arg):
            url = arg
            asin = extract_asin_from_url(url)
            domain = "in" # Default or infer?
            print(f"Identified as URL. Extracted ASIN: {asin}")
        else:
            asin = arg
            domain = "in"
            url = construct_amazon_url(asin, domain)
            print(f"Identified as ASIN. Constructed URL: {url}")
            
        print(f"Constructed URL: {url}")
        print('='*80)
        
        result = get_amazon_product_details(url)
        if result:
            print(f"\n✓ Title: {result['title']}")
            print(f"✓ ASIN: {result['asin']}")
            print(f"✓ Price: {result['currency']} {result['price']}")
            print(f"✓ MRP: {result['currency']} {result['mrp']}")
            print(f"✓ Availability: {result['instock']}")
            print(f"✓ Brand: {result['brand']}")
            print(f"✓ Category: {result['category']}")
            print(f"✓ Size: {result['size']}")
            print(f"✓ Rating: {result['rating']}")
            print(f"✓ Reviews: {result['reviews']}")
            print(f"✓ Images: {len(result['images'])} found")
            print(f"✓ Specs: {len(result['specs'])} found")
            print(f"✓ Content: {result.get('content', 'N/A')[:500]}...")
        else:
            print("\n✗ Failed to extract product details")
        
        print(f"\n{'='*80}\n")
        
    else:
        # Default test if no args provided
        test_asins = [
            # {"asin": "B08N5WRWNW", "domain": "in"},  # Amazon India
            {"asin": "B0DRKKVB61", "domain": "in"},
        ]
        
        print("="*80)
        print("TESTING WITH HARDCODED ASINS (No CLI args provided)")
        print("Usage: python az_scraper.py <ASIN_OR_URL>")
        print("="*80)
        
        for test in test_asins:
            asin = test["asin"]
            domain = test["domain"]
            url = construct_amazon_url(asin, domain)
            
            print(f"\n{'='*80}")
            print(f"Testing ASIN: {asin} (Domain: {domain})")
            print(f"Constructed URL: {url}")
            print('='*80)
            
            result = get_amazon_product_details(url)
            if result:
                print(f"\n✓ Title: {result['title']}")
                print(f"✓ ASIN: {result['asin']}")
                print(f"✓ Price: {result['currency']} {result['price']}")
                print(f"✓ MRP: {result['currency']} {result['mrp']}")
                print(f"✓ Availability: {result['instock']}")
                print(f"✓ Brand: {result['brand']}")
                print(f"✓ Category: {result['category']}")
                print(f"✓ Size: {result['size']}")
                print(f"✓ Rating: {result['rating']}")
                print(f"✓ Reviews: {result['reviews']}")
                print(f"✓ Images: {len(result['images'])} found")
                print(f"✓ Specs: {len(result['specs'])} found")
                print(f"✓ Content: {result.get('content', 'N/A')[:500]}...")
            else:
                print("\n✗ Failed to extract product details")
            
            print(f"\n{'='*80}\n")