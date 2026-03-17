import json
import re
import os
import base64
import logging
import random
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import threading
import gc
from urllib.parse import urlparse
import hashlib

import requests
from bs4 import BeautifulSoup

# =============================================================================
# CANONICAL SIZE LAYER (Phase 1) - Import shared size functions
# =============================================================================
from size_mappings import (
    normalize_size,
    get_size_equivalents,
    is_valid_size as canonical_is_valid_size,
    get_size_info,
    size_similarity
)

# =============================================================================
# LOGGING SETUP (REQUIRED)
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
    )
logger = logging.getLogger(__name__)

# Playwright imports
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout, Error as PlaywrightError
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    PlaywrightTimeout = Exception
    PlaywrightError = Exception

# Configuration
MAX_WORKERS = 2
FETCH_WORKERS = 1
DEBUG_MODE = False
MAX_URL_LENGTH = 2048
MAX_URLS_PER_REQUEST = 10
REQUEST_TIMEOUT = 30
PLAYWRIGHT_TIMEOUT = 60000  # Increased for size selection
MAX_HTML_SIZE = 15 * 1024 * 1024

# Data limits
MAX_TITLE_LENGTH = 1000
MAX_DESCRIPTION_LENGTH = 5000
MAX_SPEC_KEY_LENGTH = 200
MAX_SPEC_VALUE_LENGTH = 1000
MAX_BRAND_LENGTH = 200
MAX_IMAGES = 10
MAX_SPECS = 50
MAX_SIZES = 50

# =============================================================================
# SECURITY & SANITIZATION (from code2)
# =============================================================================

def generate_url_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]

def get_safe_domain(url: str) -> str:
    try:
        parsed = urlparse(url)
        return parsed.netloc
    except:
        return "unknown"

def validate_url(url: str) -> Tuple[bool, str]:
    if not url or not isinstance(url, str):
        return False, "Invalid URL format"
    
    if len(url) > MAX_URL_LENGTH:
        return False, f"URL too long"
    
    try:
        parsed = urlparse(url)
        
        if parsed.scheme not in ['https', 'http']:
            return False, "Only HTTP/HTTPS allowed"
        
        if 'flipkart.com' not in parsed.netloc.lower():
            return False, "Only Flipkart URLs allowed"
        
        return True, "OK"
        
    except Exception:
        return False, "URL validation error"

def safe_truncate(text: str, max_length: int) -> str:
    if not text or not isinstance(text, str):
        return ""
    return text[:max_length] if len(text) > max_length else text


# NOTE: normalize_size() is now imported from size_mappings.py (canonical layer)
# The old implementation has been moved to size_mappings.py for single source of truth

# NOTE: is_valid_size() is now imported from size_mappings.py as canonical_is_valid_size
# We create an alias here for backward compatibility
is_valid_size = canonical_is_valid_size

# =============================================================================
# UTILITY FUNCTIONS (from code2)
# =============================================================================

def human_mouse_move(page):
    """
    Simulates human-like mouse movement with random curves and speed variations.
    """
    try:
        # Get start position (current mouse position)
        # Note: Playwright doesn't expose current mouse pos directly easily, so we start from random or center
        width = page.viewport_size['width']
        height = page.viewport_size['height']
        
        start_x = random.randint(0, width)
        start_y = random.randint(0, height)
        
        # Target position (random point on screen)
        end_x = random.randint(0, width)
        end_y = random.randint(0, height)
        
        # Bezier curve control points
        control_1_x = start_x + random.randint(-200, 200)
        control_1_y = start_y + random.randint(-200, 200)
        control_2_x = end_x + random.randint(-200, 200)
        control_2_y = end_y + random.randint(-200, 200)
        
        steps = random.randint(20, 50)
        for i in range(steps):
            t = i / steps
            # Cubic Bezier formula
            x = (1-t)**3 * start_x + 3*(1-t)**2 * t * control_1_x + 3*(1-t) * t**2 * control_2_x + t**3 * end_x
            y = (1-t)**3 * start_y + 3*(1-t)**2 * t * control_1_y + 3*(1-t) * t**2 * control_2_y + t**3 * end_y
            
            page.mouse.move(x, y)
            # Variable speed
            time.sleep(random.uniform(0.01, 0.05))
            
    except Exception:
        pass

def _norm(text: str) -> str:
    if not text or not isinstance(text, str):
        return ""
    return ' '.join(text.split()).strip()

def _get_text(soup: BeautifulSoup, selector: str, max_length: int = 5000) -> str:
    try:
        el = soup.select_one(selector)
        if el and el.get_text(strip=True):
            text = el.get_text()
            return safe_truncate(_norm(text), max_length)
        return ""
    except Exception:
        return ""

def _parse_price(text: str) -> Tuple[Optional[float], Optional[str]]:
    if not text or len(text) > 100:
        return None, None
    try:
        t = text.replace(",", "").strip()
        m = re.search(r"₹\s*(\d+(?:\.\d{1,2})?)", t)
        if m:
            price = float(m.group(1))
            if 0 < price < 10000000:
                return price, "INR"
        m = re.search(r"(\d+(?:\.\d{1,2})?)", t)
        if m:
            price = float(m.group(1))
            if 0 < price < 10000000:
                return price, None
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

def _is_product_image(url: str) -> bool:
    if not url or not isinstance(url, str) or len(url) > 1000:
        return False
    try:
        url_lower = url.lower()
        reject = ["promos/", "banner", "logo", "icon", "gif", "{@width}", "{@height}", "www/"]
        if any(k in url_lower for k in reject):
            return False
        has_ext = any(ext in url_lower for ext in ['.jpg', '.jpeg', '.png', '.webp'])
        is_cdn = 'rukminim' in url_lower or 'flipkart.com' in url_lower
        return has_ext or is_cdn
    except:
        return False

def is_size_disabled(page, size: str) -> bool:
    """
    Returns True if Flipkart UI marks the size as disabled / unavailable.
    
    Supports BOTH UI versions:
    - Classical UI: Uses class 'N2SywC' for OOS, 'WLkY3m' for available
    - New React Native Web UI: Uses computed border-style:dashed for OOS
    """
    try:
        return page.evaluate("""
            (sz) => {
                const szUpper = sz.toUpperCase();
                
                // ── Strategy 1: New UI — a[href*="swatchAttr"] ──
                const newLinks = document.querySelectorAll('a[href*="swatchAttr"]');
                for (let link of newLinks) {
                    let rawText = link.innerText.replace(/[\\n\\r]+/g, ' ').trim();
                    // Strip stock count suffix ("L4" → "L", "S10" → "S")
                    rawText = rawText.replace(/\s*\d+\s*left$/i, '');
                    if (/^[a-z]+\d+$/i.test(rawText)) rawText = rawText.replace(/\d+$/, '');
                    
                    if (rawText.toUpperCase() === szUpper) {
                        // Check computed style for OOS indicators
                        const buttonDiv = [...link.querySelectorAll('div')].find(d => {
                            const s = window.getComputedStyle(d);
                            return parseFloat(s.borderWidth) > 0 && parseFloat(s.borderRadius) > 0;
                        });
                        if (buttonDiv) {
                            const style = window.getComputedStyle(buttonDiv);
                            // OOS: dashed border or non-interactive
                            if (style.borderStyle === 'dashed' || 
                                style.pointerEvents === 'none') {
                                return true;
                            }
                        }
                        return false;  // Size found and not disabled
                    }
                }
                
                // ── Strategy 2: Classical UI — a[href*="sattr"] ──
                const oldLinks = document.querySelectorAll('a[href*="sattr"][href*="size"]');
                for (let l of oldLinks) {
                    const linkText = l.textContent.trim().toUpperCase();
                    if (linkText === szUpper) {
                        if (l.classList.contains('N2SywC')) return true;
                        if (l.classList.contains('disabled') || 
                            l.getAttribute('aria-disabled') === 'true' ||
                            l.hasAttribute('disabled')) return true;
                        return false;
                    }
                }
                
                // Size not found in either UI → consider unavailable
                return true;
            }
        """, size)
    except Exception as e:
        logger.warning(f"⚠️ is_size_disabled check failed: {e}")
        return True

def detect_ui_type(html: str) -> str:
    """
    Detect which UI framework Flipkart is using.
    
    Returns:
        "classical" - Traditional Flipkart UI with stable class names
        "react_native_web" - Modern UI with obfuscated/hash-based classes
    
    Detection logic (data-driven from 7 HTML samples):
        - Classical: a[href*="sattr"], classes N2SywC/WLkY3m/HduqIE/CEn5rD
        - New React: a[href*="swatchAttr"], classes v1zwn*/css-175oi2r/_1psv1ze*
    """
    if not html:
        return "unknown"
    
    # ── Most reliable single indicators (from analysis) ──
    # swatchAttr is UNIQUE to new UI and present in ALL new UI samples
    if 'swatchAttr' in html:
        return "react_native_web"
    
    # sattr is UNIQUE to old UI
    if 'sattr' in html and ('N2SywC' in html or 'WLkY3m' in html):
        return "classical"
    
    # ── Scoring-based fallback ──
    classical_indicators = [
        'class="N2SywC"', 'class="HduqIE', 'class="hZ3P6w',
        'class="WLkY3m"', 'class="CEn5rD"', 'href="/sattr',
    ]
    react_native_indicators = [
        'class="css-175oi2r', 'class="v1zwn', 'class="_1psv1ze',
        'class="css-1rynq56', 'class="r-13awgt0', 'class="_1o6mltl',
    ]
    
    classical_score = sum(1 for i in classical_indicators if i in html)
    react_score = sum(1 for i in react_native_indicators if i in html)
    
    if react_score >= 2:
        return "react_native_web"
    elif classical_score >= 2:
        return "classical"
    elif react_score >= 1:
        return "react_native_web"
    elif classical_score >= 1:
        return "classical"
    
    return "unknown"


# =============================================================================
# ENHANCED SIZE SELECTION (from code1 with improvements)
# =============================================================================

def extract_and_select_size_enhanced(page, target_size: str) -> Tuple[List[str], Optional[str], bool]:
    """Enhanced size selection using code1's approach"""
    available_sizes = []
    selected_size = None
    size_changed = False
    
    try:
        # Bug 15 FIX: Removed redundant networkidle/timeout wait here. 
        # The caller (fetch_with_playwright_combined) has already waited for hydration.
        
        # Close any popups
        try:
            popup_selectors = [
                "//span[contains(text(),'✕')]",
                "button[class*='close']",
                "[aria-label='Close']"
            ]
            for selector in popup_selectors:
                try:
                    if page.locator(selector).is_visible(timeout=2000):
                        page.locator(selector).click(timeout=2000)
                        page.wait_for_timeout(500)
                        break
                except:
                    continue
        except:
            pass
        
        # Extract available sizes using multiple strategies
        size_selectors = [
            'li:has-text("{}")',
            'div:has-text("{}")',
            'span:has-text("{}")',
            'button:has-text("{}")',
            'a[href*="swatchAttr"]',
            'a[href*="sattr"][href*="size"]'
        ]
        
        # First, collect all available sizes
        all_size_elements = page.locator(
        "a[href*='swatchAttr'], "
        "div[class*='size'] a[href*='sattr'], "
        "div[class*='Size'] a[href*='sattr']")

        size_count = min(all_size_elements.count(), MAX_SIZES)
        
        for i in range(size_count):
            try:
                element = all_size_elements.nth(i)
                if element.is_visible(timeout=2000):
                    size_text = element.text_content().strip()
                    
                    # CRITICAL FIX: Use strict validation instead of permissive regex
                    # Old: re.match(r'^[A-Z0-9\s\-]+$', ...) allowed garbage like "FABRIC QUALITY"
                    # New: is_valid_size() strictly validates actual sizes only
                    if size_text and is_valid_size(size_text):
                        available_sizes.append(size_text)
                        logger.debug(f"✅ Valid size found: '{size_text}'")
                    elif size_text:
                        logger.debug(f"❌ Rejected garbage: '{size_text}'")

            except:
                continue
        
        # If no sizes found via links, try other methods
        if not available_sizes:
            page_content = page.content()
            size_pattern = r'<li[^>]*>.*?<a[^>]*>([^<]+)</a>.*?</li>'
            size_matches = re.findall(size_pattern, page_content)
            
            for match in size_matches:
                clean_size = match.strip()
                # Apply same strict validation to fallback extraction
                if clean_size and is_valid_size(clean_size) and clean_size not in available_sizes:
                    available_sizes.append(clean_size)
                    logger.debug(f"✅ Valid size found (fallback): '{clean_size}'")
        
        # Now try to select the target size using code1's approach
        if target_size:
            size_clicked = False
            
            # Strategy 1: Direct text matching
            for selector_template in size_selectors:
                try:
                    selector = selector_template.format(target_size)
                    elements = page.locator(selector)
                    for i in range(elements.count()):
                        element = elements.nth(i)
                        if element.is_visible(timeout=2000):
                            element.click(timeout=3000)
                            size_clicked = True
                            page.wait_for_load_state('networkidle', timeout=PLAYWRIGHT_TIMEOUT)
                            page.wait_for_timeout(4000)
                            selected_size = target_size
                            size_changed = True
                            break
                    if size_clicked:
                        break
                except:
                    continue
            
            # Strategy 2: Find by normalized size match in available sizes
            if not size_clicked:
                for i, size in enumerate(available_sizes):
                    # Use enhanced size similarity (handles equivalents and CM normalization)
                    similarity_score = size_similarity(size, target_size)
                    
                    if similarity_score == 1.0:
                        try:
                            # Log the match details for debugging
                            match_info = f"'{size}' ≈ '{target_size}'"
                            logger.info(f"✅ Size matched via similarity: {match_info}")
                            
                            size_element = all_size_elements.nth(i)
                            if size_element.is_visible(timeout=2000):
                                size_element.click(timeout=3000, force=True)
                                size_clicked = True
                                page.wait_for_load_state('networkidle', timeout=PLAYWRIGHT_TIMEOUT)
                                page.wait_for_timeout(4000)
                                selected_size = size  # Use the actual size text from page
                                size_changed = True
                                break
                        except:
                            continue
            
            # Strategy 3: JavaScript click with normalized matching
            if not size_clicked:
                try:
                    # Pass both target size and normalized version to JavaScript
                    normalized_target = normalize_size(target_size)
                    
                    js_result = page.evaluate("""
                        (targetSize, normalizedTarget) => {
                            // Normalization function in JavaScript (mirrors Python logic)
                            function normalizeSize(size) {
                                if (!size) return "";
                                let s = size.trim().toUpperCase();
                                
                                // Year pattern: "4 - 5 Year", "4 - 5 Years" -> "4-5Y"
                                // Fixed: YEARS? matches both "YEAR" and "YEARS"
                                let yearMatch = s.match(/(\d+)\s*-\s*(\d+)\s*(?:YEARS?|Y|YR)/);
                                if (yearMatch) {
                                    return yearMatch[1] + "-" + yearMatch[2] + "Y";
                                }
                                
                                // Month pattern: "6-9M", "6-9 Months" -> "6-9M"
                                // Fixed: MONTHS? matches both "MONTH" and "MONTHS"
                                let monthMatch = s.match(/(\d+)\s*-\s*(\d+)\s*(?:MONTHS?|M)/);
                                if (monthMatch) {
                                    return monthMatch[1] + "-" + monthMatch[2] + "M";
                                }
                                
                                return s;
                            }
                            
                            // ── New UI: a[href*="swatchAttr"] ──
                            const newLinks = document.querySelectorAll('a[href*="swatchAttr"]');
                            for (let link of newLinks) {
                                let rawText = link.innerText.replace(/[\\n\\r]+/g, ' ').trim();
                                if (rawText.toLowerCase().includes('ask')) continue;
                                // Strip stock-count suffix
                                rawText = rawText.replace(/\s*\d+\s*left$/i, '').trim();
                                if (/^[a-z]+\d+$/i.test(rawText)) rawText = rawText.replace(/\d+$/, '');
                                
                                let normalizedLink = normalizeSize(rawText);
                                if (rawText.toUpperCase() === targetSize.toUpperCase() || 
                                    normalizedLink === normalizedTarget) {
                                    link.click();
                                    return true;
                                }
                            }
                            
                            // ── Old UI: a[href*="sattr"] ──
                            const oldLinks = document.querySelectorAll('a[href*="sattr"][href*="size"]');
                            for (let link of oldLinks) {
                                let linkText = link.textContent.trim();
                                let normalizedLink = normalizeSize(linkText);
                                if (linkText.toUpperCase() === targetSize.toUpperCase() || 
                                    normalizedLink === normalizedTarget) {
                                    link.click();
                                    return true;
                                }
                            }
                            return false;
                        }
                    """, target_size, normalized_target)
                    
                    if js_result:
                        size_clicked = True
                        page.wait_for_load_state('networkidle', timeout=PLAYWRIGHT_TIMEOUT)
                        page.wait_for_timeout(4000)
                        selected_size = target_size
                        size_changed = True
                        logger.info(f"✅ JavaScript click succeeded for size: {target_size}")
                except Exception as e:
                    logger.warning(f"⚠️ JavaScript click failed: {e}")
                    pass
                    pass
        
        # -------------------------------------------------------------------------
        # IMPLICIT FREE SIZE HANDLING (Handling the "1" case)
        # -------------------------------------------------------------------------
        # If no size buttons were found (available_sizes is empty) AND 
        # the target size maps to "FREESIZE", assume it's a Single-SKU match.
        if not available_sizes and target_size:
            normalized_target = normalize_size(target_size)
            if normalized_target == "FREESIZE":
                # BUG 3 FIX: Validate by checking for "Free Size" text on page
                page_text = page.evaluate("() => document.body.innerText") or ""
                free_size_indicators = ["free size", "freesize", "one size", "f (free size)", "free"]
                has_free_size_text = any(ind in page_text.lower() for ind in free_size_indicators)
                
                if has_free_size_text:
                    logger.info(f"ℹ️ No sizes found + 'Free Size' text confirmed on page. Implicit FREESIZE match.")
                    return ["FREESIZE"], target_size, False
                else:
                    logger.warning(f"⚠️ No sizes found but no 'Free Size' text on page. NOT assuming FREESIZE.")
                    # Fall through to return available_sizes as-is

        return available_sizes, selected_size, size_changed
        
    except Exception as e:
        return available_sizes, None, False

def evaluate_buy_box_stock_status(page, retry_on_ambiguous: bool = True) -> str:
    """
    AUTHORITATIVE Buy Box Evaluation for stock status.
    
    NEW LOGIC (2026-01-28):
    - Buy Box buttons are the ONLY source of truth
    - Priority: Add to Cart > Buy Now > Notify Me
    - Retry once if ambiguous
    - Default to "In Stock" if still ambiguous
    
    Returns:
        "In Stock" | "Out of Stock"
    """
    
    def _evaluate_buttons() -> tuple[str, str]:
        """
        Internal function to evaluate button states.
        Returns: (stock_status, detection_method)
        """
        try:
            # Priority 1: Check "Add to Cart" (Broader Selectors)
            add_to_cart_selectors = [
                "button:has-text('ADD TO CART')", "button:has-text('Add to Cart')",
                "div:has-text('ADD TO CART')", "div:has-text('Add to Cart')",
                "[id='ADD_TO_CART_BUTTON']", "._2KpZ6l._2U9uOA._3v1-ww"  # Common class
            ]
            
            for sel in add_to_cart_selectors:
                try:
                    elements = page.locator(sel).all()
                    for btn in elements:
                        if not btn.is_visible(timeout=200): continue
                        
                        text = btn.inner_text().strip().upper()
                        if "ADD TO CART" not in text: continue

                        # Check disabled states
                        is_disabled = btn.get_attribute("disabled") is not None
                        is_aria_disabled = btn.get_attribute("aria-disabled") == "true"
                        btn_class = btn.get_attribute("class") or ""
                        has_disabled_class = "_2AkmmA" in btn_class or "disabled" in btn_class.lower()
                        
                        if not (is_disabled or is_aria_disabled or has_disabled_class):
                            logger.info("✅ 'Add to Cart' found & enabled → IN STOCK")
                            return "In Stock", "add_to_cart_enabled"
                except:
                    continue
            
            # Priority 2: Check "Buy Now" / "Pre Order" (Broader Selectors)
            buy_now_selectors = [
                "button:has-text('BUY NOW')", "button:has-text('Buy Now')",
                "button:has-text('PRE ORDER')", "button:has-text('Pre Order')",
                "div:has-text('BUY NOW')", "div:has-text('Buy Now')",
                "[id='BUY_NOW_BUTTON']", "._2KpZ6l._2U9uOA.ihZ75k._3AWRsL"
            ]
            
            for sel in buy_now_selectors:
                try:
                    elements = page.locator(sel).all()
                    for btn in elements:
                        if not btn.is_visible(timeout=200): continue
                        
                        text = btn.inner_text().strip().upper()
                        # BUG 19 FIX: Added "PRE ORDER" support
                        if "BUY NOW" not in text and "PRE ORDER" not in text: continue
                        
                        is_disabled = btn.get_attribute("disabled") is not None
                        is_aria_disabled = btn.get_attribute("aria-disabled") == "true"
                        btn_class = btn.get_attribute("class") or ""
                        has_disabled_class = "_2AkmmA" in btn_class or "disabled" in btn_class.lower()
                        
                        if not (is_disabled or is_aria_disabled or has_disabled_class):
                            logger.info(f"✅ '{text}' found & enabled → IN STOCK")
                            return "In Stock", "buy_now_enabled"
                except:
                    continue
            
            # Priority 3: Check "Notify Me" / "Sold Out"
            notify_selectors = [
                "button:has-text('NOTIFY ME')", "div:has-text('NOTIFY ME')",
                "div:has-text('Sold Out')", "div:has-text('Currently Unavailable')"
            ]
            
            for sel in notify_selectors:
                try:
                    elements = page.locator(sel).all()
                    for btn in elements:
                        if btn.is_visible(timeout=200):
                            logger.info("❌ 'Notify Me' / 'Sold Out' found → OUT OF STOCK")
                            return "Out of Stock", "notify_me_present"
                except:
                    continue
            
            # Ambiguous: No clear button state detected
            # NEW: Dump HTML for debugging
            try:
                with open("ambiguous_buttons_debug.html", "w", encoding="utf-8") as f:
                    f.write(page.content())
                logger.warning("⚠️ Ambiguous button state - HTML dumped to ambiguous_buttons_debug.html")
            except: pass
            
            return "Unknown", "ambiguous"
            
        except Exception as e:
            logger.error(f"❌ Button evaluation failed: {e}")
            return "Unknown", "error"
    
    # ========================================================================
    # MAIN EVALUATION
    # ========================================================================
    
    try:
        # Small wait for variant stabilization (if size was just selected)
        page.wait_for_timeout(1500)
        
        # First attempt
        stock_status, method = _evaluate_buttons()
        
        # Retry logic if ambiguous
        if stock_status == "Unknown" and retry_on_ambiguous:
            logger.info("🔄 Retrying button evaluation due to ambiguous state...")
            page.wait_for_timeout(2000)  # Additional wait
            stock_status, method = _evaluate_buttons()
        
        # BUG 5 FIX: Don't blindly default to "In Stock" — keep as "Unknown"
        # so callers can handle ambiguity explicitly
        if stock_status == "Unknown":
            logger.warning("⚠️ Still ambiguous after retry → Keeping as UNKNOWN")
            method = "ambiguous_unresolved"
        
        logger.info(f"🔍 Final Stock Status: {stock_status} (method: {method})")
        return stock_status
        
    except Exception as e:
        logger.error(f"❌ Buy Box evaluation failed: {e}")
        logger.warning("⚠️ Critical failure → Returning UNKNOWN")
        return "Unknown"

# =============================================================================
# DATA EXTRACTION HELPERS (New UI Support)
# =============================================================================

def _extract_initial_state(html: str) -> Dict[str, Any]:
    """Extract and parse window.__INITIAL_STATE__ JSON from HTML."""
    if not html:
        return {}
    try:
        match = re.search(r'window\.__INITIAL_STATE__\s*=\s*({.*?});', html, re.DOTALL)
        if match:
            return json.loads(match.group(1))
    except Exception as e:
        logger.debug(f"Failed to parse __INITIAL_STATE__: {e}")
    return {}

def _extract_json_ld(soup: BeautifulSoup) -> Dict[str, Any]:
    """Extract JSON-LD from page."""
    try:
        scripts = soup.find_all("script", type="application/ld+json")
        for script in scripts:
            data = json.loads(script.string)
            if isinstance(data, dict) and data.get("@type") == "Product":
                return data
    except Exception as e:
        logger.debug(f"Failed to parse JSON-LD: {e}")
    return {}

# =============================================================================
# NETWORK INTERCEPTION: API Response Deep Scanner
# =============================================================================

def _extract_product_data_from_api(json_body: Any) -> Dict[str, Any]:
    """
    Deep-scan an API JSON response for product data fields.
    Handles both flat and deeply nested Flipkart API structures.
    
    Returns dict with keys: price, mrp, title, brand, category, rating,
    reviews, images, stock, seller, offers, discount
    """
    result = {
        "price": None, "mrp": None, "title": None, "brand": None,
        "category": None, "rating": None, "reviews": None,
        "images": [], "stock": None, "seller": None,
        "offers": None, "discount": None,
    }
    
    if not json_body or not isinstance(json_body, (dict, list)):
        return result
    
    # Key mappings: (result_key, api_keys_to_search, value_type)
    PRICE_KEYS = {"finalPrice", "fsp", "sellingPrice", "price", "selling_price", "sp"}
    MRP_KEYS = {"mrp", "maximumRetailPrice", "basePrice", "maximum_retail_price", "listingPrice"}
    TITLE_KEYS = {"title", "name", "productName", "product_name"}
    BRAND_KEYS = {"brand", "brandName", "manufacturer", "brand_name"}
    CATEGORY_KEYS = {"category", "categoryPath", "breadcrumb", "superCategory", "category_name"}
    RATING_KEYS = {"rating", "averageRating", "overallRating", "average_rating", "ratingValue"}
    REVIEW_KEYS = {"reviewCount", "ratingCount", "totalCount", "review_count", "rating_count", "numberOfRatings"}
    IMAGE_KEYS = {"imageUrls", "images", "media", "heroImage", "image_urls", "imageUrl"}
    STOCK_KEYS = {"inStock", "availability", "stockStatus", "serviceable", "in_stock", "isServiceable"}
    SELLER_KEYS = {"sellerName", "seller", "merchantName", "seller_name"}
    DISCOUNT_KEYS = {"discount", "discountPercentage", "discount_percentage", "totalDiscount"}
    
    found_prices = []
    found_mrps = []
    
    def _deep_scan(obj, depth=0):
        """Recursively scan JSON for product data fields."""
        if depth > 30:  # Prevent infinite recursion
            return
        
        if isinstance(obj, dict):
            for key, value in obj.items():
                key_lower = key.lower() if isinstance(key, str) else ""
                
                # --- Price ---
                if key in PRICE_KEYS or key_lower in {k.lower() for k in PRICE_KEYS}:
                    parsed = _try_parse_number(value)
                    if parsed and parsed > 0:
                        found_prices.append(parsed)
                        if not result["price"]:
                            result["price"] = parsed
                
                # --- MRP ---
                elif key in MRP_KEYS or key_lower in {k.lower() for k in MRP_KEYS}:
                    parsed = _try_parse_number(value)
                    if parsed and parsed > 0:
                        found_mrps.append(parsed)
                        if not result["mrp"]:
                            result["mrp"] = parsed
                
                # --- Title ---
                elif key in TITLE_KEYS and isinstance(value, str) and len(value) > 5 and not result["title"]:
                    # Avoid picking up generic titles like "Home" or "Flipkart"
                    if len(value) > 10 and "flipkart" not in value.lower():
                        result["title"] = value.strip()
                
                # --- Brand ---
                elif key in BRAND_KEYS and not result["brand"]:
                    if isinstance(value, str) and value.strip():
                        result["brand"] = value.strip()
                    elif isinstance(value, dict) and value.get("name"):
                        result["brand"] = str(value["name"]).strip()
                
                # --- Category ---
                elif key in CATEGORY_KEYS and isinstance(value, str) and not result["category"]:
                    result["category"] = value.strip()
                
                # --- Rating ---
                elif key in RATING_KEYS and not result["rating"]:
                    parsed = _try_parse_number(value)
                    if parsed and 0 < parsed <= 5:
                        result["rating"] = round(parsed, 1)
                
                # --- Reviews ---
                elif key in REVIEW_KEYS and not result["reviews"]:
                    parsed = _try_parse_number(value)
                    if parsed and parsed > 0:
                        result["reviews"] = int(parsed)
                
                # --- Images ---
                elif key in IMAGE_KEYS:
                    if isinstance(value, list):
                        for img in value:
                            img_url = img if isinstance(img, str) else (img.get("url") or img.get("src") if isinstance(img, dict) else None)
                            if img_url and ("rukminim" in img_url or "img.fkcdn" in img_url):
                                result["images"].append(img_url)
                    elif isinstance(value, str) and ("rukminim" in value or "img.fkcdn" in value):
                        result["images"].append(value)
                
                # --- Stock ---
                elif key in STOCK_KEYS and not result["stock"]:
                    if isinstance(value, bool):
                        result["stock"] = "In Stock" if value else "Out of Stock"
                    elif isinstance(value, str):
                        val_lower = value.lower()
                        if "instock" in val_lower or "in stock" in val_lower or val_lower == "true":
                            result["stock"] = "In Stock"
                        elif "outofstock" in val_lower or "out of stock" in val_lower or val_lower == "false":
                            result["stock"] = "Out of Stock"
                
                # --- Seller ---
                elif key in SELLER_KEYS and isinstance(value, str) and not result["seller"]:
                    result["seller"] = value.strip()
                
                # --- Discount ---
                elif key in DISCOUNT_KEYS and not result["discount"]:
                    parsed = _try_parse_number(value)
                    if parsed and parsed > 0:
                        result["discount"] = parsed
                
                # Recurse into nested structures
                if isinstance(value, (dict, list)):
                    _deep_scan(value, depth + 1)
        
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, (dict, list)):
                    _deep_scan(item, depth + 1)
    
    _deep_scan(json_body)
    
    # Post-processing: if we found multiple prices, use the smallest (selling price)
    if found_prices:
        result["price"] = min(found_prices)
    if found_mrps:
        result["mrp"] = max(found_mrps)  # MRP is typically the highest
    
    # Deduplicate images
    result["images"] = list(dict.fromkeys(result["images"]))[:10]
    
    return result


def _try_parse_number(value) -> Optional[float]:
    """Safely parse a number from various formats."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        # Remove currency symbols and commas
        cleaned = re.sub(r'[₹$,\s]', '', value)
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


# =============================================================================
# MULTI-SOURCE RECONCILIATION ENGINE
# =============================================================================

def _reconcile_all_fields(sources: Dict[str, Dict[str, Any]]) -> Tuple[Dict[str, Any], Dict[str, str], Dict[str, str]]:
    """
    Multi-source reconciliation for all product data fields.
    
    Args:
        sources: Dict of source_name -> {field: value} for each extraction layer.
                 Expected keys: 'api', 'playwright', 'initial_state', 'json_ld', 'soup'
    
    Returns:
        (reconciled_data, field_confidence, field_method)
        - reconciled_data: Best value per field
        - field_confidence: 'high'|'medium'|'low' per field
        - field_method: Description of how the value was chosen
    """
    # Source priority order (highest first)
    PRIORITY_ORDER = ['api', 'playwright', 'initial_state', 'json_ld', 'soup']
    
    # Fields to reconcile via consensus/priority voting
    VOTABLE_FIELDS = ['price', 'mrp', 'title', 'brand', 'category', 'rating', 'reviews', 'stock']
    
    # Fields to merge (union)
    MERGE_FIELDS = ['images']
    MERGE_DICT_FIELDS = ['specs']
    
    reconciled = {}
    confidence = {}
    method = {}
    
    # ---- Votable fields: consensus or priority ----
    for field in VOTABLE_FIELDS:
        candidates = []  # (source_name, value)
        
        for src_name in PRIORITY_ORDER:
            src_data = sources.get(src_name, {})
            val = src_data.get(field)
            if val is not None and val != "" and val != 0:
                candidates.append((src_name, val))
        
        if not candidates:
            reconciled[field] = None
            confidence[field] = "none"
            method[field] = "no_data"
            continue
        
        if len(candidates) == 1:
            reconciled[field] = candidates[0][1]
            confidence[field] = "low"
            method[field] = f"single_source:{candidates[0][0]}"
            logger.debug(f"🔬 {field}: Single source → {candidates[0][0]}={candidates[0][1]}")
            continue
        
        # Try consensus: check if values agree
        # For numeric fields, consider values "agreeing" if within 1% of each other
        if field in ('price', 'mrp', 'rating', 'reviews', 'discount'):
            # Numeric consensus
            numeric_vals = []
            for src_name, val in candidates:
                if isinstance(val, (int, float)):
                    numeric_vals.append((src_name, float(val)))
                elif isinstance(val, str):
                    parsed = _try_parse_number(val)
                    if parsed:
                        numeric_vals.append((src_name, parsed))
            
            if numeric_vals:
                # Group by approximate value (within 1% tolerance)
                best_val = numeric_vals[0][1]
                agreeing = [nv for nv in numeric_vals if abs(nv[1] - best_val) / max(best_val, 1) < 0.01]
                
                # ── SPECIAL MRP HANDLING ──
                # On Flipkart's React Native Web UI, the DOM often contains
                # line-through prices from OTHER packs/sellers/offers — not the
                # product's own MRP. INITIAL_STATE is server-rendered and more
                # reliable for MRP. If INITIAL_STATE disagrees and is LOWER,
                # prefer it (the real MRP for this SKU).
                if field == 'mrp':
                    is_sources = [nv for nv in numeric_vals if nv[0] == 'initial_state']
                    dom_sources = [nv for nv in numeric_vals if nv[0] in ('playwright', 'soup')]
                    if is_sources and dom_sources:
                        is_mrp = is_sources[0][1]
                        dom_mrp = dom_sources[0][1]
                        if is_mrp < dom_mrp:
                            reconciled[field] = is_mrp
                            confidence[field] = "high"
                            method[field] = f"initial_state_preferred_over_dom:{is_mrp}<{dom_mrp}"
                            logger.info(f"⚠️ MRP: INITIAL_STATE={is_mrp} < DOM={dom_mrp} → Using INITIAL_STATE (server-truth, DOM has cross-sell MRP)")
                            continue
                
                if len(agreeing) >= 3:
                    reconciled[field] = agreeing[0][1]
                    confidence[field] = "high"
                    method[field] = f"consensus_{len(agreeing)}_of_{len(numeric_vals)}:{','.join(a[0] for a in agreeing)}"
                elif len(agreeing) >= 2:
                    reconciled[field] = agreeing[0][1]
                    confidence[field] = "medium" if 'api' not in [a[0] for a in agreeing] else "high"
                    method[field] = f"consensus_{len(agreeing)}_of_{len(numeric_vals)}:{','.join(a[0] for a in agreeing)}"
                else:
                    # No consensus - use priority
                    reconciled[field] = numeric_vals[0][1]
                    confidence[field] = "high" if numeric_vals[0][0] == 'api' else "medium"
                    method[field] = f"priority:{numeric_vals[0][0]}"
                    
                    # Log conflict for debugging
                    conflict_info = ", ".join(f"{nv[0]}={nv[1]}" for nv in numeric_vals)
                    logger.info(f"⚠️ {field} CONFLICT: {conflict_info} → Using {numeric_vals[0][0]}={numeric_vals[0][1]}")
            else:
                reconciled[field] = candidates[0][1]
                confidence[field] = "low"
                method[field] = f"priority:{candidates[0][0]}"
        
        else:
            # String consensus (exact match)
            value_groups = {}
            for src_name, val in candidates:
                val_str = str(val).strip().lower()
                if val_str not in value_groups:
                    value_groups[val_str] = []
                value_groups[val_str].append(src_name)
            
            # Find largest consensus group
            best_group = max(value_groups.values(), key=len)
            best_val_key = [k for k, v in value_groups.items() if v == best_group][0]
            # Use the original (non-lowered) value from the highest-priority source in the group
            for src_name, val in candidates:
                if src_name in best_group:
                    reconciled[field] = val
                    break
            
            if len(best_group) >= 3:
                confidence[field] = "high"
                method[field] = f"consensus_{len(best_group)}_of_{len(candidates)}:{','.join(best_group)}"
            elif len(best_group) >= 2:
                confidence[field] = "medium"
                method[field] = f"consensus_{len(best_group)}_of_{len(candidates)}:{','.join(best_group)}"
            else:
                # No consensus - use priority
                reconciled[field] = candidates[0][1]
                confidence[field] = "high" if candidates[0][0] == 'api' else "medium"
                method[field] = f"priority:{candidates[0][0]}"
    
    # ---- Merge fields: union of all sources ----
    for field in MERGE_FIELDS:
        all_items = []
        for src_name in PRIORITY_ORDER:
            src_data = sources.get(src_name, {})
            items = src_data.get(field, [])
            if isinstance(items, list):
                all_items.extend(items)
        
        # Deduplicate while preserving order
        reconciled[field] = list(dict.fromkeys(all_items))
        confidence[field] = "high" if len(all_items) > 0 else "none"
        contributing = [s for s in PRIORITY_ORDER if sources.get(s, {}).get(field)]
        method[field] = f"merged:{','.join(contributing)}" if contributing else "no_data"
    
    # ---- Merge dict fields (specs): API overrides others ----
    for field in MERGE_DICT_FIELDS:
        merged = {}
        # Process in reverse priority (lowest first, so highest priority overwrites)
        for src_name in reversed(PRIORITY_ORDER):
            src_data = sources.get(src_name, {})
            items = src_data.get(field, {})
            if isinstance(items, dict):
                merged.update(items)
        
        reconciled[field] = merged
        confidence[field] = "high" if merged else "none"
        contributing = [s for s in PRIORITY_ORDER if sources.get(s, {}).get(field)]
        method[field] = f"merged:{','.join(contributing)}" if contributing else "no_data"
    
    # ---- Log reconciliation summary ----
    high_count = sum(1 for v in confidence.values() if v == "high")
    med_count = sum(1 for v in confidence.values() if v == "medium")
    low_count = sum(1 for v in confidence.values() if v == "low")
    logger.info(f"🔬 RECONCILIATION COMPLETE: {high_count} high, {med_count} medium, {low_count} low confidence fields")
    
    return reconciled, confidence, method


# =============================================================================
# DATA EXTRACTION (from code2)
# =============================================================================

def extract_data_from_soup(soup: BeautifulSoup, html: str, url: str, url_hash: str) -> Dict[str, Any]:
    """Extract data from BeautifulSoup object"""
    product = {
        "url_hash": url_hash,
        "domain": get_safe_domain(url),
        "title": "",
        "description": "",
        "content": "",
        "mrp": None,
        "price": None,
        "currency": None,
        "brand": None,
        "category": None,
        "size": None,
        "available_sizes": [],
        "instock": None,
        "specs": {},
        "images": [],
        "rating": None,
        "reviews": None,
    }
    
    # Structured source data for reconciliation engine
    json_ld_source = {}
    initial_state_source = {}
    
    try:
        # JSON-LD & INITIAL_STATE (High priority structured data)
        json_ld = _extract_json_ld(soup)
        initial_state = _extract_initial_state(html)
        
        # ================================================================
        # ENHANCED JSON-LD EXTRACTION (pricing, rating, reviews, images, stock)
        # ================================================================
        if json_ld:
            # Price from JSON-LD offers
            offers = json_ld.get("offers", {})
            if isinstance(offers, dict):
                ld_price = _try_parse_number(offers.get("price"))
                if ld_price and ld_price > 0:
                    json_ld_source["price"] = ld_price
                    json_ld_source["currency"] = offers.get("priceCurrency", "INR")
                    logger.info(f"📋 JSON-LD Price: {ld_price}")
                
                # Stock from JSON-LD
                availability = offers.get("availability", "")
                if "InStock" in str(availability):
                    json_ld_source["stock"] = "In Stock"
                elif "OutOfStock" in str(availability):
                    json_ld_source["stock"] = "Out of Stock"
            
            # Rating from JSON-LD aggregateRating
            agg_rating = json_ld.get("aggregateRating", {})
            if isinstance(agg_rating, dict):
                ld_rating = _try_parse_number(agg_rating.get("ratingValue"))
                if ld_rating and 0 < ld_rating <= 5:
                    json_ld_source["rating"] = round(ld_rating, 1)
                
                ld_reviews = _try_parse_number(agg_rating.get("reviewCount") or agg_rating.get("ratingCount"))
                if ld_reviews and ld_reviews > 0:
                    json_ld_source["reviews"] = int(ld_reviews)
            
            # Title from JSON-LD
            if json_ld.get("name"):
                json_ld_source["title"] = json_ld["name"]
            
            # Brand from JSON-LD
            if json_ld.get("brand"):
                brand_data = json_ld["brand"]
                if isinstance(brand_data, dict):
                    json_ld_source["brand"] = brand_data.get("name", "")
                elif isinstance(brand_data, str):
                    json_ld_source["brand"] = brand_data
            
            # Images from JSON-LD
            ld_images = json_ld.get("image", [])
            if isinstance(ld_images, str):
                ld_images = [ld_images]
            if isinstance(ld_images, list):
                json_ld_source["images"] = [img for img in ld_images if isinstance(img, str) and img.startswith("http")]
            
            if json_ld_source:
                logger.info(f"📋 JSON-LD extracted: {list(json_ld_source.keys())}")
        
        # ================================================================
        # ENHANCED __INITIAL_STATE__ EXTRACTION (pricing, brand, title)
        # ================================================================
        if initial_state:
            # Deep-scan INITIAL_STATE for pricing data using the API scanner
            is_data = _extract_product_data_from_api(initial_state)
            if is_data.get("price"):
                initial_state_source["price"] = is_data["price"]
                logger.info(f"📋 INITIAL_STATE Price: {is_data['price']}")
            if is_data.get("mrp"):
                initial_state_source["mrp"] = is_data["mrp"]
                logger.info(f"📋 INITIAL_STATE MRP: {is_data['mrp']}")
            if is_data.get("title"):
                initial_state_source["title"] = is_data["title"]
            if is_data.get("brand"):
                initial_state_source["brand"] = is_data["brand"]
            if is_data.get("rating"):
                initial_state_source["rating"] = is_data["rating"]
            if is_data.get("reviews"):
                initial_state_source["reviews"] = is_data["reviews"]
            if is_data.get("images"):
                initial_state_source["images"] = is_data["images"]
            if is_data.get("stock"):
                initial_state_source["stock"] = is_data["stock"]
            if is_data.get("seller"):
                initial_state_source["seller"] = is_data["seller"]
            if is_data.get("discount"):
                initial_state_source["discount"] = is_data["discount"]
            
            if initial_state_source:
                logger.info(f"📋 INITIAL_STATE extracted: {list(initial_state_source.keys())}")
        
        # Title
        if json_ld and json_ld.get("name"):
            product["title"] = json_ld["name"]
        
        if not product["title"] and initial_state:
            try:
                # Common paths for title in React state
                product["title"] = initial_state.get("pageDataV4", {}).get("pageContext", {}).get("title", "")
            except: pass

        if not product["title"]:
            title_selectors = [
                "span.VU-ZEz", "span.B_NuCI", "h1.yhB1nd",
                "h1._6EBuvT", "h1", "[data-tkid*='TITLE']",
                "span[class*='Title']", "div[class*='v1zwn21q']", "div.r-1kihuf0 h1"
            ]
            for sel in title_selectors:
                text = _get_text(soup, sel, MAX_TITLE_LENGTH)
                if text and 'Size Chart' not in text:
                    product["title"] = text
                    break
        
        # Brand (Structured)
        if json_ld and json_ld.get("brand"):
            product["brand"] = json_ld["brand"].get("name", product.get("brand"))
        
        # Semantic title fallback (works on new UI)
        if not product["title"]:
            # 1. Try finding any H1 tag first (highest semantic value)
            h1 = soup.find('h1')
            if h1:
                product["title"] = _get_text(soup, 'h1', MAX_TITLE_LENGTH)
            
            # 2. If no title, try URL-based fallback (Very reliable for Flipkart)
            if not product["title"]:
                url_parts = url.split('/')
                # e.g., https://www.flipkart.com/slug/p/itm... -> slug is at index 3
                if len(url_parts) > 3:
                     slug = url_parts[3]
                     # Avoid generic slugs
                     if slug and 'product' not in slug and 'search' not in slug:
                        product["title"] = safe_truncate(slug.replace('-', ' ').title(), MAX_TITLE_LENGTH)
                        logger.info(f"🏷️ Title extracted from URL slug: {product['title']}")
            
            # 3. Last result: Semantic font-size check (improved)
            if not product["title"]:
                for tag in soup.find_all(['h1', 'h2', 'div', 'span']): # prioritized order
                    text = tag.get_text(strip=True)
                    # Filter out common non-title text
                    if (text and len(text) > 10 and len(text) < 200 and 
                        '₹' not in text and 
                        'Apply offers' not in text and 
                        'Bank Offer' not in text and
                        'Size Chart' not in text):
                        
                        # Check inline font-size if present
                        style = tag.get('style', '') or ''
                        font_match = re.search(r'font-size:\s*(\d+)', style)
                        if font_match and int(font_match.group(1)) >= 16:
                            product["title"] = safe_truncate(text, MAX_TITLE_LENGTH)
                            break
        
        # ================================================================
        # CRITICAL: Check for 500 Internal Server Error
        # ================================================================
        if product["title"] and "500 Internal Server Error" in product["title"]:
            # Set error flag - this will be checked after the try-except
            product["_critical_error"] = True
            product["_critical_error_msg"] = (
                "🚨 FLIPKART SERVER ERROR DETECTED 🚨\n"
                "Flipkart returned a '500 Internal Server Error' page.\n"
                "This is likely due to:\n"
                "  - Bot detection blocking the scraper\n"
                "  - Rate limiting (too many requests)\n"
                "  - Temporary server issues\n"
                "  - Invalid/expired product URL\n\n"
                "⚠️ PROCESS STOPPED to prevent unnecessary resource usage.\n"
                "Please retry after some time or check the URL."
            )
            logger.error(product["_critical_error_msg"])
        
        # ================================================================
        # CRITICAL: Check for CAPTCHA Page ("Are you a human?")
        # ================================================================
        if product["title"] and "Are you a human?" in product["title"]:
            product["_critical_error"] = True
            product["_critical_error_msg"] = (
                "🚨 FLIPKART CAPTCHA BLOCK 🚨\n"
                "Flipkart returned 'Are you a human?' page.\n"
                "The scraper is being blocked. If running locally, please solve the CAPTCHA."
            )
            logger.error(product["_critical_error_msg"])
        
        # Brand & Category
        bc_selectors = [
            "._2whKao a", "a[class*='breadcrumb']",
            ".breadcrumb a", "div._2whKao a", "a.yFHEvU"
        ]
        for sel in bc_selectors:
            bc = [safe_truncate(a.get_text(strip=True), MAX_BRAND_LENGTH) 
                  for a in soup.select(sel) if a.get_text(strip=True)]
            if bc and len(bc) >= 2:
                product["brand"] = bc[0]
                product["category"] = bc[-1]
                break
        
        if not product["brand"] and product["title"]:
            title_parts = product["title"].split()
            if title_parts:
                product["brand"] = safe_truncate(title_parts[0], MAX_BRAND_LENGTH)
        
        if not product["category"]:
            url_parts = url.split('/')
            if len(url_parts) > 1:
                potential_category = url_parts[3] if len(url_parts) > 3 else url_parts[1]
                product["category"] = safe_truncate(potential_category.replace('-', ' ').title(), MAX_BRAND_LENGTH)
        
        # Prices
        price_selectors = [
            'div.Nx9bqj.CxhGGd', 'div.Nx9bqj', 'div._30jeq3',
            'div._30jeq3._16Jk6d', 'div[class*="price"]', '[class*="Price"]'
        ]
        for sel in price_selectors:
            price_elem = soup.select_one(sel)
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                price, curr = _parse_price(price_text)
                if price:
                    product["price"] = price
                    product["currency"] = curr or "INR"
                    break
        
        # MRP
        mrp_selectors = ['div._3I9_wc._27UcVY', 'div._3I9_wc', 'span._3I9_wc', '[class*="mrp"]']
        for sel in mrp_selectors:
            mrp_elem = soup.select_one(sel)
            if mrp_elem:
                mrp_text = mrp_elem.get_text(strip=True)
                mrp, _ = _parse_price(mrp_text)
                if mrp and (not product["price"] or mrp > product["price"]):
                    product["mrp"] = mrp
                    logger.info(f"Found MRP via selectors: {mrp}")
                    break
        
        # MRP semantic fallback: find FIRST line-through element with a price
        # IMPORTANT: Pick the FIRST match only — it's the main product MRP.
        # Later line-through elements are from other sizes/sellers/offers.
        if not product["mrp"]:
            for tag in soup.find_all(['div', 'span']):
                style = tag.get('style', '') or ''
                if 'line-through' in style:
                    text = tag.get_text(strip=True)
                    mrp, _ = _parse_price(text)
                    if mrp and mrp > 0:
                        product["mrp"] = mrp
                        logger.info(f"Found MRP via fallback (first line-through): {mrp}")
                        break
        
        # Stock
        # BUG 12 FIX: Removed redundant 'product["instock"] = "In Stock"' 
        # because it is always overwritten by Playwright's authoritative check later.
        stock_selectors = [
            'button._2KpZ6l._2U9uOA', 'button[class*="cart"]',
            'button[class*="buy"]', 'button._2KpZ6l'
        ]
        for sel in stock_selectors:
            btn = soup.select_one(sel)
            if btn:
                btn_text = btn.get_text(strip=True).lower()
                if btn.get('disabled') or 'out of stock' in btn_text or 'sold out' in btn_text:
                    product["instock"] = "Out of Stock"
                break
        
        # Rating
        rating_selectors = ["div._3LWZlK", "span._1lRcqv", "div[class*='rating']"]
        for sel in rating_selectors:
            rating_text = _get_text(soup, sel, 50)
            if rating_text:
                m = re.search(r"(\d(?:\.\d)?)", rating_text)
                if m:
                    try:
                        rating = float(m.group(1))
                        if 0 <= rating <= 5:
                            product["rating"] = rating
                            break
                    except ValueError:
                        pass
        
        # Reviews
        review_selectors = ["span._2_R_DZ", "span[class*='review']", "span[class*='Rating']"]
        for sel in review_selectors:
            review_text = _get_text(soup, sel, 100)
            if review_text:
                m = re.search(r"([\d,]+)\s*(?:Reviews?|Ratings?)", review_text, re.I)
                if m:
                    product["reviews"] = _parse_int(m.group(1))
                    if product["reviews"]:
                        break
        
        # Images
        def _boost_image_quality(url: str) -> str:
            """Boost Flipkart image resolution from thumbnail to high-res."""
            if not url: return url
            # Common pattern: /image/128/128/ or /image/300/300/
            # Target: /image/832/832/
            return re.sub(r'/image/\d+/\d+/', '/image/832/832/', url)

        imgs = []
        image_selectors = [
            "img[src*='rukminim']",            # CDN-first (works on ALL UIs)
            "div._396cs4 img", "div._2r_T1I img",
            "div.CXW8mj img", "img[class*='_396cs4']",
        ]
        
        for sel in image_selectors:
            for el in soup.select(sel)[:MAX_IMAGES]:
                src = el.get("src") or el.get("data-src")
                if src and _is_product_image(src):
                    if src.startswith("//"):
                        src = "https:" + src
                    # BOOST QUALITY
                    src = _boost_image_quality(src)
                    imgs.append(src)
            if imgs:
                break
        
        product["images"] = list(dict.fromkeys(imgs))[:MAX_IMAGES]
        
        # Specs Extraction
        specs = {}
        
        # 1. window.__INITIAL_STATE__ Parsing (Highest precision)
        if initial_state:
            try:
                # Blacklist of noise labels
                noise_labels = [
                    "Home", "Shop", "Clothing and Accessories", "Bottomwear", "Jeans", 
                    "Men's Jeans", "Location not set", "Delivery details", "Specifications",
                    "Description", "Questions and Answers", "Flipkart Assured", "All details",
                    "Try on", "Selected Size", "Price Details", "Product highlights", "Verified Buyers",
                    "Maximum Retail Price", "Questions", "Answers", "View Similar", "Product Sold",
                    "Quality Score", "Speed Score", "Fulfilled by", "based on"
                ]

                def get_dls_text(node):
                    """Extracts text from a variety of DLS nested structures."""
                    if isinstance(node, str): return node
                    if isinstance(node, list) and node: return get_dls_text(node[0])
                    if not isinstance(node, dict): return None
                    
                    # Direct 'text' key
                    if "text" in node: 
                        return get_dls_text(node["text"])
                    
                    # 'value' key which might be str, list or dict
                    v = node.get("value")
                    if v is not None:
                        return get_dls_text(v)
                    
                    return None

                def scan_for_specs(obj, in_spec_branch=False, depth=0):
                    if depth > 100: return 
                    if not isinstance(obj, (dict, list)): return
                    
                    if isinstance(obj, dict):
                        is_now_spec_branch = in_spec_branch
                        # Explicit trigger for specification grids
                        for k in obj.keys():
                            if isinstance(k, str) and ("specification" in k.lower() or "textspecifications_grid_layout" in k.lower()):
                                is_now_spec_branch = True
                                break
                        
                        # DLS Spec Row Pattern: label_0 (Key) and label_1/label_2/text_0 (Value)
                        if "label_0" in obj:
                            k_raw = get_dls_text(obj["label_0"])
                            if k_raw and k_raw.strip():
                                # Try multiple value slots
                                value = None
                                for v_key in ["label_1", "label_2", "text_0", "text_1", "value"]:
                                    if v_key in obj and v_key != "label_0":
                                        cand = get_dls_text(obj[v_key])
                                        if cand and cand.strip() and cand != k_raw:
                                            # Avoid redundant UI artifacts
                                            if len(cand) > 300: continue 
                                            value = cand
                                            break
                                
                                if k_raw and value:
                                    k_clean = k_raw.strip().rstrip(":")
                                    v_clean = value.strip()
                                    
                                    # Filters
                                    is_noise = any(nl.lower() == k_clean.lower() or nl.lower() in k_clean.lower() for nl in noise_labels)
                                    is_ui = (
                                        k_clean.replace(".", "").replace("+", "").isdigit() or 
                                        "%" in k_clean or
                                        "ratings by" in k_clean.lower()
                                    )
                                    
                                    if (is_now_spec_branch or not is_noise) and not is_ui:
                                        if len(k_clean) < 50 and k_clean not in specs:
                                            specs[k_clean] = v_clean

                        for k, v in obj.items():
                            scan_for_specs(v, is_now_spec_branch, depth + 1)
                            
                    elif isinstance(obj, list):
                        for item in obj:
                            scan_for_specs(item, in_spec_branch, depth + 1)

                scan_for_specs(initial_state)
            except Exception as e:
                logger.debug(f"Deep Initial State parse error: {e}")

        # 2. JSON-LD Fallback for basic specs
        if not specs and json_ld:
            for key in ["color", "model", "material", "sku"]:
                if json_ld.get(key):
                    specs[key.capitalize()] = str(json_ld[key])

        # 3. Traditional Table Extraction
        if not specs:
            spec_table_selectors = [
                "div._2418kt table", "table._14cfVK",
                "table[class*='spec']", "div.GNDEQ- table"
            ]
            for sel in spec_table_selectors:
                table = soup.select_one(sel)
                if table:
                    for tr in table.find_all("tr")[:MAX_SPECS]:
                        try:
                            cells = tr.find_all(["th", "td"])
                            if len(cells) >= 2:
                                k = _norm(cells[0].get_text())
                                v = _norm(cells[1].get_text())
                                if k and v:
                                    k = safe_truncate(k.rstrip(":").strip(), MAX_SPEC_KEY_LENGTH)
                                    v = safe_truncate(v, MAX_SPEC_VALUE_LENGTH)
                                    if len(specs) < MAX_SPECS:
                                        specs[k] = v
                        except: continue
                    if specs: break

        # 4. React Native Web Grid Extraction (New UI) - Stricter Scoping
        if not specs:
            # Find all containers that look like a specification group
            # In New UI, specs are often in a div with "Specifications" text nearby
            spec_containers = soup.find_all("div", class_=re.compile(r"css-175oi2r"))
            for container in spec_containers:
                if "Specifications" in container.get_text():
                    labels = container.find_all(class_=re.compile(r"v1zwn21k"))
                    values = container.find_all(class_=re.compile(r"v1zwn21j"))
                    if labels and values:
                        for k_el, v_el in zip(labels, values):
                            k = _norm(k_el.get_text())
                            v = _norm(v_el.get_text())
                            if k and v and len(k) < 30 and "Specifications" not in k:
                                specs[k] = v
                        if specs: break

        product["specs"] = specs
        
        # Description
        desc_selectors = [
            "div._1mXcCf", "div[class*='description']",
            "p[class*='description']"
        ]
        for sel in desc_selectors:
            product["description"] = _get_text(soup, sel, MAX_DESCRIPTION_LENGTH)
            if product["description"]:
                break
        
        if not product["description"]:
            product["description"] = product["title"]
        
        # Compose content from available fields
        content_parts = [product["title"]]
        if product["description"] and product["description"] != product["title"]:
            content_parts.append(product["description"])
        
        if product["specs"]:
            specs_text = "Specifications: " + ", ".join([f"{k}: {v}" for k, v in product["specs"].items()])
            content_parts.append(specs_text)
            
        product["content"] = " | ".join(filter(None, content_parts))
        
    except Exception as e:
        # Log non-critical extraction errors but continue
        logger.warning(f"Non-critical extraction error: {e}")
    
    # ================================================================
    # STORE STRUCTURED SOURCE DATA FOR RECONCILIATION
    # ================================================================
    product["_structured_sources"] = {
        "json_ld": json_ld_source,
        "initial_state": initial_state_source,
        "soup": {
            "price": product.get("price"),
            "mrp": product.get("mrp"),
            "title": product.get("title"),
            "brand": product.get("brand"),
            "category": product.get("category"),
            "rating": product.get("rating"),
            "reviews": product.get("reviews"),
            "images": product.get("images", []),
            "stock": product.get("instock"),
            "specs": product.get("specs", {}),
        }
    }
    
    # ================================================================
    # CRITICAL: Re-raise 500 errors AFTER the try-except
    # This ensures they propagate to scrape_single_url
    # ================================================================
    if product.get("_critical_error"):
        raise Exception(product.get("_critical_error_msg", "Flipkart 500 Internal Server Error"))
    
    return product

# =============================================================================
# COMBINED PLAYWRIGHT FETCHING
# =============================================================================

def fetch_with_playwright_combined(url: str, target_size: str = None) -> Tuple[Optional[str], List[str], Optional[str], str]:
    """
    NEW SIMPLIFIED WORKFLOW (2026-01-28):
    1. Hydration wait (sizes + buy box visible)
    2. Extract available sizes (ALWAYS)
    3. If target_size: N2SWWC check → Best-effort click → Variant wait
    4. Buy Box evaluation (AUTHORITATIVE)
    
    Returns: (html, available_sizes, purchasable_sizes, selected_size, stock_status, price_text, mrp_text, rating_text, review_text, api_captured_data)
    """
    if not PLAYWRIGHT_AVAILABLE:
        return None, [], [], None, "Unknown", None, None, None, None, {}
    
    playwright_instance = None
    browser = None
    context = None
    page = None
    
    try:
        # ================================================================
        # LAUNCH BROWSER
        # ================================================================
        playwright_instance = sync_playwright().start()
        
        # ================================================================
        # ENVIRONMENT DETECTION: Auto-switch between headed/headless
        # ================================================================
        # AWS Lambda has no display, so we MUST use headless mode there
        # Locally, we use headed mode for better anti-bot detection
        is_lambda = os.environ.get('AWS_EXECUTION_ENV') is not None or \
                    os.environ.get('AWS_LAMBDA_FUNCTION_NAME') is not None
        
        if is_lambda:
            logger.info("🔧 Running in AWS Lambda - Using headless mode (required)")
            headless_mode = True
        else:
            logger.info("💻 Running locally - Using headed mode (better bot evasion)")
            # headless_mode = False
            headless_mode = True
        
        
        # ================================================================
        # ADVANCED ANTI-BOT DETECTION: Randomization & Stealth
        # ================================================================
        
        # 1. Randomized User Agents (rotate to avoid fingerprinting)
        # 1. Randomized User Agents (rotate to avoid fingerprinting)
        # Use recent versions to ensure consistency with sec-ch-ua
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        ]
        selected_user_agent = random.choice(user_agents)
        
        # Extract Chrome version for consistent headers
        chrome_version = "131" # fallback
        try:
            # BUG 14 FIX: Removed redundant 'import re'
            m = re.search(r'Chrome/(\d+)', selected_user_agent)
            if m:
                chrome_version = m.group(1)
        except:
            pass
        
        logger.info(f"🔄 Selected UA Version: Chrome {chrome_version}")
        
        # WC-6 FIX: Removed mobile-like viewports causing Mobile Web view
        viewports = [
            {'width': 1920, 'height': 1080},
            {'width': 1536, 'height': 864},
            {'width': 1440, 'height': 900},
            {'width': 1366, 'height': 768},
        ]
        selected_viewport = random.choice(viewports)
        
        browser = playwright_instance.chromium.launch(
            headless=headless_mode,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',  # Hide automation
                '--disable-features=IsolateOrigins,site-per-process',
                '--disable-infobars',
                '--window-position=0,0',
                f'--window-size={selected_viewport["width"]},{selected_viewport["height"]}',
            ],
            timeout=PLAYWRIGHT_TIMEOUT
        )
        
        if os.path.exists("flipkart_session.json"):
            logger.info("🍪 Loading existing session from flipkart_session.json")
            context = browser.new_context(
                storage_state="flipkart_session.json",
                user_agent=selected_user_agent,
                viewport=selected_viewport,
                java_script_enabled=True,
                locale='en-IN',
                timezone_id='Asia/Kolkata',
                permissions=['geolocation'],
                geolocation={'latitude': 28.4595, 'longitude': 77.0266},  # Gurgaon
                extra_http_headers={
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                    'Accept-Language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1',
                    'Cache-Control': 'max-age=0',
                    'sec-ch-ua': f'"Not(A:Brand";v="99", "Google Chrome";v="{chrome_version}", "Chromium";v="{chrome_version}"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                }
            )
        else:
            if is_lambda:
                logger.info("☁️ AWS Environment detected: Starting fresh session (No pre-baked cookies found).")
            else:
                logger.info("🆕 No existing session found. Starting fresh.")
            context = browser.new_context(
                user_agent=selected_user_agent,
                viewport=selected_viewport,
                java_script_enabled=True,
                locale='en-IN',
                timezone_id='Asia/Kolkata',
                permissions=['geolocation'],
                geolocation={'latitude': 28.4595, 'longitude': 77.0266},  # Gurgaon
                extra_http_headers={
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                    'Accept-Language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1',
                    'Cache-Control': 'max-age=0',
                    # BUG 11 FIX: Removed duplicate Cache-Control header here
                    'sec-ch-ua': f'"Not(A:Brand";v="99", "Google Chrome";v="{chrome_version}", "Chromium";v="{chrome_version}"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                }
            )
        context.set_default_timeout(PLAYWRIGHT_TIMEOUT)
        
        # WC-5 FIX: Log the geolocation being used
        logger.info(f"📍 Geolocation set to: Gurgaon (Lat: 28.4595, Long: 77.0266)")
        
        page = context.new_page()
        
        # ================================================================
        # NETWORK INTERCEPTION: Capture pricing API responses
        # ================================================================
        api_captured_data = {}
        _api_response_count = [0]  # Mutable counter for closure
        
        API_URL_PATTERNS = ['/api/', '1/page/', 'graphql', 'product-analytics', 'page/fetch']
        
        def _on_response(response):
            """Intercept API responses and extract product data."""
            try:
                url_str = response.url
                
                # Only process JSON API responses
                if not any(pattern in url_str for pattern in API_URL_PATTERNS):
                    return
                
                content_type = response.headers.get('content-type', '')
                if 'application/json' not in content_type:
                    return
                
                status = response.status
                if status != 200:
                    return
                
                _api_response_count[0] += 1
                
                try:
                    body = response.json()
                except Exception:
                    return
                
                # Deep-scan for product data
                extracted = _extract_product_data_from_api(body)
                
                # Merge into captured data (don't overwrite existing non-None values)
                has_new = False
                for key, value in extracted.items():
                    if value is not None and value != [] and value != 0:
                        if key == 'images' and isinstance(value, list):
                            existing_imgs = api_captured_data.get('images', [])
                            new_imgs = [img for img in value if img not in existing_imgs]
                            if new_imgs:
                                api_captured_data.setdefault('images', []).extend(new_imgs)
                                has_new = True
                        elif key not in api_captured_data or api_captured_data[key] is None:
                            api_captured_data[key] = value
                            has_new = True
                
                if has_new:
                    short_url = url_str.split('?')[0][-60:]
                    new_keys = [k for k, v in extracted.items() if v is not None and v != [] and v != 0]
                    logger.info(f"📡 API INTERCEPTED [{_api_response_count[0]}]: ...{short_url} → {new_keys}")
                    
            except Exception as e:
                logger.debug(f"API interception error (non-fatal): {e}")
        
        page.on("response", _on_response)
        logger.info("📡 Network interception ACTIVE — capturing API responses")
        
        # ================================================================
        # COMPREHENSIVE ANTI-DETECTION SCRIPTS
        # ================================================================
        page.add_init_script("""
            // 1. Hide webdriver property
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            
            // 2. Add Chrome runtime
            window.chrome = {
                runtime: {},
                loadTimes: function() {},
                csi: function() {},
                app: {}
            };
            
            // 3. Override permissions
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
            
            // 4. Mock plugins (real browsers have plugins)
            Object.defineProperty(navigator, 'plugins', {
                get: () => [
                    {
                        0: {type: "application/x-google-chrome-pdf", suffixes: "pdf", description: "Portable Document Format"},
                        description: "Portable Document Format",
                        filename: "internal-pdf-viewer",
                        length: 1,
                        name: "Chrome PDF Plugin"
                    },
                    {
                        0: {type: "application/pdf", suffixes: "pdf", description: "Portable Document Format"},
                        description: "Portable Document Format", 
                        filename: "mhjfbmdgcfjbbpaeojofohoefgiehjai",
                        length: 1,
                        name: "Chrome PDF Viewer"
                    }
                ]
            });
            
            // 5. Languages (real browsers have multiple)
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-IN', 'en-GB', 'en-US', 'en']
            });
            
            // 6. Platform consistency
            Object.defineProperty(navigator, 'platform', {
                get: () => 'Win32'
            });
            
            // 7. Vendor
            Object.defineProperty(navigator, 'vendor', {
                get: () => 'Google Inc.'
            });
            
            // 8. Hardware concurrency (CPU cores)
            Object.defineProperty(navigator, 'hardwareConcurrency', {
                get: () => 8
            });
            
            // 9. Device memory
            Object.defineProperty(navigator, 'deviceMemory', {
                get: () => 8
            });
            
            // 10. Add realistic screen properties
            Object.defineProperty(screen, 'availWidth', {
                get: () => window.screen.width
            });
            Object.defineProperty(screen, 'availHeight', {
                get: () => window.screen.height - 40
            });
            
            // 11. WebGL Vendor & Renderer (avoid headless detection)
            const getParameter = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(parameter) {
                if (parameter === 37445) {
                    return 'Intel Inc.';
                }
                if (parameter === 37446) {
                    return 'Intel Iris OpenGL Engine';
                }
                return getParameter.call(this, parameter);
            };
            
            // 12. Canvas fingerprint randomization
            const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
            HTMLCanvasElement.prototype.toDataURL = function() {
                const context = this.getContext('2d');
                if (context) {
                    const imageData = context.getImageData(0, 0, this.width, this.height);
                    for (let i = 0; i < imageData.data.length; i += 4) {
                        imageData.data[i] += Math.floor(Math.random() * 3) - 1;
                    }
                    context.putImageData(imageData, 0, 0);
                }
                return originalToDataURL.apply(this, arguments);
            };
            
            // 13. Overwrite getComputedStyle to avoid bot detection via strict style checking
            // But we can't fully mock it for our CSS Logic, so we leave it.
        """)
        
        logger.info(f"🌐 Navigating to URL...")
        logger.info(f"   User-Agent: {selected_user_agent[:50]}...")
        logger.info(f"   Viewport: {selected_viewport['width']}x{selected_viewport['height']}")
        
        # Human-like delay before navigation
        initial_delay = random.uniform(0.5, 1.5)
        page.wait_for_timeout(int(initial_delay * 1000))
        
        page.goto(url, timeout=PLAYWRIGHT_TIMEOUT, wait_until="domcontentloaded")

        # ================================================================
        # CAPTCHA / BOT DETECTION HANDLING (Updated 2026-02-18)
        # 
        # STRATEGY IF CAPTCHA APPEARS:
        # 1. The script will PAUSE and ask you to solve it manually in the window.
        # 2. Once solved, it waits for the product page to load and continues.
        # 3. It then SAVES your session (cookies) to 'flipkart_session.json'.
        # 4. Next time, it loads these cookies to skip the CAPTCHA.
        #
        # PREVENTATIVE MEASURE:
        # Run 'python session_generator.py' to login and save a clean session
        # BEFORE running this scraper.
        # ================================================================
        try:
            page_title = page.title()
            if "Are you a human?" in page_title:
                if headless_mode:
                    logger.error("❌ CAPTCHA detected in HEADLESS mode. Cannot solve manually. Aborting.")
                    # Take screenshot for debug if possible
                    try:
                        page.screenshot(path="captcha_block_headless.png")
                    except: pass
                    
                    # Return empty/failed state immediately
                    if page: page.close()
                    if context: context.close()
                    if browser: browser.close()
                    if playwright_instance: playwright_instance.stop()
                    return None, [], [], None, "Unknown", None, None, None, None
                else:
                    logger.warning("🚨 CAPTCHA DETECTED! Pausing for 60 seconds for manual resolution...")
                    logger.warning("👉 Please solve the CAPTCHA in the browser window NOW.")
                    logger.warning("💡 PRO TIP: Run 'python session_generator.py' to login & avoid this in future.")
                    
                    # Wait up to 60s for title to change (user solves CAPTCHA)
                    captcha_solved = False
                    for i in range(30):
                        if "Are you a human?" not in page.title():
                            logger.info("✅ CAPTCHA solved! Resuming extraction...")
                            captcha_solved = True
                            # Give a little time for redirect/reload
                            page.wait_for_timeout(3000)
                            break
                        
                        if i % 5 == 0:
                            logger.info(f"⏳ Waiting for CAPTCHA solution... ({30 - i * 2}s remaining)")
                        time.sleep(2)
                    
                        if not captcha_solved:
                            logger.error("❌ CAPTCHA not solved in time. Aborting.")
                            if page: page.close()
                            if context: context.close()
                            if browser: browser.close()
                            if playwright_instance: playwright_instance.stop()
                            return None, [], [], None, "Unknown", None, None, None, None
                        else:
                            # CRITICAL FIX: Wait for PRODUCT PAGE to load after CAPTCHA
                            logger.info("⏳ Waiting for product page to load after CAPTCHA...")
                            try:
                                # Wait for title or price
                                page.wait_for_selector('h1, div.Nx9bqj, div._30jeq3', timeout=10000)
                                logger.info("✅ Product page loaded!")
                            except:
                                logger.warning("⚠️ Product page load timeout after CAPTCHA - proceeding anyway...")

        except Exception as e:
            logger.warning(f"⚠️ CAPTCHA check failed: {e}")
        
        # ================================================================
        # HUMAN-LIKE BEHAVIOR: Random mouse movement & scrolling
        # ================================================================
        try:
            # 1. Curve mouse movement
            human_mouse_move(page)
            
            # 2. Random scroll to simulate reading
            scroll_distance = random.randint(300, 700)
            page.evaluate(f"window.scrollBy(0, {scroll_distance})")
            page.wait_for_timeout(random.randint(500, 1200))
            
            # 3. Micro-scroll (up/down a bit)
            page.evaluate(f"window.scrollBy(0, {random.randint(-100, 100)})")
            page.wait_for_timeout(random.randint(200, 500))

            # 4. Scroll back up
            page.evaluate("window.scrollTo(0, 0)")
            page.wait_for_timeout(random.randint(200, 500))
        except:
            pass
        
        # ================================================================
        # HYDRATION WAIT (size + buy box visible)
        # ================================================================
        logger.info("⏳ Waiting for page hydration (sizes + buy box)...")
        try:
            # Wait for either size selector OR buy box buttons to be visible
            page.wait_for_selector(
                'a[href*="swatchAttr"], a[href*="sattr"][href*="size"], button:has-text("ADD TO CART"), button:has-text("BUY NOW"), button:has-text("Add to cart")',
                timeout=10000,
                state="visible"
            )
            logger.info("✅ Page hydrated - elements visible")
            
            # SAVE SESSION (COOKIES) FOR PERSISTENCE
            try:
                context.storage_state(path="flipkart_session.json")
                logger.info("💾 Session (Cookies) Saved to flipkart_session.json")
            except Exception as e:
                logger.warning(f"⚠️ Failed to save session: {e}")
        except:
            logger.warning("⚠️ Hydration timeout - checking for CAPTCHA...")
            
            # ================================================================
            # REDUNDANT CAPTCHA CHECK (On Timeout)
            # ================================================================
            try:
                page_title = page.title()
                if "Are you a human?" in page_title or "Are you a human?" in page.content():
                    if not headless_mode:
                        logger.warning("🚨 CAPTCHA DETECTED (on timeout)! Pausing for 60 seconds...")
                        logger.warning("👉 Please solve the CAPTCHA in the browser window NOW.")
                        logger.warning("💡 PRO TIP: Run 'python session_generator.py' to login & avoid this in future.")
                        
                        captcha_solved = False
                        for i in range(30):
                            if "Are you a human?" not in page.title():
                                logger.info("✅ CAPTCHA solved! Resuming extraction...")
                                captcha_solved = True
                                page.wait_for_timeout(3000)
                                break
                            if i % 5 == 0:
                                logger.info(f"⏳ Waiting for CAPTCHA solution... ({30 - i * 2}s remaining)")
                            time.sleep(2)
                        
                        if not captcha_solved:
                            logger.error("❌ CAPTCHA not solved in time. Aborting.")
                            if page: page.close()
                            if context: context.close()
                            if browser: browser.close()
                            if playwright_instance: playwright_instance.stop()
                            return None, [], [], None, "Unknown", None, None, None, None
                        else:
                            # CRITICAL FIX: Wait for PRODUCT PAGE to load after CAPTCHA
                            logger.info("⏳ Waiting for product page to load (timeout recovery)...")
                            try:
                                page.evaluate('window.location.reload()') # Force reload if stuck
                                page.wait_for_selector('h1, div.Nx9bqj, div._30jeq3', timeout=10000)
                                logger.info("✅ Product page loaded!")
                                
                                # SAVE SESSION (COOKIES) AFTER RECOVERY
                                try:
                                    context.storage_state(path="flipkart_session.json")
                                    logger.info("💾 Session (Cookies) Saved to flipkart_session.json (Recovery)")
                                except Exception as e:
                                    logger.warning(f"⚠️ Failed to save session: {e}")
                            except:
                                logger.warning("⚠️ Product page load timeout - proceeding anyway...")
            except Exception as e:
                logger.warning(f"⚠️ Secondary CAPTCHA check failed: {e}")
                
            logger.warning("⚠️ Proceeding after hydration timeout/CAPTCHA check...")
        
        # Small additional wait for dynamic content
        page.wait_for_timeout(2000)
        
        # ================================================================
        # EXTRACT AVAILABLE SIZES (ALWAYS)
        # ================================================================
        available_sizes = []
        purchasable_sizes = []
        try:

            
            size_data = page.evaluate("""
                () => {
                    const allSizes = [];
                    const purchasableSizes = [];
                    
                    // Helper: Check if an element looks OOS via CSS
                    function isOOS(el) {
                        try {
                            const style = window.getComputedStyle(el);
                            
                            // Check 1: Dashed border (common for OOS)
                            if (style.borderStyle === 'dashed' || style.borderTopStyle === 'dashed') return true;
                            
                            // Check 2: Grey background (common for OOS)
                            // rgb(240, 240, 240) -> #f0f0f0, rgb(255, 255, 255) -> white
                            const bg = style.backgroundColor;
                            if (bg === 'rgb(240, 240, 240)' || bg === 'rgb(214, 214, 214)') return true;
                            
                            // Check 3: Disabled class/attribute
                            if (el.classList.contains('disabled') || el.classList.contains('_1YI_yD') || el.getAttribute('aria-disabled') === 'true') return true;
                            
                            // Check 4: Opacity
                            if (parseFloat(style.opacity) < 0.5) return true;
                            
                            // Check 5: Grey text color (sometimes used)
                            const color = style.color;
                            if (color === 'rgb(194, 194, 194)' || color === 'rgb(112, 112, 112)') return true;
                            
                            return false;
                        } catch (e) {
                            return false;
                        }
                    }

                    // ── Strategy 1: "swatchAttr" Links (often OOS/variants) ──
                    const links = document.querySelectorAll('a[href*="swatchAttr"]');
                    for (let link of links) {
                        let rawText = link.innerText.replace(/[\\n\\r]+/g, ' ').trim();
                        // Validation: Must be reasonably short (allow "8 left" text initially)
                        if (rawText.length > 20) continue;
                        if (/verified|buyer|seller|product|quality|duplicate|elastic|area/i.test(rawText)) continue;
                        
                        // Strip stock-count suffix
                        rawText = rawText.replace(/\\s*\\d+\\s*left$/i, '').trim();
                        if (/^[a-z]+\\d+$/i.test(rawText)) rawText = rawText.replace(/\\d+$/, '');
                        
                        // Final strict length check
                        if (rawText && rawText.length <= 8) {
                            if (!allSizes.includes(rawText)) allSizes.push(rawText);
                            
                            const innerDiv = link.querySelector('div');
                            let is_oos = isOOS(link) || (innerDiv && isOOS(innerDiv));
                            
                            if (!is_oos) {
                                 if (!purchasableSizes.includes(rawText)) purchasableSizes.push(rawText);
                            }
                        }
                    }
                    
                    // ── Strategy 2: Common Size Button Lists (often IN-STOCK) ──
                    const buttonSelectors = ['ul._1q8vHb li', 'div._3OiksN', 'div.CDDksN', 'div.h_12p', '.size-buttons div'];
                    
                    for (const sel of buttonSelectors) {
                        const buttons = document.querySelectorAll(sel);
                        for (let btn of buttons) {
                            let text = btn.innerText.replace(/[\\n\\r]+/g, ' ').trim();
                            
                            // Loose Validation first
                            if (text.length > 20) continue;
                            if (/verified|buyer|seller|product|quality|duplicate|elastic|area/i.test(text)) continue;
                            if (!text) continue;
                            
                            // Strip stock count
                            text = text.replace(/\\s*\\d+\\s*left$/i, '').trim();
                            
                            // Final strict validation
                            if (text.length > 8) continue;
                            
                            if (!allSizes.includes(text)) {
                                allSizes.push(text);
                                
                                // OOS Logic for buttons
                                const innerDiv = btn.querySelector('div');
                                let is_oos = isOOS(btn) || (innerDiv && isOOS(innerDiv));
                                
                                if (!is_oos) {
                                    if (!purchasableSizes.includes(text)) purchasableSizes.push(text);
                                }
                            }
                        }
                    }
                    
                    
                    const debugInfo = [];
                    
                    return { 
                        allSizes: [...new Set(allSizes)], 
                        purchasableSizes: [...new Set(purchasableSizes)],
                        debug: (() => {
                            // Re-run minimal logic just to capture debug info for the log
                            const res = [];
                            const seen = new Set();
                             // Strategy 1 & 2 merged for debug
                            const allCandidates = [...document.querySelectorAll('a[href*="swatchAttr"]'), ...document.querySelectorAll('ul._1q8vHb li, div._3OiksN, div.CDDksN, div.h_12p, .size-buttons div')];
                            
                            for (let el of allCandidates) {
                                let text = el.innerText.replace(/[\\n\\r]+/g, ' ').trim();
                                if (text.length > 20) continue;
                                text = text.replace(/\\s*\\d+\\s*left$/i, '').trim();
                                if (/^[a-z]+\\d+$/i.test(text)) text = text.replace(/\\d+$/, '');
                                if (text.length > 8 || !text) continue;
                                if (/verified|buyer|seller|product|quality|duplicate|elastic|area/i.test(text)) continue;
                                
                                if (seen.has(text)) continue;
                                seen.add(text);
                                
                                const style = window.getComputedStyle(el);
                                const innerDiv = el.querySelector('div');
                                const innerStyle = innerDiv ? window.getComputedStyle(innerDiv) : null;
                                
                                let border = style.borderStyle;
                                if (border === 'none' && innerStyle) border = innerStyle.borderStyle;
                                
                                let is_oos_val = isOOS(el) || (innerDiv && isOOS(innerDiv));
                                
                                res.push({
                                    size: text,
                                    status: is_oos_val ? 'OOS' : 'IN_STOCK',
                                    border: border,
                                    reason: is_oos_val ? (border.includes('dashed') ? 'dashed_border' : 'other_css') : 'solid/none'
                                });
                            }
                            return res;
                        })()
                    };
                }""")
            available_sizes = size_data.get("allSizes", [])
            purchasable_sizes = size_data.get("purchasableSizes", [])
            size_debug = size_data.get("debug", [])
            
            logger.info(f"📏 All sizes detected: {available_sizes}")
            logger.info(f"🛒 Purchasable sizes: {purchasable_sizes}")
            
            if size_debug:
                 logger.info("🔍 Size Confidence & Style Debug:")
                 for d in size_debug:
                     logger.info(f"   • Size {d['size']:<3} : {d['status']:<8} (Border: {d['border']}, Reason: {d['reason']})")

        except Exception as e:
            logger.warning(f"⚠️ Failed to extract available sizes: {e}")
        
        # ================================================================
        # SIZE SELECTION (if target_size provided)
        # ================================================================
        selected_size = None
        
        if target_size:
            logger.info(f"🎯 Target size requested: {target_size}")
            
            # ENHANCED CHECK: Does target size (or its equivalents) exist?
            normalized_target = normalize_size(target_size)
            normalized_available = [normalize_size(s) for s in available_sizes]
            
            # Get all valid equivalents for the target size (e.g. XL -> XL, 95, 100)
            target_equivalents = get_size_equivalents(target_size)
            
            # DEBUG: Show normalization details
            logger.info(f"🔍 Size matching debug:")
            logger.info(f"   Target (raw): '{target_size}'")
            logger.info(f"   Target (normalized): '{normalized_target}'")
            logger.info(f"   Target Equivalents: {target_equivalents}")
            logger.info(f"   Available (raw): {available_sizes}")
            logger.info(f"   Available (normalized): {normalized_available}")
            
            # Find the first equivalent that exists in available sizes
            matched_normalized_size = None
            for equiv in target_equivalents:
                if equiv in normalized_available:
                    matched_normalized_size = equiv
                    logger.info(f"✅ Match found: Target '{target_size}' matches available '{equiv}'")
                    break
            
            # BUG 3 FIX: Validate FREESIZE by checking for "Free Size" text on page
            if not matched_normalized_size and not available_sizes and normalized_target == "FREESIZE":
                page_text = page.evaluate("() => document.body.innerText") or ""
                free_size_indicators = ["free size", "freesize", "one size", "f (free size)", "free"]
                has_free_size_text = any(ind in page_text.lower() for ind in free_size_indicators)
                
                if has_free_size_text:
                    logger.info(f"ℹ️ No sizes found + 'Free Size' text confirmed. Implicit FREESIZE match.")
                    matched_normalized_size = "FREESIZE"
                    matching_raw_size = None  # No button to click
                else:
                    logger.warning(f"⚠️ No sizes found and no 'Free Size' text. NOT assuming FREESIZE.")
                    html = page.content()
                    return html, available_sizes, purchasable_sizes, None, "Out of Stock", None, None, None, None, api_captured_data
            
            elif not matched_normalized_size:
                logger.error(f"❌ Target size '{target_size}' (and equivalents) NOT FOUND in available sizes")
                # Return immediately - size doesn't exist
                html = page.content()
                return html, available_sizes, purchasable_sizes, None, "Out of Stock", None, None, None, None, api_captured_data
            
            # Match target to correct raw size string for clicking
            matching_raw_size = None
            
            if matched_normalized_size == "FREESIZE" and not available_sizes:
                logger.info("ℹ️ Implicit Free Size: No raw size to click")
            else:
                for idx, norm_avail in enumerate(normalized_available):
                    if norm_avail == matched_normalized_size:
                        matching_raw_size = available_sizes[idx]
                        break
                
                # Fallback if something went wrong (shouldn't happen if check passed)
                if not matching_raw_size:
                    matching_raw_size = target_size

            logger.info(f"🎯 Resolved raw size to click: '{matching_raw_size}' (matched via '{matched_normalized_size}')")
            
            # LAYER 1: CSS Fast-Path (before click)
            if matching_raw_size:
                if matching_raw_size not in purchasable_sizes:
                     logger.error(f"❌ LAYER 1 (CSS): Size '{matching_raw_size}' NOT in purchasable_sizes — HARD OOS, skipping click")
                     html = page.content()
                     return html, available_sizes, purchasable_sizes, None, "Out of Stock", None, None, None, None, api_captured_data
                else:
                    logger.info(f"✅ LAYER 1 (CSS): Size '{matching_raw_size}' matches purchasable list")
            
            # BEST-EFFORT CLICK (with Layer 2 verification)
            if matching_raw_size:
                # Human-like delay before clicking (users don't click instantly)
                think_time = random.uniform(0.5, 1.2)
                page.wait_for_timeout(int(think_time * 1000))
                
                logger.info(f"🖱️ Attempting best-effort click on size '{matching_raw_size}'...")
                
                # BEFORE click — capture the target size's expected pid from its href
                # FIX: Extract full href from JS, then parse PID in Python (urllib.parse)
                # This is more robust than JS regex which breaks if URL structure changes
                target_href = page.evaluate("""
                    (targetSize) => {
                        const links = document.querySelectorAll('a[href*="swatchAttr"]');
                        for (let link of links) {
                            let rawText = link.innerText.replace(/[\\n\\r]+/g, ' ').trim();
                            rawText = rawText.replace(/\\s*\\d+\\s*left$/i, '').trim();
                            if (/^[a-z]+\\d+$/i.test(rawText)) rawText = rawText.replace(/\\d+$/, '');
                            if (rawText.toUpperCase() === targetSize.toUpperCase()) {
                                return link.href;  // Return full href, not regex match
                            }
                        }
                        return null;
                    }
                """, matching_raw_size)
                
                # Parse PID from href using urllib.parse (robust against URL format changes)
                target_pid = None
                if target_href:
                    from urllib.parse import urlparse, parse_qs
                    parsed_qs = parse_qs(urlparse(target_href).query)
                    target_pid = parsed_qs.get("pid", [None])[0]
                    logger.info(f"🔗 Extracted target PID from href: {target_pid}")
                
                try:
                    clicked = page.evaluate("""
                        (targetSize) => {
                            const tUpper = targetSize.toUpperCase();
                            
                            // ── Strategy 1: New UI — a[href*="swatchAttr"] ──
                            const newLinks = document.querySelectorAll('a[href*="swatchAttr"]');
                            for (let link of newLinks) {
                                let rawText = link.innerText.replace(/[\\n\\r]+/g, ' ').trim();
                                if (rawText.toLowerCase().includes('ask')) continue;
                                
                                // Strip stock-count suffix
                                rawText = rawText.replace(/\s*\d+\s*left$/i, '').trim();
                                if (/^[a-z]+\d+$/i.test(rawText)) {
                                    rawText = rawText.replace(/\d+$/, '');
                                }
                                
                                if (rawText.toUpperCase() === tUpper) {
                                    link.click();
                                    return true;
                                }
                            }
                            
                            // ── Strategy 2: Old UI — a[href*="sattr"] ──
                            const oldLinks = document.querySelectorAll('a[href*="sattr"][href*="size"]');
                            for (let link of oldLinks) {
                                const linkText = link.textContent.trim();
                                if (linkText === targetSize || linkText.toUpperCase() === tUpper) {
                                    link.click();
                                    return true;
                                }
                            }
                            return false;
                        }
                    """, matching_raw_size)
                    
                    if clicked:
                        logger.info(f"✅ Size '{target_size}' clicked")
                        selected_size = target_size
                        
                        # VARIANT STABILIZATION WAIT
                        # FIX 20: The simple timeout wasn't enough for React to re-render
                        # the price DOM. Use networkidle + explicit DOM change detection.
                        logger.info("⏳ Variant stabilization wait...")
                        
                        # Wait for network to settle (API calls from size change)
                        try:
                            page.wait_for_load_state('networkidle', timeout=8000)
                            logger.info("✅ Network settled after size click")
                        except Exception:
                            logger.info("⚠️ Network didn't fully settle, continuing...")
                        
                        # Additional wait for React re-render
                        page.wait_for_timeout(1500)
                        
                        # LAYER 2: URL Verification (after click)
                        if target_pid:
                            current_url = page.url
                            if target_pid not in current_url:
                                logger.error(f"❌ LAYER 2 (URL): Ghost click — pid {target_pid} NOT in URL → OOS")
                                html = page.content()
                                return html, available_sizes, purchasable_sizes, None, "Out of Stock", None, None, None, None, api_captured_data
                            else:
                                logger.info(f"✅ LAYER 2 (URL): Verified — pid {target_pid} found in URL")
                        
                    else:
                        logger.warning(f"⚠️ Could not click size '{target_size}' - element not found")
                        logger.error(f"❌ Click failed for available size '{target_size}' - returning Out of Stock")
                        html = page.content()
                        return html, available_sizes, purchasable_sizes, None, "Out of Stock", None, None, None, None, api_captured_data
                        
                except Exception as e:
                    logger.warning(f"⚠️ Size click failed: {e}")
                    logger.error(f"❌ Exception during click → HARD Out of Stock")
                    html = page.content()
                    return html, available_sizes, purchasable_sizes, None, "Out of Stock", None, None, None, None, api_captured_data
            else:
                # Implicit match - treat as selected
                selected_size = target_size
                logger.info(f"✅ Implicit match for '{target_size}' - treated as selected")
        
        # ================================================================
        # BUY BOX EVALUATION (AUTHORITATIVE)
        # ================================================================
        # NOTE: We only reach here if:
        # 1. No target size was specified, OR
        # 2. Target size was successfully clicked
        logger.info("📦 Evaluating Buy Box for stock status...")
        stock_status = evaluate_buy_box_stock_status(page, retry_on_ambiguous=True)
        
        # ================================================================
        # GET HTML FOR DATA EXTRACTION
        # ================================================================
        # ================================================================
        # GET HTML AND PRICE
        # ================================================================
        html = page.content()
        if len(html) > MAX_HTML_SIZE:
            html = html[:MAX_HTML_SIZE]
            
        # Extract price via Playwright (more reliable)
        price_text = None
        try:
            # Strategy 1: Common Selectors (old UI)
            selectors = [
                "div.Nx9bqj",       # Current standard
                "div._30jeq3",      # Previous standard
                "div.CxhGGd",       # Variant
                "div.CEmiEU",       # Mobile/App view
                "div.DiRJPp"        # Another variant
            ]
            
            for sel in selectors:
                el = page.locator(sel).first
                if el.is_visible():
                    text = el.inner_text().strip()
                    if text and ("₹" in text or any(c.isdigit() for c in text)):
                        price_text = text
                        logger.info(f"💰 Price extracted via Selector '{sel}': {price_text}")
                        break
            
            # Strategy 2: Semantic JS - find ₹ element with large font (works on new UI)
            # FIX 6: Exclude elements inside popups, modals, overlays
            # FIX 19: Scope to main product pricing area (top 600px) to avoid
            #   picking up prices from recommended products, combos, or other sellers.
            #   Also: prefer the FIRST large-font ₹ element (closest to top) rather than
            #   the absolute largest font, since recommendation widgets sometimes use bigger fonts.
            if not price_text:
                price_result = page.evaluate("""() => {
                    const els = document.querySelectorAll('div, span');
                    let candidates = [];
                    for (let el of els) {
                        const text = el.innerText.trim();
                        // Match ₹ followed by digits, max 15 chars (avoids picking up paragraphs)
                        if (/^₹[\d,]+$/.test(text) && el.offsetHeight > 0) {
                            // FIX 6: Skip elements inside popups/modals/overlays
                            if (el.closest('.popup, .modal, .overlay, [role="dialog"], [role="alertdialog"], .loginModal')) continue;
                            // Skip line-through (MRP) elements
                            const style = window.getComputedStyle(el);
                            if (style.textDecorationLine && style.textDecorationLine.includes('line-through')) continue;
                            const parent = el.parentElement;
                            if (parent) {
                                const pStyle = window.getComputedStyle(parent);
                                if (pStyle.textDecorationLine && pStyle.textDecorationLine.includes('line-through')) continue;
                            }
                            const rect = el.getBoundingClientRect();
                            const fontSize = parseFloat(style.fontSize);
                            candidates.push({ text, fontSize, top: Math.round(rect.top), left: Math.round(rect.left), tag: el.tagName });
                        }
                    }
                    if (candidates.length === 0) return { price: null, debug: [] };
                    
                    // FIX 19: First, try candidates in the main product area (top 600px)
                    const mainArea = candidates.filter(c => c.top < 600 && c.top > 50);
                    let chosen = null;
                    if (mainArea.length > 0) {
                        // Among main area candidates, pick the one with largest font
                        mainArea.sort((a, b) => b.fontSize - a.fontSize);
                        chosen = mainArea[0].text;
                    } else {
                        // Fallback: pick largest font anywhere
                        candidates.sort((a, b) => b.fontSize - a.fontSize);
                        chosen = candidates[0].text;
                    }
                    return { price: chosen, debug: candidates.slice(0, 10) };
                }""")
                if price_result and isinstance(price_result, dict):
                    # LOG ALL CANDIDATES for debugging
                    debug_candidates = price_result.get('debug', [])
                    if debug_candidates:
                        logger.info(f"💰 Price candidates found ({len(debug_candidates)}):")
                        for c in debug_candidates:
                            logger.info(f"   → {c['text']} | font={c['fontSize']}px | top={c['top']} left={c['left']} | {c['tag']}")
                    price_text = price_result.get('price')
                    if price_text:
                        logger.info(f"💰 Price extracted via Semantic JS (font-size): {price_text}")
                    
        except Exception as e:
            logger.warning(f"⚠️ Price extraction via Playwright failed: {e}")
            
            # BUG 2 FIX: Moved JS fallback into this except block (was unreachable dead code before)
            try:
                if not price_text:
                    price_text = page.evaluate("""() => {
                        // 1. Try finding specific price classes directly
                        const priceSelectors = ['div.Nx9bqj', 'div._30jeq3', 'div.CxhGGd'];
                        for (let sel of priceSelectors) {
                            const el = document.querySelector(sel);
                            if (el && el.offsetHeight > 0) return el.innerText;
                        }

                        // 2. Strict Search: Find element containing ONLY price
                        const allDivs = document.querySelectorAll('div');
                        for (let div of allDivs) {
                            if (/^₹\d{1,3}(,\d{3})*$/.test(div.innerText.trim()) && div.offsetHeight > 0) {
                                return div.innerText;
                            }
                        }
                        
                        return null;
                    }""")
                    
                    if price_text:
                        logger.info(f"💰 Price extracted via JS Fallback: {price_text}")
            except Exception as e2:
                logger.warning(f"⚠️ JS Fallback price extraction also failed: {e2}")
            
        if not price_text:
            logger.warning("⚠️ Could not extract price using any method")
        
        # ----------------------------------------------------------------
        # 2. MRP Extraction
        # ----------------------------------------------------------------
        mrp_text = None
        try:
            mrp_selectors = [
                "div.yRaY8j",      # New MRP
                "div._3I9_wc",     # Old MRP
                "div._27UcVY", 
                "div._30jeq3 + div"
            ]
            for sel in mrp_selectors:
                el = page.locator(sel).first
                if el.is_visible():
                    text = el.inner_text().strip()
                    # MRP often comes as "₹5,999" but might have strike-through
                    if text and ("₹" in text or any(c.isdigit() for c in text)):
                        # Check bounding box to ensure it's not a footer/cross-sell item
                        try:
                            box = el.bounding_box()
                            if box and box['y'] > 800:
                                logger.debug(f"Skipping MRP {text} because it is too far down the page (y={box['y']})")
                                continue 
                        except Exception:
                            pass
                        
                        mrp_text = text
                        logger.info(f"🏷️ MRP extracted via Selector '{sel}': {mrp_text}")
                        break

            
            # Strategy 2: JS Evaluation (MRP Fallback)
            if not mrp_text:
                mrp_result = page.evaluate("""() => {
                    const els = document.querySelectorAll('div, span');
                    let candidates = [];
                    for (let el of els) {
                        const style = window.getComputedStyle(el);
                        // Look for strikethrough AND ₹ symbol
                        if (style.textDecoration.includes('line-through') && el.innerText.includes('₹')) {
                            const rect = el.getBoundingClientRect();
                            // Skip hidden elements or elements Way down the page (cross-sells)
                            if (rect.top > 0 && rect.top < 800) {
                                candidates.push({
                                    text: el.innerText.trim(),
                                    top: rect.top,
                                    fontSize: parseFloat(style.fontSize) || 0
                                });
                            }
                        }
                    }
                    if (candidates.length > 0) {
                        // Sort by how close they are to the top of the page (primary product area)
                        candidates.sort((a, b) => a.top - b.top);
                        // Often the FIRST strikethrough price near the top is the actual MRP
                        return candidates[0].text;
                    }
                    return null;
                }""")
                if mrp_result:
                    mrp_text = mrp_result
                    logger.info(f"🏷️ MRP extracted via JS Bounding Box Fallback: {mrp_text}")
        except Exception as e: 
            logger.debug(f"MRP Extraction failed: {e}")

        # ----------------------------------------------------------------
        # 3. Rating Extraction
        # ----------------------------------------------------------------
        rating_text = None
        try:
            rating_selectors = ["div.XQDdHH", "div._3LWZlK", "div._31DaGX"]
            for sel in rating_selectors:
                el = page.locator(sel).first
                if el.is_visible():
                    text = el.inner_text().strip()
                    if text and text[0].isdigit():
                        rating_text = text
                        logger.info(f"⭐ Rating extracted via Selector '{sel}': {rating_text}")
                        break
            
            if not rating_text:
                rating_text = page.evaluate("""() => {
                    // Fallback: Find pattern like "4.2" in short divs
                    const divs = document.querySelectorAll('div');
                    for (let div of divs) {
                        const t = div.innerText.trim();
                        // 1.0 to 5.0
                        if (/^[1-5]\\.\\d$/.test(t) && t.length < 6) {
                            return t;
                        }
                    }
                    return null;
                }""")
                if rating_text: logger.info(f"⭐ Rating extracted via JS Fallback: {rating_text}")

        except Exception: pass

        # ----------------------------------------------------------------
        # 4. Review Count Extraction
        # ----------------------------------------------------------------
        review_text = None
        try:
            review_selectors = ["span.Wphh3N", "span._2_R_DZ"]
            for sel in review_selectors:
                el = page.locator(sel).first
                if el.is_visible():
                    review_text = el.inner_text().strip()
                    logger.info(f"💬 Reviews extracted via Selector '{sel}': {review_text}")
                    break
            
            if not review_text:
                review_text = page.evaluate("""() => {
                    // Fallback: Find "Ratings & Reviews" text
                    const spans = document.querySelectorAll('span');
                    for (let span of spans) {
                        const t = span.innerText;
                        if (t.includes('Ratings') && t.includes('Reviews')) {
                            return t;
                        }
                    }
                    return null;
                }""")
                if review_text: logger.info(f"💬 Reviews extracted via JS Fallback: {review_text}")

        except Exception: pass
        
        # Log API interception summary
        if api_captured_data:
            api_keys = [k for k, v in api_captured_data.items() if v is not None and v != [] and v != 0]
            logger.info(f"📡 API INTERCEPTION SUMMARY: {_api_response_count[0]} responses captured, fields: {api_keys}")
            if api_captured_data.get('price'):
                logger.info(f"📡 API Price: {api_captured_data['price']}, API MRP: {api_captured_data.get('mrp')}")
        else:
            logger.info(f"📡 API INTERCEPTION: {_api_response_count[0]} responses checked, no product data found")
        
        logger.info(f"✅ Fetch complete - Stock: {stock_status}, Selected Size: {selected_size}")
        return html, available_sizes, purchasable_sizes, selected_size, stock_status, price_text, mrp_text, rating_text, review_text, api_captured_data
        
    except PlaywrightTimeout as e:
        logger.error(f"❌ Playwright timeout: {e}")
        return None, [], [], None, "Unknown", None, None, None, None, {}
    except PlaywrightError as e:
        logger.error(f"❌ Playwright error: {e}")
        return None, [], [], None, "Unknown", None, None, None, None, {}
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        return None, [], [], None, "Unknown", None, None, None, None, {}
        
    finally:
        # FIX 8: Added explicit logging on cleanup failure
        try:
            if page:
                page.close()
        except Exception as e:
            logger.warning(f"⚠️ page.close() failed: {e}")
        
        try:
            if context:
                context.close()
        except Exception as e:
            logger.warning(f"⚠️ context.close() failed: {e}")
        
        try:
            if browser:
                browser.close()
        except Exception as e:
            logger.warning(f"⚠️ browser.close() failed: {e}")
        
        try:
            if playwright_instance:
                playwright_instance.stop()
        except Exception as e:
            logger.warning(f"⚠️ playwright.stop() failed: {e}")
        
        del page, context, browser, playwright_instance
        gc.collect()


# =============================================================================
# MAIN SCRAPER
# =============================================================================

def scrape_single_url_internal(url: str, target_size: str = None, attempt_number: int = 1) -> Dict[str, Any]:
    """
    Internal scraper function (single attempt).
    This is called by scrape_single_url which handles retries.
    """
    url_hash = generate_url_hash(url)
    
    is_valid, validation_msg = validate_url(url)
    if not is_valid:
        return {
            "url_hash": url_hash,
            "error": validation_msg,
            "scraped_at": datetime.now().isoformat(),
            "ui_type": "unknown",
            "attempt_number": attempt_number,
        }
    
    html = None
    soup = None
    
    try:
        # ================================================================
        # NEW WORKFLOW: fetch_with_playwright_combined returns stock directly
        # ================================================================
        # Fetch with Playwright (returns HTML + Sizes + Stock + Price + MRP + Rating + Review + API Data)
        html, available_sizes, purchasable_sizes, selected_size, stock_status, raw_price, raw_mrp, raw_rating, raw_review, api_captured_data = fetch_with_playwright_combined(url, target_size)
        
        if not html:
            raise Exception("Failed to fetch")
        
        # ================================================================
        # UI TYPE DETECTION (for logging - both UIs now supported)
        # ================================================================
        ui_type = detect_ui_type(html)
        logger.info(f"🔍 Detected UI type: {ui_type}")
        
        try:
            soup = BeautifulSoup(html, "lxml")
        except:
            soup = BeautifulSoup(html, "html.parser")
        
        # ================================================================
        # Extract data from soup (NO STOCK - already determined)
        # ================================================================
        product = extract_data_from_soup(soup, html, url, url_hash)
        
        # ================================================================
        # Add size and stock information from Playwright
        # ================================================================
        product["available_sizes"] = available_sizes
        product["purchasable_sizes"] = purchasable_sizes
        product["size"] = selected_size
        product["instock"] = stock_status  # AUTHORITATIVE from Buy Box
        product["stock_detection_method"] = "buy_box_authoritative"
        product["ui_type"] = ui_type
        product["attempt_number"] = attempt_number
        
        # ================================================================
        # MULTI-SOURCE RECONCILIATION ENGINE (5-Layer)
        # ================================================================
        logger.info("🔬 Starting multi-source reconciliation...")
        
        # Build Playwright source data
        playwright_source = {}
        if raw_price:
            pw_price, pw_curr = _parse_price(raw_price)
            if pw_price:
                playwright_source["price"] = pw_price
                playwright_source["currency"] = pw_curr or "INR"
        if raw_mrp:
            pw_mrp, _ = _parse_price(raw_mrp)
            if pw_mrp:
                playwright_source["mrp"] = pw_mrp
        if raw_rating:
            try:
                pw_rating = float(raw_rating)
                if 0 < pw_rating <= 5:
                    playwright_source["rating"] = pw_rating
            except (ValueError, TypeError):
                pass
        if raw_review:
            playwright_source["reviews"] = safe_truncate(_norm(raw_review), 100)
        playwright_source["stock"] = stock_status
        
        # Build API source data (from network interception)
        api_source = {}
        if api_captured_data:
            if api_captured_data.get("price"):
                api_source["price"] = api_captured_data["price"]
            if api_captured_data.get("mrp"):
                api_source["mrp"] = api_captured_data["mrp"]
            if api_captured_data.get("title"):
                api_source["title"] = api_captured_data["title"]
            if api_captured_data.get("brand"):
                api_source["brand"] = api_captured_data["brand"]
            if api_captured_data.get("category"):
                api_source["category"] = api_captured_data["category"]
            if api_captured_data.get("rating"):
                api_source["rating"] = api_captured_data["rating"]
            if api_captured_data.get("reviews"):
                api_source["reviews"] = api_captured_data["reviews"]
            if api_captured_data.get("images"):
                api_source["images"] = api_captured_data["images"]
            if api_captured_data.get("stock"):
                api_source["stock"] = api_captured_data["stock"]
            if api_captured_data.get("seller"):
                api_source["seller"] = api_captured_data["seller"]
            if api_captured_data.get("discount"):
                api_source["discount"] = api_captured_data["discount"]
        
        # Get structured sources from extract_data_from_soup
        structured = product.pop("_structured_sources", {})
        
        # Assemble all 5 sources
        all_sources = {
            "api": api_source,
            "playwright": playwright_source,
            "initial_state": structured.get("initial_state", {}),
            "json_ld": structured.get("json_ld", {}),
            "soup": structured.get("soup", {}),
        }
        
        # Log source availability
        active_sources = [name for name, data in all_sources.items() if data]
        logger.info(f"📊 Active data sources: {active_sources}")
        
        # Run reconciliation
        reconciled, field_confidence, field_method = _reconcile_all_fields(all_sources)
        
        # Apply reconciled data to product
        if reconciled.get("price"):
            product["price"] = reconciled["price"]
            product["currency"] = playwright_source.get("currency") or api_source.get("currency") or product.get("currency") or "INR"
        if reconciled.get("mrp"):
            product["mrp"] = reconciled["mrp"]
        if reconciled.get("title") and len(str(reconciled["title"])) > len(str(product.get("title", ""))):
            product["title"] = reconciled["title"]
        if reconciled.get("brand") and not product.get("brand"):
            product["brand"] = reconciled["brand"]
        if reconciled.get("category") and not product.get("category"):
            product["category"] = reconciled["category"]
        if reconciled.get("rating"):
            product["rating"] = reconciled["rating"]
        if reconciled.get("reviews"):
            product["reviews"] = reconciled["reviews"]
        if reconciled.get("images"):
            # Merge: keep existing + add new unique from reconciliation
            existing_imgs = set(product.get("images", []))
            for img in reconciled["images"]:
                if img not in existing_imgs:
                    product.setdefault("images", []).append(img)
                    existing_imgs.add(img)
        if reconciled.get("specs"):
            # Merge specs: reconciled overrides existing
            existing_specs = product.get("specs", {})
            existing_specs.update(reconciled["specs"])
            product["specs"] = existing_specs
        
        # Stock: Buy Box stays AUTHORITATIVE, but API can confirm
        product["instock"] = stock_status  # Always trust Buy Box
        if stock_status == "Unknown" and reconciled.get("stock"):
            product["instock"] = reconciled["stock"]
            logger.info(f"📦 Stock fallback from reconciliation: {reconciled['stock']}")
        
        # Add seller info if available from API
        if api_captured_data.get("seller"):
            product["seller"] = api_captured_data["seller"]
        
        # Add discount info if available
        if api_captured_data.get("discount"):
            product["discount"] = api_captured_data["discount"]
        
        # ================================================================
        # CONFIDENCE SCORING & AUDIT TRAIL
        # ================================================================
        product["data_sources"] = all_sources
        product["field_confidence"] = field_confidence
        product["field_method"] = field_method
        product["reconciliation_method"] = "multi_source_5_layer"
        
        # Overall confidence: based on price and MRP confidence
        price_conf = field_confidence.get("price", "none")
        mrp_conf = field_confidence.get("mrp", "none")
        if price_conf == "high" or api_source.get("price"):
            product["price_confidence"] = "high"
        elif price_conf == "medium":
            product["price_confidence"] = "medium"
        else:
            product["price_confidence"] = "low"
        
        logger.info(f"📊 FINAL: Price={product.get('price')} (conf={product['price_confidence']}), MRP={product.get('mrp')} (conf={mrp_conf})")
        logger.info(f"📊 Methods: price={field_method.get('price')}, mrp={field_method.get('mrp')}")
        
        product["stock_detection_method"] = "buy_box_authoritative"
        
        if selected_size:
            product["size_selection_status"] = "SUCCESS"
        elif target_size:
            product["size_selection_status"] = "FAILED"
        else:
            product["size_selection_status"] = "NO_TARGET"
        
        # BUG 9 FIX: Explicitly set should_retry for React Native Web UI
        # This enables the retry circuit breaker in scrape_single_url to actually works
        if ui_type == "react_native_web":
            product["should_retry"] = True
        else:
            product["should_retry"] = False
            
        product["extraction_method"] = "playwright_combined"
        product["scraped_at"] = datetime.now().isoformat()
        product["target_size_requested"] = safe_truncate(target_size, 100) if target_size else None
        product["success"] = True
        
        return product
                
    except Exception as e:
        error_message = str(e)
        
        # Check if this is a 500 error
        if "500 Internal Server Error" in error_message:
            logger.error(f"❌ CRITICAL: {error_message}")
            return {
                "url_hash": url_hash,
                "error": "Flipkart 500 Internal Server Error - Bot detection or server issue",
                "error_details": error_message,
                "scraped_at": datetime.now().isoformat(),
                "success": False,
                "attempt_number": attempt_number,
                "ui_type": "unknown"
            }
        
        # Generic error
        logger.error(f"❌ Scraping failed: {error_message}")
        return {
            "url_hash": url_hash,
            "error": "Scraping failed",
            "error_details": error_message,
            "scraped_at": datetime.now().isoformat(),
            "success": False,
            "attempt_number": attempt_number,
            "ui_type": "unknown"
        }
    finally:
        if soup:
            soup.decompose()
        del soup, html
        gc.collect()


def scrape_single_url(url: str, target_size: str = None) -> Dict[str, Any]:
    """
    Combined scraper with UI detection and retry logic.
    
    RETRY LOGIC:
    - Detects if response has React Native Web UI (bad UI)
    - Retries up to MAX_RETRY_ATTEMPTS (5) times to get classical UI
    - Returns failure after max retries
    
    Returns:
        Product data dict with success flag
    """
    MAX_RETRY_ATTEMPTS = 3  # FIX 7: Reduced from 5 — circuit breaker for persistent React Native UI
    RETRY_DELAY_SECONDS = 2  # Wait between retries
    
    for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
        logger.info(f"🔄 Scrape attempt {attempt}/{MAX_RETRY_ATTEMPTS} for URL: {url[:100]}...")
        
        result = scrape_single_url_internal(url, target_size, attempt_number=attempt)
        
        # Check if we should retry
        should_retry = result.get("should_retry", False)
        ui_type = result.get("ui_type", "unknown")
        
        if result.get("success", False):
            # Success! Got classical UI and extracted data
            logger.info(f"✅ Successfully scraped with {ui_type} UI on attempt {attempt}")
            return result
        
        if should_retry and ui_type == "react_native_web":
            # Got bad UI, retry
            logger.warning(f"⚠️ React Native Web UI detected on attempt {attempt}. Retrying...")
            
            if attempt < MAX_RETRY_ATTEMPTS:
                # Add delay before retry (with slight randomization)
                delay = RETRY_DELAY_SECONDS + random.uniform(0, 1)
                logger.info(f"⏳ Waiting {delay:.1f}s before retry...")
                time.sleep(delay)
                continue
            else:
                # Max retries reached with bad UI
                logger.error(f"❌ MAX RETRIES REACHED ({MAX_RETRY_ATTEMPTS}). Unable to fetch classical UI.")
                url_hash = generate_url_hash(url)
                return {
                    "url_hash": url_hash,
                    "error": "Unable to fetch details - React Native Web UI persisted after max retries",
                    "error_details": f"Attempted {MAX_RETRY_ATTEMPTS} times but only got React Native Web UI",
                    "ui_type": ui_type,
                    "total_attempts": MAX_RETRY_ATTEMPTS,
                    "scraped_at": datetime.now().isoformat(),
                    "success": False
                }
        else:
            # Other error (not UI-related), don't retry
            logger.error(f"❌ Non-retryable error on attempt {attempt}: {result.get('error', 'Unknown error')}")
            return result
    
    # Fallback (should never reach here)
    url_hash = generate_url_hash(url)
    return {
        "url_hash": url_hash,
        "error": "Unable to fetch details - Unexpected retry loop exit",
        "scraped_at": datetime.now().isoformat(),
        "success": False,
        "total_attempts": MAX_RETRY_ATTEMPTS
    }

# =============================================================================
# LAMBDA HANDLER
# =============================================================================

def lambda_handler(event, context):
    """AWS Lambda handler"""
    try:
        if isinstance(event.get("body"), str):
            try:
                body = json.loads(event["body"])
            except json.JSONDecodeError:
                return {
                    "statusCode": 400,
                    "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                    "body": json.dumps({"error": "Invalid JSON"})
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
                "body": ""
            }
        
        url = body.get("url")
        target_size = body.get("size") or body.get("target_size")
        
        if not url:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"error": "No URL provided"})
            }
        
        product = scrape_single_url(url, target_size)
        
        # BUG 13 FIX: Use product.get("success") instead of checking for "error" key
        # to match the logic used throughout the scraper
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
            "body": json.dumps(response_data, ensure_ascii=False)
        }
    
    except Exception:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({
                "success": False,
                "error": "Internal server error"
            })
        }
    finally:
        gc.collect()

# =============================================================================
# LOCAL TESTING
# =============================================================================

if __name__ == "__main__":
    print("\n" + "="*80)
    print("COMBINED FLIPKART SCRAPER - Enhanced Size Selection + Comprehensive Data")
    print("="*80 + "\n")
    

    TEST_URL = "https://www.flipkart.com/w-women-solid-straight-kurta/p/itmfds3bf2vccyhj?pid=KTAFDSFXUCBMXD3Q"
    TARGET_SIZE = "S"

    print(f"URL: {TEST_URL}")
    print(f"Target Size: {TARGET_SIZE}\n")
    
    start_time = time.time()
    result = scrape_single_url(TEST_URL, target_size=TARGET_SIZE)
    elapsed_time = time.time() - start_time
    
    print("\n" + "="*80)
    print("RESULTS")
    print("="*80 + "\n")
    
    # ================================================================
    # CHECK FOR ERRORS FIRST
    # ================================================================
    if 'error' in result:
        print("❌" * 40)
        print("🚨 SCRAPING FAILED - ERROR DETECTED 🚨")
        print("❌" * 40)
        print(f"\n⚠️  Error: {result.get('error', 'Unknown error')}")
        if result.get('error_details'):
            print(f"\n📋 Details:\n{result.get('error_details')}\n")
        print(f"\n⏱️  Execution Time: {elapsed_time:.2f}s")
        print(f"📅 Timestamp: {result.get('scraped_at', 'N/A')}")
        
        # Save error result
        output_file = "combined_flipkart_result.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"\n💾 Error details saved to: {output_file}")
        
        print("\n" + "="*80)
        print("⚠️  PROCESS TERMINATED DUE TO ERROR")
        print("="*80 + "\n")
        exit(1)  # Exit with error code
    
    # ================================================================
    # SUCCESS PATH - Display Results
    # ================================================================
    
    print(f"⏱️  Execution Time: {elapsed_time:.2f}s")
    print(f"✅ Success: {'error' not in result}")
    print(f"\n📊 PRODUCT INFORMATION:")
    print(f"   Title: {result.get('title', 'N/A')}")
    print(f"   Brand: {result.get('brand', 'N/A')}")
    print(f"   Category: {result.get('category', 'N/A')}")
    
    print(f"\n💰 PRICING:")
    print(f"   Price: ₹{result.get('price', 'N/A')}")
    print(f"   MRP: ₹{result.get('mrp', 'N/A')}")
    print(f"   Currency: {result.get('currency', 'N/A')}")
    print(f"   In Stock: {result.get('instock', 'N/A')}")
    
    print(f"\n📏 SIZE INFORMATION:")
    print(f"   Size Selection Status: {result.get('size_selection_status', 'N/A')}")
    print(f"   Selected Size: {result.get('size', 'N/A')}")
    print(f"   Available Sizes: {result.get('available_sizes', [])}")
    print(f"   Target Size Requested: {result.get('target_size_requested', 'N/A')}")
    
    if result.get('price_extraction_method'):
        print(f"   Price Extraction Method: {result.get('price_extraction_method')}")
    
    print(f"\n⭐ RATINGS & REVIEWS:")
    print(f"   Rating: {result.get('rating', 'N/A')}")
    print(f"   Reviews: {result.get('reviews', 'N/A')}")
    
    print(f"\n🖼️  IMAGES:")
    print(f"   Total Images: {len(result.get('images', []))}")
    if result.get('images'):
        for i, img in enumerate(result.get('images', [])[:3], 1):
            print(f"   Image {i}: {img[:80]}...")
    
    print(f"\n📋 SPECIFICATIONS:")
    specs = result.get('specs', {})
    print(f"   Total Specs: {len(specs)}")
    if specs:
        for key, value in list(specs.items())[:5]:
            print(f"   {key}: {value}")
    
    print(f"\n📝 DESCRIPTION:")
    desc = result.get('description', '')
    if desc:
        print(f"   {desc[:200]}{'...' if len(desc) > 200 else ''}")
    
    print(f"\n🔧 METADATA:")
    print(f"   Extraction Method: {result.get('extraction_method', 'N/A')}")
    print(f"   Scraped At: {result.get('scraped_at', 'N/A')}")
    print(f"   URL Hash: {result.get('url_hash', 'N/A')}")
    print(f"   Domain: {result.get('domain', 'N/A')}")
    
    # Save to file
    output_file = "combined_flipkart_result.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    print(f"\n✅ Full results saved to: {output_file}")
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    
    success_items = []
    if result.get('title'):
        success_items.append("✅ Title extracted")
    if result.get('price'):
        success_items.append("✅ Price extracted")
    if result.get('available_sizes'):
        success_items.append(f"✅ Found {len(result['available_sizes'])} sizes")
    if result.get('size_selection_status') == 'SUCCESS':
        success_items.append("✅ Target size successfully selected")
    if result.get('images'):
        success_items.append(f"✅ Found {len(result['images'])} images")
    if result.get('specs'):
        success_items.append(f"✅ Extracted {len(result['specs'])} specifications")
    
    for item in success_items:
        print(item)
    
    if result.get('size_selection_status') == 'FAILED' and TARGET_SIZE:
        print(f"\n⚠️  WARNING: Could not select target size '{TARGET_SIZE}'")
        print(f"   Available sizes were: {result.get('available_sizes', [])}")
    
    print("\n" + "="*80 + "\n")
