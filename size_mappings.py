"""
Canonical Size Mapping Layer - Phase 1
Single source of truth for size equivalents across the entire system.

This module provides:
1. Normalized size representations
2. Size equivalents (many-to-many mappings)
3. Consistent size matching logic

Used by:
- flipkart_scraper.py
- similarity.py
- Any other module that needs size matching
"""

import re
from typing import List, Set, Optional, Dict

# =============================================================================
# CANONICAL SIZE MAPPINGS
# =============================================================================

# Letter to Numeric Size Mappings
SIZE_MAPPINGS = {
    "XS": ["70", "75"],
    "S": ["80"],
    "M": ["85"],
    "L": ["90", "95"],
    "XL": ["100", "105"],
    "XXL": ["110"],
    "XXXL": ["115", "120"],
    "XXXXL": ["125"],
}

NUMERIC_TO_LETTER: Dict[str, List[str]] = {}
for letter, numerics in SIZE_MAPPINGS.items():
    for numeric in numerics:
        if numeric not in NUMERIC_TO_LETTER:
            NUMERIC_TO_LETTER[numeric] = []
        NUMERIC_TO_LETTER[numeric].append(letter)

# Size Equivalents (2XL = XXL, etc.)
SIZE_EQUIVALENTS = {
    "2XL": "XXL",
    "3XL": "XXXL",
    "4XL": "XXXXL",
    "5XL": "XXXXXL",
    "ONESIZE": "FREESIZE",
    "ONE SIZE": "FREESIZE",
    "FREE SIZE": "FREESIZE",
    "F SIZE": "FREESIZE",
}

# Special Size Mappings
SIZE_MAPPINGS["FREESIZE"] = ["1", "OS", "FS"]

# Update reverse mapping for FREESIZE
# (Since the loop above ran before we added FREESIZE)
for numeric in SIZE_MAPPINGS["FREESIZE"]:
    if numeric not in NUMERIC_TO_LETTER:
        NUMERIC_TO_LETTER[numeric] = []
    NUMERIC_TO_LETTER[numeric].append("FREESIZE")

# =============================================================================
# NORMALIZATION FUNCTIONS
# =============================================================================

def normalize_size(size: str) -> str:
    """
    Normalize size strings to a canonical format.
    
    Handles:
    - Basic sizes: S, M, L, XL, XXL, XXXL
    - Numeric sizes: 80, 85, 90, 95, 100, 105
    - Size equivalents: 2XL→XXL, 3XL→XXXL
    - Age-based years: "4 - 5 Year" → "4-5Y"
    - Age-based months: "6-9M" → "6-9M"
    - Free Size: "1", "Free Size" → "FREESIZE"
    
    Returns normalized size string for consistent matching.
    """
    if not size:
        return ""
    
    # Convert to uppercase and strip whitespace
    s = str(size).strip().upper()
    
    # Handle "1" specially as Free Size (common scraping artifact)
    if s == "1":
        return "FREESIZE"
    
    # Handle size equivalents first (2XL → XXL)
    if s in SIZE_EQUIVALENTS:
        s = SIZE_EQUIVALENTS[s]
    
    # Normalize age-based sizes (Years)
    # Patterns: "4 - 5 Year", "4 - 5 Years", "5 - 6 Y", "7-8Y"
    year_pattern = r'(\d+)\s*-\s*(\d+)\s*(?:YEARS?|Y|YR)'
    year_match = re.match(year_pattern, s)
    if year_match:
        num1, num2 = year_match.groups()
        return f"{num1}-{num2}Y"
    
    # Normalize age-based sizes (Months)
    # Patterns: "6-9M", "6-9 Months", "6 - 9 M"
    month_pattern = r'(\d+)\s*-\s*(\d+)\s*(?:MONTHS?|M)'
    month_match = re.match(month_pattern, s)
    if month_match:
        num1, num2 = month_match.groups()
        return f"{num1}-{num2}M"

    # Normalize centimeter sizes
    # Patterns: "85 CM", "90cm", "95 cms"
    cm_pattern = r'^(\d+)\s*(?:CM|CMS|CENTIMETERS?)\.?$'
    cm_match = re.match(cm_pattern, s)
    if cm_match:
        return cm_match.group(1)
    
    # Return normalized (uppercase, stripped)
    return s


def get_size_equivalents(size: str) -> List[str]:
    """
    Get all equivalent sizes for a given size.
    
    This is the CORE function that enables many-to-many size matching.
    
    Examples:
        "L" → ["L", "90", "95"]  (letter + numeric equivalents)
        "95" → ["95", "L", "XL"]  (numeric + letter equivalents)
        "4-5Y" → ["4-5Y"]  (age-based, no equivalents)
        "M" → ["M", "85", "90"]
        "85 CM" → ["85", "M"] (normalized first)
    
    Args:
        size: Raw size string (e.g., "L", "95", "4 - 5 Year")
    
    Returns:
        List of equivalent sizes including the normalized input
    """
    if not size:
        return []
    
    # Normalize first
    normalized = normalize_size(size)
    
    # Start with the normalized size itself
    equivalents = [normalized]
    
    # Check if it's a letter size with numeric equivalents
    if normalized in SIZE_MAPPINGS:
        equivalents.extend(SIZE_MAPPINGS[normalized])
    
    # Check if it's a numeric size with letter equivalents
    if normalized in NUMERIC_TO_LETTER:
        equivalents.extend(NUMERIC_TO_LETTER[normalized])
    
    # Remove duplicates while preserving order
    seen = set()
    unique_equivalents = []
    for equiv in equivalents:
        if equiv not in seen:
            seen.add(equiv)
            unique_equivalents.append(equiv)
    
    return unique_equivalents


def get_size_category(size: str) -> Optional[str]:
    """
    Determine the category of a size for partial matching.
    
    Categories:
    - "letter": S, M, L, XL, XXL, etc.
    - "numeric": 80, 85, 90, 95, 100, etc.
    - "age_years": 4-5Y, 6-7Y, etc.
    - "age_months": 6-9M, 12-18M, etc.
    - "shoe_uk": UK 8, UK 9, etc (standard shoe sizes not in mapping)
    - "shoe_us": US 8, US 9, etc
    - "shoe_eur": EUR 42, EUR 43, etc
    
    Returns:
        Category string or None if unknown
    """
    if not size:
        return None
    
    normalized = normalize_size(size)
    
    # Age-based years
    if re.match(r'^\d+-\d+Y$', normalized):
        return "age_years"
    
    # Age-based months
    if re.match(r'^\d+-\d+M$', normalized):
        return "age_months"
    
    # Letter sizes
    if normalized in SIZE_MAPPINGS:
        return "letter"
    
    # Numeric sizes
    if re.match(r'^\d+$', normalized):
        return "numeric"
        
    # Standard Shoe Sizes Check (UK, US, EUR) 
    # Example: "UK 8", "US 9.5", "EUR 42"
    if re.match(r'^UK\s*\d+(\.\d+)?$', normalized):
        return "shoe_uk"
    if re.match(r'^US\s*\d+(\.\d+)?$', normalized):
        return "shoe_us"
    if re.match(r'^(EUR|EU)\s*\d+(\.\d+)?$', normalized):
        return "shoe_eur"
    
    return None


def size_similarity(size1: str, size2: str) -> Optional[float]:
    """
    Enhanced size matching using canonical equivalents with SOFT CONSTRAINT support.
    
    This replaces the old size_similarity() functions in both files.
    
    Matching logic (GRADUATED SCORING):
    1. If either size is missing → None (skip step, don't penalize)
    2. Get equivalents for both sizes
    3. If equivalents overlap → 1.0 (perfect match)
    4. If same category but different size → 0.5 (partial match, soft failure)
    5. If completely unhandled mapping format → -1.0 (ambiguous match, triggers manual review)
    6. Otherwise → 0.0 (no match)
    
    Examples:
        size_similarity("L", "95") → 1.0  (L includes 95)
        size_similarity("L", "M") → 0.5   (both letter sizes, different)
        size_similarity("4-5Y", "4-5 Years") → 1.0  (normalized match)
        size_similarity("XL", "100") → 1.0  (XL includes 100)
        size_similarity("L", "") → None  (missing size, skip)
        size_similarity("80", "85") → 0.5  (both numeric, different)
        size_similarity("85 CM", "M") → 1.0 (normalized match)
        size_similarity("UK 8", "UK 9") → -1.0 (unmapped equivalents/categories)
    
    Args:
        size1: First size (e.g., from Amazon)
        size2: Second size (e.g., from Flipkart)
    
    Returns:
        None if either size is missing (skip step)
        1.0 if sizes match (including equivalents)
        0.5 if same category but different size (partial match)
        -1.0 if unhandled size or unmapped category (triggers ambiguity)
        0.0 if completely different mapped categories
    """
    # Missing size → skip step (don't penalize)
    if not size1 or not size2:
        return None
    
    # Get equivalents for both sizes
    equiv1 = set(get_size_equivalents(size1))
    equiv2 = set(get_size_equivalents(size2))
    
    # Perfect match: equivalents overlap
    if equiv1 & equiv2:
        return 1.0
    
    # Partial match: same category but different size
    # This allows products to still be approved if overall similarity is high
    category1 = get_size_category(size1)
    category2 = get_size_category(size2)
    
    if category1 and category2 and category1 == category2:
        return 0.5  # Soft failure - same category, different size
    
    # Unhandled/Unmapped Size Condition
    # If a size fails categorization entirely, or it falls into a standard 
    # category that isn't fully mapped to 1.0/0.5/0.0 (like custom string sets)
    if category1 is None or category2 is None:
        return -1.0
        
    # Custom Categories that have no explicit 0.5 partial matching rule defined yet
    if (category1 and category1.startswith("shoe_")) or (category2 and category2.startswith("shoe_")):
         return -1.0
    
    # No match: completely different categories
    return 0.0


# =============================================================================
# VALIDATION FUNCTIONS
# =============================================================================

def is_valid_size(size_text: str) -> bool:
    """
    Strict validation to filter out garbage text.
    
    This is imported from flipkart_scraper.py for consistency.
    
    Rejects:
    - "FABRIC QUALITY", "AGE GROUP", "SIZE CHART", etc.
    - Multi-word phrases that aren't valid sizes
    - Common UI labels and instructions
    
    Accepts:
    - Letter sizes: S, M, L, XL, XXL, XXXL
    - Numeric sizes: 80, 85, 90, 95, 100, 105
    - Age-based: "4 - 5 Year", "6-9M", "11-12Y"
    - Combined: "32B", "34C", "36D"
    - Ranges: "28-30", "S-M"
    - CM sizes: "85 CM", "90cms"
    """
    if not size_text or not isinstance(size_text, str):
        return False
    
    s = size_text.strip().upper()
    
    # Length check - real sizes are typically short
    if len(s) > 20:
        return False
    
    # Reject common garbage phrases
    GARBAGE_PHRASES = {
        "SIZE CHART", "SIZE GUIDE", "FABRIC QUALITY", "AGE GROUP",
        "SELECT SIZE", "CHOOSE SIZE", "PICK SIZE", "SIZE INFO",
        "MEASUREMENT", "FIT GUIDE", "PRODUCT INFO", "DETAILS",
        "DESCRIPTION", "SPECIFICATIONS", "MATERIAL", "CARE",
        "WASH CARE", "STYLE", "PATTERN", "COLOR", "COLOUR",
        "BRAND", "SELLER", "DELIVERY", "RETURN", "POLICY",
        "ADD TO CART", "BUY NOW", "NOTIFY ME", "OUT OF STOCK",
        "IN STOCK", "AVAILABLE", "UNAVAILABLE", "SOLD OUT"
    }
    
    if s in GARBAGE_PHRASES:
        return False
    
    # Reject if too many words
    words = s.split()
    if len(words) > 4:
        return False
    
    # If multi-word, check if it matches valid patterns
    if len(words) >= 2:
        # Age-based patterns
        if re.match(r'^\d+\s*-?\s*\d*\s*(YEAR|YEARS|Y|YR|MONTH|MONTHS|M)S?$', s):
            return True
        
        # CM-based patterns (e.g., "85 CM")
        if re.match(r'^\d+\s*(CM|CMS|CENTIMETERS?)$', s):
            return True
        
        # Size name patterns
        if s in {"EXTRA SMALL", "EXTRA LARGE", "DOUBLE XL", "TRIPLE XL"}:
            return True
        
        # Check for garbage words
        GARBAGE_WORDS = {
            "CHART", "GUIDE", "QUALITY", "GROUP", "SELECT", "CHOOSE",
            "PICK", "INFO", "MEASUREMENT", "FIT", "PRODUCT", "DETAILS",
            "DESCRIPTION", "SPECIFICATIONS", "MATERIAL", "CARE", "WASH",
            "STYLE", "PATTERN", "COLOR", "COLOUR", "BRAND", "SELLER",
            "DELIVERY", "RETURN", "POLICY", "CART", "BUY", "NOTIFY",
            "STOCK", "AVAILABLE", "SOLD"
        }
        
        if any(word in GARBAGE_WORDS for word in words):
            return False
    
    # Valid single-letter sizes
    VALID_LETTER_SIZES = {
        "XS", "S", "M", "L", "XL", "XXL", "XXXL", "XXXXL",
        "2XL", "3XL", "4XL", "5XL"
    }
    
    if s in VALID_LETTER_SIZES:
        return True
    
    # Valid numeric sizes
    if re.match(r'^\d{1,3}[A-Z]?$', s):
        num_match = re.match(r'^\d+', s)
        if num_match:
            num = int(num_match.group())
            if 20 <= num <= 200:
                return True
    
    # Check for CM suffix without space (e.g. 85CM)
    if re.match(r'^\d+(CM|CMS)$', s):
        return True
    
    # Age-based sizes
    if re.match(r'^\d+-\d+[YM]$', s):
        return True
    
    # Size ranges
    if re.match(r'^[A-Z0-9]+-[A-Z0-9]+$', s) and len(s) <= 10:
        return True
    
    return False


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_size_info(size: str) -> dict:
    """
    Get detailed information about a size.
    
    Useful for debugging and logging.
    
    Returns:
        {
            "original": "L",
            "normalized": "L",
            "equivalents": ["L", "90", "95"],
            "type": "letter" | "numeric" | "age"
        }
    """
    normalized = normalize_size(size)
    equivalents = get_size_equivalents(size)
    
    # Determine type
    size_type = "unknown"
    if re.match(r'^\d+-\d+[YM]$', normalized):
        size_type = "age"
    elif re.match(r'^\d+$', normalized):
        size_type = "numeric"
    elif normalized in SIZE_MAPPINGS:
        size_type = "letter"
    
    return {
        "original": size,
        "normalized": normalized,
        "equivalents": equivalents,
        "type": size_type
    }


# =============================================================================
# TESTING / VALIDATION
# =============================================================================

if __name__ == "__main__":
    """Quick test of the canonical size layer."""
    
    print("="*80)
    print("CANONICAL SIZE LAYER - PHASE 1 TEST")
    print("="*80)
    print()
    
    test_cases = [
        ("L", "95", True, "Letter to numeric size"),
        ("M", "90", False, "M is 85, L is 90"),
        ("XL", "100", True, "Letter to numeric size"),
        ("4 - 5 Year", "4-5Y", True, "Age normalization"),
        ("6-9 Months", "6-9M", True, "Age normalization"),
        ("L", "M", False, "Different sizes"),
        ("95", "90", False, "Different numeric"),
        ("2XL", "XXL", True, "Size equivalent"),
        ("85 CM", "M", True, "CM normalization to Letter"),
        ("90cms", "L", True, "CMS normalization to Letter"),
        ("80 cm", "S", True, "cm normalization to Letter"),
        ("85", "85 CM", True, "Numeric to CM Match"),
    ]
    
    print("Testing size_similarity():")
    print("-" * 80)
    
    for size1, size2, should_match, description in test_cases:
        score = size_similarity(size1, size2)
        matches = score == 1.0
        status = "✅" if matches == should_match else "❌"
        
        equiv1 = get_size_equivalents(size1)
        equiv2 = get_size_equivalents(size2)
        
        print(f"{status} {description}")
        print(f"   '{size1}' → {equiv1}")
        print(f"   '{size2}' → {equiv2}")
        print(f"   Match: {matches} (expected: {should_match})")
        print()
    
    print("="*80)
    print("Size Info Examples:")
    print("-" * 80)
    
    for size in ["L", "95", "4-5Y", "M"]:
        info = get_size_info(size)
        print(f"{size}: {info}")
    
    print()
    print("="*80)
