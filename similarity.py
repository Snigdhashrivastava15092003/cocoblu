"""
Production-Ready Product Comparison Lambda - Perfect Final Version
Version: 4.3.0 - Zero Errors, Root-Level Architecture
Last Updated: 2024

EXECUTION CONTEXT:
- Only invoked for NEW ASINs (first-time comparisons)
- Receives fresh scraped data from orchestrator
- Runs full 7-step waterfall comparison
- Stores results in DynamoDB

Environment Variables (REQUIRED):
- DYNAMO_RESULTS_TABLE_NAME: DynamoDB table for results

Environment Variables (OPTIONAL):
- IMAGE_COMPARISON_LAMBDA_NAME: Image comparison Lambda
- ENABLE_IMAGE_COMPARISON: Enable image comparison (default: true)
- ENABLE_GENAI_MATCHING: Enable GenAI matching (default: true)
- BEDROCK_MODEL_ID: Bedrock model (default: apac.amazon.nova-lite-v1:0)
- S3_BUCKET_NAME: S3 bucket for exports (default: cocoblu-comparison-exports)
- THRESHOLD_TITLE: Title threshold (default: 0.75)
- THRESHOLD_CONTENT: Content threshold (default: 0.60)
- THRESHOLD_FINAL: Final threshold (default: 0.80)
"""

import re
import json
import os
import uuid
import csv
from datetime import datetime
from io import StringIO
from typing import Dict, Any, Optional, List, Tuple

import boto3
from rapidfuzz import fuzz

import requests

# ============================================================================
# OPERATIONAL MODE (environment-configurable)
# ============================================================================
# Set LOCAL_MODE=false in environment to enable DynamoDB storage
LOCAL_MODE = os.getenv("LOCAL_MODE", "true").lower() == "true"
ENABLE_IMAGE_COMPARISON = True
ENABLE_GENAI_MATCHING = True

# =============================================================================
# CANONICAL SIZE LAYER (Phase 1) - Import shared size functions
# =============================================================================
from size_mappings import (
    normalize_size,
    size_similarity,
    get_size_equivalents,
    get_size_info
)

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
import logging
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.style import Style
from rich import box
# Use 'rich' logger if configured by app.py, otherwise standard fallback
logger = logging.getLogger("rich") if "rich" in logging.root.manager.loggerDict else logging.getLogger(__name__)

if not logger.handlers and logger.name != "rich":
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# ============================================================================
# AWS CLIENTS (DECLARE FIRST)
# ============================================================================
# Only initialize DynamoDB client if not in LOCAL_MODE
dynamodb_client = None
if not LOCAL_MODE:
    try:
        dynamodb_client = boto3.client("dynamodb")
        logger.info("✓ DynamoDB client initialized (LOCAL_MODE=false)")
    except Exception as e:
        logger.warning(f"⚠️ Failed to initialize DynamoDB client: {e}")
        dynamodb_client = None
else:
    logger.info("ℹ️ DynamoDB disabled (LOCAL_MODE=true)")

# s3_client = boto3.client("s3")  # Uncomment if S3 export needed
# lambda_client = boto3.client("lambda")
# bedrock_runtime = boto3.client("bedrock-runtime")

# ============================================================================
# ENVIRONMENT VARIABLES (DECLARE BEFORE USE)
# ============================================================================
DYNAMO_RESULTS_TABLE_NAME = os.getenv("DYNAMO_RESULTS_TABLE_NAME")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "cocoblu-comparison-exports")

IMAGE_COMPARISON_LAMBDA_NAME = os.getenv("IMAGE_COMPARISON_LAMBDA_NAME")
ENABLE_IMAGE_COMPARISON = os.getenv("ENABLE_IMAGE_COMPARISON", "true").lower() == "true"
ENABLE_GENAI_MATCHING = os.getenv("ENABLE_GENAI_MATCHING", "true").lower() == "true"
BEDROCK_MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "apac.anthropic.claude-3-haiku-20240307-v1:0")

# ============================================================================
# THRESHOLDS (DECLARE BEFORE USE)
# ============================================================================
PRICE_TOLERANCE = float(os.getenv("PRICE_TOLERANCE", "0.01"))
THRESHOLD_STOCK = float(os.getenv("THRESHOLD_STOCK", "1.0"))
THRESHOLD_SIZE = float(os.getenv("THRESHOLD_SIZE", "1.0"))
THRESHOLD_TITLE = float(os.getenv("THRESHOLD_TITLE", "0.75"))
THRESHOLD_CONTENT = float(os.getenv("THRESHOLD_CONTENT", "0.60"))
THRESHOLD_FINAL = float(os.getenv("THRESHOLD_FINAL", "0.80"))
THRESHOLD_MRP_DEVIATION = 12.0 # Maximum allowed percentage difference in MRP

STEP_NAMES = {
    1: "image_similarity",
    2: "stock_availability",
    3: "nudge_price",
    4: "size_match",
    5: "title_similarity",
    6: "content_similarity",
    7: "overall_comparison"
}

WORKFLOW_STATUS = {
    "PROCESSING": "comparison_in_progress",
    "COMPLETED": "comparison_completed",
    "TERMINATED": "waterfall_terminated_early",
    "ERROR": "comparison_error"
}

# ============================================================================
# ENVIRONMENT VALIDATION
# ============================================================================
def validate_environment():
    """Validate environment."""
    if not DYNAMO_RESULTS_TABLE_NAME and not LOCAL_MODE:
        raise ValueError("DYNAMO_RESULTS_TABLE_NAME not set")
    
    global ENABLE_IMAGE_COMPARISON
    # Only enforce Lambda name if NOT in Local Mode
    if ENABLE_IMAGE_COMPARISON and not IMAGE_COMPARISON_LAMBDA_NAME and not LOCAL_MODE:
        logger.warning("Image comparison disabled (no Lambda name)")
        ENABLE_IMAGE_COMPARISON = False
    elif ENABLE_IMAGE_COMPARISON and not IMAGE_COMPARISON_LAMBDA_NAME and LOCAL_MODE:
        logger.info("ℹ️  Image comparison enabled (LOCAL_MODE fallback)")
    
    logger.info(f"✓ Similarity v4.3.0 validated")

try:
    validate_environment()
except ValueError as e:
    logger.error(f"FATAL: {str(e)}")

# ============================================================================
# ENHANCED GENAI FUNCTIONS (AWS BEDROCK NOVA)
# ============================================================================
import boto3
import json

# --- HELPER: Call Bedrock and parse JSON response ---
def _invoke_bedrock_and_parse(prompt: str, label: str) -> Dict[str, Any]:
    """
    Internal helper: Sends a prompt to Bedrock, parses the JSON response.
    Returns the parsed dict or an error dict.
    """
    try:
        bedrock_runtime = boto3.client("bedrock-runtime")
        request_body = {
            "messages": [{
                "role": "user",
                "content": [{"text": prompt}]
            }],
            "inferenceConfig": {
                "max_new_tokens": 2048,
                "temperature": 0.0
            }
        }
        response = bedrock_runtime.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            body=json.dumps(request_body)
        )
        response_body = json.loads(response['body'].read())
        output_text = response_body['output']['message']['content'][0]['text'].strip()

        # --- DEBUG LOGGING ---
        print("\n" + "="*60)
        print(f"🤖 GEN AI RAW OUTPUT ({label}):")
        print("-" * 60)
        print(output_text)
        print("="*60 + "\n")

        # Clean potential markdown from response
        if "```json" in output_text:
            output_text = output_text.split("```json")[1].split("```")[0].strip()
        elif "```" in output_text:
            output_text = output_text.split("```")[1].split("```")[0].strip()

        result = json.loads(output_text)
        return result
    except Exception as e:
        logger.error(f"Bedrock {label} error: {str(e)}")
        return {"overall_score": 0.0, "reason": f"Error: {str(e)}"}


# ============================================================================
# LAYER 1: GenAI TITLE Similarity (20% of Overall Score)
# ============================================================================
def invoke_bedrock_title_similarity(p1: Dict[str, Any], p2: Dict[str, Any]) -> Dict[str, Any]:
    """
    Dedicated GenAI function for TITLE comparison ONLY.
    Focuses on: Brand, Model, Series, Quantity, Core Identity.
    Returns: {"title_score": 0.0-1.0, "reason": "..."}
    """
    if not ENABLE_GENAI_MATCHING:
        return {"title_score": 0.0, "reason": "GenAI disabled"}

    title_a = p1.get("title", "")
    title_b = p2.get("title", "")

    if not title_a or not title_b:
        return {"title_score": 0.0, "reason": "Missing title(s)"}

    prompt = f"""You are an expert product matching AI for an e-commerce catalog.
Task: Compare ONLY the TITLES of two products to determine if they refer to the EXACT SAME ITEM.

Title A: "{title_a}"
Title B: "{title_b}"

Guidelines:
1. Focus on: Brand Name, Model/Series, Pack Size (e.g., "Pack of 2" vs "Set of 2"), and Core Product Type.
2. Ignore minor wording differences (e.g., "Men's" vs "Mens", "T-Shirt" vs "Tee").
3. A mismatch in Brand, Model Number, or Pack Size is a HARD MISMATCH (score < 0.4).
4. If titles describe the same product concept with different words, score HIGH (0.8-1.0).

Output Format (STRICT JSON ONLY):
{{
  "title_score": 0.0-1.0,
  "brand_match": true/false,
  "reason": "Detailed explanation citing specific differences. Example: 'Mismatch in Pack Size: Title A says \"Pack of 2\", Title B says \"Single\".'"
}}"""

    result = _invoke_bedrock_and_parse(prompt, "TITLE")
    # Ensure score is float and within bounds
    result["title_score"] = max(0.0, min(1.0, float(result.get("title_score", 0.0))))
    return result


# ============================================================================
# LAYER 2: GenAI CONTENT Similarity (35% of Overall Score)
# ============================================================================

# --- ATTRIBUTE WEIGHTS (Local weights within the 35% Content slice) ---
CONTENT_ATTRIBUTE_WEIGHTS = {
    "brand": 0.35,
    "quantity": 0.20,
    "color": 0.20,
    "gender": 0.25,
}

def _calculate_content_score_from_attributes(attributes: Dict[str, Any]) -> float:
    """
    Calculates a weighted content score from GenAI attribute statuses.
    MATCH = 1.0, UNKNOWN = 0.5, MISMATCH = 0.0.
    If Brand mismatches, the entire content score is forced to 0.0.
    """
    if not isinstance(attributes, dict):
        return 0.0

    STATUS_SCORES = {"MATCH": 1.0, "UNKNOWN": 0.5, "MISMATCH": 0.0}

    # BRAND GATE: If brand mismatches, force entire content to 0
    brand_attr = attributes.get("brand", {})
    brand_status = brand_attr.get("status", "UNKNOWN") if isinstance(brand_attr, dict) else str(brand_attr)
    if brand_status == "MISMATCH":
        logger.warning("❌ BRAND MISMATCH detected -> Content Score forced to 0.0")
        return 0.0

    weighted_sum = 0.0
    total_weight = 0.0

    for attr_name, weight in CONTENT_ATTRIBUTE_WEIGHTS.items():
        attr_data = attributes.get(attr_name, {})
        if isinstance(attr_data, dict):
            status = attr_data.get("status", "UNKNOWN")
        else:
            status = str(attr_data) if attr_data else "UNKNOWN"
        
        score = STATUS_SCORES.get(status, 0.5)  # Default to 0.5 for unknown status
        weighted_sum += score * weight
        total_weight += weight

    # Normalize (in case some attributes are missing)
    if total_weight > 0:
        return round(weighted_sum / total_weight, 4)
    return 0.0


def invoke_bedrock_content_similarity(p1: Dict[str, Any], p2: Dict[str, Any]) -> Dict[str, Any]:
    """
    Dedicated GenAI function for CONTENT & SPECS comparison.
    Focuses on: Brand, Quantity, Color, Gender.
    Size is EXCLUDED (handled by canonical size mapping).
    Returns: {"content_score": 0.0-1.0, "attributes": {...}, "reason": "..."}
    """
    if not ENABLE_GENAI_MATCHING:
        return {"content_score": 0.0, "attributes": {}, "reason": "GenAI disabled"}

    # Prepare data package (NO size field)
    data_a = {
        "title": p1.get("title", ""),
        "description": (p1.get("content", "") or p1.get("description", ""))[:1500],
        "specs": p1.get("specs", {}),
        "item_dimensions": p1.get("item_dimensions", "N/A"),  # Added per user request
        "item_weight": p1.get("item_weight", "N/A")          # Added per user request
    }
    data_b = {
        "title": p2.get("title", ""),
        "description": (p2.get("content", "") or p2.get("description", ""))[:1500],
        "specs": p2.get("specs", {}),
        "item_dimensions": p2.get("item_dimensions", "N/A"),  # Added per user request
        "item_weight": p2.get("item_weight", "N/A")          # Added per user request
    }

    prompt = f"""You are an expert product matching AI for an e-commerce catalog.
Task: Compare the CONTENT, SPECIFICATIONS, DIMENSIONS, QUANTITY and WEIGHT of two products to determine if they are the EXACT SAME ITEM.
Do NOT compare sizes (size is handled separately).

Product A:
{json.dumps(data_a, indent=2)}

Product B:
{json.dumps(data_b, indent=2)}

Guidelines:
1. Semantic Matching: Resolve field name differences. "Unit Count" in A is the same as "Quantity" or "Pack of" in B.
2. Quantities: Treat "Pack of X", "Set of X", "Net Quantity X", and plain "X" as IDENTICAL if the number X is the same. Example: "Pack of 2" == "2" is a MATCH. A number mismatch (1 vs 2) is a HARD MISMATCH.
3. Colors: **CRITICAL MULTICOLOR RULE**: If EITHER product's color contains "Multicolor", "Multi", "Assorted", "Print", or is a comma-separated list of multiple colors (e.g. "Red,Blue,Green"), you MUST set the color status to "MATCH". This is because retailers label the same product differently — one may list individual colors while the other says "Multicolor". Example: "LT NAVY,GREY MEL,COFFEE BROWN" vs "Multicolor" = **MATCH**. "Red" vs "Multicolor" = **MATCH**. Only flag a color MISMATCH when BOTH products have a single, distinct, clearly different solid color (e.g., "Red" vs "Blue"). Setting color to MISMATCH when one side is Multicolor is a CRITICAL ERROR.
4. Gender: Extract gender from title/metadata (Men, Women, Kids, Unisex). Mismatch in gender (e.g. Men vs Women) is a HARD MISMATCH.
5. Brand: Compare brands case-insensitively. If the brands are the same (e.g., "ARROW" and "ARROW", or "NIKE" and "nike"), you MUST set the status to "MATCH". A Brand mismatch (e.g. "Nike" vs "Adidas") is a HARD MISMATCH.
6. Scoring: If there is a HARD MISMATCH (Quantity, different model, different Brand, or Gender), the overall_score MUST be below 0.4. Color matches involving Multicolor should NOT reduce the overall_score at all.
7. Dimensions/Weight: If 'item_dimensions' or 'item_weight' are present in both, compare them. A significant difference (e.g. 100g vs 1kg, or 10cm vs 50cm) is a MISMATCH and should be mentioned in the reason. Ignore minor variances.

Output Format (STRICT JSON ONLY):
{{
  "overall_score": 0.0-1.0,
  "attributes": {{
    "brand": {{ "status": "MATCH/MISMATCH/UNKNOWN", "value_a": "extracted value", "value_b": "extracted value" }},
    "quantity": {{ "status": "MATCH/MISMATCH/UNKNOWN", "value_a": "...", "value_b": "..." }},
    "color": {{ "status": "MATCH/MISMATCH/UNKNOWN", "value_a": "...", "value_b": "..." }},
    "gender": {{ "status": "MATCH/MISMATCH/UNKNOWN", "value_a": "...", "value_b": "..." }},
    "item_dimensions": {{ "status": "MATCH/MISMATCH/UNKNOWN", "value_a": "...", "value_b": "..." }},
    "item_weight": {{ "status": "MATCH/MISMATCH/UNKNOWN", "value_a": "...", "value_b": "..." }}
  }},
  "is_exact_match": true/false,
  "reason": "Detailed explanation citing specific values. Example: 'Mismatch in Color: Product A is Red, Product B is Blue. Match in Brand: Both are Nike.'"
}}"""

    result = _invoke_bedrock_and_parse(prompt, "CONTENT")

    # --- DETERMINISTIC SAFETY CHECKS (Anti-Hallucination) ---
    EXPECTED_ATTRIBUTES = {"brand", "quantity", "color", "gender", "item_dimensions", "item_weight"}
    EMPTY_VALUES = {"n/a", "na", "none", "", "unknown", "not available", "not specified"}
    
    attributes = result.get("attributes", {})
    if isinstance(attributes, dict):
        # 1. Filter out any attributes the model returned that we didn't ask for
        attributes = {k: v for k, v in attributes.items() if k in EXPECTED_ATTRIBUTES}
        
        for key, attr in attributes.items():
            if isinstance(attr, dict):
                val_a = str(attr.get("value_a", "")).strip().lower()
                val_b = str(attr.get("value_b", "")).strip().lower()
                status = attr.get("status")
                
                # 2. If BOTH values are empty/N/A, force UNKNOWN (not MATCH)
                if val_a in EMPTY_VALUES and val_b in EMPTY_VALUES:
                    if status != "UNKNOWN":
                        print(f"🔧 OVERRIDE: Forcing UNKNOWN for '{key}' (both values are N/A)")
                        attr["status"] = "UNKNOWN"
                    continue
                
                # 3. If values are identical non-empty strings but marked MISMATCH, fix to MATCH
                if val_a and val_b and val_a == val_b and val_a not in EMPTY_VALUES and status != "MATCH":
                    print(f"🔧 OVERRIDE: Forcing MATCH for '{key}' ({val_a} == {val_b})")
                    attr["status"] = "MATCH"

    # Use AI's own overall_score as the content score (holistic reasoning)
    content_score = max(0.0, min(1.0, float(result.get("overall_score", 0.0))))
    result["content_score"] = content_score
    result["attributes"] = attributes

    return result



def title_similarity_fuzzy(a: str, b: str) -> float:
    """
    Enhanced fuzzy title matching with better preprocessing.
    
    Args:
        a: First title
        b: Second title
    
    Returns:
        Similarity score between 0.0 and 1.0
    """
    if not a or not b:
        return 0.0
    
    def preprocess_title(title: str) -> str:
        """Normalize title for better matching."""
        title = str(title).lower().strip()
        # Remove common noise words
        noise_words = {'the', 'a', 'an', 'and', '&', '-', '|', '/', '\\'}
        words = title.split()
        words = [w for w in words if w not in noise_words]
        # Remove extra whitespace
        return ' '.join(words)
    
    title_a = preprocess_title(a)
    title_b = preprocess_title(b)
    
    # Fast exact match after preprocessing
    if title_a == title_b:
        return 1.0
    
    # Empty after preprocessing
    if not title_a or not title_b:
        return 0.0
    
    try:
        # Use weighted combination of different fuzzy methods
        token_set = fuzz.token_set_ratio(title_a, title_b) / 100.0      # Best for word order
        token_sort = fuzz.token_sort_ratio(title_a, title_b) / 100.0    # Good for reordered words
        partial = fuzz.partial_ratio(title_a, title_b) / 100.0          # Good for substrings
        ratio = fuzz.ratio(title_a, title_b) / 100.0                    # Baseline similarity
        
        # Weighted average - token_set usually most reliable for titles
        scores = [token_set, token_sort, partial, ratio]
        weights = [0.4, 0.3, 0.2, 0.1]
        
        weighted_score = sum(s * w for s, w in zip(scores, weights))
        max_score = max(scores)
        
        # Return the better of weighted or max (sometimes max is more accurate)
        final_score = max(weighted_score, max_score * 0.9)  # Slight penalty for max
        
        return min(1.0, final_score)
        
    except Exception as e:
        logger.error(f"Fuzzy title error: {str(e)}", exc_info=True)
        return 0.0


def content_similarity_fuzzy(a: str, b: str) -> float:
    """
    Enhanced fuzzy content matching for descriptions.
    
    Args:
        a: First content/description
        b: Second content/description
    
    Returns:
        Similarity score between 0.0 and 1.0
    """
    if not a or not b:
        return 0.0
    
    def normalize_content(text: str) -> str:
        """Aggressive normalization for content comparison."""
        text = str(text).strip().lower()
        # Remove HTML tags if present
        text = re.sub(r'<[^>]+>', ' ', text)
        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' ', text)
        # Remove extra punctuation
        text = re.sub(r'[^\w\s]', ' ', text)
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    norm_a = normalize_content(a)
    norm_b = normalize_content(b)
    
    # Fast exact match
    if norm_a == norm_b:
        return 1.0
    
    # Empty after normalization
    if not norm_a or not norm_b:
        return 0.0
    
    try:
        # For long descriptions, truncate intelligently
        max_len = 1000
        if len(norm_a) > max_len:
            # Take first portion + last portion
            norm_a = norm_a[:max_len//2] + ' ' + norm_a[-max_len//2:]
        if len(norm_b) > max_len:
            norm_b = norm_b[:max_len//2] + ' ' + norm_b[-max_len//2:]
        
        # Multiple fuzzy matching approaches
        token_set = fuzz.token_set_ratio(norm_a, norm_b) / 100.0
        token_sort = fuzz.token_sort_ratio(norm_a, norm_b) / 100.0
        partial = fuzz.partial_ratio(norm_a, norm_b) / 100.0
        
        # For content, token_set is usually most reliable
        scores = [token_set, token_sort, partial]
        weights = [0.5, 0.3, 0.2]
        
        weighted_score = sum(s * w for s, w in zip(scores, weights))
        
        # Additional bonus for high partial ratio (substring matches)
        if partial > 0.9:
            weighted_score = max(weighted_score, partial * 0.95)
        
        return min(1.0, weighted_score)
        
    except Exception as e:
        logger.error(f"Fuzzy content error: {str(e)}", exc_info=True)
        return 0.0
# ============================================================================
# IMAGE COMPARISON (DECLARE BEFORE USE IN WATERFALL)
# ============================================================================
def invoke_image_comparison(image_url_a: str, image_url_b: str) -> Optional[Dict[str, Any]]:
    """Invoke image comparison - calls local function directly."""
    if not ENABLE_IMAGE_COMPARISON:
        return None
    
    if not image_url_a or not image_url_b:
        return None
    
    try:
        # Import image_similarity module
        import image_similarity
        
        # Call the local compare_images function directly
        result = image_similarity.compare_images(image_url_a, image_url_b)
        
        if result and result.get('status') == 'completed':
            logger.info(f"✓ Image similarity: {result.get('overall_similarity', 0):.3f}")
            return result
        return None
    except Exception as e:
        logger.error(f"Image comparison error: {str(e)}")
        return None

# ============================================================================
# PRICE & BINARY COMPARISONS (DECLARE BEFORE WATERFALL)
# ============================================================================
def compare_prices(nudge_price: float, flipkart_price: float) -> Tuple[float, Dict[str, Any]]:
    """Compare prices."""
    try:
        # Handle None values gracefully
        if nudge_price is None:
            nudge_price = 0
        if flipkart_price is None:
            flipkart_price = 0
            
        nudge_float = float(nudge_price)
        flipkart_float = float(flipkart_price)
        
        if nudge_float <= 0 or flipkart_float <= 0:
            return 0.0, {"error": "Invalid prices", "match": False}
        
        price_diff = abs(nudge_float - flipkart_float)
        price_diff_percent = (price_diff / nudge_float * 100)
        match = price_diff <= PRICE_TOLERANCE
        
        return (1.0 if match else 0.0), {
            "nudge_price": round(nudge_float, 2),
            "flipkart_price": round(flipkart_float, 2),
            "difference": round(price_diff, 2),
            "difference_percent": round(price_diff_percent, 2),
            "tolerance": PRICE_TOLERANCE,
            "match": match
        }
    except Exception as e:
        return 0.0, {"error": str(e), "match": False}

def check_stock_availability(stock1: str, stock2: str) -> tuple[float, str]:
    """Check stock and return (score, failure_reason)."""
    def is_in_stock(status: str) -> bool:
        if not status:
            return False
        status_lower = str(status).lower()
        return any(k in status_lower for k in ['in stock', 'available', 'yes', 'instock'])
    
    def is_unknown(status: str) -> bool:
        if not status:
            return False
        return str(status).lower() == 'unknown'
    
    logger.info(f"Stock Check - Amazon: '{stock1}', Flipkart: '{stock2}'")

    s1_in = is_in_stock(stock1)
    s2_in = is_in_stock(stock2)
    s1_unk = is_unknown(stock1)
    s2_unk = is_unknown(stock2)

    # 1. Both In Stock -> PASS
    if s1_in and s2_in:
        logger.info("✓ Both products in stock")
        return 1.0, None
    
    # 2. One Unknown, One In Stock -> PASS (Amazon rate limit workaround)
    if (s1_unk and s2_in) or (s1_in and s2_unk):
        logger.info("✓ One unknown, one in stock - PASSING")
        return 1.0, None
        
    # 3. Failures
    if not s1_in and not s2_in:
        reason = "Out Of Stock Both"
    elif not s1_in:
        reason = "Out Of Stock AZ"
    else:
        reason = "Out Of Stock FK"

    logger.warning(f"✗ Stock check FAILED - Amazon: '{stock1}', Flipkart: '{stock2}' -> {reason}")
    return 0.0, reason

def check_mrp_match(az_mrp: float, fk_mrp: float) -> Tuple[bool, Optional[str]]:
    """
    Check if Flipkart MRP deviates from Amazon MRP by more than 10-12%.
    Returns (pass_status, failure_reason).
    """
    if not az_mrp or not fk_mrp:
        return True, None # Cannot compare if missing
        
    try:
        az = float(az_mrp)
        fk = float(fk_mrp)
        
        if az <= 0 or fk <= 0:
            return True, None
            
        # Calculate percentage difference relative to Amazon MRP
        diff_percent = abs(az - fk) / az * 100
        
        if diff_percent > THRESHOLD_MRP_DEVIATION:
            reason = f"Incorrect_List_Price ({diff_percent:.1f}%)"
            logger.warning(f"❌ MRP Validation FAILED: AZ={az}, FK={fk}, Diff={diff_percent:.1f}% > {THRESHOLD_MRP_DEVIATION}%")
            return False, reason
            
        logger.info(f"✓ MRP Validation PASSED: AZ={az}, FK={fk}, Diff={diff_percent:.1f}%")
        return True, None
        
    except Exception as e:
        logger.warning(f"MRP check error: {e}")
        return True, None

# NOTE: normalize_size() is now imported from size_mappings.py (canonical layer)
# The duplicate function has been removed to maintain single source of truth

# NOTE: size_similarity() is now imported from size_mappings.py (canonical layer)
# The new implementation uses get_size_equivalents() for many-to-many matching
# The old implementation below is kept for reference but not used
# def size_similarity(size1: str, size2: str) -> float:
#     ... (moved to size_mappings.py with enhanced equivalents support)

# ============================================================================
# DATABASE OPERATIONS - DynamoDB Storage with High Priority Fields
# ============================================================================
# HIGH PRIORITY FIELDS (per DYNAMODB_SCHEMA_ANALYSIS.md):
#   - critical_failures: List of blocking failure reasons
#   - informational_failures: List of soft failure reasons
#   - flipkart_pid: Flipkart product ID from URL
#   - flipkart_url: Full Flipkart product URL
#   - amazon_url: Full Amazon product URL
#   - nudge_price: Target price specified by user
#   - size_selection_status: SUCCESS/FAILED/INVALID_SIZE/NO_TARGET
#   - available_sizes: All sizes available on Flipkart
# ============================================================================

def _extract_flipkart_pid(url: str) -> str:
    """
    Extract Flipkart Product ID (PID) from URL.
    Pattern: /p/itm[PRODUCTID] or pid=[PRODUCTID]
    """
    if not url:
        return "N/A"
    
    import re
    # Pattern 1: /p/itm... format
    match = re.search(r'/p/itm([A-Za-z0-9]+)', url)
    if match:
        return match.group(1)
    
    # Pattern 2: pid= query parameter
    match = re.search(r'pid=([A-Za-z0-9]+)', url)
    if match:
        return match.group(1)
    
    # Pattern 3: Full path segment after /p/
    match = re.search(r'/p/([A-Za-z0-9]+)', url)
    if match:
        return match.group(1)
    
    return "N/A"


def store_result_in_dynamodb(result: Dict[str, Any]) -> Optional[str]:
    """
    Store comparison result in DynamoDB with HIGH PRIORITY fields.
    
    Schema includes:
    - Core fields: comparison_id, asin, timestamp, workflow_status, etc.
    - HIGH PRIORITY new fields:
        - critical_failures (SS): Set of critical failure reasons
        - informational_failures (SS): Set of soft failure reasons  
        - flipkart_pid (S): Flipkart product ID from URL
        - flipkart_url (S): Full Flipkart product URL
        - amazon_url (S): Full Amazon product URL
        - nudge_price (N): Target price specified by user
        - size_selection_status (S): SUCCESS/FAILED/INVALID_SIZE/NO_TARGET
        - available_sizes (SS): All sizes available on Flipkart
    
    Returns:
        comparison_id if stored successfully, None otherwise
    """
    # Skip if LOCAL_MODE enabled or no DynamoDB client
    if LOCAL_MODE:
        logger.debug("ℹ️ DynamoDB storage skipped (LOCAL_MODE=true)")
        return None
    
    if not dynamodb_client:
        logger.warning("⚠️ DynamoDB client not initialized")
        return None
    
    if not DYNAMO_RESULTS_TABLE_NAME:
        logger.warning("⚠️ DYNAMO_RESULTS_TABLE_NAME not configured")
        return None
    
    comparison_id = str(uuid.uuid4())
    timestamp = datetime.utcnow().isoformat()
    
    try:
        # Extract product data
        product_az = result.get("product_az", {})
        product_ic = result.get("product_ic", {})
        
        if not isinstance(product_az, dict):
            product_az = {}
        if not isinstance(product_ic, dict):
            product_ic = {}
        
        # Get ASIN for sort key
        product_az_asin = product_az.get("asin")
        if product_az_asin:
            product_az_asin = str(product_az_asin).strip().upper()
        primary_asin = product_az_asin if product_az_asin and len(product_az_asin) == 10 else f"NOASIN-{comparison_id[:13]}"
        
        # ============================================================
        # HIGH PRIORITY FIELD EXTRACTION
        # ============================================================
        
        # 1. Critical & Informational Failures
        critical_failures = result.get("critical_failures", [])
        informational_failures = result.get("informational_failures", [])
        
        # 2. URLs and IDs
        amazon_url = product_az.get("url", "") or ""
        flipkart_url = product_ic.get("url", "") or result.get("flipkart_url", "") or ""
        flipkart_pid = _extract_flipkart_pid(flipkart_url)
        
        # 3. Nudge Price (target price)
        nudge_price = result.get("nudge_price") or product_az.get("nudge_price")
        if nudge_price is None:
            # Try to get from dict_parameter_scores.price_details
            price_details = result.get("dict_parameter_scores", {}).get("price_details", {})
            if isinstance(price_details, dict):
                nudge_price = price_details.get("nudge_price")
        
        # 3.5 MRPs (New High Priority Fields)
        az_mrp = product_az.get("mrp", 0)
        fk_mrp = product_ic.get("mrp", 0)
        
        # 4. Size Selection Status
        size_selection_status = product_ic.get("size_selection_status", "NO_TARGET")
        
        # 5. Available Sizes & Purchasable Sizes
        available_sizes = product_ic.get("available_sizes", [])
        if not isinstance(available_sizes, list):
            available_sizes = []
            
        purchasable_sizes = product_ic.get("purchasable_sizes", [])
        if not isinstance(purchasable_sizes, list):
            purchasable_sizes = []
        
        # ============================================================
        # BUILD DYNAMODB ITEM
        # ============================================================
        item = {
            # Primary Keys
            "comparison_id": {"S": comparison_id},
            "asin": {"S": primary_asin},
            
            # Timestamps
            "timestamp": {"S": timestamp},
            "ttl": {"N": str(int(datetime.utcnow().timestamp()) + (90 * 86400))},  # 90-day expiry
            
            # Status & Results
            "workflow_status": {"S": result.get("workflow_status", WORKFLOW_STATUS["COMPLETED"])},
            "step_completed": {"N": str(result.get("step_completed", 0))},
            "overall_similarity_percentage": {"N": str(round(result.get("overall_similarity_percentage", 0.0), 4))},
            "approved_match": {"BOOL": result.get("approved_match", False)},
            "recommendation_action": {"S": result.get("recommendation_action", "manual_review")},
            
            # Scores (JSON encoded)
            "dict_parameter_scores": {"S": json.dumps(result.get("dict_parameter_scores", {}))},
            
            # Product IDs
            "product_az_asin": {"S": product_az_asin or "N/A"},
            
            # Full Product Data (JSON encoded)
            "product_az": {"S": json.dumps(product_az)},
            "product_ic": {"S": json.dumps(product_ic)},
            
            # ============================================================
            # HIGH PRIORITY NEW FIELDS
            # ============================================================
            
            # URLs & IDs
            "amazon_url": {"S": amazon_url or "N/A"},
            "flipkart_url": {"S": flipkart_url or "N/A"},
            "flipkart_pid": {"S": flipkart_pid},
            
            # MRPs
            "az_mrp": {"N": str(az_mrp) if az_mrp else "0"},
            "fk_mrp": {"N": str(fk_mrp) if fk_mrp else "0"},
            
            # Size Information
            "size_selection_status": {"S": size_selection_status or "NO_TARGET"},
        }
        
        # Nudge Price (only add if present and valid)
        if nudge_price is not None:
            try:
                item["nudge_price"] = {"N": str(float(nudge_price))}
            except (ValueError, TypeError):
                pass
        
        # Critical Failures (String Set - requires non-empty list)
        if critical_failures and len(critical_failures) > 0:
            item["critical_failures"] = {"SS": [str(f) for f in critical_failures]}
        else:
            # Store as empty marker for consistency
            item["critical_failures_empty"] = {"BOOL": True}
        
        # Informational Failures (String Set - requires non-empty list)
        if informational_failures and len(informational_failures) > 0:
            item["informational_failures"] = {"SS": [str(f) for f in informational_failures]}
        else:
            item["informational_failures_empty"] = {"BOOL": True}
        
        # Combined Failures (String Set - all failures merged)
        combined_failures = result.get("combined_failures", [])
        if combined_failures and len(combined_failures) > 0:
            item["combined_failures"] = {"SS": [str(f) for f in combined_failures]}
        
        # Failure Details (JSON - categorized hierarchy)
        failure_details = result.get("failure_details", {})
        if failure_details:
            item["failure_details"] = {"S": json.dumps(failure_details)}
        
        # Available Sizes (String Set - requires non-empty list)
        if available_sizes and len(available_sizes) > 0:
            item["available_sizes"] = {"SS": [str(s) for s in available_sizes]}
        else:
            item["available_sizes_empty"] = {"BOOL": True}

        # Purchasable Sizes (String Set - requires non-empty list)
        if purchasable_sizes and len(purchasable_sizes) > 0:
            item["purchasable_sizes"] = {"SS": [str(s) for s in purchasable_sizes]}
        else:
            item["purchasable_sizes_empty"] = {"BOOL": True}
        
        # ============================================================
        # OPTIONAL FIELDS
        # ============================================================
        if result.get("termination_reason"):
            item["termination_reason"] = {"S": str(result["termination_reason"])}
            
        if result.get("primary_failure_reason"):
            item["primary_failure_reason"] = {"S": str(result["primary_failure_reason"])}
        
        if result.get("genai_reason"):
            item["genai_reason"] = {"S": str(result["genai_reason"])}
        
        if result.get("genai_attributes"):
            item["genai_attributes"] = {"S": json.dumps(result["genai_attributes"])}

        if result.get("image_comparison"):
            item["image_comparison"] = {"S": json.dumps(result["image_comparison"])}
        
        if result.get("size_analysis"):
            item["size_analysis"] = {"S": json.dumps(result["size_analysis"])}
        
        # ============================================================
        # STORE IN DYNAMODB
        # ============================================================
        dynamodb_client.put_item(TableName=DYNAMO_RESULTS_TABLE_NAME, Item=item)
        
        logger.info(f"✓ Stored in DynamoDB: {comparison_id} (ASIN: {primary_asin})")
        logger.debug(f"  → Flipkart PID: {flipkart_pid}")
        logger.debug(f"  → Critical Failures: {critical_failures}")
        logger.debug(f"  → Available Sizes: {available_sizes}")
        logger.debug(f"  → Purchasable Sizes: {purchasable_sizes}")
        
        return comparison_id
        
    except Exception as e:
        logger.error(f"❌ DynamoDB storage error: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        return None


def get_results_from_dynamodb(limit: int = 100) -> List[Dict[str, Any]]:
    """
    Retrieve comparison results from DynamoDB.
    
    Args:
        limit: Maximum number of results to retrieve (default: 100)
    
    Returns:
        List of comparison result dictionaries
    """
    if LOCAL_MODE or not dynamodb_client or not DYNAMO_RESULTS_TABLE_NAME:
        return []
    
    try:
        response = dynamodb_client.scan(
            TableName=DYNAMO_RESULTS_TABLE_NAME,
            Limit=limit
        )
        
        items = []
        for item in response.get('Items', []):
            try:
                parsed = {
                    'comparison_id': item.get('comparison_id', {}).get('S', ''),
                    'asin': item.get('asin', {}).get('S', ''),
                    'timestamp': item.get('timestamp', {}).get('S', ''),
                    'workflow_status': item.get('workflow_status', {}).get('S', ''),
                    'step_completed': int(item.get('step_completed', {}).get('N', 0)),
                    'overall_similarity_percentage': float(item.get('overall_similarity_percentage', {}).get('N', 0)),
                    'approved_match': item.get('approved_match', {}).get('BOOL', False),
                    'recommendation_action': item.get('recommendation_action', {}).get('S', ''),
                    'dict_parameter_scores': json.loads(item.get('dict_parameter_scores', {}).get('S', '{}')),
                    'product_az': json.loads(item.get('product_az', {}).get('S', '{}')),
                    'product_ic': json.loads(item.get('product_ic', {}).get('S', '{}')),
                    # HIGH PRIORITY FIELDS
                    'amazon_url': item.get('amazon_url', {}).get('S', ''),
                    'flipkart_url': item.get('flipkart_url', {}).get('S', ''),
                    'flipkart_pid': item.get('flipkart_pid', {}).get('S', ''),
                    'nudge_price': float(item.get('nudge_price', {}).get('N', 0)) if item.get('nudge_price') else None,
                    'size_selection_status': item.get('size_selection_status', {}).get('S', ''),
                    'primary_failure_reason': item.get('primary_failure_reason', {}).get('S', ''),
                    'critical_failures': list(item.get('critical_failures', {}).get('SS', [])),
                    'informational_failures': list(item.get('informational_failures', {}).get('SS', [])),
                    'available_sizes': list(item.get('available_sizes', {}).get('SS', [])),
                    'purchasable_sizes': list(item.get('purchasable_sizes', {}).get('SS', [])),
                }
                items.append(parsed)
            except Exception as parse_error:
                logger.warning(f"⚠️ Failed to parse DynamoDB item: {parse_error}")
                continue
        
        logger.info(f"✓ Retrieved {len(items)} results from DynamoDB")
        return items
        
    except Exception as e:
        logger.error(f"❌ DynamoDB retrieval error: {str(e)}")
        return []


def get_comparison_by_id(comparison_id: str, asin: str) -> Optional[Dict[str, Any]]:
    """
    Get a specific comparison result by ID and ASIN.
    
    Args:
        comparison_id: The comparison UUID
        asin: The Amazon ASIN (sort key)
    
    Returns:
        Comparison result dictionary or None if not found
    """
    if LOCAL_MODE or not dynamodb_client or not DYNAMO_RESULTS_TABLE_NAME:
        return None
    
    try:
        response = dynamodb_client.get_item(
            TableName=DYNAMO_RESULTS_TABLE_NAME,
            Key={
                "comparison_id": {"S": comparison_id},
                "asin": {"S": asin}
            }
        )
        
        if "Item" not in response:
            return None
        
        item = response["Item"]
        return {
            'comparison_id': item.get('comparison_id', {}).get('S', ''),
            'asin': item.get('asin', {}).get('S', ''),
            'timestamp': item.get('timestamp', {}).get('S', ''),
            'workflow_status': item.get('workflow_status', {}).get('S', ''),
            'step_completed': int(item.get('step_completed', {}).get('N', 0)),
            'overall_similarity_percentage': float(item.get('overall_similarity_percentage', {}).get('N', 0)),
            'approved_match': item.get('approved_match', {}).get('BOOL', False),
            'product_az': json.loads(item.get('product_az', {}).get('S', '{}')),
            'product_ic': json.loads(item.get('product_ic', {}).get('S', '{}')),
            # HIGH PRIORITY FIELDS
            'amazon_url': item.get('amazon_url', {}).get('S', ''),
            'flipkart_url': item.get('flipkart_url', {}).get('S', ''),
            'flipkart_pid': item.get('flipkart_pid', {}).get('S', ''),
            'nudge_price': float(item.get('nudge_price', {}).get('N', 0)) if item.get('nudge_price') else None,
            'size_selection_status': item.get('size_selection_status', {}).get('S', ''),
            'primary_failure_reason': item.get('primary_failure_reason', {}).get('S', ''),
            'critical_failures': list(item.get('critical_failures', {}).get('SS', [])),
            'informational_failures': list(item.get('informational_failures', {}).get('SS', [])),
            'available_sizes': list(item.get('available_sizes', {}).get('SS', [])),
        }
        
    except Exception as e:
        logger.error(f"❌ DynamoDB get_by_id error: {str(e)}")
        return None

# ============================================================================
# OUTPUT FORMATTING (DECLARE BEFORE WATERFALL)
# ============================================================================
def format_comparison_output(result: Dict[str, Any]) -> Dict[str, Any]:
    """Format output."""
    step_completed = result.get("step_completed", 0)
    scores = result.get("dict_parameter_scores", {})
    failed_steps = set(result.get("failed_steps", []))
    
    parameters_checked = {}
    failed_parameters = []
    
    PARAM_TO_SCORE = {
        "image_similarity": "image",
        "stock_availability": "stock_check",
        "nudge_price": "nudge_price", 
        "size_match": "size",
        "title_similarity": "title",
        "content_similarity": "content",
        
    }
    
    for param_name, score_key in PARAM_TO_SCORE.items():
        score = scores.get(score_key, 0.0)
        
        # Handle None scores (e.g., from missing size in soft constraint)
        if score is None:
            confidence = 0
            status = "SKIPPED"
        else:
            confidence = round(score * 100, 1)
            # Check specific failure reasons for stock
            if param_name == "stock_availability":
                stock_failures = [
                    "stock_availability", 
                    "Out Of Stock AZ", 
                    "Out Of Stock FK", 
                    "Out Of Stock Both"
                ]
                status = "FAILED" if any(f in failed_steps for f in stock_failures) else "PASSED"
            else:
                status = "FAILED" if param_name in failed_steps else "PASSED"

        if status == "FAILED":
            failed_parameters.append(param_name)
        
        parameters_checked[param_name] = {
            "status": status,
            "confidence_score": confidence,
            "score": round(score, 3) if score is not None else None
        }
    
    product_az = result.get("product_az", {})
    product_ic = result.get("product_ic", {})
    
    return {
        "comparison_status": "PASSED" if result.get("approved_match") else "FAILED",
        "message": result.get("message", "Comparison completed"),
        "overall_confidence_score": round(result.get("overall_similarity_percentage", 0) * 100, 2),
        "step_completed": step_completed,
        "termination_reason": result.get("termination_reason"),
        "genai_reason": result.get("genai_reason"),
        "genai_attributes": result.get("genai_attributes"),
        "parameters_checked": parameters_checked,
        "failed_parameters": failed_parameters,
        "critical_failures": result.get("critical_failures", []),
        "informational_failures": result.get("informational_failures", []),
        "combined_failures": result.get("combined_failures", []),
        "failure_details": result.get("failure_details", {}),
        "metadata": {
            "comparison_id": None,
            "product_az_asin": product_az.get("asin", ""),
            "similarity_version": "4.4.0"  # Updated version
        },
        "product_details": {
            "product_az": {
                "asin": product_az.get("asin", ""),
                "title": product_az.get("title", ""),
                "price": product_az.get("price", 0),
                "size": product_az.get("size", ""),
                "instock": product_az.get("instock", "")
            },
            "product_ic": {
                "title": product_ic.get("title", ""),
                "price": product_ic.get("price", 0),
                "size": product_ic.get("size", ""),
                "instock": product_ic.get("instock", "")
            }
        }
    }

# ============================================================================
# WATERFALL MATCHING (ALL DEPENDENCIES DECLARED ABOVE)
# ============================================================================
def sequential_product_matching(p1: Dict[str, Any], p2: Dict[str, Any]) -> Dict[str, Any]:
    """7-step waterfall matching."""
    logger.info("="*80)
    logger.info("WATERFALL v4.3.0 - Starting")
    logger.info("="*80)
    
    result = {
        "workflow_status": WORKFLOW_STATUS["PROCESSING"],
        "step_completed": 0,
        "termination_reason": None,
        "dict_parameter_scores": {},
        "overall_similarity_percentage": 0.0,
        "recommendation_action": "manual_review",
        "approved_match": False,
        "image_comparison": None,
        "failed_steps": []
    }
    step_counter = 1
    failed_steps = []
    # Step 1: Image (informational, but weighted)
    image_url_a = p1.get("image_url", "") or p1.get("images", "")
    image_url_b = p2.get("image_url", "") or p2.get("images", "")
    
    # Ensure items are strings (scraper sometimes returns lists)
    if isinstance(image_url_a, list) and image_url_a:
        image_url_a = image_url_a[0]
    if isinstance(image_url_b, list) and image_url_b:
        image_url_b = image_url_b[0]
    
    image_score = 0.0
    
    if ENABLE_IMAGE_COMPARISON and image_url_a and image_url_b:
        image_result = invoke_image_comparison(image_url_a, image_url_b)
        if image_result:
            image_score = image_result.get("overall_similarity", 0.0)
            result["image_comparison"] = image_result

    result["dict_parameter_scores"]["image"] = image_score
    result["step_completed"] = step_counter


# Step 2: Stock
    step_counter += 1
    stock_score, stock_failure_reason = check_stock_availability(p1.get("instock", ""), p2.get("instock", ""))
    result["dict_parameter_scores"]["stock_check"] = stock_score
    result["step_completed"] = step_counter
    
    if stock_score != 1.0 and stock_failure_reason:
        failed_steps.append(stock_failure_reason)
    elif stock_score != 1.0:
        failed_steps.append("stock_availability")
    

    # Step 3: Price
    step_counter += 1
    nudge_price = p1.get("nudge_price") or p2.get("nudge_price")
    flipkart_price = p2.get("price", 0)
    
    price_score, price_details = compare_prices(nudge_price, flipkart_price)
    result["dict_parameter_scores"]["nudge_price"] = price_score
    result["dict_parameter_scores"]["price_details"] = price_details
    result["step_completed"] = step_counter
    
    if price_score != 1.0:
        failed_steps.append("nudge_price")
    
    # --- Step 3.5: MRP Deviation Check (New Critical Failure) ---
    az_mrp = p1.get("mrp", 0)
    fk_mrp = p2.get("mrp", 0)
    mrp_pass, mrp_fail_reason = check_mrp_match(az_mrp, fk_mrp)
    
    if not mrp_pass:
        # User preference: Use the specific reason string if available, else generic
        mrp_tag = mrp_fail_reason if mrp_fail_reason else "mrp_mismatch"
        failed_steps.append(mrp_tag)

    
    
# Step 4: Size (SOFT CONSTRAINT - never blocks approval)
    step_counter += 1
    flipkart_size = p2.get("size", "")
    amazon_size = p1.get("size", "")
    available_sizes = p2.get("available_sizes", [])
    size_status = p2.get("size_selection_status")
    
    # Log size information for debugging
    logger.info(f"🔍 Size Check Debug - Amazon: '{amazon_size}', Flipkart: '{flipkart_size}'")
    logger.info(f"   (Type Debug) Amazon: {type(amazon_size)}, Flipkart: {type(flipkart_size)}")
    logger.info(f"   Size selection status: '{size_status}'")
    logger.info(f"   Available Flipkart sizes: {available_sizes}") 
    
    # --- SIZE ANALYSIS DEBUG INFO ---
    # --- SIZE ANALYSIS DEBUG INFO ---
    result["size_analysis"] = {
        "target": get_size_info(amazon_size),
        "selected": get_size_info(flipkart_size),
        "available_sizes": available_sizes,
        "purchasable_sizes": p2.get("purchasable_sizes", []),
        "selection_status": size_status
    }
    
    # Handle INVALID_SIZE status (not purchasable on Flipkart)
    # Per user request: This is a warning, not a critical failure
    if size_status == "INVALID_SIZE":
        logger.warning(f"⚠️ Size '{flipkart_size}' is INVALID (not purchasable)")
        result["termination_reason"] = "flipkart_size_not_purchasable"
        result["step_completed"] = step_counter
        result["dict_parameter_scores"]["size"] = 0.0
        failed_steps.append("size_match")  # Informational only
        # Continue to other checks instead of returning early
    else:
        # Pre-check: If Flipkart size is clearly garbage text, skip straight to -1.0 (ambiguity)
        from size_mappings import is_valid_size
        if flipkart_size and not is_valid_size(flipkart_size):
            size_score = -1.0
        else:
            # Calculate size similarity with graduated scoring
            # Returns: None (skip), 1.0 (perfect), 0.5 (partial), 0.0 (no match), -1.0 (ambiguous/unhandled)
            size_score = size_similarity(amazon_size, flipkart_size)
            
        result["step_completed"] = step_counter
        
        if size_score is None:
            # Missing size logic
            if amazon_size or flipkart_size:
                 # One-sided missing -> Ambiguity (Client Rule: Manual Review)
                 logger.warning(f"⚠️ Missing Size: One-sided missing data ('{amazon_size}' vs '{flipkart_size}')")
                 failed_steps.append("size_missing")
            else:
                 logger.info("ℹ️ Size missing - skipping size check")
            result["dict_parameter_scores"]["size"] = None

        elif size_score == 1.0:
            # Perfect match
            logger.info(f"✅ Size match: '{amazon_size}' ≈ '{flipkart_size}'")
            result["dict_parameter_scores"]["size"] = 1.0
            
        elif size_score == -1.0:
            # Unhandled/Unmapped Size (e.g. Shoe sizes, garbage text)
            logger.warning(f"⚠️ Ambiguous Size Match (Unhandled/Unmapped): '{amazon_size}' vs '{flipkart_size}' (score: -1.0)")
            result["dict_parameter_scores"]["size"] = -1.0
            failed_steps.append("size_ambiguity") # Triggers Manual Review

        elif size_score == 0.5:
            # Partial match - same category, different size (SOFT FAILURE)
            # FORCE MANUAL REVIEW per Client Request
            logger.warning(f"⚠️ Size Mismatch (Partial/Same Category): '{amazon_size}' vs '{flipkart_size}' (score: 0.5)")
            result["dict_parameter_scores"]["size"] = 0.5
            failed_steps.append("size_mismatch") # Triggers Manual Review

        else:
            # No match - different categories (e.g. Bra vs Letter)
            # FORCE MANUAL REVIEW per Client Request
            logger.warning(f"❌ Size Mismatch (Different Categories): '{amazon_size}' vs '{flipkart_size}' (score: 0.0)")
            result["dict_parameter_scores"]["size"] = 0.0
            result["termination_reason"] = "size_mismatch"
            failed_steps.append("size_mismatch") # Triggers Manual Review
            # Continue to other checks instead of returning early


    # Step 5: Title (GenAI PRIMARY, Fuzzy FALLBACK)
    step_counter += 1
    title_a = p1.get("title", "")
    title_b = p2.get("title", "")
    
    if ENABLE_GENAI_MATCHING:
        genai_title_result = invoke_bedrock_title_similarity(p1, p2)
        title_score = genai_title_result.get("title_score", 0.0)
        result["genai_title_reason"] = genai_title_result.get("reason", "")
        logger.info(f"✨ GenAI Title Match: {title_score:.3f} - {result['genai_title_reason']}")
    else:
        # FALLBACK: Use fuzzy logic if GenAI is disabled
        title_score = title_similarity_fuzzy(title_a, title_b)
        logger.info(f"📝 Fuzzy Title Match (Fallback): {title_score:.3f}")
    
    result["dict_parameter_scores"]["title"] = round(title_score, 4)
    result["step_completed"] = step_counter
    
    if title_score < THRESHOLD_TITLE:
        failed_steps.append("title_similarity")

    
    # Step 6: Content & Specs (GenAI Granular Attribute Scoring)
    step_counter += 1
    content_a = p1.get("content", "") or p1.get("description", "")
    content_b = p2.get("content", "") or p2.get("description", "")
    
    if ENABLE_GENAI_MATCHING:
        genai_content_result = invoke_bedrock_content_similarity(p1, p2)
        content_score = genai_content_result.get("content_score", 0.0)
        result["genai_reason"] = genai_content_result.get("reason", "")
        genai_attrs = genai_content_result.get("attributes", {})
        result["genai_attributes"] = genai_attrs
        logger.info(f"✨ GenAI Content Match: {content_score:.3f} - {result['genai_reason']}")
        
        # Color Exclusion Logic: If Color is MISMATCH, check Visual Evidence
        color_attr = genai_attrs.get("color", {})
        color_status = color_attr if isinstance(color_attr, str) else color_attr.get("status")
        
        if color_status == "MISMATCH":
            # Deterministic Override: "Multicolor" or "Print" matches ANYTHING
            def _is_multicolor(p):
                # Check explicit color field
                c_val = str(p.get("color") or "").lower()
                # Check specs
                if not c_val:
                    specs = p.get("specs", {})
                    c_val = str(specs.get("Color") or specs.get("Colour") or "").lower()
                
                valid_keywords = ["multi", "print", "assorted", "rainbow", "graphics", "abstract"]
                return any(k in c_val for k in valid_keywords)

            if _is_multicolor(p1) or _is_multicolor(p2):
                logger.info("🌈 Multicolor/Print detected - Overriding GenAI color mismatch to MATCH")
                # Boost content_score to undo the false color penalty baked into GenAI score
                old_score = content_score
                content_score = min(1.0, content_score + 0.20)
                logger.info(f"🔧 Score correction: {old_score:.3f} → {content_score:.3f} (+0.20 color penalty removed)")
                # Do NOT append to failed_steps
            else:
                logger.info("ℹ️  Color mismatch detected by GenAI (Informational)")
                failed_steps.append("color_mismatch")
        
        # Gender Match Catch
        gender_attr = genai_attrs.get("gender", {})
        gender_status = gender_attr if isinstance(gender_attr, str) else gender_attr.get("status")
        
        if gender_status == "MISMATCH":
             logger.warning("❌ Gender mismatch detected by GenAI")
             failed_steps.append("gender_mismatch")
        
        # ---------------------------------------------------------------
        # NEW: Explicit Attribute Mismatch Detection (7 new failure tags)
        # ---------------------------------------------------------------
        ATTRIBUTE_FAILURE_MAP = {
            "brand":              "brand_mismatch",
            "quantity":           "quantity_mismatch",
            "item_dimensions":    "dimensions_mismatch",
            "item_weight":        "weight_mismatch",
        }

        for attr_key, failure_tag in ATTRIBUTE_FAILURE_MAP.items():
            attr_data = genai_attrs.get(attr_key, {})
            attr_status = attr_data if isinstance(attr_data, str) else attr_data.get("status")
            if attr_status == "MISMATCH":
                logger.warning(f"❌ {attr_key} mismatch detected by GenAI")
                failed_steps.append(failure_tag)
    else:
        # FALLBACK: Use fuzzy logic if GenAI is disabled
        content_score = content_similarity_fuzzy(content_a, content_b)
        logger.info(f"📝 Fuzzy Content Match (Fallback): {content_score:.3f}")
    
    result["dict_parameter_scores"]["content"] = round(content_score, 4)
    result["step_completed"] = step_counter
    
    if content_score < THRESHOLD_CONTENT:
        failed_steps.append("content_similarity")
    
    
# Step 7: Overall
    step_counter += 1
    # -------------------------------------------------------------------------
    # NEW WEIGHTING LOGIC (2026-02-19 Revision 4 - GenAI Centric v6.0)
    # Weights: Title(GenAI) 20%, Content(GenAI) 35%, Visual 25%, Stock 10%, Size 10%
    # -------------------------------------------------------------------------
    
    # Handle missing scores gracefully (treat as 0 if missing)
    score_image =   image_score if image_score is not None else 0.0
    score_title =   title_score if title_score is not None else 0.0
    score_content = content_score if content_score is not None else 0.0
    
    # Stock score
    if "stock_check" in result["dict_parameter_scores"] and isinstance(result["dict_parameter_scores"], dict) and result["dict_parameter_scores"].get("stock_check") is not None:
         score_stock = float(result["dict_parameter_scores"]["stock_check"])
    else:
         score_stock = 0.0
    
    # Size score check
    if "size" in result["dict_parameter_scores"] and isinstance(result["dict_parameter_scores"], dict) and result["dict_parameter_scores"].get("size") is not None:
        raw_size_score = result["dict_parameter_scores"]["size"]
        # Treat -1.0 (ambiguity) as 0.0 for math purposes so it doesn't subtract from overall score
        score_size = max(0.0, float(raw_size_score))
    else:
        score_size = 0.0

    overall_similarity = (
        (score_title * 0.20) +     # GenAI Title (20%)
        (score_content * 0.35) +   # GenAI Content (35%)
        (score_image * 0.25) +     # Visual (25%)
        (score_stock * 0.10) +     # Stock (10%)
        (score_size * 0.10)        # Size (10%)
    )
    
    result["overall_similarity_percentage"] = round(overall_similarity, 4)
    result["step_completed"] = step_counter
    
    # ───────────────────────── FINAL DECISION LOGIC
    # 
    # CRITICAL INSIGHT: In retail catalog matching, not all failures are equal
    # 
    # REJECT FAILURES (hard blockers, automatically reject):
    #   - stock_availability: Different stock status
    #   - nudge_price: Price difference (must meet target price)
    #   - low_visual_similarity: Image score too low
    #   - gender_mismatch: Different targeted gender
    #   - brand_mismatch: Different brand
    #   - mrp_mismatch / Incorrect_List_Price: Large MRP deviation
    #   - quantity_mismatch: Pack of 1 vs Pack of 2
    # 
    # MANUAL REVIEW FAILURES (requires human eye before approval/rejection):
    #   - size_ambiguity: Ambiguous size match
    #   - title_similarity: Low title match
    #   - content_similarity: Low content match
    #   - color_mismatch: Hard color mismatch
    #   - dimensions_mismatch: Size/Dimensions bounds broken
    #   - weight_mismatch: Weight bounds broken
    # 
    # APPROVE FAILURES (Informational only):
    #   - flipkart_size_not_purchasable: Size is unclickable
    # 
    # DECISION CRITERIA:
    #   1. If REJECT failures exist → REJECT
    #   2. If MANUAL REVIEW failures exist OR overall_similarity < threshold → MANUAL_REVIEW
    #   3. Otherwise → APPROVE
    
    # Identify critical vs informational failures
    REJECT_FAILURES = {
        "stock_availability", 
        "nudge_price",
        "Out Of Stock AZ",
        "Out Of Stock FK",
        "Out Of Stock Both",
        "gender_mismatch",
        "brand_mismatch",
        "quantity_mismatch",
        "low_visual_similarity"
    }

    MANUAL_REVIEW_FAILURES = {
        "size_ambiguity",
        "size_missing",
        "title_similarity",
        "content_similarity",
        "color_mismatch",
        "dimensions_mismatch",
        "weight_mismatch",
    }
    
    # Add MRP failure if present (using the exact tag we determined earlier)
    if not mrp_pass:
        mrp_tag = mrp_fail_reason if mrp_fail_reason else "mrp_mismatch"
        REJECT_FAILURES.add(mrp_tag)

    # NEW: Add critical failure for low visual similarity
    # Threshold increased to 0.69 per user request
    if score_image < 0.69:
        failed_steps.append("low_visual_similarity")
        REJECT_FAILURES.add("low_visual_similarity")
        logger.warning(f"❌ CRITICAL: Visual similarity {score_image} < 0.69")
    
    # -------------------------------------------------------------------------
    # CONDITIONAL FAILURE LOGIC (User Request 2026-02-13)
    # If Product is OOS on Flipkart AND Nudge Price fails:
    # -> OOS FK is Critical
    # -> Nudge Price becomes Informational (downgraded)
    # -------------------------------------------------------------------------
    # Check if Flipkart is specifically OOS
    is_fk_oos = any(x in failed_steps for x in ["Out Of Stock FK", "Out Of Stock Both"])
    
    if is_fk_oos:
        if "nudge_price" in REJECT_FAILURES:
            REJECT_FAILURES.remove("nudge_price")
            MANUAL_REVIEW_FAILURES.add("nudge_price")
            logger.info("ℹ️  Downgraded 'nudge_price' to MANUAL_REVIEW because Flipkart is OOS")
    
    critical_failures = [step for step in failed_steps if step in REJECT_FAILURES]
    manual_review_failures = [step for step in failed_steps if step in MANUAL_REVIEW_FAILURES]
    
    # Log failure breakdown
    if critical_failures:
        logger.warning(f"🔴 REJECT failures (Critical Blocks): {critical_failures}")
    if manual_review_failures:
        logger.warning(f"🟡 MANUAL REVIEW failures (Ambiguous): {manual_review_failures}")
    
    # ─────────────────────────────────────────────────────────────────
    # NEW: PRIORITY-BASED PRIMARY FAILURE REASON
    # ─────────────────────────────────────────────────────────────────
    INCORRECT_CATALOG_DATA_TAGS = {
        "color_mismatch"
    }
    CMT_MISMATCH_TAGS = {
        "brand_mismatch", "quantity_mismatch", "gender_mismatch",
        "low_visual_similarity", "title_similarity",
        "size_ambiguity", "size_mismatch", "dimensions_mismatch", "weight_mismatch"
    }
    OPERATIONAL_TAGS = {
        "stock_availability", "Out Of Stock AZ", "Out Of Stock FK",
        "Out Of Stock Both", "nudge_price", "flipkart_size_not_purchasable"
    }
    
    all_failure_set = set(failed_steps)
    
    # Build MRP tag set dynamically (includes reason string like "Incorrect_List_Price (15.2%)")
    incorrect_list_price_tags = {"mrp_mismatch"}
    if not mrp_pass and mrp_fail_reason:
        incorrect_list_price_tags.add(mrp_fail_reason)
        
    # -------------------------------------------------------------------------
    # NEW: PRIORITY-BASED PRIMARY FAILURE REASON
    # -------------------------------------------------------------------------
    primary_failure_reason = None
    if "title_similarity" in all_failure_set:
        primary_failure_reason = "title_similarity"
    else:
        has_image = "low_visual_similarity" in all_failure_set
        has_color = "color_mismatch" in all_failure_set
        if has_image or has_color:
            items = []
            if has_image: items.append("low_visual_similarity")
            if has_color: items.append("color_mismatch")
            primary_failure_reason = ", ".join(items)
        else:
            FAILURE_PRIORITY_HIERARCHY = [
                "size_ambiguity",
                "brand_mismatch",
                "quantity_mismatch",
                "gender_mismatch",
                "size_mismatch",
                "dimensions_mismatch",
                "weight_mismatch",
                "content_similarity",
                "stock_availability",
                "Out Of Stock AZ",
                "Out Of Stock FK",
                "Out Of Stock Both",
            ]
            # Add dynamic MRP tags to hierarchy
            FAILURE_PRIORITY_HIERARCHY.extend(list(incorrect_list_price_tags))
            FAILURE_PRIORITY_HIERARCHY.append("nudge_price")
            
            for priority_tag in FAILURE_PRIORITY_HIERARCHY:
                if priority_tag in all_failure_set:
                    primary_failure_reason = priority_tag
                    break
            
    # Fallback to any generic failure if none found in hierarchy
    # Exclude informational flipkart size issue from being primary
    filtered_failures = [f for f in all_failure_set if f != "flipkart_size_not_purchasable"]
    if not primary_failure_reason and filtered_failures:
        primary_failure_reason = filtered_failures[0]
        
    # Determine the category of the primary failure reason and format it
    if primary_failure_reason:
        category = "UNDEFINED"
        if "low_visual_similarity" in primary_failure_reason or "color_mismatch" in primary_failure_reason or any(tag in primary_failure_reason for tag in CMT_MISMATCH_TAGS):
            category = "CMT MISMATCH"
        elif any(tag in primary_failure_reason for tag in INCORRECT_CATALOG_DATA_TAGS):
            category = "INCORRECT CATALOG DATA"
        elif any(tag in primary_failure_reason for tag in OPERATIONAL_TAGS):
            category = "OPERATIONAL"
        elif any(tag in primary_failure_reason for tag in incorrect_list_price_tags):
            category = "INCORRECT LIST PRICE"
            
        formatted_primary_reason = f"{category} -> ({primary_failure_reason})"
        result["primary_failure_reason"] = formatted_primary_reason
        # Keep a raw version just in case it's needed programmatically later
        result["raw_primary_failure_reason"] = primary_failure_reason
    else:
        result["primary_failure_reason"] = None
        result["raw_primary_failure_reason"] = None
    
    # ─────────────────────────────────────────────────────────────────
    # MAKE FINAL WORKFLOW DECISION (Driven by Primary Failure)
    # ─────────────────────────────────────────────────────────────────
    if primary_failure_reason:
        primary_tags = [t.strip() for t in primary_failure_reason.split(",")]
        is_reject = any(t in REJECT_FAILURES for t in primary_tags)
        is_manual = any(t in MANUAL_REVIEW_FAILURES for t in primary_tags) or "flipkart_size_not_purchasable" in primary_tags or overall_similarity < THRESHOLD_FINAL

        if is_reject:
            # The most important failure is a critical REJECT
            result["recommendation_action"] = "reject"
            result["approved_match"] = False
            result["workflow_status"] = WORKFLOW_STATUS["COMPLETED"]
            logger.warning(f"❌ REJECTED due to primary critical failure: {primary_failure_reason}")
        elif is_manual:
            # The most important failure is ambiguous or low similarity
            result["recommendation_action"] = "manual_review"
            result["approved_match"] = False
            result["manual_review_reason"] = primary_failure_reason
            result["workflow_status"] = WORKFLOW_STATUS["COMPLETED"]
            logger.warning(f"⚠️ MANUAL REVIEW FORCED due to primary failure: {primary_failure_reason}")
        else:
            # Fallback for unknown failures
            result["recommendation_action"] = "manual_review"
            result["approved_match"] = False
            result["manual_review_reason"] = primary_failure_reason
            result["workflow_status"] = WORKFLOW_STATUS["COMPLETED"]
            logger.warning(f"⚠️ MANUAL REVIEW FORCED due to unknown primary failure: {primary_failure_reason}")
            
    elif overall_similarity >= THRESHOLD_FINAL:
        # High similarity, no failures
        result["recommendation_action"] = "approve"
        result["approved_match"] = True
        result["workflow_status"] = WORKFLOW_STATUS["COMPLETED"]
        logger.info(f"✅ APPROVED: {overall_similarity:.3f}")
    else:
        # Low similarity requires manual review
        result["recommendation_action"] = "manual_review"
        result["approved_match"] = False
        result["manual_review_reason"] = f"Overall Similarity below {THRESHOLD_FINAL}"
        result["workflow_status"] = WORKFLOW_STATUS["COMPLETED"]
        logger.warning(f"⚠️ MANUAL REVIEW: {overall_similarity:.3f} < threshold")
    
    # Store failure breakdown for transparency
    result["failed_steps"] = failed_steps
    result["critical_failures"] = critical_failures
    result["informational_failures"] = manual_review_failures # Maintain compatibility with Dynamo schemas
    
    # ─────────────────────────────────────────────────────────────────
    # FAILURE CATEGORIZATION (Structured Hierarchy)
    # ─────────────────────────────────────────────────────────────────
    temp_fd = {
        "incorrect_list_price": sorted(all_failure_set & incorrect_list_price_tags),
        "incorrect_catalog_data": sorted(all_failure_set & INCORRECT_CATALOG_DATA_TAGS),
        "cmt_mismatch": sorted(all_failure_set & CMT_MISMATCH_TAGS),
        "operational": sorted(all_failure_set & OPERATIONAL_TAGS),
    }
    
    result["failure_details"] = {k: v for k, v in temp_fd.items() if v}
    result["combined_failures"] = sorted(all_failure_set)
    
    # ─────────────────────────────────────────────────────────────────
    # TERMINAL OUTPUT: Categorized Failure Summary
    # ─────────────────────────────────────────────────────────────────
    fd = result["failure_details"]
    decision_map = {
        "approve": ("✅", "APPROVED"),
        "reject": ("❌", "REJECTED (Critical Failures)"),
        "manual_review": ("⚠️", "MANUAL REVIEW REQUIRED")
    }
    d_icon, d_text = decision_map.get(result.get("recommendation_action", "manual_review"), ("⚠️", "UNKNOWN"))

    if any(fd.values()) or result.get("recommendation_action") != "approve":
        print("\n" + "="*60)
        print(f"{d_icon} WORKFLOW DECISION: {d_text}")
        if result.get("primary_failure_reason"):
            print(f"🎯 PRIMARY REASON: {result['primary_failure_reason']}")
        print("="*60)
        print("📋 STRUCTURAL CATEGORY BREAKDOWN")
        print("-"*60)
        
        category_labels = {
            "incorrect_list_price": ("�", "INCORRECT LIST PRICE"),
            "incorrect_catalog_data": ("�", "INCORRECT CATALOG DATA"),
            "cmt_mismatch": ("🔴", "CMT MISMATCH"),
            "operational": ("⚙️", "OPERATIONAL"),
        }
        
        for key, (icon, label) in category_labels.items():
            items = fd.get(key, [])
            if items:
                for item in items:
                    print(f"  {icon} {label}: {item}")
        
        if not any(fd.values()):
            print("  ✅ No categorized failures")
        print("="*60 + "\n")
    
    # -------------------------------------------------------------------------
    # NEW: RICH TERMINAL OUTPUT FOR MANUAL REVIEW
    # -------------------------------------------------------------------------
    # Show side-by-side comparison if Title or Content failed
    if "title_similarity" in failed_steps or "content_similarity" in failed_steps:
        try:
            print_manual_review_table(p1, p2, result["dict_parameter_scores"], result.get("genai_attributes", {}), result.get("image_comparison", {}))
        except Exception as e:
            logger.error(f"Rich table error: {e}")

    # Clean up termination reason if approved
    if result.get("approved_match"):
        result.pop("termination_reason", None)

    return result

def print_manual_review_table(p1: Dict[str, Any], p2: Dict[str, Any], scores: Dict[str, Any], genai_attrs: Dict[str, Any] = None, image_comp: Dict[str, Any] = None) -> None:
    """
    Prints a comprehensive side-by-side comparison table using Rich.
    Shows title, content, image score, and all GenAI attribute comparisons.
    """
    console = Console()
    genai_attrs = genai_attrs or {}
    image_comp = image_comp or {}
    
    # Create main table
    table = Table(
        title="[bold yellow]⚠️ PRODUCT COMPARISON BREAKDOWN[/bold yellow]",
        box=box.ROUNDED,
        show_lines=True,
        width=140
    )
    
    # Add columns
    table.add_column("Field", style="cyan bold", width=18)
    table.add_column("Amazon (Source)", style="white", width=40, overflow="fold")
    table.add_column("Flipkart (Candidate)", style="white", width=40, overflow="fold")
    table.add_column("Status", justify="center", width=12)
    table.add_column("Score", justify="right", width=10)
    
    # --- Row: Title ---
    title_score = scores.get("title", 0.0)
    title_color = "green" if title_score >= THRESHOLD_TITLE else "red"
    table.add_row(
        "Title",
        p1.get("title", "N/A"),
        p2.get("title", "N/A"),
        f"[{title_color}]{'✅ PASS' if title_score >= THRESHOLD_TITLE else '❌ FAIL'}[/{title_color}]",
        f"[{title_color}]{title_score:.3f}[/{title_color}]"
    )
    
    # --- Row: Content ---
    def _trunc(text, limit=120):
        t = str(text or "")
        return (t[:limit] + "...") if len(t) > limit else t
    
    content_score = scores.get("content", 0.0)
    content_color = "green" if content_score >= THRESHOLD_CONTENT else "red"
    table.add_row(
        "Content",
        _trunc(p1.get("content") or p1.get("description")),
        _trunc(p2.get("content") or p2.get("description")),
        f"[{content_color}]{'✅ PASS' if content_score >= THRESHOLD_CONTENT else '❌ FAIL'}[/{content_color}]",
        f"[{content_color}]{content_score:.3f}[/{content_color}]"
    )
    
    # --- Row: Image ---
    img_score = scores.get("image", 0.0)
    img_color = "green" if img_score >= 0.65 else "red"
    img_analysis = image_comp.get("genai_analysis", "N/A") if image_comp else "N/A"
    table.add_row(
        "Image",
        _trunc(img_analysis, 80),
        "",
        f"[{img_color}]{'✅ PASS' if img_score >= 0.65 else '❌ FAIL'}[/{img_color}]",
        f"[{img_color}]{img_score:.3f}[/{img_color}]"
    )
    
    # --- Separator ---
    table.add_row("[dim]─" * 18, "[dim]─" * 40, "[dim]─" * 40, "[dim]─" * 12, "[dim]─" * 10)
    
    # --- GenAI Attribute Rows ---
    STATUS_ICONS = {
        "MATCH": "[green]✅ MATCH[/green]",
        "MISMATCH": "[red]❌ MISMATCH[/red]",
        "UNKNOWN": "[yellow]❓ UNKNOWN[/yellow]"
    }
    
    ATTR_LABELS = {
        "brand": "Brand",
        "quantity": "Quantity",
        "color": "Color",
        "gender": "Gender",
        "item_dimensions": "Dimensions",
        "item_weight": "Weight",
    }
    
    for attr_key, label in ATTR_LABELS.items():
        attr_data = genai_attrs.get(attr_key, {})
        if isinstance(attr_data, dict):
            status = attr_data.get("status", "UNKNOWN")
            val_a = str(attr_data.get("value_a", "N/A"))
            val_b = str(attr_data.get("value_b", "N/A"))
        else:
            status = str(attr_data) if attr_data else "UNKNOWN"
            val_a = "N/A"
            val_b = "N/A"
        
        status_display = STATUS_ICONS.get(status, f"[yellow]{status}[/yellow]")
        table.add_row(label, val_a, val_b, status_display, "")
    
    console.print("\n")
    console.print(table)
    console.print("\n")

# ============================================================================
# MAIN COMPARISON (ALL DEPENDENCIES DECLARED)
# ============================================================================
def compare_products(product_a: Dict[str, Any], product_b: Dict[str, Any]) -> Dict[str, Any]:
    """Main comparison."""
    try:
        result = sequential_product_matching(product_a, product_b)
        
        result["product_az"] = {
            "asin": product_a.get("asin", ""),
            "title": product_a.get("title", ""),
            "nudge_price": product_a.get("nudge_price") or product_a.get("price", 0),
            "price": product_a.get("price", 0),
            "currency": product_a.get("currency", ""),
            "instock": product_a.get("instock", ""),
            "size": product_a.get("size", ""),
            "content": product_a.get("content", ""),
            "image_url": product_a.get("image_url", ""),
            "mrp": product_a.get("mrp", 0),
            "url": product_a.get("url", ""),
            "specs": product_a.get("specs", {})  # Preserve raw specs
        }
        
        result["product_ic"] = {
            "asin": product_b.get("asin", ""),
            "title": product_b.get("title", ""),
            "nudge_price": product_b.get("nudge_price") or product_b.get("price", 0),
            "price": product_b.get("price", 0),
            "currency": product_b.get("currency", ""),
            "instock": product_b.get("instock", ""),
            "size": product_b.get("size", ""),
            "content": product_b.get("content", ""),
            "image_url": product_b.get("image_url", ""),
            "mrp": product_b.get("mrp", 0),
            "url": product_b.get("url", ""),  
            "available_sizes": product_b.get("available_sizes", []),
            "purchasable_sizes": product_b.get("purchasable_sizes", []),
            "size_selection_status": product_b.get("size_selection_status", "NO_TARGET"),
            "specs": product_b.get("specs", {})  # Preserve raw specs
        }
        
        return result
    except Exception as e:
        logger.error(f"Comparison error: {str(e)}")
        return {
            "workflow_status": WORKFLOW_STATUS["ERROR"],
            "error": str(e),
            "step_completed": 0,
            "approved_match": False,
            "dict_parameter_scores": {},
            "overall_similarity_percentage": 0.0
        }

# ============================================================================
# LAMBDA HANDLER
# ============================================================================
# def lambda_handler(event, context):
#     """Lambda handler."""
#     try:
#         logger.info("SIMILARITY v4.3.0 - Invoked")
        
#         if not DYNAMO_RESULTS_TABLE_NAME and not LOCAL_MODE:
#             return {
#                 "statusCode": 500,
#                 "body": json.dumps({"error": "Table not configured"})
#             }
        
#         action = event.get("action", "compare")
        
#         # Compare action
#         if action == "compare":
#             product_a = event.get("product_a")
#             product_b = event.get("product_b")
            
#             if not product_a or not product_b:
#                 return {
#                     "statusCode": 400,
#                     "body": json.dumps({"error": "Both products required"})
#                 }
            
#             result = compare_products(product_a, product_b)
#             
#             # Store in DynamoDB (updated logic)
#             comparison_id = store_result_in_dynamodb(result)
#             
#             formatted = format_comparison_output(result)
#             formatted["metadata"]["comparison_id"] = comparison_id
            
#             return {
#                 "statusCode": 200,
#                 "body": json.dumps(formatted, indent=2, default=str)
#             }
        
#         # Get comparison
#         elif action == "get_comparison":
#             comparison_id = event.get("comparison_id")
#             asin = event.get("asin")
            
#             if not comparison_id or not asin:
#                 return {
#                     "statusCode": 400,
#                     "body": json.dumps({"error": "comparison_id and asin required"})
#                 }
            
#             comparison = get_comparison_by_id(comparison_id, asin)
            
#             if not comparison:
#                 return {
#                     "statusCode": 404,
#                     "body": json.dumps({"error": "Not found"})
#                 }
            
#             return {
#                 "statusCode": 200,
#                 "body": json.dumps(comparison, indent=2, default=str)
#             }
        
#         # Batch compare
#         elif action == "compare_batch":
#             comparisons = event.get("comparisons", [])
            
#             if not comparisons:
#                 return {
#                     "statusCode": 400,
#                     "body": json.dumps({"error": "No comparisons"})
#                 }
            
#             results = []
#             for idx, comp in enumerate(comparisons):
#                 try:
#                     result = compare_products(comp.get("product_a", {}), comp.get("product_b", {}))
#                     comparison_id = None
#                     formatted = format_comparison_output(result)
#                     formatted["metadata"]["comparison_id"] = comparison_id
#                     formatted["batch_index"] = idx
#                     results.append(formatted)
#                 except Exception as e:
#                     results.append({
#                         "batch_index": idx,
#                         "comparison_status": "ERROR",
#                         "error": str(e)
#                     })
            
#             passed = sum(1 for r in results if r.get("comparison_status") == "PASSED")
#             failed = sum(1 for r in results if r.get("comparison_status") == "FAILED")
            
#             return {
#                 "statusCode": 200,
#                 "body": json.dumps({
#                     "total": len(results),
#                     "summary": {"passed": passed, "failed": failed},
#                     "results": results
#                 }, indent=2, default=str)
#             }
        
#         # Export
#         elif action == "export":
#             limit = event.get("limit", 1000)
#             comparisons = get_results_from_dynamodb(limit)
            
#             if not comparisons:
#                 return {
#                     "statusCode": 404,
#                     "body": json.dumps({"message": "No results"})
#                 }
            
#             s3_key = export_comparisons_to_csv(comparisons)
            
#             if s3_key:
#                 return {
#                     "statusCode": 200,
#                     "body": json.dumps({
#                         "message": f"Exported {len(comparisons)}",
#                         "s3_key": s3_key
#                     })
#                 }
#             return {
#                 "statusCode": 500,
#                 "body": json.dumps({"error": "Export failed"})
#             }
        
#         # Health
#         elif action == "health":
#             return {
#                 "statusCode": 200,
#                 "body": json.dumps({
#                     "status": "healthy",
#                     "version": "4.3.0",
#                     "features": {
#                         "image": ENABLE_IMAGE_COMPARISON,
#                         "genai": ENABLE_GENAI_MATCHING
#                     }
#                 })
#             }
        
#         else:
#             return {
#                 "statusCode": 400,
#                 "body": json.dumps({"error": f"Unknown action: {action}"})
#             }
    
#     except Exception as e:
#         logger.error(f"Handler error: {str(e)}", exc_info=True)
#         return {
#             "statusCode": 500,
#             "body": json.dumps({"error": "Internal error", "message": str(e)})
#         }