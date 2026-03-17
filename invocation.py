"""
Production-Ready Orchestrator Lambda - Final Corrected Version
Version: 4.3.0 - Always Scrape Both + Smart Comparison Logic
Last Updated: 2024

CRITICAL FLOW:
1. Extract and validate ASIN
2. ALWAYS scrape Amazon (fresh data, no cache)
3. Extract size from Amazon product
4. ALWAYS scrape Flipkart with size parameter (fresh data, no cache)
5. Check if ASIN exists in database via GSI
6. Decision logic:
   - EXISTING ASIN: Price match → PASS, Price mismatch → FAIL (no similarity)
   - NEW ASIN: Run full similarity comparison (Steps 1-7)

Environment Variables (REQUIRED):
- AMAZON_SCRAPER_LAMBDA: Amazon scraper Lambda function name
- FLIPKART_SCRAPER_LAMBDA: Flipkart scraper Lambda function name
- SIMILARITY_FUNCTION_LAMBDA: Similarity comparison Lambda function name
- DYNAMO_RESULTS_TABLE_NAME: DynamoDB table name for storing results

Environment Variables (OPTIONAL):
- STATUS_TABLE_NAME: DynamoDB table for async status (default: comparison-status)
- AWS_LAMBDA_FUNCTION_NAME: Auto-populated by Lambda runtime
- ENABLE_ASIN_TRACKING: Enable ASIN lookup (default: true)
- ASIN_INDEX_NAME: GSI name for ASIN lookup (default: asin-index)
- PRICE_TOLERANCE: Price match tolerance (default: 0.01)
"""

import json
import boto3
import os
import logging
import re
import uuid
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
logger = logging.getLogger()
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
    logger.addHandler(handler)

# ============================================================================
# AWS SERVICE CLIENTS
# ============================================================================
lambda_client = boto3.client("lambda")
dynamodb_client = boto3.client("dynamodb")

# ============================================================================
# ENVIRONMENT VARIABLES
# ============================================================================
AMAZON_SCRAPER = os.environ.get("AMAZON_SCRAPER_LAMBDA")
FLIPKART_SCRAPER = os.environ.get("FLIPKART_SCRAPER_LAMBDA")
SIMILARITY_FUNCTION = os.environ.get("SIMILARITY_FUNCTION_LAMBDA")
RESULTS_TABLE = os.environ.get("DYNAMO_RESULTS_TABLE_NAME")
STATUS_TABLE = os.environ.get("STATUS_TABLE_NAME", "comparison-status")
ORCHESTRATOR_FUNCTION_NAME = os.environ.get("AWS_LAMBDA_FUNCTION_NAME")

ENABLE_ASIN_TRACKING = os.environ.get("ENABLE_ASIN_TRACKING", "true").lower() == "true"
ASIN_INDEX_NAME = os.environ.get("ASIN_INDEX_NAME", "asin-index")
PRICE_TOLERANCE = float(os.environ.get("PRICE_TOLERANCE", "0.01"))

# ============================================================================
# ENVIRONMENT VALIDATION
# ============================================================================
def validate_environment():
    """Validate all required environment variables."""
    errors = []
    
    if not AMAZON_SCRAPER:
        errors.append("AMAZON_SCRAPER_LAMBDA not set")
    if not FLIPKART_SCRAPER:
        errors.append("FLIPKART_SCRAPER_LAMBDA not set")
    if not SIMILARITY_FUNCTION:
        errors.append("SIMILARITY_FUNCTION_LAMBDA not set")
    if not RESULTS_TABLE:
        errors.append("DYNAMO_RESULTS_TABLE_NAME not set")
    
    if errors:
        error_msg = "; ".join(errors)
        logger.error(f"Environment validation failed: {error_msg}")
        raise ValueError(error_msg)
    
    logger.info(f"✓ Environment validated - ASIN tracking: {ENABLE_ASIN_TRACKING}")

try:
    validate_environment()
except ValueError as e:
    logger.error(f"FATAL: {str(e)}")

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================
def safe_json_parse(data_str: str) -> Dict[str, Any]:
    """Safely parse JSON with fallback."""
    if not isinstance(data_str, str):
        return data_str if isinstance(data_str, dict) else {}
    
    if not data_str.strip():
        return {}
    
    try:
        return json.loads(data_str)
    except json.JSONDecodeError:
        try:
            return json.loads(data_str.replace('#', ''))
        except json.JSONDecodeError:
            logger.error(f"JSON parse failed")
            return {}

def extract_asin_from_url(url: str) -> Optional[str]:
    """Extract ASIN from Amazon URL."""
    if not url:
        return None
    
    patterns = [
        r'/dp/([A-Z0-9]{10})',
        r'/gp/product/([A-Z0-9]{10})',
        r'ASIN[=/]([A-Z0-9]{10})',
        r'/product/([A-Z0-9]{10})',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url, re.IGNORECASE)
        if match:
            asin = match.group(1).upper()
            logger.info(f"Extracted ASIN from URL: {asin}")
            return asin
    
    return None

def extract_asin_from_scraped_data(amazon_data: Dict[str, Any]) -> Optional[str]:
    """Extract ASIN from scraped Amazon data."""
    try:
        body = amazon_data.get("body", "{}")
        if isinstance(body, str):
            body = safe_json_parse(body)
        
        product = body.get("data", {})
        asin = product.get("asin")
        
        if asin:
            asin = str(asin).strip().upper()
            logger.info(f"✓ ASIN from scraped data: {asin}")
            return asin
        return None
    except Exception as e:
        logger.error(f"Error extracting ASIN: {str(e)}")
        return None

def extract_size_from_amazon(amazon_data: Dict[str, Any]) -> Optional[str]:
    """
    Extract size/variant from Amazon scraped data.
    RESTORED FROM ORIGINAL CODE - handles multiple sources.
    """
    try:
        if isinstance(amazon_data.get("body"), str):
            body = safe_json_parse(amazon_data["body"])
        else:
            body = amazon_data.get("body", {})
        
        product = body.get("data", {})
        size = product.get("size")
        
        # Try specs if no direct size
        if not size:
            specs = product.get("specs", {})
            size = specs.get("size") or specs.get("Size") or specs.get("variant")
        
        # Try extracting from title if still no size
        if not size:
            title = product.get("title", "")
            size_patterns = [
                r'\b(\d+\s*(?:ml|l|litre|liter|oz|fl\.?\s*oz))\b',
                r'\b(\d+\s*(?:g|gm|gram|kg|kilogram|lb|pound))\b',
                r'\b(X{0,3}[SML])\b',
                r'\b(\d+(?:\.\d+)?\s*(?:inch|in|cm|mm))\b',
                r'\b(\d+\s*(?:Pack|Count|Piece))\b',
            ]
            for pattern in size_patterns:
                match = re.search(pattern, title, re.IGNORECASE)
                if match:
                    size = match.group(1)
                    break
        
        if size:
            size = str(size).strip()
            logger.info(f"✓ Extracted size from Amazon: {size}")
            return size
        
        logger.warning("No size found in Amazon data")
        return None
    except Exception as e:
        logger.error(f"Error extracting size: {str(e)}")
        return None

# ============================================================================
# INPUT VALIDATION
# ============================================================================
def validate_input(detail: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Validate input parameters."""
    nudge_price = detail.get("nudge_price")
    amazon_url = detail.get("amazon_url")
    flipkart_url = detail.get("flipkart_url")
    amazon_asin = detail.get("amazon_asin") or detail.get("asin")
    
    if not nudge_price:
        return False, "Missing nudge_price"
    
    try:
        price_val = float(nudge_price)
        if price_val <= 0:
            return False, "nudge_price must be > 0"
    except (ValueError, TypeError):
        return False, "nudge_price must be a valid number"
    
    if not (amazon_url or amazon_asin):
        return False, "amazon_url or amazon_asin required"
    
    if not flipkart_url:
        return False, "flipkart_url required"
    
    return True, None

# ============================================================================
# ASIN TRACKING
# ============================================================================
def check_asin_exists(asin: str) -> bool:
    """Check if ASIN exists in database via GSI."""
    if not ENABLE_ASIN_TRACKING or not RESULTS_TABLE:
        return False
    
    try:
        response = dynamodb_client.query(
            TableName=RESULTS_TABLE,
            IndexName=ASIN_INDEX_NAME,
            KeyConditionExpression="asin = :asin",
            ExpressionAttributeValues={":asin": {"S": asin}},
            Limit=1,
            Select="COUNT"
        )
        
        count = response.get("Count", 0)
        exists = count > 0
        
        if exists:
            logger.info(f"✓ ASIN exists: {asin}")
        else:
            logger.info(f"✓ New ASIN: {asin}")
        
        return exists
    except Exception as e:
        logger.error(f"ASIN check error: {str(e)}")
        return False

# ============================================================================
# SCRAPER INVOCATION
# ============================================================================
def invoke_scraper_sync(function_name: str, payload: Dict[str, Any], scraper_name: str) -> Dict[str, Any]:
    """Invoke scraper Lambda synchronously."""
    try:
        logger.info(f"→ Invoking {scraper_name} scraper")
        
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload)
        )
        
        if response.get('StatusCode') != 200:
            return {
                "status": "failed",
                "error": f"Invoke failed: {response.get('StatusCode')}",
                "data": None
            }
        
        response_payload = json.loads(response['Payload'].read())
        
        if response_payload.get('statusCode') != 200:
            error_body = response_payload.get('body', {})
            if isinstance(error_body, str):
                error_body = safe_json_parse(error_body)
            return {
                "status": "error",
                "error": error_body.get('error', 'Scraper error'),
                "data": None
            }
        
        logger.info(f"✓ {scraper_name} completed")
        return {"status": "success", "data": response_payload, "error": None}
        
    except Exception as e:
        logger.error(f"{scraper_name} error: {str(e)}")
        return {"status": "failed", "error": str(e), "data": None}

# ============================================================================
# SIMILARITY INVOCATION
# ============================================================================
def invoke_similarity_function(amazon_data: Dict[str, Any], flipkart_data: Dict[str, Any], nudge_price: Any) -> Dict[str, Any]:
    """Invoke similarity Lambda."""
    if not SIMILARITY_FUNCTION:
        return {"status": "failed", "error": "Similarity function not configured", "data": None}
    
    try:
        amazon_body = safe_json_parse(amazon_data.get("body", "{}"))
        flipkart_body = safe_json_parse(flipkart_data.get("body", "{}"))
        
        amazon_product = amazon_body.get("data", {})
        flipkart_product = flipkart_body.get("data", {})
        
        amazon_product["nudge_price"] = nudge_price
        flipkart_product["nudge_price"] = nudge_price
        
        payload = {
            "action": "compare",
            "product_a": amazon_product,
            "product_b": flipkart_product
        }
        
        logger.info(f"→ Invoking similarity function")
        
        response = lambda_client.invoke(
            FunctionName=SIMILARITY_FUNCTION,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        
        if response.get('StatusCode') != 200:
            return {"status": "failed", "error": "Similarity invoke failed", "data": None}
        
        response_payload = json.loads(response['Payload'].read())
        
        if response_payload.get('statusCode') != 200:
            return {"status": "failed", "error": "Similarity returned error", "data": None}
        
        body = response_payload.get('body', '{}')
        if isinstance(body, str):
            body = safe_json_parse(body)
        
        logger.info("✓ Similarity completed")
        return {"status": "success", "data": body, "error": None}
        
    except Exception as e:
        logger.error(f"Similarity error: {str(e)}")
        return {"status": "failed", "error": str(e), "data": None}

# ============================================================================
# STATUS UPDATES
# ============================================================================
def update_status(request_id: str, status: str, message: str, result: dict = None):
    """Update async status."""
    if not STATUS_TABLE:
        return
    
    try:
        item = {
            "request_id": {"S": request_id},
            "status": {"S": status},
            "timestamp": {"S": datetime.utcnow().isoformat()},
            "progress": {"S": message},
            "ttl": {"N": str(int(datetime.utcnow().timestamp()) + 86400)}
        }
        
        if result:
            result_str = json.dumps(result, default=str)
            if len(result_str) > 400000:
                result_str = result_str[:400000]
            item["result"] = {"S": result_str}
        
        dynamodb_client.put_item(TableName=STATUS_TABLE, Item=item)
    except Exception as e:
        logger.error(f"Status update error: {str(e)}")

# ============================================================================
# PRICE VALIDATION
# ============================================================================
def validate_price_match(nudge_price: float, flipkart_price: float) -> Tuple[bool, Dict[str, Any]]:
    """Validate price match."""
    try:
        nudge_float = float(nudge_price)
        flipkart_float = float(flipkart_price)
        
        if nudge_float <= 0 or flipkart_float <= 0:
            return False, {"error": "Invalid prices", "match": False}
        
        price_diff = abs(nudge_float - flipkart_float)
        price_diff_percent = (price_diff / nudge_float * 100)
        match = price_diff <= PRICE_TOLERANCE
        
        details = {
            "nudge_price": round(nudge_float, 2),
            "flipkart_price": round(flipkart_float, 2),
            "difference": round(price_diff, 2),
            "difference_percent": round(price_diff_percent, 2),
            "tolerance": PRICE_TOLERANCE,
            "match": match
        }
        
        if match:
            logger.info(f"✓ Price match: {nudge_float:.2f} ≈ {flipkart_float:.2f}")
        else:
            logger.warning(f"❌ Price mismatch: {nudge_float:.2f} ≠ {flipkart_float:.2f}")
        
        return match, details
    except Exception as e:
        logger.error(f"Price validation error: {str(e)}")
        return False, {"error": str(e), "match": False}

# ============================================================================
# RESPONSE BUILDERS
# ============================================================================
def create_instant_response(
    asin: str,
    status: str,
    nudge_price: float,
    flipkart_price: float,
    amazon_product: Dict[str, Any],
    flipkart_product: Dict[str, Any],
    price_details: Dict[str, Any]
) -> Dict[str, Any]:
    """Create instant response for existing ASINs."""
    
    if status == "PASSED":
        message = "Existing ASIN - Price match (instant approval)"
        termination_reason = None
        confidence = 100.0
    else:
        message = "Existing ASIN - Price mismatch (instant failure)"
        termination_reason = "nudge_price_mismatch_existing_asin"
        confidence = 0.0
    
    return {
        "comparison_status": status,
        "message": message,
        "step_completed": 1 if status == "PASSED" else 0,
        "termination_reason": termination_reason,
        "overall_confidence_score": confidence,
        "similarity_executed": False,
        "parameters_checked": {
            "nudge_price": {
                "status": status,
                "confidence_score": confidence,
                "type": "existing_asin_price_gate",
                "match": status == "PASSED",
                "details": price_details
            }
        },
        "failed_parameters": [] if status == "PASSED" else ["nudge_price"],
        "metadata": {
            "asin": asin,
            "asin_status": "existing",
            "orchestrator_version": "4.3.0",
            "comparison_mode": "instant_decision",
            "optimization": {
                "similarity_skipped": True,
                "reason": "existing_asin_price_gate",
                "cost_saved": {
                    "similarity_lambda": True,
                    "bedrock_calls": True,
                    "image_comparison": True
                }
            },
            "data_freshness": {
                "amazon": "fresh_scrape",
                "flipkart": "fresh_scrape"
            }
        },
        "product_details": {
            "product_az": {
                "asin": amazon_product.get("asin", ""),
                "title": amazon_product.get("title", ""),
                "price": amazon_product.get("price", 0),
                "currency": amazon_product.get("currency", ""),
                "size": amazon_product.get("size", ""),
                "instock": amazon_product.get("instock", "")
            },
            "product_ic": {
                "title": flipkart_product.get("title", ""),
                "price": flipkart_price,
                "nudge_price": nudge_price,
                "currency": flipkart_product.get("currency", ""),
                "size": flipkart_product.get("size", ""),
                "instock": flipkart_product.get("instock", "")
            }
        }
    }

def format_comparison_output(similarity_result: Dict[str, Any], additional_info: Dict[str, Any] = None) -> Dict[str, Any]:
    """Format similarity result."""
    if similarity_result.get("status") != "success":
        return {
            "comparison_status": "FAILED",
            "message": "Comparison failed",
            "error": similarity_result.get("error"),
            "similarity_executed": False,
            "metadata": {"orchestrator_version": "4.3.0"}
        }
    
    similarity_data = similarity_result.get("data", {})
    
    response = {
        "comparison_status": similarity_data.get("comparison_status", "FAILED"),
        "message": similarity_data.get("message", "Comparison completed"),
        "overall_confidence_score": similarity_data.get("overall_confidence_score", 0.0),
        "step_completed": similarity_data.get("step_completed", 0),
        "parameters_checked": similarity_data.get("parameters_checked", {}),
        "failed_parameters": similarity_data.get("failed_parameters", []),
        "termination_reason": similarity_data.get("termination_reason"),
        "similarity_executed": True,
        "metadata": similarity_data.get("metadata", {})
    }
    
    if additional_info:
        response["metadata"].update(additional_info)
    
    if similarity_data.get("product_details"):
        response["product_details"] = similarity_data["product_details"]
    
    response["metadata"]["orchestrator_version"] = "4.3.0"
    response["metadata"]["comparison_mode"] = "full_similarity"
    
    return response

def create_error_response(message: str, error_details: str = None, asin: str = None) -> Dict[str, Any]:
    """Create error response."""
    response = {
        "comparison_status": "FAILED",
        "message": message,
        "similarity_executed": False,
        "metadata": {"orchestrator_version": "4.3.0"}
    }
    
    if error_details:
        response["error"] = error_details
    if asin:
        response["metadata"]["asin"] = asin
    
    return response

# ============================================================================
# MAIN ORCHESTRATION FLOW
# ============================================================================
def orchestrate_comparison(detail: Dict[str, Any], request_id: str = None) -> Dict[str, Any]:
    """
    Main orchestration logic - CORRECTED with size extraction.
    """
    try:
        nudge_price = detail.get("nudge_price")
        amazon_url = detail.get("amazon_url")
        flipkart_url = detail.get("flipkart_url")
        amazon_asin = detail.get("amazon_asin") or detail.get("asin")
        
        logger.info("="*80)
        logger.info("ORCHESTRATOR v4.3.0 - Starting")
        logger.info("="*80)
        
        # ====================================================================
        # STEP 1: EXTRACT ASIN
        # ====================================================================
        if not amazon_asin and amazon_url:
            amazon_asin = extract_asin_from_url(amazon_url)
        
        if not amazon_asin:
            return create_error_response("ASIN required", "missing_asin")
        
        amazon_asin = amazon_asin.strip().upper()
        
        if len(amazon_asin) != 10 or not amazon_asin.isalnum():
            return create_error_response(f"Invalid ASIN: {amazon_asin}", "invalid_asin", amazon_asin)
        
        logger.info(f"✓ ASIN: {amazon_asin}")
        
        # ====================================================================
        # STEP 2: SCRAPE AMAZON (ALWAYS)
        # ====================================================================
        logger.info("-"*80)
        logger.info("STEP 2: Scraping Amazon (ALWAYS FRESH)")
        logger.info("-"*80)
        
        if request_id:
            update_status(request_id, "PROCESSING", "Scraping Amazon")
        
        amazon_payload = {"asin": amazon_asin, "nudge_price": nudge_price}
        if amazon_url:
            amazon_payload["url"] = amazon_url
        
        amazon_result = invoke_scraper_sync(AMAZON_SCRAPER, amazon_payload, "Amazon")
        
        if amazon_result["status"] != "success":
            return create_error_response(
                "Amazon scraping failed",
                amazon_result.get('error'),
                amazon_asin
            )
        
        amazon_data = amazon_result["data"]
        
        # Extract and inject ASIN
        scraped_asin = extract_asin_from_scraped_data(amazon_data)
        if scraped_asin:
            amazon_asin = scraped_asin
        
        amazon_body = safe_json_parse(amazon_data.get("body", "{}"))
        amazon_product = amazon_body.get("data", {})
        
        if not amazon_product:
            return create_error_response("No Amazon product data", "amazon_no_data", amazon_asin)
        
        if not amazon_product.get("asin"):
            amazon_product["asin"] = amazon_asin
            amazon_body["data"] = amazon_product
            amazon_data["body"] = json.dumps(amazon_body)
        
        logger.info("✓ Amazon scraped")
        
        # ====================================================================
        # CRITICAL: EXTRACT SIZE FROM AMAZON (RESTORED ORIGINAL LOGIC)
        # ====================================================================
        amazon_size = extract_size_from_amazon(amazon_data)
        
        if amazon_size:
            logger.info(f"✓ Amazon size extracted: {amazon_size}")
        else:
            logger.warning("⚠️ No size found in Amazon data")
        
        # ====================================================================
        # STEP 3: SCRAPE FLIPKART (ALWAYS) WITH SIZE
        # ====================================================================
        logger.info("-"*80)
        logger.info("STEP 3: Scraping Flipkart (ALWAYS FRESH)")
        logger.info("-"*80)
        
        if request_id:
            update_status(request_id, "PROCESSING", "Scraping Flipkart")
        
        # RESTORED: Pass size to Flipkart scraper
        flipkart_payload = {
            "url": flipkart_url,
            "nudge_price": nudge_price,
            "size": amazon_size  # ← RESTORED FROM ORIGINAL
        }
        
        flipkart_result = invoke_scraper_sync(
            FLIPKART_SCRAPER,
            flipkart_payload,
            "Flipkart"
        )
        
        if flipkart_result["status"] != "success":
            return create_error_response(
                "Flipkart scraping failed",
                flipkart_result.get('error'),
                amazon_asin
            )
        
        flipkart_data = flipkart_result["data"]
        flipkart_body = safe_json_parse(flipkart_data.get("body", "{}"))
        flipkart_product = flipkart_body.get("data", {})
        print(flipkart_product)
        
        if not flipkart_product:
            return create_error_response("No Flipkart product data", "flipkart_no_data", amazon_asin)
        
        flipkart_price = flipkart_product.get("price", 0)
        flipkart_size = flipkart_product.get("size", "")
        
        if not flipkart_price or flipkart_price <= 0:
            return create_error_response(
                f"Invalid Flipkart price: {flipkart_price}",
                "flipkart_invalid_price",
                amazon_asin
            )
            print(flipkart_price)
        
        logger.info(f"✓ Flipkart scraped - Price: ₹{flipkart_price}, Size: {flipkart_size or 'NOT FOUND'}")
        
        # ====================================================================
        # STEP 4: CHECK ASIN EXISTS
        # ====================================================================
        logger.info("-"*80)
        logger.info("STEP 4: Checking ASIN in Database")
        logger.info("-"*80)
        
        asin_exists = check_asin_exists(amazon_asin)
        asin_status = "existing" if asin_exists else "new"
        
        logger.info(f"✓ ASIN status: {asin_status.upper()}")
        
        # ====================================================================
        # STEP 5: DECISION LOGIC
        # ====================================================================
        logger.info("-"*80)
        logger.info(f"STEP 5: Decision Logic ({asin_status.upper()} ASIN)")
        logger.info("-"*80)
        
        if asin_exists:
            # EXISTING ASIN: Price gate only
            logger.info("→ EXISTING ASIN: Applying price gate")
            
            if request_id:
                update_status(request_id, "PROCESSING", "Price validation (existing ASIN)")
            
            price_match, price_details = validate_price_match(nudge_price, flipkart_price)
            
            if price_match:
                logger.info("✓✓✓ INSTANT PASS - Price matches")
                response = create_instant_response(
                    amazon_asin, "PASSED", nudge_price, flipkart_price,
                    amazon_product, flipkart_product, price_details
                )
                if request_id:
                    update_status(request_id, "COMPLETED", "Instant pass", result=response)
                
                logger.info("="*80)
                logger.info("RESULT: INSTANT PASS (existing ASIN, price match)")
                logger.info(f"Cost saved: Similarity + Bedrock + Image")
                logger.info("="*80)
                return response
            else:
                logger.warning("❌❌❌ INSTANT FAIL - Price mismatch")
                response = create_instant_response(
                    amazon_asin, "FAILED", nudge_price, flipkart_price,
                    amazon_product, flipkart_product, price_details
                )
                if request_id:
                    update_status(request_id, "FAILED", "Instant fail", result=response)
                
                logger.info("="*80)
                logger.info("RESULT: INSTANT FAIL (existing ASIN, price mismatch)")
                logger.info(f"Cost saved: Similarity + Bedrock + Image")
                logger.info("="*80)
                return response
        
        else:
            # NEW ASIN: Full similarity
            logger.info("→ NEW ASIN: Running full similarity")
            
            if request_id:
                update_status(request_id, "PROCESSING", "Running similarity (new ASIN)")
            
            similarity_result = invoke_similarity_function(amazon_data, flipkart_data, nudge_price)
            
            if similarity_result["status"] != "success":
                return create_error_response(
                    "Similarity failed",
                    similarity_result.get("error"),
                    amazon_asin
                )
            
            additional_info = {
                "asin": amazon_asin,
                "asin_status": asin_status,
                "data_freshness": {
                    "amazon": "fresh_scrape",
                    "flipkart": "fresh_scrape"
                }
            }
            
            response = format_comparison_output(similarity_result, additional_info)
            
            if request_id:
                final_status = "COMPLETED" if response.get("comparison_status") == "PASSED" else "FAILED"
                update_status(request_id, final_status, "Comparison completed", result=response)
            
            logger.info("="*80)
            logger.info(f"RESULT: {response.get('comparison_status')} (new ASIN)")
            logger.info(f"Steps completed: {response.get('step_completed')}/7")
            logger.info("="*80)
            return response
        
    except Exception as e:
        logger.error(f"Orchestration error: {str(e)}", exc_info=True)
        if request_id:
            update_status(request_id, "ERROR", str(e))
        return {
            "comparison_status": "ERROR",
            "error": "Internal error",
            "message": str(e),
            "similarity_executed": False,
            "metadata": {"orchestrator_version": "4.3.0"}
        }

# ============================================================================
# LAMBDA HANDLER
# ============================================================================
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """AWS Lambda handler."""
    logger.info("ORCHESTRATOR v4.3.0 - Invoked")
    
    try:
        # Async processing mode
        if event.get("async_processing"):
            request_id = event.get("request_id")
            detail = event.get("detail", {})
            result = orchestrate_comparison(detail, request_id)
            return {
                "statusCode": 200,
                "body": json.dumps({"message": "Async completed", "request_id": request_id})
            }
        
        # Parse input
        if "body" in event:
            body = json.loads(event["body"]) if isinstance(event["body"], str) else event["body"]
            detail = body.get("detail", body)
        elif "detail" in event:
            detail = event["detail"]
        else:
            detail = event
        
        # Validate input
        is_valid, error_message = validate_input(detail)
        if not is_valid:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"error": error_message, "comparison_status": "FAILED"})
            }
        
        # Check async mode
        query_params = event.get("queryStringParameters", {}) or {}
        is_async = query_params.get("async", "false").lower() == "true"
        
        if is_async:
            # Async mode - return request ID
            request_id = str(uuid.uuid4())
            
            try:
                dynamodb_client.put_item(
                    TableName=STATUS_TABLE,
                    Item={
                        "request_id": {"S": request_id},
                        "status": {"S": "PROCESSING"},
                        "timestamp": {"S": datetime.utcnow().isoformat()},
                        "input_data": {"S": json.dumps(detail, default=str)},
                        "progress": {"S": "Accepted"},
                        "ttl": {"N": str(int(datetime.utcnow().timestamp()) + 86400)}
                    }
                )
            except Exception as e:
                return {
                    "statusCode": 500,
                    "body": json.dumps({"error": "Failed to init async"})
                }
            
            try:
                lambda_client.invoke(
                    FunctionName=ORCHESTRATOR_FUNCTION_NAME,
                    InvocationType='Event',
                    Payload=json.dumps({
                        "detail": detail,
                        "request_id": request_id,
                        "async_processing": True
                    })
                )
            except Exception as e:
                return {
                    "statusCode": 500,
                    "body": json.dumps({"error": "Failed to start async"})
                }
            
            return {
                "statusCode": 202,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps({
                    "status": "ACCEPTED",
                    "message": "Request accepted",
                    "request_id": request_id,
                    "status_url": f"/api/status/{request_id}"
                }, indent=2)
            }
        
        # Sync mode - process immediately
        result = orchestrate_comparison(detail)
        status_code = 200 if result.get("comparison_status") in ["PASSED", "FAILED"] else 500
        
        return {
            "statusCode": status_code,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps(result, indent=2, default=str)
        }
    
    except Exception as e:
        logger.error(f"Handler error: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({
                "comparison_status": "ERROR",
                "error": "Internal error",
                "message": str(e),
                "metadata": {"orchestrator_version": "4.3.0"}
            })
        }