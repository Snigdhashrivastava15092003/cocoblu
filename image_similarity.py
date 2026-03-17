"""
Production-Ready Image Comparison Lambda with GenAI Integration
OPTIMIZED: Minimal logging for maximum performance
Uses Amazon Bedrock (Nova) for visual similarity analysis
"""

import json
import os
import base64
import hashlib
from io import BytesIO
from typing import Dict, Any, Optional, Tuple
from urllib.parse import urlparse

import boto3
import requests
from botocore.exceptions import ClientError
from PIL import Image

# ============================================================================
# AWS SERVICE CLIENTS
# ============================================================================
bedrock_runtime = boto3.client("bedrock-runtime")
dynamodb_client = boto3.client("dynamodb")

# ============================================================================
# CONFIGURATION
# ============================================================================
BEDROCK_MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "apac.anthropic.claude-3-haiku-20240307-v1:0")
DYNAMO_CACHE_TABLE_NAME = os.getenv("DYNAMO_IMAGE_CACHE_TABLE_NAME")
ENABLE_CACHE = os.getenv("ENABLE_CACHE", "true").lower() == "true"

# Image processing settings
MAX_IMAGE_SIZE_MB = 10
IMAGE_RESIZE_MAX = 1024  # Max dimension for processing
DOWNLOAD_TIMEOUT = 10

# Thresholds
SIMILARITY_THRESHOLD = float(os.getenv("SIMILARITY_THRESHOLD", "0.75"))

# ============================================================================
# IMAGE DOWNLOAD & PROCESSING
# ============================================================================
def download_image(url: str, timeout: int = DOWNLOAD_TIMEOUT) -> Optional[bytes]:
    """Download image from URL or local path."""
    try:
        # Support local files for testing
        if os.path.exists(url):
            with open(url, "rb") as f:
                return f.read()

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=timeout, stream=True)
        response.raise_for_status()
        
        content = b''.join(chunk for chunk in response.iter_content(8192))
        
        if len(content) > MAX_IMAGE_SIZE_MB * 1024 * 1024:
            return None
        
        return content
    except:
        return None

def load_and_process_image(image_bytes: bytes) -> Optional[Image.Image]:
    """Load and process image."""
    try:
        img = Image.open(BytesIO(image_bytes))
        
        if img.mode not in ('RGB', 'RGBA'):
            img = img.convert('RGB')
        elif img.mode == 'RGBA':
            # Convert RGBA to RGB with white background
            background = Image.new('RGB', img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[3])
            img = background
        
        # Resize if too large
        if max(img.size) > IMAGE_RESIZE_MAX:
            img.thumbnail((IMAGE_RESIZE_MAX, IMAGE_RESIZE_MAX), Image.Resampling.LANCZOS)
        
        return img
    except:
        return None

def image_to_base64(image: Image.Image, format: str = 'JPEG', quality: int = 85) -> str:
    """Convert PIL Image to base64 string."""
    try:
        buffer = BytesIO()
        image.save(buffer, format=format, quality=quality)
        return base64.b64encode(buffer.getvalue()).decode('utf-8')
    except:
        return ""

# ============================================================================
# CACHE OPERATIONS
# ============================================================================
def generate_image_comparison_key(url1: str, url2: str) -> str:
    """Generate cache key for image comparison."""
    # Ensure inputs are strings
    u1 = str(url1) if url1 else ""
    u2 = str(url2) if url2 else ""
    
    sorted_urls = sorted([u1.strip(), u2.strip()])
    key_string = f"{sorted_urls[0]}|{sorted_urls[1]}"
    return hashlib.md5(key_string.encode()).hexdigest()

def check_cache(comparison_key: str) -> Optional[Dict[str, Any]]:
    """Check cache for existing comparison."""
    if not ENABLE_CACHE or not DYNAMO_CACHE_TABLE_NAME:
        return None
    
    try:
        response = dynamodb_client.get_item(
            TableName=DYNAMO_CACHE_TABLE_NAME,
            Key={"comparison_key": {"S": comparison_key}}
        )
        
        if "Item" in response:
            item = response["Item"]
            return {
                "comparison_key": item.get("comparison_key", {}).get("S", ""),
                "overall_similarity": float(item.get("overall_similarity", {}).get("N", 0)),
                "match": item.get("match", {}).get("BOOL", False),
                "genai_similarity": float(item.get("genai_similarity", {}).get("N", 0)),
                "genai_analysis": item.get("genai_analysis", {}).get("S", ""),
                "status": item.get("status", {}).get("S", "completed"),
                "cached": True
            }
        return None
    except:
        return None

def store_in_cache(comparison_key: str, result: Dict[str, Any]) -> bool:
    """Store comparison result in cache."""
    if not ENABLE_CACHE or not DYNAMO_CACHE_TABLE_NAME:
        return False
    
    try:
        item = {
            "comparison_key": {"S": comparison_key},
            "overall_similarity": {"N": str(result.get("overall_similarity", 0.0))},
            "match": {"BOOL": result.get("match", False)},
            "genai_similarity": {"N": str(result.get("scores", {}).get("genai", 0.0))},
            "genai_analysis": {"S": result.get("genai_analysis", "")},
            "status": {"S": result.get("status", "completed")},
            "ttl": {"N": str(result.get("ttl", 0))}
        }
        
        dynamodb_client.put_item(TableName=DYNAMO_CACHE_TABLE_NAME, Item=item)
        return True
    except:
        return False

# ============================================================================
# GENAI IMAGE COMPARISON
# ============================================================================
def invoke_bedrock_for_image_comparison(
    image1_base64: str, 
    image2_base64: str
) -> Tuple[float, str]:
    """
    Use Amazon Bedrock (Claude 3 Haiku) for visual similarity analysis.
    Returns: (similarity_score, analysis_text)
    """
    try:
        prompt = """You are an expert e-commerce product visual matcher. Compare these two product images side-by-side, acting as a strict quality inspector.

STEP 1: Analyze Image 1 to determine:
[Color of A]: Its primary color(s)
[Pattern of A]: Its pattern/print (e.g., solid, striped, floral, check)
[Brand Logo of A]: Note the exact brand logo or text visible (e.g. Nike, URBAN ALFAMi). If none, write "No visible logo".

STEP 2: Analyze Image 2 to determine the exact same specific attributes.
[Color of B]
[Pattern of B]
[Brand Logo of B]

STEP 3: Calculate the Match Score. 
Each visual aspect is worth exactly 1/3 of the total score (approx 0.33 each).
Start at Base Score = 0.0

- COLOR MATCH (0.33): 
    - Full (+0.33) if BOTH have the same color (e.g., [Black] vs [Black]).
    - Full (+0.33) if BOTH have NO visible color (e.g., [None] vs [None]).
    - Zero (+0.0) if one has a color and the other does not (e.g., [Black] vs [None]).
    - Zero (+0.0) if they have different colors (e.g., [Red] vs [Blue]).

- PATTERN MATCH (0.33):
    - Full (+0.33) if BOTH have the same pattern (e.g., [Solid] vs [Solid]).
    - Full (+0.33) if BOTH have NO visible pattern (e.g., [None] vs [None]).
    - Zero (+0.0) if one has a pattern and the other does not (e.g., [Striped] vs [None]).
    - Zero (+0.0) if they have different patterns (e.g., [Striped] vs [Dotted]).

- LOGO MATCH (0.34):
    - Full (+0.34) if BOTH have the same logo (e.g., [Nike] vs [Nike]).
    - Full (+0.34) if BOTH have NO visible logo (e.g., [No visible logo] vs [No visible logo]).
    - Zero (+0.0) if one has a logo and the other does not (e.g., [URBAN ALFAMi] vs [No visible logo]).
    - Zero (+0.0) if they have different logos (e.g., [Nike] vs [Puma]).

Respond in this exact format WITH NO OTHER CONVERSATIONAL TEXT:
SIMILARITY: [Final calculated score out of 1.0, e.g. 0.33, 0.67, 1.00]
ANALYSIS: [Color of A] vs [Color of B], [Pattern of A] vs [Pattern of B], [Brand Logo of A] vs [Brand Logo of B]. [Brief explanation which attributes matched and which failed]"""

        import base64
        
        # Bedrock Converse API format (universal across Nova, Claude 3, etc.)
        messages = [{
            "role": "user",
            "content": [
                {
                    "image": {
                        "format": "jpeg",
                        "source": {
                            "bytes": base64.b64decode(image1_base64)
                        }
                    }
                },
                {
                    "image": {
                        "format": "jpeg",
                        "source": {
                            "bytes": base64.b64decode(image2_base64)
                        }
                    }
                },
                {
                    "text": prompt
                }
            ]
        }]
        
        response = bedrock_runtime.converse(
            modelId=BEDROCK_MODEL_ID,
            messages=messages,
            inferenceConfig={
                "maxTokens": 300,
                "temperature": 0.1,
                "topP": 0.9
            }
        )
        
        output_text = response.get('output', {}).get('message', {}).get('content', [{}])[0].get('text', '')
        
        # --- DEBUG PRINT ---
        print("\n" + "="*50)
        print("🖼️  IMAGE GENAI OUTPUT:")
        print(output_text)
        print("="*50 + "\n")
        
        # Parse response
        similarity_score = 0.0
        analysis = ""
        
        lines = output_text.strip().split('\n')
        for line in lines:
            if line.startswith('SIMILARITY:'):
                score_text = line.replace('SIMILARITY:', '').strip()
                try:
                    similarity_score = float(score_text)
                    similarity_score = max(0.0, min(1.0, similarity_score))
                except:
                    pass
            elif line.startswith('ANALYSIS:'):
                analysis = line.replace('ANALYSIS:', '').strip()
        
        # If parsing failed, try to extract any number
        if similarity_score == 0.0:
            import re
            score_match = re.search(r'(\d+\.?\d*)', output_text)
            if score_match:
                score = float(score_match.group(1))
                if score > 1.0:
                    score = score / 100.0
                similarity_score = max(0.0, min(1.0, score))
        
        if not analysis:
            analysis = output_text[:200]  # Fallback to first 200 chars
        
        return similarity_score, analysis, True
        
    except Exception as e:
        return 0.0, f"Error during GenAI analysis: {str(e)}", False

# ============================================================================
# FALLBACK: BASIC PERCEPTUAL HASH
# ============================================================================
def calculate_phash(image: Image.Image, hash_size: int = 8) -> str:
    """Calculate perceptual hash (fallback method)."""
    try:
        img = image.resize((hash_size, hash_size), Image.Resampling.LANCZOS).convert('L')
        pixels = list(img.getdata())
        avg = sum(pixels) / len(pixels)
        bits = ''.join('1' if p > avg else '0' for p in pixels)
        return bits
    except:
        return ""

def phash_similarity(hash1: str, hash2: str) -> float:
    """Calculate similarity between two perceptual hashes."""
    if not hash1 or not hash2 or len(hash1) != len(hash2):
        return 0.0
    
    hamming_dist = sum(c1 != c2 for c1, c2 in zip(hash1, hash2))
    return 1.0 - (hamming_dist / len(hash1))

# ============================================================================
# MAIN COMPARISON FUNCTION
# ============================================================================
def compare_images(url1: str, url2: str) -> Dict[str, Any]:
    """Main image comparison function using GenAI."""
    
    result = {
        "status": "processing",
        "url1": url1,
        "url2": url2,
        "overall_similarity": 0.0,
        "match": False,
        "scores": {},
        "genai_analysis": "",
        "error": None,
        "cached": False,
        "comparison_key": ""
    }
    
    try:
        # Generate comparison key
        comparison_key = generate_image_comparison_key(url1, url2)
        result["comparison_key"] = comparison_key
        
        # Check cache
        cached_result = check_cache(comparison_key)
        if cached_result:
            # --- DEBUG PRINT (CACHED) ---
            print("\n" + "="*50)
            print("🖼️  IMAGE GENAI OUTPUT (CACHED):")
            print(cached_result.get("genai_analysis", ""))
            print("="*50 + "\n")
            
            result.update(cached_result)
            result["cached"] = True
            return result
        
        # Download images
        img_bytes1 = download_image(url1)
        img_bytes2 = download_image(url2)
        
        if not img_bytes1 or not img_bytes2:
            result["status"] = "error"
            result["error"] = "Failed to download one or both images"
            return result
        
        # Process images
        img1 = load_and_process_image(img_bytes1)
        img2 = load_and_process_image(img_bytes2)
        
        if not img1 or not img2:
            result["status"] = "error"
            result["error"] = "Failed to process one or both images"
            return result
        
        # Method 1: GenAI Comparison (Primary)
        img1_base64 = image_to_base64(img1)
        img2_base64 = image_to_base64(img2)
        
        genai_success = False  # Track whether GenAI actually ran successfully
        
        if img1_base64 and img2_base64:
            genai_score, genai_analysis, genai_success = invoke_bedrock_for_image_comparison(
                img1_base64, 
                img2_base64
            )
            result["scores"]["genai"] = round(genai_score, 4)
            result["genai_analysis"] = genai_analysis
        else:
            result["scores"]["genai"] = 0.0
            result["genai_analysis"] = "Failed to encode images"
            genai_success = False
        
        # Method 2: Perceptual Hash (Fallback)
        phash1 = calculate_phash(img1)
        phash2 = calculate_phash(img2)
        phash_score = phash_similarity(phash1, phash2)
        result["scores"]["phash"] = round(phash_score, 4)
        
        # Calculate overall similarity
        # FIXED: Use genai_success flag instead of score > 0
        # This ensures GenAI's valid 0.0 (different products) is trusted,
        # while still falling back to pHash on actual API errors.
        if genai_success:
            # GenAI ran successfully — TRUST its score (even if 0.0)
            overall = (result["scores"]["genai"] * 0.9) + (phash_score * 0.1)
        else:
            # GenAI genuinely FAILED (API error, timeout, etc.)
            # Fall back to pHash as the only available signal
            overall = phash_score
            result["genai_analysis"] = (result.get("genai_analysis", "") +
                                         " [FALLBACK: Using pHash only]")
        
        result["overall_similarity"] = round(overall, 4)
        result["match"] = overall >= SIMILARITY_THRESHOLD
        result["genai_success"] = genai_success
        
        # Debug logging for transparency
        print(f"🖼️  IMAGE SCORE BREAKDOWN: GenAI={result['scores']['genai']:.4f} "
              f"(success={genai_success}), pHash={phash_score:.4f}, "
              f"Overall={result['overall_similarity']:.4f}")
        
        # Add details
        result["details"] = {
            "image1_size": img1.size,
            "image2_size": img2.size,
            "threshold_used": SIMILARITY_THRESHOLD,
            "primary_method": "genai_vision",
            "fallback_method": "perceptual_hash",
            "model_used": BEDROCK_MODEL_ID
        }
        
        result["status"] = "completed"
        
        # Store in cache (with 30-day TTL)
        import time
        result["ttl"] = int(time.time()) + (30 * 24 * 60 * 60)
        store_in_cache(comparison_key, result)
        
        return result
        
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)
        return result

# ============================================================================
# LAMBDA HANDLER
# ============================================================================
def lambda_handler(event, context):
    """Lambda handler - OPTIMIZED."""
    print(f"Received event: {json.dumps(event)}")
    try:
        # Get image URLs
        #url1 = event.get("image_url_a") or event.get("image_url1") or event.get("image_url_1")
        url1 = event["image_url_a"][0]
        print(f"URL1: {url1}")
        #url2 = event.get("image_url_b") or event.get("image_url2") or event.get("image_url_2")
        url2 = event["image_url_b"][0]
        print(f"URL2: {url2}")
        if not url1 or not url2:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "status": "error",
                    "error": "Both image_url_a and image_url_b are required"
                })
            }
        
        # Validate URLs
        for url in [url1, url2]:
            parsed = urlparse(url)
            if not parsed.scheme or not parsed.netloc:
                return {
                    "statusCode": 400,
                    "body": json.dumps({
                        "status": "error",
                        "error": f"Invalid URL: {url}"
                    })
                }
        
        # Perform comparison
        result = compare_images(url1, url2)
        print(f"Comparison result: {json.dumps(result)}")
        status_code = 200 if result.get("status") == "completed" else 500
        
        return {
            "statusCode": status_code,
            "body": json.dumps(result, indent=2)
        }
        
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "status": "error",
                "error": "Internal server error",
                "message": str(e)
            })
        }