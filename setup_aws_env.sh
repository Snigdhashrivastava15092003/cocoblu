#!/bin/bash

# ============================================================================
# AWS + Microsoft Auth Environment Setup
# ============================================================================

echo "Setting up environment variables..."

# ============================================================================
# AWS / DynamoDB
# ============================================================================
export DYNAMO_RESULTS_TABLE_NAME="Cocoblu-Scraped-Products"

# Optional tables
# export DYNAMO_IMAGE_CACHE_TABLE_NAME="image-cache"
# export STATUS_TABLE_NAME="comparison-status"

# ============================================================================
# Bedrock Configuration
# ============================================================================
export BEDROCK_MODEL_ID="apac.amazon.nova-lite-v1:0"

# ============================================================================
# Feature Flags
# ============================================================================
export IMAGE_COMPARISON_LAMBDA_NAME="image-comparison-local"
export ENABLE_IMAGE_COMPARISON="true"
export ENABLE_GENAI_MATCHING="true"
export ENABLE_ASIN_TRACKING="true"
export ENABLE_CACHE="false"

# ============================================================================
# Thresholds
# ============================================================================
export THRESHOLD_TITLE="0.55"
export THRESHOLD_CONTENT="0.60"
export THRESHOLD_FINAL="0.80"
export PRICE_TOLERANCE="0.01"

# ============================================================================
# Microsoft / Azure AD Authentication
# ============================================================================
export MICROSOFT_CLIENT_ID="2a3e6ce2-515b-412f-83c5-a551371bf181"
export MICROSOFT_TENANT_ID="7ca82b07-78e5-488e-9e41-fcdaafc9884d"
export MICROSOFT_CLIENT_SECRET="YOUR_MICROSOFT_CLIENT_SECRET"
export MICROSOFT_REDIRECT_URI="http://localhost:8005/retail-agent"

# ============================================================================
# Streamlit App Settings
# ============================================================================
export STREAMLIT_SERVER_PORT="8005"
export STREAMLIT_BASE_URL_PATH="retail-agent"

echo "✓ Environment variables set!"
echo ""
echo "DynamoDB Table: $DYNAMO_RESULTS_TABLE_NAME"
echo "Bedrock Model: $BEDROCK_MODEL_ID"
echo "Microsoft Client ID: $MICROSOFT_CLIENT_ID"
echo "Microsoft Tenant ID: $MICROSOFT_TENANT_ID"
echo "Redirect URI: $MICROSOFT_REDIRECT_URI"
echo "Streamlit Port: $STREAMLIT_SERVER_PORT"
echo "Base Path: $STREAMLIT_BASE_URL_PATH"
echo ""
echo "To use these settings locally:"
echo "  source setup_aws_env.sh"
echo "  streamlit run app_v2.py --server.port 8005 --server.baseUrlPath Retail"