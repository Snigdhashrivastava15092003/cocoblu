# =============================================================================
# Multi-stage Dockerfile optimized for size, stability, and AWS Deployment
# =============================================================================

# =============================================================================
# Builder stage
# =============================================================================
FROM python:3.11-slim AS builder

WORKDIR /app

# Install build dependencies for compiling Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Create a virtual environment to make copying to runtime isolated and clean
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install ONLY Chromium using Playwright to save space (skips webkit & firefox)
# Store it in a predictable path for copying to the runtime stage
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
RUN playwright install chromium

# =============================================================================
# Runtime stage
# =============================================================================
FROM python:3.11-slim

# Set environment variables optimized for local & AWS EC2 
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    STREAMLIT_GLOBAL_DEVELOPMENT_MODE=false \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
    STREAMLIT_SERVER_ENABLE_CORS=false \
    STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION=true

WORKDIR /app

# Copy the virtual environment from builder (all pip packages)
COPY --from=builder /opt/venv /opt/venv

# Copy the Playwright downloaded Chromium binaries from builder
COPY --from=builder /ms-playwright /ms-playwright

# Install Playwright OS dependencies and curl for HEALTHCHECK
# - Uses playwright install-deps chromium to get ONLY the required apt packages
# - Cleans the apt cache in the same RUN step to reduce the layer size
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    playwright install-deps chromium && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy all required application files in a single layer to reduce image size
COPY app.py app_v2.py az_scraper.py batch_process.py flipkart_scraper.py \
     invocation.py session_generator.py similarity.py size_mappings.py \
     requirements.txt setup_aws_env.sh ./

# Create outputs directory, add nonroot user for security, set correct permissions
RUN chmod +x setup_aws_env.sh \
    && mkdir -p /app/outputs \
    && useradd -m nonroot \
    && chown -R nonroot:nonroot /app /ms-playwright

USER nonroot

# Health check for AWS load balancer
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8005/retail-agent/_stcore/health || exit 1

# Expose port
EXPOSE 8005

ENTRYPOINT []

CMD ["python", "-m", "streamlit", "run", "app_v2.py", \
     "--global.developmentMode=false", \
     "--server.fileWatcherType=none", \
     "--server.address=0.0.0.0", \
     "--server.port=8005", \
     "--server.baseUrlPath=retail-agent", \
     "--server.headless=true", \
     "--server.enableCORS=false", \
     "--server.enableXsrfProtection=true"]
