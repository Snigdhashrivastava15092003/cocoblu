# Use Chainguard Python base image for security and smaller size
FROM cgr.dev/chainguard/python:latest-dev AS builder

WORKDIR /tmp

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --target /tmp/packages -r requirements.txt

# ==============================
# Final runtime image
# ==============================
FROM cgr.dev/chainguard/python:latest

WORKDIR /app

# Copy installed packages
COPY --from=builder /tmp/packages /home/nonroot/.local/lib/python3.14/site-packages

# Copy application files
COPY app_v2.py .
COPY az_scraper.py .
COPY flipkart_scraper.py .
COPY similarity.py .
COPY size_mappings.py .
COPY myntra_scraper.py .
COPY image_similarity.py .
COPY session_generator.py .

# Set environment variables
ENV PYTHONPATH="/home/nonroot/.local/lib/python3.14/site-packages" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Expose port
EXPOSE 8005

# Run the application
ENTRYPOINT []
CMD ["python", "-m", "streamlit", "run", "app_v2.py", "--server.address", "0.0.0.0", "--server.port", "8005", "--server.baseUrlPath", "retail-agent", "--server.headless", "true"]