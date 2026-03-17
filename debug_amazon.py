import sys
import json
import logging
from az_scraper import get_amazon_product_details, construct_amazon_url, extract_asin_from_url

# Configure logging to show info in terminal
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def main():
    """
    Main function to run the Amazon scraper debugger.
    Usage: python debug_amazon.py <ASIN_OR_URL> [OPTIONAL_URL]
    """
    if len(sys.argv) < 2:
        print("Usage: python debug_amazon.py <ASIN_OR_URL>")
        sys.exit(1)

    arg1 = sys.argv[1]
    
    # Check if first argument is a URL
    if "amazon" in arg1.lower() and (arg1.startswith("http") or "www" in arg1):
        url = arg1
        asin = extract_asin_from_url(url) or "Unknown_ASIN"
        logger.info(f"Detected URL input. Extracted ASIN: {asin}")
    else:
        # Assume it's an ASIN
        asin = arg1
        url = sys.argv[2] if len(sys.argv) > 2 else None
        
        # Construct URL if not provided
        if not url:
            logger.info(f"No URL provided. Constructing from ASIN: {asin}")
            url = construct_amazon_url(asin, domain="in")
    
    logger.info(f"Starting Amazon Scraper Debugger...")
    logger.info(f"ASIN: {asin}")
    logger.info(f"URL:  {url}")

    try:
        # Run the scraper
        logger.info("Initiating scrape...")
        product_data = get_amazon_product_details(url)

        if product_data:
            logger.info("✅ Scraping Successful!")
            print("\n" + "="*80)
            print("                 FULL SCRAPED DATA OUTPUT                 ")
            print("="*80)
            # Use json.dumps with indent for pretty printing
            print(json.dumps(product_data, indent=4, ensure_ascii=False))
            print("="*80 + "\n")
        else:
            logger.error("❌ Scraping Failed. No data returned.")

    except Exception as e:
        logger.exception(f"An error occurred during execution: {e}")

if __name__ == "__main__":
    main()
