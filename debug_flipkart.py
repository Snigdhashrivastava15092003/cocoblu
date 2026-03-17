import sys
import json
import logging
import argparse
from flipkart_scraper import scrape_single_url

# Configure logging to show info in terminal
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def main():
    """
    Main function to run the Flipkart scraper debugger.
    Usage: python debug_flipkart.py <URL> [--size SIZE]
    """
    parser = argparse.ArgumentParser(description="Debug Flipkart Scraper")
    parser.add_argument("url", help="Flipkart product URL")
    parser.add_argument("--size", help="Target size to select (optional)", default=None)
    
    args = parser.parse_args()
    
    url = args.url
    target_size = args.size

    logger.info(f"Starting Flipkart Scraper Debugger...")
    logger.info(f"URL: {url}")
    if target_size:
        logger.info(f"Target Size: {target_size}")
    else:
        logger.info("No target size specified, will scrape default variant.")

    try:
        # Run the scraper
        logger.info("Initiating scrape... (This uses Playwright and may take a few seconds)")
        result = scrape_single_url(url, target_size=target_size)

        if result:
            # Check for error key in result
            if "error" in result:
                 logger.error(f"❌ Scraping returned an error: {result['error']}")
            
            logger.info("✅ Scraping Completed!")
            print("\n" + "="*80)
            print("                 FULL SCRAPED DATA OUTPUT                 ")
            print("="*80)
            # Use json.dumps with indent for pretty printing
            print(json.dumps(result, indent=4, ensure_ascii=False))
            print("="*80 + "\n")
            
            # Print brief summary
            print(f"Title: {result.get('title', 'N/A')}")
            print(f"Price: {result.get('price', 'N/A')}")
            print(f"Stock: {result.get('instock', 'N/A')}")
            print(f"Available Sizes: {result.get('available_sizes', [])}")
            
        else:
            logger.error("❌ Scraping Failed. No data returned.")

    except Exception as e:
        logger.exception(f"An error occurred during execution: {e}")

if __name__ == "__main__":
    main()
