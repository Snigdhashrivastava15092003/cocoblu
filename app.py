#!/usr/bin/env python3
"""
Local Unified Product Comparison Application
Integrates all Lambda functions into a single local execution flow
"""

import json
import sys
import os
import time
from typing import Dict, Any, Optional
from datetime import datetime
import logging

# Rich Imports
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.logging import RichHandler
from rich import print as rprint
from rich.columns import Columns
from rich.console import Group

# Initialize Console
console = Console()

# Configure Logging with Rich
logging.basicConfig(
    level="INFO",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(console=console, rich_tracebacks=True, show_path=False)]
)
logger = logging.getLogger("rich")

# Load environment variables from setup_aws_env.sh
def load_env_from_file():
    """Load environment variables from setup_aws_env.sh"""
    env_file = os.path.join(os.path.dirname(__file__), 'setup_aws_env.sh')
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('export '):
                    # Parse: export VAR="value"
                    line = line[7:]  # Remove 'export '
                    if '=' in line:
                        key, value = line.split('=', 1)
                        # Remove quotes and comments
                        value = value.strip('"').strip("'")
                        # Remove inline comments (anything after #)
                        if '#' in value:
                            value = value.split('#')[0].strip()
                        # Remove any trailing quotes after comment removal
                        value = value.strip('"').strip("'")
                        os.environ[key] = value
                        # console.log(f"[dim]Loaded: {key}[/dim]") # Too verbose

# Load environment variables at startup
load_env_from_file()

# Import all Lambda modules
import az_scraper
import flipkart_scraper
import myntra_scraper
import similarity
import image_similarity

# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Configuration for local execution"""
    # Set to True to enable detailed logging
    VERBOSE = True
    
    # Thresholds (matching your Lambda environment)
    PRICE_TOLERANCE = 0.01
    THRESHOLD_TITLE = 0.55
    THRESHOLD_CONTENT = 0.60
    THRESHOLD_FINAL = 0.80
    
    # Feature flags
    ENABLE_IMAGE_COMPARISON = True
    ENABLE_GENAI_MATCHING = True

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def create_product_panel(product: Dict[str, Any], title: str, style: str = "blue") -> Panel:
    """Create a panel for product details"""
    grid = Table.grid(expand=True)
    grid.add_column(style="bold dim")
    grid.add_column()
    
    grid.add_row("ASIN:", str(product.get('asin', 'N/A')))
    grid.add_row("Title:", str(product.get('title', 'N/A'))[:60] + "...")
    grid.add_row("Price:", f"{product.get('currency', '')} {product.get('price', 'N/A')}")
    grid.add_row("Size:", str(product.get('size', 'N/A')))
    grid.add_row("Stock:", str(product.get('instock', 'N/A')))
    
    # Optional image display (link)
    images = product.get('images', [])
    if images:
        grid.add_row("Image:", f"[link={images[0]}]Click to View[/link]")

    return Panel(grid, title=title, border_style=style)

# ============================================================================
# SCRAPER WRAPPERS
# ============================================================================

def scrape_amazon(asin: str = None, url: str = None, domain: str = "in") -> Dict[str, Any]:
    """Scrape Amazon product using Local Module (Robust)"""
    
    # Import locally to avoid circular dependency issues if any
    from az_scraper import get_amazon_product_details, construct_amazon_url
    
    # 1. Resolve URL
    if not url and asin:
        url = construct_amazon_url(asin, domain)
    elif not url and not asin:
        raise ValueError("Either ASIN or URL is required")
        
    logger.info(f"Scraping Amazon URL: {url}")
    
    # 2. Call Local Scraper
    try:
        # get_amazon_product_details already has retry logic (3 attempts by default)
        product = get_amazon_product_details(url)
        
        if not product:
            raise Exception("Scraper returned None (blocked or invalid URL)")
            
        if not product.get("title"):
             raise Exception("Scraper returned empty product data (likely CAPTCHA/soft-block)")
             
        # Add basic validation
        if not product.get("price") and not product.get("mrp") and not product.get("instock"):
             logger.warning("⚠ Weak scrape data: Missing price/stock info")
             
        return product
        
    except Exception as e:
        raise Exception(f"Local Amazon Scraper failed: {str(e)}")

def scrape_flipkart(url: str, target_size: str = None) -> Dict[str, Any]:
    """Scrape Flipkart product using flipkart_scraper module"""
    
    # Call the core scraping function directly
    try:
        product = flipkart_scraper.scrape_single_url(url, target_size=target_size)
    except Exception as e:
        raise Exception(f"Flipkart scraping exception: {str(e)}")
    
    return product

def scrape_myntra(url: str, target_size: str = None) -> Dict[str, Any]:
    """Scrape Myntra product using myntra_scraper module"""
    
    try:
        product = myntra_scraper.scrape_single_url(url, target_size=target_size)
    except Exception as e:
        raise Exception(f"Myntra scraping exception: {str(e)}")
    
    return product

def compare_images(image_url_a: str, image_url_b: str) -> Dict[str, Any]:
    """Compare images using image_similarity module"""
    
    result = image_similarity.compare_images(image_url_a, image_url_b)
    
    if result.get("status") != "completed":
        console.print(f"[yellow]⚠ Image comparison failed: {result.get('error', 'Unknown error')}[/yellow]")
        return None
    
    return result

def compare_products(amazon_product: Dict[str, Any], flipkart_product: Dict[str, Any], 
                    nudge_price: float, amazon_url: str = None, 
                    flipkart_url: str = None) -> Dict[str, Any]:
    """Compare products using similarity module"""
    
    # Inject nudge_price
    amazon_product["nudge_price"] = nudge_price
    flipkart_product["nudge_price"] = nudge_price
    
    # Inject URLs for DynamoDB storage
    if amazon_url:
        amazon_product["url"] = amazon_url
    if flipkart_url:
        flipkart_product["url"] = flipkart_url
    
    # Prepare image URLs for comparison
    amazon_images = amazon_product.get("images", [])
    flipkart_images = flipkart_product.get("images", [])
    
    if amazon_images and flipkart_images:
        amazon_product["image_url"] = amazon_images[0]
        flipkart_product["image_url"] = flipkart_images[0]
    
    # Call the core comparison function directly
    comparison_result = similarity.compare_products(amazon_product, flipkart_product)
    
    # Store in DynamoDB
    comparison_id = similarity.store_result_in_dynamodb(comparison_result)
    
    # Format the output
    formatted_result = similarity.format_comparison_output(comparison_result)
    formatted_result["metadata"]["comparison_id"] = comparison_id
    
    return formatted_result

# ============================================================================
# MAIN ORCHESTRATION
# ============================================================================

def run_comparison(amazon_asin: str = None, amazon_url: str = None, 
                  flipkart_url: str = None, nudge_price: float = None,
                  amazon_domain: str = "in") -> Dict[str, Any]:
    """Run complete product comparison workflow"""
    
    console.clear()
    console.print(Panel("[bold white]Unified Product Comparison Agent[/bold white]", style="purple"))
    
    start_time = datetime.now()
    
    try:
        # Validate inputs
        if not (amazon_asin or amazon_url):
            raise ValueError("Either amazon_asin or amazon_url must be provided")
        if not flipkart_url:
            raise ValueError("flipkart_url must be provided")
        if not nudge_price:
            raise ValueError("nudge_price must be provided")
            
        console.print(f"[dim]Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}[/dim]")
        console.print(f"[bold cyan]Nudge Price:[/bold cyan] ₹{nudge_price}\n")
        
        # --- Step 1: Scrape Amazon ---
        with console.status("[bold blue]Scraping Amazon Product...[/bold blue]", spinner="dots"):
             amazon_product = scrape_amazon(asin=amazon_asin, url=amazon_url, domain=amazon_domain)
        
        console.print("[green]✓ Amazon Scrape Complete[/green]")
        
        # Display Amazon Details temporarily
        # console.print(create_product_panel(amazon_product, "Amazon Product", "blue"))

        # Extract size from Amazon for Flipkart
        amazon_size = amazon_product.get("size")
        
        # --- Step 2: Scrape Flipkart ---
        with console.status(f"[bold orange3]Scraping Flipkart (Target Size: {amazon_size or 'Any'})...[/bold orange3]", spinner="dots"):
            flipkart_product = scrape_flipkart(flipkart_url, target_size=amazon_size)
            
        if flipkart_product.get("error"):
            error_msg = flipkart_product.get("error")
            error_details = flipkart_product.get("error_details", "")
            console.print(f"[bold red]❌ Flipkart Scraping Failed:[/bold red] {error_msg}")
            if error_details:
                console.print(f"[red]Details: {error_details}[/red]")
            
            # Log failure to DynamoDB
            try:
                # Construct failed result object
                amazon_product_url = amazon_url if amazon_url else f"https://www.amazon.{amazon_domain}/dp/{amazon_asin}"
                
                failed_result = {
                    "workflow_status": "SCRAPER_FAILED",
                    "termination_reason": error_msg,
                    "approved_match": False,
                    "overall_similarity_percentage": 0.0,
                    "step_completed": 0,
                    
                    # Store Amazon data we successfully fetched
                    "product_az": amazon_product,
                    
                    # Store minimal Flipkart info with the error
                    "product_ic": {
                        "url": flipkart_url,
                        "error": error_msg,
                        "error_details": error_details,
                        "scraped_at": datetime.now().isoformat()
                    },
                    "flipkart_url": flipkart_url,
                    "amazon_url": amazon_product_url,
                    
                    # Mark as critical failure
                    "critical_failures": [error_msg],
                    "failed_steps": ["flipkart_scraping"],
                    "dict_parameter_scores": {}
                }
                
                # Store in DynamoDB
                failure_id = similarity.store_result_in_dynamodb(failed_result)
                if failure_id:
                    console.print(f"[dim]💾 Saved FAILURE record to DynamoDB: {failure_id}[/dim]")
            except Exception as e:
                console.print(f"[yellow]⚠ Failed to log error to DynamoDB: {e}[/yellow]")
            
            # Stop execution
            raise Exception(f"Flipkart scraping failed: {error_msg}")

        console.print("[green]✓ Flipkart Scrape Complete[/green]")

        # --- Display Side-by-Side ---
        amazon_panel = create_product_panel(amazon_product, "Amazon Data", "blue")
        flipkart_panel = create_product_panel(flipkart_product, "Flipkart Data", "orange3")
        console.print(Columns([amazon_panel, flipkart_panel]))

        # --- Step 3: Run similarity comparison ---
        # Build Amazon URL if we only have ASIN
        amazon_product_url = amazon_url if amazon_url else f"https://www.amazon.in/dp/{amazon_asin}"
        
        with console.status("[bold magenta]🤖 Running AI Comparison...[/bold magenta]", spinner="aesthetic"):
            comparison_result = compare_products(
                amazon_product, flipkart_product, nudge_price,
                amazon_url=amazon_product_url, flipkart_url=flipkart_url
            )
        
        # Calculate execution time
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Add metadata
        comparison_result["metadata"]["execution_time_seconds"] = round(execution_time, 2)
        comparison_result["metadata"]["execution_mode"] = "local"
        comparison_result["metadata"]["timestamp"] = end_time.isoformat()
        
        # --- Comparison Summary ---
        console.print("\n[bold]COMPARISON RESULTS[/bold]")
        
        # Status with visual indicator
        status = comparison_result.get("comparison_status", "UNKNOWN")
        if status == "PASSED":
            status_style = "bold green"
            status_icon = "✅"
        elif status == "FAILED":
            status_style = "bold red"
            status_icon = "❌"
        else:
            status_style = "bold yellow"
            status_icon = "⚠️"
            
        console.print(f"Status: [{status_style}]{status} {status_icon}[/{status_style}]")
        console.print(f"Overall Confidence: [bold]{comparison_result.get('overall_confidence_score', 0):.2f}%[/bold]")
        console.print(f"Steps Completed: {comparison_result.get('step_completed', 0)}/7")
        console.print(f"Execution Time: [dim]{execution_time:.2f}s[/dim]")
        
        if comparison_result.get("termination_reason"):
             console.print(Panel(f"[bold red]Termination Reason:[/bold red] {comparison_result.get('termination_reason')}", title="Terminated", style="red"))

        # --- Parameter Scores Table ---
        table = Table(title="Parameter Scores", header_style="bold magenta", expand=True)
        table.add_column("Parameter", style="cyan")
        table.add_column("Status", justify="center")
        table.add_column("Score", justify="right")
        table.add_column("Visual", justify="center")

        for param, details in comparison_result.get("parameters_checked", {}).items():
            status = details.get("status", "UNKNOWN")
            confidence = details.get("confidence_score", 0)
            
            if status == "PASSED":
                status_str = "[green]PASSED[/green]"
                bar = "[green]" + "█" * int(confidence/10) + "[/green]" + "[dim]" + "░" * (10 - int(confidence/10)) + "[/dim]"
            elif status == "FAILED":
                status_str = "[red]FAILED[/red]"
                bar = "[red]" + "█" * int(confidence/10) + "[/red]" + "[dim]" + "░" * (10 - int(confidence/10)) + "[/dim]"
            elif status == "SKIPPED":
                status_str = "[yellow]SKIPPED[/yellow]"
                bar = "[dim]----------[/dim]"
            else:
                status_str = status
                bar = ""
            
            param_display = param.replace("_", " ").title()
            score_display = f"{confidence:.1f}%" if confidence is not None else "N/A"
            
            table.add_row(param_display, status_str, score_display, bar)
            
        console.print(table)
        
        # --- Failures Breakdown ---
        critical_failures = comparison_result.get("critical_failures", [])
        informational_failures = comparison_result.get("informational_failures", [])
        
        if critical_failures or informational_failures:
            fail_text = ""
            if critical_failures:
                fail_text += "[bold red]Critical Failures (Hard Blockers):[/bold red]\n"
                for failure in critical_failures:
                    fail_text += f" • {failure}\n"
            
            if informational_failures:
                fail_text += "\n[bold yellow]Informational (Soft) Failures:[/bold yellow]\n"
                for failure in informational_failures:
                    fail_text += f" • {failure}\n"
            
            console.print(Panel(fail_text, title="Failure Details", style="red"))

        return comparison_result
        
    except Exception as e:
        console.print(f"\n[bold red]✗ Fatal Error:[/bold red] {str(e)}\n")
        logger.exception("Fatal error occurred")
        raise

# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    """Main CLI interface"""
    
    # Example usage - modify these values for your test
    EXAMPLE_CONFIG = {
        "amazon_asin": "B0DZNRHP1M",  # Replace with your ASIN
        "amazon_domain": "in",
        "flipkart_url": "https://www.flipkart.com/van-heusen-solid-women-round-neck-pink-t-shirt/p/itme4cac3a0fa242?pid=TSHH5Q7YPBG46NKE&lid=LSTTSHH5Q7YPBG46NKE0X88QR&marketplace=FLIPKART&q=van%2520huesen%2520women%2520tshirt&sattr%5B%5D=color&sattr%5B%5D=size&st=size",  # Full URL with params
        "nudge_price": 499  # Actual price from your test
    }
    
    # Check if custom arguments provided
    if len(sys.argv) > 1:
        # console.print("[dim]Using command-line arguments[/dim]")
        if len(sys.argv) < 4:
            console.print("\n[bold red]Usage:[/bold red] python app.py <nudge_price> <az_asin> <ic_url> [amazon_domain]")
            sys.exit(1)
        
        EXAMPLE_CONFIG["nudge_price"] = float(sys.argv[1])
        EXAMPLE_CONFIG["amazon_asin"] = sys.argv[2]
        EXAMPLE_CONFIG["flipkart_url"] = sys.argv[3]
        if len(sys.argv) > 4:
            EXAMPLE_CONFIG["amazon_domain"] = sys.argv[4]
    
    try:
        # Run the comparison
        result = run_comparison(
            amazon_asin=EXAMPLE_CONFIG["amazon_asin"],
            flipkart_url=EXAMPLE_CONFIG["flipkart_url"],
            nudge_price=EXAMPLE_CONFIG["nudge_price"],
            amazon_domain=EXAMPLE_CONFIG["amazon_domain"]
        )
        
        # Save result to file in outputs folder
        output_dir = os.path.join(os.path.dirname(__file__), 'outputs')
        os.makedirs(output_dir, exist_ok=True)
        
        output_file = f"comparison_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Note: We rely on the internal functions to save, but let's confirm
        # Actually app.py was saving it.
        output_path = os.path.join(output_dir, output_file)
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False, default=str)
        
        console.print(f"\n[dim]Full result saved to: outputs/{output_file}[/dim]")
        
        # Exit with appropriate code
        if result.get("comparison_status") == "PASSED":
            sys.exit(0)
        else:
            sys.exit(1)
        
    except KeyboardInterrupt:
        console.print("\n[yellow]Operation cancelled by user[/yellow]")
        sys.exit(130)
    except Exception as e:
        sys.exit(2)

if __name__ == "__main__":
    main()
