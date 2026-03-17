import os
import time
import random
from playwright.sync_api import sync_playwright

# Identical User Agent to the Scraper for consistency
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"

def run_session_generator():
    print("========================================================")
    print("      FLIPKART SESSION GENERATOR (Human Mimic)          ")
    print("========================================================")
    print("1. This script will open a Chrome window.")
    print("2. Navigate to Flipkart.")
    print("3. LOGIN (Highly Recommended) or just Browse.")
    print("4. Search for products, click around, solve CAPTCHAs.")
    print("5. When you are done, come back here and press ENTER.")
    print("========================================================")
    
    with sync_playwright() as p:
        # Launch Headed Browser
        browser = p.chromium.launch(
            headless=False,
            channel="chrome", # Try to use actual Chrome if available
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--window-size=1920,1080',
            ]
        )
        
        # Create Context with similar settings to scraper
        context = browser.new_context(
            user_agent=USER_AGENT,
            viewport={'width': 1920, 'height': 1080},
            locale='en-IN',
            timezone_id='Asia/Kolkata',
            permissions=['geolocation'],
            geolocation={'latitude': 12.9716, 'longitude': 77.5946}
        )
        
        # Stealth scripts
        page = context.new_page()
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        """)
        
        print("\n🌐 Navigating to Flipkart...")
        page.goto("https://www.flipkart.com", timeout=60000)
        
        input("\n👉 INTERACT with the browser now! Login, browse 3-4 items.\n   Press ENTER here when you are done and the session is 'Primed'...")
        
        # Save State
        print("\n💾 Saving Session to 'flipkart_session.json'...")
        context.storage_state(path="flipkart_session.json")
        print("✅ Session Saved Successfully!")
        print("   Now run the main scraper - it will use these cookies.")
        
        browser.close()

if __name__ == "__main__":
    run_session_generator()
