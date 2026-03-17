# COCOBLUE Retail Agent - Version History & Change Log

This document tracks the evolution of the **Cocoblu Retail Agent** system, from its initial release to the current AI-powered version.

---

## [2.0.0] - 2026-02-24 (Current)
### "The GenAI & Structural Audit Update"
**Major focus:** Decision precision, auditing transparency, and high-performance AI integration.

*   **Decision Engine:**
    *   Introduced the **Priority Failure Chain** (Title -> Visual -> Size -> Identity -> Specs -> Ops).
    *   Implemented **Categorical Failures**: Grouping errors into CMT Mismatch, Incorrect List Price, Incorrect Catalog Data, and Operational buckets.
    *   Transitioned to **Workflow Decisions**: APPROVE, REJECT, and MANUAL REVIEW statuses.
*   **GenAI Upgrades:**
    *   Migrated to **AWS Bedrock (Claude 3 Haiku)** for faster reasoning and lower latency.
    *   Implemented **Granular Attribute Comparison**: Explicit extraction and verification of Brand, Color, Gender, Quantity, Weight, and Dimensions.
    *   **Size Ambiguity Feature:** New logic to identify and flag unclear size mappings for manual audit.
*   **Storage & Audit:**
    *   Expanded **DynamoDB Schema**: Storing `genai_reason`, `genai_attributes` (JSON), `model_id`, and `image_comparison_genai_analysis`.
    *   Added metadata tracking for `az_mrp`, `fk_mrp`, and `available_sizes` for full parity auditing.
*   **Scrapers:**
    *   **Flipkart:** Added `session_generator.py` and `flipkart_session.json` persistence to bypass bot detection.
    *   **Amazon:** Revamped MRP extraction logic.

---

## [1.8.0] - 2026-02-19
### "Anti-Bot & Debugging Expansion"
**Major focus:** Stability in harsh bot-detection environments and developer tools.

*   **Scraping Defense:**
    *   Implemented **12-Layer Anti-Bot System**: Randomized User-Agents, Viewports, Canvas/WebGL noise, and humanized mouse simulations.
    *   Removed non-Windows User-Agents to ensure consistent UI rendering.
*   **Image Analysis:**
    *   Raised **Visual Similarity Threshold to 0.69** based on client feedback.
    *   Updated prompt logic for better logo and brand detection.
*   **Debugging:**
    *   Introduced `extraction_inspector.py` and `session_generator.py` for troubleshooting scraping blocks.

---

## [1.5.0] - 2026-02-06
### "Batch Processing & UI Adaptability"
**Major focus:** Scaling operations and handling platform UI changes.

*   **Platform Adaptability:**
    *   **Flipkart:** Added UI detection for "Classical" vs "React Native Web" layouts.
    *   **Amazon:** Implemented strict description prioritization and Playwright-fallback for CAPTCHA pages.
*   **Scalability:**
    *   Introduced `batch_process.py`: Support for processing thousands of products via CSV/JSON.
    *   Added `--limit` and detailed failure reporting to batch runs.
*   **Media Improvements:**
    *   Implemented **Resolution Booster**: Intercepting 128x128 thumbnails and rewriting to 832x832 high-res URLs.

---

## [1.2.0] - 2025-01-30
### "Console Modernization & Local Modularity"
**Major focus:** Developer experience and code modularity.

*   **CLI Overhaul:** Integrated **Rich** library for beautiful, color-coded terminal output during scans.
*   **Architecture:** Migrated Amazon scraping from remote APIs to local Playwright/Request modules for better control.
*   **Tools:** Added `debug_scrapers.py` and dedicated ASIN extraction utilities.

---

## [1.1.0] - 2025-01-15
### "Canonical Size Logic"
**Major focus:** Fixing the most common mismatch—Product Sizes.

*   **Size Mapping Layer:** Created `size_mappings.py` to handle "Letter-to-Numeric" conversions (e.g., L = 95 CM).
*   **Availability Tracking:** Added logic to detect "Disabled" size buttons on Flipkart using CSS class audits.
*   **Stock Authority:** Implemented "Button Priority Logic" (Add to Cart > Buy Now > Notify Me).

---

## [1.0.0] - 2025-11-13
### "Initial Release"
**Major focus:** Minimum Viable Product for cross-platform comparison.

*   **Core Systems:** Basic Amazon and Flipkart scrapers using BeautifulSoup.
*   **Similarity V1:** AWS Bedrock (Claude 2.1) and Ollama local fallback for text comparison.
*   **Image Comparison:** Basic pHash (Perceptual Hashing) similarity.
*   **Integration:** Initial DynamoDB table setup for AWS Lambda deployment.
