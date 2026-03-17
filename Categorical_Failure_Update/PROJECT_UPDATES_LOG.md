# COCOBLUE Retail Agent - Project Change Log
**Report Date:** February 24, 2026
**Version:** 2.0.0 (GenAI & Structural Update)

## Executive Summary
This update introduces a sophisticated **Priority Failure Hierarchy**, a transition to high-performance **AWS Bedrock (Claude 3 Haiku) GenAI**, and a complete overhaul of the **DynamoDB storage schema** to support advanced retail auditing and transparent AI reasoning.

---

## 1. Priority Failure Hierarchy (The "Decision Chain")
The system now implements a strict **Priority Failure Chain** to determine the *Primary Reason* for a mismatch. This ensures that the most critical business discrepancy is highlighted first.

**Order of Precedence (High to Low):**
1.  **Title Similarity:** (GenAI-based)
2.  **Visual & Color Match:** Checks both image score and GenAI color confirmation.
3.  **Size Ambiguity:** A new feature to flag cases where size mappings are unclear or missing.
4.  **Identity Mismatches:** Brand, Quantity, and Gender.
5.  **Physical Mismatches:** Weight and Dimensions.
6.  **Content/Specs:** Technical specification comparison.
7.  **Operational Failures:** Stock status and Price Protocol.

---

## 2. Categorical Failures & Workflow Decisioning
We have moved beyond simple similarity scores to a structured **Categorical Failure System**. All errors are now automatically grouped into four business buckets:
*   🔴 **CMT MISMATCH:** Brand, Quantity, Gender, Low Visual Similarity, Title Similarity, Size Ambiguity, Dimensions, and Weight.
*   🏷️ **INCORRECT LIST PRICE:** Deviations in MRP (Incorrect_List_Price tags).
*   📋 **INCORRECT CATALOG DATA:** Color mismatches and low content/description similarity.
*   ⚙️ **OPERATIONAL:** Stock availability (OOS), Nudge Price (Target) failures, and Size purchasability.

**Decision Engine:**
*   **REJECT:** Hard blockers (Brand, Quantity, Gender, OOS, Price, Low Visual < 0.69).
*   **MANUAL REVIEW:** Triggered by **Size Ambiguity**, Color variations, or Similarity < 0.80.
*   **APPROVE:** Full match with no critical or manual-review flags.

---

## 3. High-Performance GenAI Integration (`similarity.py`)
*   **New Model Migration:** Transitioned to **AWS Bedrock (Claude 3 Haiku)** for faster inference and superior reasoning.
*   **Granular Attribute Analysis:** Added support for multi-point comparison of **Brand**, **Quantity**, **Color**, **Gender**, **Dimensions**, and **Weight**.
*   **Multicolor Smart Override:** Automatically handles complex color names if "Print" or "Multicolor" is detected.

---

## 4. UI Enhancements (`app.py`)
*   **Categorized Failure Display:** The interface now visually buckets errors into the four business categories (CMT, Price, Catalog, Ops) for easier auditing.
*   **GenAI Reasoning Display:** Added a "GENAI ANALYSIS" expander showing the specific logic used for the decision.
*   **Side-by-Side Comparison:** Triggered for any "Manual Review" record, providing a rich table of Amazon vs Flipkart attributes (Color, Gender, etc.).
*   **Decision Transparency:** Clearly displays the **Primary Failure Reason** and the specific **Decision Icon** (✅/❌/⚠️).

---

## 5. Image & Visual Recon (`image_similarity.py`)
*   **Prompt Update:** Optimized prompts for better identification of branding and model variations.
*   **No-Logo Match Logic:** Treats absence of logos on both images as a match (+0.34).
*   **Threshold:** Visual similarity rejection threshold raised to **0.69**.

---

## 6. Scraper & Storage Updates
### Flipkart & Amazon Scrapers
*   **Session State:** Added `flipkart_session.json` for cookie persistence.
*   **MRP Extraction:** Revamped Amazon MRP logic for accurate benchmarking.

### DynamoDB Storage
*   **Audit Fields:** Added `primary_failure_reason`, `critical_failures`, and `size_selection_status`.
*   **AI Metadata:** Now storing `genai_reason`, `genai_attributes` (JSON), and `image_comparison_genai_analysis` for full audit transparency.
