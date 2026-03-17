#!/usr/bin/env python3
"""
Batch Product Comparison Processor
Process multiple products from a CSV or JSON file
"""

import json
import csv
import sys
import os
from datetime import datetime
from typing import List, Dict, Any

# Load environment variables
sys.path.insert(0, os.path.dirname(__file__))
import app

def process_from_csv(csv_file: str, limit: int = None) -> Dict[str, Any]:
    """
    Process products from CSV file
    
    CSV Format:
    nudge_price,amazon_asin,flipkart_url,amazon_domain
    499,B0DC46VXTR,https://flipkart.com/...,in
    599,B0XYZ123,https://flipkart.com/...,in
    """
    print(f"📂 Reading from: {csv_file}")
    
    products = []
    with open(csv_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        
        # Normalize headers (strip whitespace)
        if reader.fieldnames:
            reader.fieldnames = [name.strip() for name in reader.fieldnames]

        for row in reader:
            # Handle aliases for flipkart_url and flexible columns
            fk_url = row.get('flipkart_url') or row.get('fk_link') or row.get('link')
            asin = row.get('amazon_asin')

            if not asin or not fk_url:
                continue

            products.append({
                'nudge_price': float(row.get('nudge_price', 0) or 0),
                'amazon_asin': asin.strip(),
                'flipkart_url': fk_url.strip(),
                'amazon_domain': row.get('amazon_domain', 'in')
            })
    
    return process_batch(products, limit)

def process_from_json(json_file: str, limit: int = None) -> Dict[str, Any]:
    """
    Process products from JSON file
    
    JSON Format:
    [
        {
            "nudge_price": 499,
            "amazon_asin": "B0DC46VXTR",
            "flipkart_url": "https://flipkart.com/...",
            "amazon_domain": "in"
        }
    ]
    """
    print(f"📂 Reading from: {json_file}")
    
    with open(json_file, 'r', encoding='utf-8') as f:
        products = json.load(f)
    
    return process_batch(products, limit)

def process_batch(products: List[Dict[str, Any]], limit: int = None) -> Dict[str, Any]:
    "Process a batch of products"
    
    if limit:
        print(f"⚠️  Limiting execution to first {limit} products")
        products = products[:limit]
    
    print(f"\n{'='*80}")
    print(f"  BATCH PROCESSING - {len(products)} products")
    print(f"{'='*80}\n")
    
    results = []
    passed = 0
    failed = 0
    errors = 0
    
    start_time = datetime.now()
    
    for idx, product in enumerate(products, 1):
        print(f"\n{'='*80}")
        print(f"  [{idx}/{len(products)}] Processing: {product['amazon_asin']}")
        print(f"{'='*80}")
        
        try:
            result = app.run_comparison(
                amazon_asin=product['amazon_asin'],
                flipkart_url=product['flipkart_url'],
                nudge_price=product['nudge_price'],
                amazon_domain=product.get('amazon_domain', 'in')
            )
            
            status = result.get('comparison_status', 'UNKNOWN')
            confidence = result.get('overall_confidence_score', 0)
            
            results.append({
                'index': idx,
                'amazon_asin': product['amazon_asin'],
                'status': status,
                'confidence': confidence,
                'steps_completed': result.get('step_completed', 0),
                'termination_reason': result.get('termination_reason'),
                'comparison_id': result.get('metadata', {}).get('comparison_id'),
                'critical_failures': result.get('critical_failures', []),
                'informational_failures': result.get('informational_failures', [])
            })
            
            if status == 'PASSED':
                passed += 1
                print(f"\n  ✅ PASSED - Confidence: {confidence:.2f}%")
            else:
                failed += 1
                print(f"\n  ❌ FAILED - Reason: {result.get('termination_reason', 'Unknown')}")
        
        except Exception as e:
            errors += 1
            results.append({
                'index': idx,
                'amazon_asin': product['amazon_asin'],
                'status': 'ERROR',
                'error': str(e)
            })
            print(f"\n  ⚠️ ERROR: {str(e)}")
        
        # Small delay between requests
        if idx < len(products):
            import time
            time.sleep(2)
    
    total_time = (datetime.now() - start_time).total_seconds()
    
    # Summary
    print(f"\n{'='*80}")
    print(f"  BATCH SUMMARY")
    print(f"{'='*80}")
    print(f"\n  Total Processed: {len(products)}")
    print(f"  ✅ Passed: {passed} ({passed/len(products)*100:.1f}%)")
    print(f"  ❌ Failed: {failed} ({failed/len(products)*100:.1f}%)")
    print(f"  ⚠️  Errors: {errors} ({errors/len(products)*100:.1f}%)")
    print(f"  ⏱️  Total Time: {total_time:.2f}s")
    print(f"  ⏱️  Average Time: {total_time/len(products):.2f}s per product")
    
    # Save batch results
    output_dir = os.path.join(os.path.dirname(__file__), 'outputs')
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = f"batch_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    output_path = os.path.join(output_dir, output_file)
    
    batch_result = {
        'summary': {
            'total': len(products),
            'passed': passed,
            'failed': failed,
            'errors': errors,
            'total_time_seconds': round(total_time, 2),
            'average_time_seconds': round(total_time/len(products), 2)
        },
        'results': results,
        'timestamp': datetime.now().isoformat()
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(batch_result, f, indent=2, ensure_ascii=False)
    
    print(f"\n  ✅ Batch results saved to: outputs/{output_file}")
    print(f"\n{'='*80}\n")
    
    return batch_result

def main():
    """Main entry point"""
    import argparse
    parser = argparse.ArgumentParser(description='Batch Product Comparison')
    parser.add_argument('input_file', help='Path to input CSV or JSON file')
    parser.add_argument('--limit', '-n', type=int, default=20, help='Limit the number of products to process (default: 20)')
    
    args = parser.parse_args()
    
    input_file = args.input_file
    limit = args.limit
    
    if not os.path.exists(input_file):
        print(f"\n❌ Error: File not found: {input_file}\n")
        sys.exit(1)
    
    try:
        if input_file.endswith('.csv'):
            result = process_from_csv(input_file, limit)
        elif input_file.endswith('.json'):
            result = process_from_json(input_file, limit)
        else:
            print(f"\n❌ Error: Unsupported file format. Use .csv or .json\n")
            sys.exit(1)
        
        # Exit with appropriate code
        if result['summary']['errors'] > 0:
            sys.exit(2)  # Errors occurred
        elif result['summary']['failed'] > 0:
            sys.exit(1)  # Some comparisons failed
        else:
            sys.exit(0)  # All passed
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Batch processing interrupted by user\n")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ Fatal error: {str(e)}\n")
        sys.exit(2)

if __name__ == "__main__":
    main()
