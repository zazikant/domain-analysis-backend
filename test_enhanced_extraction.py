#!/usr/bin/env python3
"""
Test Enhanced Company Name and Location Extraction
"""

import os
import json
from domain_analyzer import DomainAnalyzer

# Test the enhanced extraction with gemengserv.com
def test_gemengserv_extraction():
    """Test company name and location extraction for gemengserv.com"""

    # Initialize analyzer with environment variables
    serper_api_key = os.getenv("SERPER_API_KEY")
    brightdata_api_token = os.getenv("BRIGHTDATA_API_TOKEN")
    google_api_key = os.getenv("GOOGLE_API_KEY")

    if not all([serper_api_key, brightdata_api_token, google_api_key]):
        print("❌ Missing required API keys in environment variables")
        return

    print("🚀 Testing Enhanced Company Name and Location Extraction")
    print("=" * 60)

    analyzer = DomainAnalyzer(
        serper_api_key=serper_api_key,
        brightdata_api_token=brightdata_api_token,
        google_api_key=google_api_key
    )

    # Test email
    test_email = "contact@gemengserv.com"
    print(f"📧 Analyzing: {test_email}")
    print("-" * 40)

    try:
        # Run analysis
        result_df = analyzer.analyze_email_domain(test_email)

        # Extract results
        result = result_df.iloc[0]

        print("📊 EXTRACTION RESULTS:")
        print(f"✅ Company Summary: {result['company_summary']}")
        print(f"🏢 Company Name: {result['company_name']}")
        print(f"📍 Base Location: {result['base_location']}")
        print(f"🏗️  Company Type: {result['company_type']}")
        print(f"🏘️  Real Estate: {result['real_estate']}")
        print(f"🏗️  Infrastructure: {result['infrastructure']}")
        print(f"🏭 Industrial: {result['industrial']}")
        print(f"🌐 Selected URL: {result['selected_url']}")
        print(f"⚡ Processing Time: {result['processing_time_seconds']:.1f}s")

        print("\n" + "=" * 60)
        print("🎯 EXPECTED vs ACTUAL:")
        print(f"Expected Company Name: 'GEM Engserv' or 'GEM Engserv Pvt Ltd'")
        print(f"Actual Company Name: '{result['company_name']}'")
        print(f"Expected Location: City/location from registered address")
        print(f"Actual Location: '{result['base_location']}'")

        # Check if improvements were successful
        if result['company_name'] != "Can't Say" and "gem" in result['company_name'].lower():
            print("✅ Company name extraction IMPROVED!")
        else:
            print("❌ Company name extraction still needs work")

        if result['base_location'] != "Can't Say":
            print("✅ Location extraction working!")
        else:
            print("❌ Location extraction still needs work")

    except Exception as e:
        print(f"❌ Error during analysis: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_gemengserv_extraction()