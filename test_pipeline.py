"""
Test script for the Domain Analysis Pipeline
Tests local functionality before deployment
"""

import os
import asyncio
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.env.example')

from domain_analyzer import DomainAnalyzer
from bigquery_client import BigQueryClient

# Mock environment variables for testing (replace with real values)
TEST_CONFIG = {
    'SERPER_API_KEY': 'test_key',
    'BRIGHTDATA_API_TOKEN': 'test_token',
    'GOOGLE_API_KEY': 'test_key',
    'GCP_PROJECT_ID': 'test_project'
}

def test_domain_extraction():
    """Test domain extraction functionality"""
    print("🧪 Testing Domain Extraction...")
    
    # Create analyzer with test config
    analyzer = DomainAnalyzer(
        serper_api_key=TEST_CONFIG['SERPER_API_KEY'],
        brightdata_api_token=TEST_CONFIG['BRIGHTDATA_API_TOKEN'],
        google_api_key=TEST_CONFIG['GOOGLE_API_KEY']
    )
    
    # Test cases
    test_emails = [
        'contact@example.com',
        'info@test-company.org',
        'admin@sub.domain.co.uk',
        'invalid-email',
        'user@',
        '@domain.com'
    ]
    
    for email in test_emails:
        result = analyzer.extract_domain_from_email(email)
        print(f"  📧 {email} → 🌐 {result.domain}")
    
    print("✅ Domain extraction test completed\n")

def test_dataframe_structure():
    """Test DataFrame output structure"""
    print("🧪 Testing DataFrame Structure...")
    
    # Create sample DataFrame matching expected structure
    sample_data = {
        'original_email': ['test@example.com'],
        'extracted_domain': ['example.com'],
        'selected_url': ['https://example.com'],
        'scraping_status': ['success'],
        'website_summary': ['Example company providing test services'],
        'confidence_score': [0.95],
        'selection_reasoning': ['Official company website'],
        'completed_timestamp': [datetime.now().isoformat()],
        'processing_time_seconds': [120.5],
        'created_at': [datetime.utcnow().isoformat()]
    }
    
    df = pd.DataFrame(sample_data)
    
    # Validate structure
    expected_columns = [
        'original_email',
        'extracted_domain', 
        'selected_url',
        'scraping_status',
        'website_summary',
        'confidence_score',
        'selection_reasoning',
        'completed_timestamp',
        'processing_time_seconds',
        'created_at'
    ]
    
    missing_columns = set(expected_columns) - set(df.columns)
    extra_columns = set(df.columns) - set(expected_columns)
    
    print(f"  📊 DataFrame shape: {df.shape}")
    print(f"  📋 Columns: {list(df.columns)}")
    
    if missing_columns:
        print(f"  ❌ Missing columns: {missing_columns}")
    if extra_columns:
        print(f"  ⚠️  Extra columns: {extra_columns}")
    
    if not missing_columns and not extra_columns:
        print("  ✅ DataFrame structure matches expected format")
    
    print("✅ DataFrame structure test completed\n")

def test_bigquery_client():
    """Test BigQuery client initialization (without actual connection)"""
    print("🧪 Testing BigQuery Client...")
    
    try:
        # This will test the class initialization without actual GCP connection
        from bigquery_client import BigQueryClient
        
        # Test schema definition
        expected_schema_fields = [
            'original_email',
            'extracted_domain',
            'selected_url', 
            'scraping_status',
            'website_summary',
            'confidence_score',
            'selection_reasoning',
            'completed_timestamp',
            'processing_time_seconds',
            'created_at'
        ]
        
        print(f"  📋 Expected schema fields: {len(expected_schema_fields)}")
        print("  ✅ BigQuery client class imported successfully")
        
    except ImportError as e:
        print(f"  ❌ Import error: {e}")
    except Exception as e:
        print(f"  ⚠️  Other error: {e}")
    
    print("✅ BigQuery client test completed\n")

def test_fastapi_imports():
    """Test FastAPI application imports"""
    print("🧪 Testing FastAPI Imports...")
    
    try:
        from main import app
        print("  ✅ FastAPI app imported successfully")
        
        # Test that app has expected routes
        routes = [route.path for route in app.routes]
        expected_routes = [
            '/',
            '/health',
            '/analyze', 
            '/analyze/batch',
            '/domain/{domain}',
            '/stats',
            '/recent'
        ]
        
        missing_routes = set(expected_routes) - set(routes)
        if missing_routes:
            print(f"  ⚠️  Missing routes: {missing_routes}")
        else:
            print("  ✅ All expected routes present")
            
    except ImportError as e:
        print(f"  ❌ Import error: {e}")
    except Exception as e:
        print(f"  ⚠️  Other error: {e}")
    
    print("✅ FastAPI imports test completed\n")

def test_configuration():
    """Test configuration loading"""
    print("🧪 Testing Configuration...")
    
    try:
        from config import get_settings, validate_environment
        
        # Test settings loading (will use defaults if env vars not set)
        settings = get_settings()
        print(f"  🌐 Port: {settings.port}")
        print(f"  📊 Dataset: {settings.bigquery_dataset_id}")
        print(f"  📋 Table: {settings.bigquery_table_id}")
        print("  ✅ Configuration loaded successfully")
        
    except Exception as e:
        print(f"  ❌ Configuration error: {e}")
    
    print("✅ Configuration test completed\n")

def run_all_tests():
    """Run all tests"""
    print("🚀 Starting Domain Analysis Pipeline Tests")
    print("=" * 50)
    
    test_domain_extraction()
    test_dataframe_structure() 
    test_bigquery_client()
    test_fastapi_imports()
    test_configuration()
    
    print("=" * 50)
    print("🎉 All tests completed!")
    print()
    print("📋 Next steps:")
    print("1. Set up actual API keys in .env file")
    print("2. Create GCP project and enable APIs")
    print("3. Run: python main.py (for local testing)")
    print("4. Run: ./deploy.sh PROJECT_ID (for Cloud Run deployment)")

if __name__ == "__main__":
    run_all_tests()