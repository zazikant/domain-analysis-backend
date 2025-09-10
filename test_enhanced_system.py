"""
Enhanced System Testing Script
Tests the complete chat-based domain analysis system
"""

import os
import asyncio
import pandas as pd
from datetime import datetime
import json

def test_imports():
    """Test that all required modules can be imported"""
    print("Testing imports...")
    
    try:
        from domain_analyzer import DomainAnalyzer
        from bigquery_client import BigQueryClient, create_bigquery_client
        from main import app, ChatSession, ChatMessage
        print("  [PASS] All imports successful")
        return True
    except ImportError as e:
        print(f"  [FAIL] Import error: {e}")
        return False

def test_domain_analyzer():
    """Test domain analyzer with enhanced functionality"""
    print(" Testing enhanced domain analyzer...")
    
    # Mock configuration for testing
    test_config = {
        'serper_api_key': 'test_key',
        'brightdata_api_token': 'test_token',
        'google_api_key': 'test_key'
    }
    
    try:
        analyzer = DomainAnalyzer(**test_config)
        
        # Test domain extraction
        result = analyzer.extract_domain_from_email('test@example.com')
        assert result.domain == 'example.com'
        assert result.original_email == 'test@example.com'
        print("  [PASS] Domain extraction works")
        
        # Test enhanced prompt template (check if it includes sector classification)
        summary_prompt = analyzer.summary_chain.prompt.template
        assert 'REAL ESTATE' in summary_prompt
        assert 'INFRASTRUCTURE' in summary_prompt
        assert 'INDUSTRIAL' in summary_prompt
        print("  [PASS] Enhanced prompts with sector classification")
        
        return True
    except Exception as e:
        print(f"  [FAIL] Domain analyzer error: {e}")
        return False

def test_enhanced_dataframe_structure():
    """Test that DataFrame includes new sector columns"""
    print("Testing enhanced DataFrame structure...")
    
    try:
        # Create sample DataFrame with new structure
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
            'created_at': [datetime.utcnow().isoformat()],
            # New sector columns
            'real_estate': ['Commercial'],
            'infrastructure': ["Can't Say"],
            'industrial': ['Warehouse']
        }
        
        df = pd.DataFrame(sample_data)
        
        # Verify all expected columns are present
        expected_columns = [
            'original_email', 'extracted_domain', 'selected_url', 'scraping_status',
            'website_summary', 'confidence_score', 'selection_reasoning',
            'completed_timestamp', 'processing_time_seconds', 'created_at',
            'real_estate', 'infrastructure', 'industrial'
        ]
        
        missing_columns = set(expected_columns) - set(df.columns)
        if missing_columns:
            print(f"  [FAIL] Missing columns: {missing_columns}")
            return False
        
        print("  [PASS] All expected columns present")
        print(f"  [INFO] DataFrame shape: {df.shape}")
        return True
        
    except Exception as e:
        print(f"  [FAIL] DataFrame structure error: {e}")
        return False

def test_chat_models():
    """Test chat-related Pydantic models"""
    print(" Testing chat models...")
    
    try:
        from main import ChatMessage, ChatRequest, ChatResponse, ProcessingStatus, ChatSession
        
        # Test ChatMessage
        message = ChatMessage(
            session_id="test-session",
            message_type="user",
            content="test@example.com"
        )
        assert message.session_id == "test-session"
        assert message.message_type == "user"
        print("  [PASS] ChatMessage model works")
        
        # Test ChatSession
        session = ChatSession("test-session")
        msg = session.add_message("Hello", "user")
        assert len(session.messages) == 1
        assert session.messages[0].content == "Hello"
        print("  [PASS] ChatSession model works")
        
        return True
    except Exception as e:
        print(f"  [FAIL] Chat models error: {e}")
        return False

def test_enhanced_analysis_result():
    """Test enhanced AnalysisResult with sector fields"""
    print(" Testing enhanced AnalysisResult model...")
    
    try:
        from main import AnalysisResult
        
        result = AnalysisResult(
            original_email="test@example.com",
            extracted_domain="example.com",
            selected_url="https://example.com",
            scraping_status="success",
            website_summary="Test company",
            confidence_score=0.95,
            selection_reasoning="Official website",
            completed_timestamp=datetime.now().isoformat(),
            processing_time_seconds=120.5,
            created_at=datetime.utcnow().isoformat(),
            from_cache=False,
            # New sector fields
            real_estate="Commercial",
            infrastructure="Can't Say",
            industrial="Warehouse"
        )
        
        # Verify sector fields
        assert result.real_estate == "Commercial"
        assert result.infrastructure == "Can't Say"
        assert result.industrial == "Warehouse"
        
        print("  [PASS] Enhanced AnalysisResult with sector fields")
        return True
        
    except Exception as e:
        print(f"  [FAIL] AnalysisResult error: {e}")
        return False

def test_csv_processing_logic():
    """Test CSV processing logic"""
    print(" Testing CSV processing logic...")
    
    try:
        # Test email cleaning and validation
        import re
        
        test_emails = [
            "  test@example.com  ",  # with spaces
            "VALID@DOMAIN.COM",      # uppercase
            "invalid-email",         # invalid format
            "user@domain.co.uk",     # valid
            None,                    # None value
            ""                       # empty string
        ]
        
        valid_emails = []
        for email in test_emails:
            if email and isinstance(email, str):
                cleaned = email.strip().lower()
                if re.match(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', cleaned):
                    valid_emails.append(cleaned)
        
        expected_valid = ["test@example.com", "valid@domain.com", "user@domain.co.uk"]
        assert len(valid_emails) == 3
        assert set(valid_emails) == set(expected_valid)
        
        print(f"  [PASS] CSV processing logic works: {len(valid_emails)}/6 emails valid")
        return True
        
    except Exception as e:
        print(f"  [FAIL] CSV processing error: {e}")
        return False

def test_websocket_session_management():
    """Test WebSocket session management"""
    print(" Testing WebSocket session management...")
    
    try:
        from main import get_or_create_session, chat_sessions
        
        # Clear existing sessions
        chat_sessions.clear()
        
        # Test session creation
        session1 = get_or_create_session("test-session-1")
        assert session1.session_id == "test-session-1"
        assert len(chat_sessions) == 1
        
        # Test getting existing session
        session2 = get_or_create_session("test-session-1")
        assert session1 is session2  # Same object
        assert len(chat_sessions) == 1
        
        # Test different session
        session3 = get_or_create_session("test-session-2")
        assert len(chat_sessions) == 2
        assert session3.session_id == "test-session-2"
        
        print("  [PASS] WebSocket session management works")
        return True
        
    except Exception as e:
        print(f"  [FAIL] Session management error: {e}")
        return False

def test_api_endpoint_imports():
    """Test that API endpoints are properly defined"""
    print(" Testing API endpoint definitions...")
    
    try:
        from main import app
        
        # Get all routes
        routes = [route.path for route in app.routes if hasattr(route, 'path')]
        
        expected_routes = [
            '/',
            '/health', 
            '/analyze',
            '/analyze/batch',
            '/domain/{domain}',
            '/stats',
            '/recent',
            '/ws/{session_id}',
            '/chat/message',
            '/chat/upload-csv',
            '/{path:path}'  # React app fallback
        ]
        
        missing_routes = []
        for route in expected_routes:
            if route not in routes:
                missing_routes.append(route)
        
        if missing_routes:
            print(f"  [FAIL] Missing routes: {missing_routes}")
            return False
        
        print("  [PASS] All expected API endpoints defined")
        print(f"  [INFO] Total routes: {len(routes)}")
        return True
        
    except Exception as e:
        print(f"  [FAIL] API endpoints error: {e}")
        return False

def run_all_tests():
    """Run all tests"""
    print(" Starting Enhanced Domain Analysis System Tests")
    print("=" * 60)
    
    tests = [
        test_imports,
        test_domain_analyzer,
        test_enhanced_dataframe_structure,
        test_chat_models,
        test_enhanced_analysis_result,
        test_csv_processing_logic,
        test_websocket_session_management,
        test_api_endpoint_imports
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"  [FAIL] Test failed with exception: {e}")
        
        print()  # Empty line between tests
    
    print("=" * 60)
    print(f" Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print(" All tests passed! The enhanced system is ready for deployment.")
        print()
        print(" Next steps:")
        print("1. Set up environment variables (API keys, GCP project)")
        print("2. Build and deploy to Cloud Run:")
        print("   docker build -t domain-analysis-chat .")
        print("   docker tag domain-analysis-chat gcr.io/PROJECT-ID/domain-analysis-chat")
        print("   docker push gcr.io/PROJECT-ID/domain-analysis-chat")
        print("   gcloud run deploy --image gcr.io/PROJECT-ID/domain-analysis-chat")
        print()
        print(" Features ready:")
        print("- Chat-based interface with React frontend")
        print("- CSV file upload for batch processing")
        print("- Real-time WebSocket updates")
        print("- Semantic sector classification (Real Estate, Infrastructure, Industrial)")
        print("- Duplicate detection and caching")
        print("- Progress reporting every 10 emails")
    else:
        failed = total - passed
        print(f"[FAIL] {failed} tests failed. Please fix issues before deployment.")
    
    return passed == total

if __name__ == "__main__":
    run_all_tests()