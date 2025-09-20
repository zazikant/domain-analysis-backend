"""
Test script for the Fire and Forget email processing system
"""

import os
import time
import requests
import pandas as pd
import io
from datetime import datetime

def test_fire_and_forget_system():
    """Test the complete fire and forget system"""

    print("🔥 Testing Fire and Forget Email Processing System")
    print("=" * 60)

    # Test configuration
    BASE_URL = "http://localhost:8080"  # Change to your API URL
    SESSION_ID = f"test-session-{int(time.time())}"

    # Test 1: Chat email input
    print("\n1. Testing chat email input...")

    chat_data = {
        "session_id": SESSION_ID,
        "message": "test@example.com",
        "message_type": "user"
    }

    try:
        response = requests.post(f"{BASE_URL}/chat/message", json=chat_data)
        if response.status_code == 200:
            result = response.json()
            print(f"   ✅ Chat email added to queue: {result['content']}")
        else:
            print(f"   ❌ Chat email failed: {response.status_code}")
    except Exception as e:
        print(f"   ❌ Chat email error: {e}")

    # Test 2: CSV upload
    print("\n2. Testing CSV upload...")

    # Create test CSV
    test_emails = [
        "contact@testcompany1.com",
        "info@testcompany2.com",
        "hello@testcompany3.com"
    ]

    csv_data = pd.DataFrame({"Email": test_emails})
    csv_buffer = io.StringIO()
    csv_data.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()

    files = {
        'file': ('test_emails.csv', csv_content, 'text/csv')
    }

    form_data = {
        'session_id': SESSION_ID
    }

    try:
        response = requests.post(f"{BASE_URL}/chat/upload-csv", files=files, data=form_data)
        if response.status_code == 200:
            result = response.json()
            print(f"   ✅ CSV uploaded: {result['message']}")
        else:
            print(f"   ❌ CSV upload failed: {response.status_code}")
    except Exception as e:
        print(f"   ❌ CSV upload error: {e}")

    # Test 3: Check queue status
    print("\n3. Checking queue status...")

    try:
        response = requests.get(f"{BASE_URL}/queue/status")
        if response.status_code == 200:
            result = response.json()
            queue_count = result['emails_in_queue']
            status = result['status']
            print(f"   📊 Queue status: {queue_count} emails in queue, status: {status}")
        else:
            print(f"   ❌ Queue status failed: {response.status_code}")
    except Exception as e:
        print(f"   ❌ Queue status error: {e}")

    # Test 4: Wait and monitor background processing
    print("\n4. Monitoring background processing...")
    print("   (Waiting 60 seconds to see if background processor works...)")

    for i in range(6):  # Check every 10 seconds for 1 minute
        time.sleep(10)
        try:
            response = requests.get(f"{BASE_URL}/queue/status")
            if response.status_code == 200:
                result = response.json()
                queue_count = result['emails_in_queue']
                print(f"   📊 After {(i+1)*10}s: {queue_count} emails remaining in queue")

                if queue_count == 0:
                    print("   🎉 Queue is empty! Background processor working!")
                    break
            else:
                print(f"   ❌ Queue check failed: {response.status_code}")
        except Exception as e:
            print(f"   ❌ Queue check error: {e}")

    # Test 5: Check if results were processed
    print("\n5. Checking processing results...")

    try:
        response = requests.get(f"{BASE_URL}/recent?limit=10")
        if response.status_code == 200:
            results = response.json()
            print(f"   📊 Found {len(results)} recent analysis results")

            # Look for our test emails
            test_email_results = [r for r in results if r['original_email'] in test_emails + ['test@example.com']]
            if test_email_results:
                print(f"   ✅ Found {len(test_email_results)} results for our test emails!")
                for result in test_email_results:
                    print(f"      - {result['original_email']}: {result['scraping_status']}")
            else:
                print("   ⏳ Test emails not yet processed (may need more time)")
        else:
            print(f"   ❌ Recent results failed: {response.status_code}")
    except Exception as e:
        print(f"   ❌ Recent results error: {e}")

    print("\n" + "=" * 60)
    print("🔥 Fire and Forget System Test Complete!")
    print("\nIf queue count decreased and results were found, the system is working!")
    print("You can now upload CSV files and close your laptop - BigQuery will handle everything!")

if __name__ == "__main__":
    test_fire_and_forget_system()