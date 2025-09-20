"""
Configuration management for the Domain Analysis API
Handles environment variables, settings, and global state
"""

import os
import logging
from typing import Optional
from contextlib import asynccontextmanager

from domain_analyzer import DomainAnalyzer
from bigquery_client import BigQueryClient, create_bigquery_client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables for clients
domain_analyzer: Optional[DomainAnalyzer] = None
bigquery_client: Optional[BigQueryClient] = None


class Settings:
    """Application settings"""
    
    def __init__(self):
        # API Keys
        self.serper_api_key = os.getenv("SERPER_API_KEY")
        self.brightdata_api_token = os.getenv("BRIGHTDATA_API_TOKEN")
        self.google_api_key = os.getenv("GOOGLE_API_KEY")
        
        # Google Cloud Configuration
        self.gcp_project_id = os.getenv("GCP_PROJECT_ID")
        self.bigquery_dataset_id = os.getenv("BIGQUERY_DATASET_ID", "advanced_csv_analysis")
        self.bigquery_table_id = os.getenv("BIGQUERY_TABLE_ID", "email_domain_results")
        
        # Application Configuration
        self.port = int(os.getenv("PORT", 8080))
        self.environment = os.getenv("ENV", "production")
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        
        # Processing Configuration
        self.max_batch_size = int(os.getenv("MAX_BATCH_SIZE", 50))
        self.cache_ttl_hours = int(os.getenv("CACHE_TTL_HOURS", 24))
        self.request_timeout_seconds = int(os.getenv("REQUEST_TIMEOUT_SECONDS", 900))
        
        # Parallel Processing Configuration
        self.max_workers = int(os.getenv("MAX_WORKERS", 5))
        self.batch_processing_size = int(os.getenv("BATCH_PROCESSING_SIZE", 10))
    
    def validate_required_vars(self):
        """Validate that all required environment variables are set"""
        required_vars = [
            ("SERPER_API_KEY", self.serper_api_key),
            ("BRIGHTDATA_API_TOKEN", self.brightdata_api_token),
            ("GOOGLE_API_KEY", self.google_api_key),
            ("GCP_PROJECT_ID", self.gcp_project_id)
        ]
        
        missing_vars = []
        for var_name, var_value in required_vars:
            if not var_value:
                missing_vars.append(var_name)
        
        return missing_vars


# Global settings instance
settings = Settings()


def get_domain_analyzer() -> DomainAnalyzer:
    """Dependency injection for domain analyzer"""
    if domain_analyzer is None:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=503, 
            detail="Domain analyzer not initialized - check environment variables"
        )
    return domain_analyzer


def get_bigquery_client() -> BigQueryClient:
    """Dependency injection for BigQuery client"""
    if bigquery_client is None:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=503, 
            detail="BigQuery client not initialized - check environment variables"
        )
    return bigquery_client


@asynccontextmanager
async def lifespan(app):
    """Application lifespan manager"""
    global domain_analyzer, bigquery_client
    
    # Startup - Initialize clients
    logger.info("Starting up Domain Analysis API...")
    
    # Validate environment variables
    missing_vars = settings.validate_required_vars()
    
    if missing_vars:
        logger.warning("Missing required environment variables:")
        for var in missing_vars:
            logger.warning(f"  ✗ {var}")
        
        # Don't fail startup - allow container to start in degraded mode
        logger.warning("Starting in degraded mode - API endpoints will return errors until env vars are set")
        yield
        return
    
    # Initialize Domain Analyzer
    try:
        domain_analyzer = DomainAnalyzer(
            serper_api_key=settings.serper_api_key,
            brightdata_api_token=settings.brightdata_api_token,
            google_api_key=settings.google_api_key
        )
        logger.info("✅ Domain analyzer initialized")
    except Exception as e:
        logger.error(f"Failed to initialize domain analyzer: {e}")
        yield
        return
    
    # Initialize BigQuery Client (this will auto-create the new job tables)
    try:
        bigquery_client = create_bigquery_client(
            settings.gcp_project_id, 
            settings.bigquery_dataset_id, 
            settings.bigquery_table_id
        )
        logger.info("✅ BigQuery client initialized")
    except Exception as e:
        logger.error(f"Failed to initialize BigQuery client: {e}")
        yield
        return
    
    logger.info("🚀 Domain Analysis API started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Domain Analysis API...")
    
    """
Pydantic models for request/response validation
Contains all data structures used by the API
"""

import uuid
from typing import List, Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, EmailStr


# Request Models
class AnalyzeEmailRequest(BaseModel):
    email: EmailStr = Field(..., description="Email address to analyze")
    force_refresh: bool = Field(False, description="Force new analysis even if cached result exists")
    
    class Config:
        schema_extra = {
            "example": {
                "email": "contact@example.com",
                "force_refresh": False
            }
        }


class BatchAnalyzeRequest(BaseModel):
    emails: List[EmailStr] = Field(..., min_items=1, max_items=50, description="List of email addresses to analyze")
    force_refresh: bool = Field(False, description="Force new analysis even if cached results exist")
    
    class Config:
        schema_extra = {
            "example": {
                "emails": ["contact@example.com", "info@company.com"],
                "force_refresh": False
            }
        }


# Response Models
# Models will be defined after imports

"""
Utility functions for data processing and analysis
Handles CSV cleaning, data conversion, and helper functions
"""

import re
import logging
from typing import List, Dict, Any, Optional
import pandas as pd

from models import AnalysisResult, ChatSession, ChatMessage, ChatResponse
from bigquery_client import BigQueryClient

logger = logging.getLogger(__name__)

# Additional models not in models.py module
class BatchAnalysisResult(BaseModel):
    results: List[AnalysisResult]
    total_processed: int
    successful: int
    failed: int
    from_cache: int
    processing_time_seconds: float


class JobCreateResponse(BaseModel):
    job_id: str
    message: str
    total_emails: int
    cleaning_stats: Dict[str, Any]
    status_url: str


class StatsResponse(BaseModel):
    total_analyses: int
    unique_domains: int
    avg_processing_time: float
    avg_confidence_score: float
    successful_scrapes: int
    failed_scrapes: int

# Global session storage (in production, use Redis or similar)
chat_sessions: Dict[str, ChatSession] = {}


def clean_email_dataframe(df: pd.DataFrame, bq_client: Optional[BigQueryClient] = None) -> tuple[List[str], Dict[str, Any]]:
    """
    Enhanced email cleaning from pandas DataFrame with comprehensive validation and BigQuery duplicate checking
    
    Args:
        df: pandas DataFrame containing email data
        bq_client: Optional BigQuery client for checking existing domains
        
    Returns:
        tuple: (list of valid emails, cleaning stats dict)
    """
    stats = {
        "total_rows": len(df),
        "email_column": None,
        "valid_emails": 0,
        "invalid_emails": 0,
        "duplicates_removed": 0,
        "empty_rows": 0,
        "bigquery_duplicates": 0,
        "new_emails": 0
    }
    
    # Auto-detect email column
    email_column = None
    email_keywords = ['email', 'e-mail', 'mail', 'address', 'contact']
    
    # Try to find column with email-like name
    for col in df.columns:
        if any(keyword in col.lower() for keyword in email_keywords):
            email_column = col
            break
    
    # If no email-named column found, use first column
    if email_column is None:
        email_column = df.columns[0]
    
    stats["email_column"] = email_column
    
    # Extract and clean emails
    emails_series = df[email_column].copy()
    
    # Convert to string and handle NaN values
    emails_series = emails_series.astype(str)
    
    # Remove obvious non-email values
    emails_series = emails_series.replace(['nan', 'NaN', 'None', 'null', ''], pd.NA)
    
    # Count empty rows
    stats["empty_rows"] = emails_series.isna().sum()
    
    # Remove empty rows
    emails_series = emails_series.dropna()
    
    # Comprehensive cleaning
    emails_series = (emails_series
                    .str.strip()                    # Remove leading/trailing spaces
                    .str.replace(r'\s+', '', regex=True)  # Remove any internal spaces
                    .str.lower()                    # Convert to lowercase
                    .str.replace(r'^mailto:', '', regex=True)  # Remove mailto: prefix
                    .str.replace(r'[<>]', '', regex=True)     # Remove angle brackets
                    )
    
    # Email validation regex (more comprehensive)
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    # Filter valid emails
    valid_mask = emails_series.str.match(email_pattern, na=False)
    valid_emails = emails_series[valid_mask].tolist()
    
    # Remove duplicates while preserving order
    seen = set()
    unique_emails = []
    duplicates = 0
    
    for email in valid_emails:
        if email not in seen:
            seen.add(email)
            unique_emails.append(email)
        else:
            duplicates += 1
    
    # Check BigQuery for existing emails if client provided
    final_emails = unique_emails
    bigquery_duplicates = 0
    
    if bq_client and unique_emails:
        try:
            # Check which specific EMAIL ADDRESSES exist in BigQuery (exact match)
            existing_emails = bq_client.check_multiple_emails_exist(unique_emails, max_age_hours=24)
            
            # Filter out emails that already exist (exact email match only)
            new_emails = []
            for email in unique_emails:
                if not existing_emails.get(email, False):
                    new_emails.append(email)
                else:
                    bigquery_duplicates += 1
            
            final_emails = new_emails
            
        except Exception as e:
            logger.error(f"Error checking BigQuery duplicates: {str(e)}")
            # On error, use all unique emails
            final_emails = unique_emails
    
    # Update stats
    stats["valid_emails"] = len(unique_emails)
    stats["invalid_emails"] = len(emails_series) - len(valid_emails)
    stats["duplicates_removed"] = duplicates
    stats["bigquery_duplicates"] = bigquery_duplicates
    stats["new_emails"] = len(final_emails)
    
    return final_emails, stats


def dataframe_to_analysis_result(df: pd.DataFrame, from_cache: bool = False) -> List[AnalysisResult]:
    """Convert DataFrame to AnalysisResult objects"""
    results = []
    
    for _, row in df.iterrows():
        # Convert pandas Timestamp objects to strings for Pydantic validation
        completed_timestamp = row.get('completed_timestamp')
        if completed_timestamp is not None and hasattr(completed_timestamp, 'isoformat'):
            completed_timestamp = completed_timestamp.isoformat()
        
        created_at = row.get('created_at')
        if created_at is not None and hasattr(created_at, 'isoformat'):
            created_at = created_at.isoformat()
        
        # Handle both old and new column names for backward compatibility
        company_summary = row.get('company_summary') or row.get('website_summary')
        
        result = AnalysisResult(
            original_email=row.get('original_email', ''),
            extracted_domain=row.get('extracted_domain', ''),
            selected_url=row.get('selected_url'),
            scraping_status=row.get('scraping_status', ''),
            company_summary=company_summary,
            confidence_score=row.get('confidence_score'),
            selection_reasoning=row.get('selection_reasoning'),
            completed_timestamp=completed_timestamp,
            processing_time_seconds=row.get('processing_time_seconds'),
            created_at=created_at or '',
            from_cache=from_cache,
            # Sector classification fields
            real_estate=row.get('real_estate', "Can't Say"),
            infrastructure=row.get('infrastructure', "Can't Say"),
            industrial=row.get('industrial', "Can't Say"),
            # Company information fields
            company_type=row.get('company_type', "Can't Say"),
            company_name=row.get('company_name', "Can't Say"),
            base_location=row.get('base_location', "Can't Say")
        )
        results.append(result)
    
    return results


# Chat helper functions (simplified for backward compatibility)
def get_or_create_session(session_id: str) -> ChatSession:
    """Get existing session or create new one"""
    if session_id not in chat_sessions:
        chat_sessions[session_id] = ChatSession(session_id)
    return chat_sessions[session_id]


async def send_chat_message(session_id: str, content: str, metadata: Optional[Dict] = None):
    """Send message to chat session via WebSocket"""
    session = get_or_create_session(session_id)
    message = session.add_message(content, "system", metadata)
    
    if session.websocket:
        try:
            response = ChatResponse(
                message_id=message.message_id,
                session_id=session_id,
                message_type="system",
                content=content,
                timestamp=message.timestamp,
                metadata=metadata
            )
            await session.websocket.send_json(response.dict())
        except Exception as e:
            logger.error(f"Failed to send WebSocket message: {e}")
            
    return message


def extract_email_from_text(text: str) -> Optional[str]:
    """Extract email address from text message"""
    email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
    if email_match:
        return email_match.group().lower().strip()
    return None


def format_processing_summary(stats: Dict[str, Any]) -> str:
    """Format CSV processing summary for user display"""
    return f"""📁 CSV file processed successfully!

📊 Processing Summary:
• Total rows: {stats['total_rows']}
• Email column used: '{stats['email_column']}'
• Valid emails found: {stats['valid_emails']}
• Invalid emails: {stats['invalid_emails']}
• CSV duplicates removed: {stats['duplicates_removed']}
• Already in database: {stats['bigquery_duplicates']} ⚡
• New emails to process: {stats['new_emails']}
• Empty rows skipped: {stats['empty_rows']}

🚀 Starting analysis for {stats['new_emails']} new emails..."""


def format_progress_message(processed: int, total: int, successful: int, failed: int, duplicates: int) -> str:
    """Format progress update message"""
    return f"Progress: {processed}/{total} emails processed. ✅ {successful} successful, ❌ {failed} failed, 🔄 {duplicates} duplicates."


def format_final_summary(processed: int, total: int, successful: int, failed: int, duplicates: int) -> str:
    """Format final processing summary"""
    return f"""🎉 Batch processing complete!

📊 **Final Results:**
- Total processed: {processed}/{total}
- ✅ Successful: {successful}
- ❌ Failed: {failed}  
- 🔄 Duplicates skipped: {duplicates}

All results have been saved to the database. You can now submit more emails or upload another CSV file!"""


def validate_file_upload(filename: str) -> bool:
    """Validate uploaded file type"""
    return filename.endswith('.csv')


def calculate_progress_percentage(processed: int, total: int) -> float:
    """Calculate progress percentage"""
    return (processed / total * 100) if total > 0 else 0


"""
Single email processing logic
Handles individual email analysis with caching and error handling
"""

import asyncio
import logging
from typing import Dict, Any
from datetime import datetime

from domain_analyzer import DomainAnalyzer
from bigquery_client import BigQueryClient
# dataframe_to_analysis_result is defined below

logger = logging.getLogger(__name__)


async def process_email_analysis(
    email: str, 
    analyzer: DomainAnalyzer, 
    bq_client: BigQueryClient,
    force_refresh: bool = False
) -> AnalysisResult:
    """
    Process single email analysis with caching
    
    Args:
        email: Email address to analyze
        analyzer: Domain analyzer instance
        bq_client: BigQuery client for caching
        force_refresh: Skip cache and force new analysis
        
    Returns:
        AnalysisResult: Complete analysis result
    """
    
    # Check if we have any previous results for this specific email (unless force refresh)
    if not force_refresh and bq_client.check_email_exists(email, max_age_hours=525600):  # 10 years
        logger.info(f"Using cached result for email: {email}")

        cached_df = bq_client.query_email_results(email, limit=1)
        if not cached_df.empty:
            results = dataframe_to_analysis_result(cached_df, from_cache=True)
            return results[0]
    
    # Perform new analysis
    logger.info(f"Performing new analysis for email: {email}")
    
    # Run analysis in thread pool to avoid blocking
    loop = asyncio.get_event_loop()
    df = await loop.run_in_executor(None, analyzer.analyze_email_domain, email)
    
    # Insert result into BigQuery
    success = bq_client.insert_dataframe(df)
    if not success:
        logger.warning(f"Failed to insert result into BigQuery for email: {email}")
    
    # Convert to result object
    results = dataframe_to_analysis_result(df, from_cache=False)
    return results[0]


async def process_single_email_for_job(email_data: Dict, analyzer: DomainAnalyzer, bq_client: BigQueryClient, semaphore: asyncio.Semaphore) -> Dict[str, Any]:
    """
    Process a single email within a batch job with rate limiting
    
    Args:
        email_data: Dictionary containing queue_id, email, etc.
        analyzer: Domain analyzer instance
        bq_client: BigQuery client
        semaphore: Asyncio semaphore for rate limiting
        
    Returns:
        Dict: Processing result with status and email info
    """
    async with semaphore:
        queue_id = email_data['queue_id']
        email = email_data['email']
        
        try:
            # Mark as processing
            bq_client.update_email_status(queue_id, "processing")
            
            # Check for duplicates first
            if bq_client.check_email_exists(email, max_age_hours=24):
                bq_client.update_email_status(queue_id, "completed")
                return {"status": "duplicate", "email": email}
            
            # Process in thread pool
            loop = asyncio.get_event_loop()
            result_df = await loop.run_in_executor(
                None, analyzer.analyze_email_domain, email
            )
            
            # Save to BigQuery
            success = bq_client.insert_dataframe(result_df)
            
            if success:
                bq_client.update_email_status(queue_id, "completed")
                return {"status": "success", "email": email}
            else:
                bq_client.update_email_status(queue_id, "failed", "Failed to save to BigQuery")
                return {"status": "failed", "email": email}
                
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error processing {email}: {error_msg}")
            bq_client.update_email_status(queue_id, "failed", error_msg)
            return {"status": "failed", "email": email, "error": error_msg}


def create_error_analysis_result(email: str, error: str):
    """
    Create an error result for failed email processing
    
    Args:
        email: Email that failed
        error: Error message
        
    Returns:
        AnalysisResult: Error result object
    """
    from datetime import datetime
    
    # Import here to avoid circular imports
    # dataframe_to_analysis_result is defined below
    import pandas as pd
    
    # Create error DataFrame
    error_data = {
        'original_email': [email],
        'extracted_domain': ['error'],
        'selected_url': [None],
        'scraping_status': ['error'],
        'company_summary': [f"Error: {error}"],
        'confidence_score': [0.0],
        'selection_reasoning': ['Processing failed'],
        'completed_timestamp': [datetime.utcnow().isoformat()],
        'processing_time_seconds': [0.0],
        'created_at': [datetime.utcnow().isoformat()],
        'real_estate': ["Can't Say"],
        'infrastructure': ["Can't Say"],
        'industrial': ["Can't Say"],
        'company_type': ["Can't Say"],
        'company_name': ["Can't Say"],
        'base_location': ["Can't Say"]
    }
    
    df = pd.DataFrame(error_data)
    results = dataframe_to_analysis_result(df, from_cache=False)
    return results[0]
    
"""
Parallel batch processing engine
Handles concurrent processing of multiple emails with rate limiting and job management
"""

import asyncio
import logging
from typing import Dict
from datetime import datetime

from domain_analyzer import DomainAnalyzer
from bigquery_client import BigQueryClient
from config import settings
# process_single_email_for_job function needed here

logger = logging.getLogger(__name__)


async def process_batch_job(job_id: str, bq_client: BigQueryClient, analyzer: DomainAnalyzer):
    """
    Process batch job with parallel email processing
    This runs independently and survives container restarts
    
    Args:
        job_id: Unique job identifier
        bq_client: BigQuery client for data persistence
        analyzer: Domain analyzer for email processing
    """
    logger.info(f"Starting parallel processing for batch job {job_id}")
    
    try:
        # Update job status to processing
        bq_client.update_job_status(job_id, "processing")
        
        # Get job details
        job_status = bq_client.get_job_status(job_id)
        if not job_status:
            logger.error(f"Job {job_id} not found")
            return
        
        total_emails = job_status['total_emails']
        
        # Parallel processing configuration
        MAX_WORKERS = settings.max_workers
        BATCH_SIZE = settings.batch_processing_size
        semaphore = asyncio.Semaphore(MAX_WORKERS)
        
        # Process emails in parallel batches
        processed = 0
        successful = 0
        failed = 0
        duplicates = 0
        
        while processed < total_emails:
            # Get next batch of pending emails
            pending_emails = bq_client.get_pending_emails(job_id, limit=BATCH_SIZE)
            
            if not pending_emails:
                break  # No more emails to process
            
            # Process batch concurrently
            tasks = [
                process_single_email_for_job(email_data, analyzer, bq_client, semaphore)
                for email_data in pending_emails
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Update statistics
            batch_successful = 0
            batch_failed = 0
            batch_duplicates = 0
            
            for result in results:
                if isinstance(result, Exception):
                    batch_failed += 1
                elif result["status"] == "success":
                    batch_successful += 1
                elif result["status"] == "duplicate":
                    batch_duplicates += 1
                else:
                    batch_failed += 1
            
            # Update running totals
            successful += batch_successful
            failed += batch_failed
            duplicates += batch_duplicates
            processed += len(pending_emails)
            
            # Update job status in BigQuery
            bq_client.update_job_status(
                job_id, 
                "processing",
                processed_emails=processed,
                successful_emails=successful,
                failed_emails=failed,
                duplicate_emails=duplicates
            )
            
            # Log progress
            progress_pct = (processed / total_emails * 100) if total_emails > 0 else 0
            progress_msg = f"Processed {processed}/{total_emails} ({progress_pct:.1f}%) - {successful} successful, {failed} failed, {duplicates} duplicates"
            
            bq_client.log_progress(job_id, progress_msg, progress_pct)
            logger.info(f"Job {job_id}: {progress_msg}")
            
            # Rate limiting delay between batches
            await asyncio.sleep(2)
        
        # Final status update
        final_status = "completed" if failed == 0 else "completed_with_errors"
        bq_client.update_job_status(
            job_id,
            final_status,
            processed_emails=processed,
            successful_emails=successful,
            failed_emails=failed,
            duplicate_emails=duplicates
        )
        
        final_msg = f"Job {job_id} completed: {processed} processed, {successful} successful, {failed} failed, {duplicates} duplicates"
        bq_client.log_progress(job_id, final_msg, 100.0)
        logger.info(f"Job {job_id} processing completed")
        
    except Exception as e:
        logger.error(f"Error in batch job processing: {str(e)}")
        bq_client.update_job_status(job_id, "failed", error_message=str(e))


async def process_small_batch_sequential(
    emails: list,
    analyzer: DomainAnalyzer,
    bq_client: BigQueryClient,
    force_refresh: bool = False
):
    """
    Process small batch of emails sequentially (for legacy /analyze/batch endpoint)
    
    Args:
        emails: List of email addresses
        analyzer: Domain analyzer
        bq_client: BigQuery client
        force_refresh: Skip cache check
        
    Returns:
        tuple: (results list, stats dict)
    """
    # process_email_analysis and create_error_analysis_result are defined above
    from datetime import datetime
    
    start_time = datetime.utcnow()
    results = []
    successful = 0
    failed = 0
    from_cache = 0
    
    # Process each email sequentially (for small batches)
    for email in emails:
        try:
            result = await process_email_analysis(
                email=email,
                analyzer=analyzer,
                bq_client=bq_client,
                force_refresh=force_refresh
            )
            
            results.append(result)
            
            if result.scraping_status in ['success', 'success_text', 'success_fallback']:
                successful += 1
            else:
                failed += 1
            
            if result.from_cache:
                from_cache += 1
                
        except Exception as e:
            logger.error(f"Error processing email {email}: {str(e)}")
            failed += 1
            
            # Add error result
            error_result = create_error_analysis_result(email, str(e))
            results.append(error_result)
    
    processing_time = (datetime.utcnow() - start_time).total_seconds()
    
    stats = {
        "total_processed": len(emails),
        "successful": successful,
        "failed": failed,
        "from_cache": from_cache,
        "processing_time_seconds": processing_time
    }
    
    return results, stats

"""
Legacy processing functions for backward compatibility with chat interface
Maintains existing chat functionality while migrating to new job-based system
"""

import asyncio
import logging
from typing import List

from domain_analyzer import DomainAnalyzer
from bigquery_client import BigQueryClient
# dataframe_to_analysis_result is defined below, format_progress_message, format_final_summary
# process_email_analysis is defined above

logger = logging.getLogger(__name__)


async def process_csv_emails_background(
    session_id: str, 
    emails: List[str], 
    analyzer: DomainAnalyzer, 
    bq_client: BigQueryClient
):
    """
    Process emails in background with progress updates (legacy chat interface)
    This is kept for backward compatibility with the chat interface
    
    Args:
        session_id: Chat session ID for WebSocket updates
        emails: List of email addresses to process
        analyzer: Domain analyzer instance
        bq_client: BigQuery client
    """
    # send_chat_message is defined above
    
    logger.info(f"Background processing started for session {session_id} with {len(emails)} emails")
    
    try:
        total_emails = len(emails)
        processed = 0
        successful = 0
        duplicates = 0
        failed = 0
        all_results = []
        
        for i, email in enumerate(emails):
            try:
                # Check for duplicates
                if bq_client.check_email_exists(email, max_age_hours=24):
                    duplicates += 1
                    processed += 1
                    logger.info(f"Email {email} is duplicate - marking as completed")
                    continue
                
                # Process email using the updated single_processing function
                result = await process_email_analysis(
                    email=email,
                    analyzer=analyzer,
                    bq_client=bq_client,
                    force_refresh=False
                )
                
                all_results.append(result)
                
                if result.scraping_status in ['success', 'success_text', 'success_fallback']:
                    successful += 1
                else:
                    failed += 1
                
                processed += 1
                
                # Send progress update every 10 completions
                if processed % 10 == 0 or processed == total_emails:
                    progress_msg = format_progress_message(processed, total_emails, successful, failed, duplicates)
                    await send_chat_message(session_id, progress_msg)
                
            except Exception as e:
                logger.error(f"Error processing email {email}: {str(e)}")
                failed += 1
                processed += 1
        
        # Send final summary
        final_msg = format_final_summary(processed, total_emails, successful, failed, duplicates)

        await send_chat_message(session_id, final_msg, {
            "batch_results": {
                "total": total_emails,
                "processed": processed,
                "successful": successful,
                "failed": failed,
                "duplicates": duplicates,
                "results": [r.dict() for r in all_results[-10:]]  # Last 10 results
            }
        })
        
        logger.info(f"Background processing completed for session {session_id}")
        
    except Exception as e:
        logger.error(f"Error in background processing: {str(e)}")
        logger.error(f"Exception details: {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        error_msg = f"Batch processing failed: {str(e)}"
        try:
            await send_chat_message(session_id, error_msg)
        except Exception as send_error:
            logger.error(f"Failed to send error message: {send_error}")


async def process_single_chat_email(
    email: str,
    session_id: str,
    analyzer: DomainAnalyzer,
    bq_client: BigQueryClient
):
    """
    Process single email from chat interface with real-time updates
    
    Args:
        email: Email address to process
        session_id: Chat session ID
        analyzer: Domain analyzer
        bq_client: BigQuery client
        
    Returns:
        AnalysisResult: Processing result
    """
    # send_chat_message is defined above
    
    try:
        # Check for duplicates first
        if bq_client.check_email_exists(email, max_age_hours=24):
            duplicate_msg = f"Email {email} was already processed recently. Please share another email address."
            await send_chat_message(session_id, duplicate_msg)
            return None
        
        # Send processing notification
        await send_chat_message(session_id, f"Processing email: {email}...")
        
        # Run analysis using updated single_processing function
        result = await process_email_analysis(
            email=email,
            analyzer=analyzer,
            bq_client=bq_client,
            force_refresh=False
        )
        
        # Format and send result
        response_content = format_single_email_result(result)
        await send_chat_message(session_id, response_content, {"analysis_result": result.dict()})
        
        return result
        
    except Exception as e:
        logger.error(f"Error processing chat email {email}: {str(e)}")
        error_msg = f"Sorry, there was an error processing your request: {str(e)}"
        await send_chat_message(session_id, error_msg)
        return None


def format_single_email_result(result) -> str:
    """
    Format single email analysis result for chat display
    
    Args:
        result: AnalysisResult object
        
    Returns:
        str: Formatted result message
    """
    # Handle both old and new field names for backward compatibility
    summary = getattr(result, 'company_summary', None) or getattr(result, 'website_summary', 'No summary available')
    
    cant_say = "Can't Say"
    return f"""Analysis complete for {result.original_email}!

**Domain**: {result.extracted_domain}
**Summary**: {summary}
**Confidence**: {result.confidence_score:.2f if result.confidence_score else 0}

**Company Information**:
- Company Name: {getattr(result, 'company_name', cant_say)}
- Company Type: {getattr(result, 'company_type', cant_say)}
- Base Location: {getattr(result, 'base_location', cant_say)}

**Sector Classifications**:
- Real Estate: {getattr(result, 'real_estate', cant_say)}
- Infrastructure: {getattr(result, 'infrastructure', cant_say)}
- Industrial: {getattr(result, 'industrial', cant_say)}

Feel free to submit another email or upload a CSV file!"""


def extract_email_from_chat_message(message: str) -> str:
    """
    Extract email from chat message
    
    Args:
        message: User's chat message
        
    Returns:
        str: Extracted email or None
    """
    import re
    email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', message)
    if email_match:
        return email_match.group().lower().strip()
    return None

"""
Core analysis API endpoints
Handles single email analysis, batch analysis, and basic queries
"""

import logging
from typing import List
from datetime import datetime

from fastapi import HTTPException, Depends

# get_domain_analyzer and get_bigquery_client are defined above
# dataframe_to_analysis_result is defined below
# process_email_analysis is defined above
# batch processing function needed

logger = logging.getLogger(__name__)


async def analyze_single_email(
    request,
    analyzer = Depends(get_domain_analyzer),
    bq_client = Depends(get_bigquery_client)
):
    """
    Analyze a single email address and return domain intelligence
    """
    try:
        result = await process_email_analysis(
            email=request.email,
            analyzer=analyzer,
            bq_client=bq_client,
            force_refresh=request.force_refresh
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error analyzing email {request.email}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


async def batch_analyze_emails(
    request,
    analyzer = Depends(get_domain_analyzer),
    bq_client = Depends(get_bigquery_client)
):
    """
    Analyze multiple email addresses in batch (legacy endpoint - use /csv/upload for large batches)
    """
    try:
        results, stats = await process_small_batch_sequential(
            emails=request.emails,
            analyzer=analyzer,
            bq_client=bq_client,
            force_refresh=request.force_refresh
        )
        
        from models import BatchAnalysisResult
        return BatchAnalysisResult(
            results=results,
            total_processed=stats["total_processed"],
            successful=stats["successful"],
            failed=stats["failed"],
            from_cache=stats["from_cache"],
            processing_time_seconds=stats["processing_time_seconds"]
        )
        
    except Exception as e:
        logger.error(f"Error in batch analysis: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Batch analysis failed: {str(e)}")


async def get_domain_results(
    domain: str,
    limit: int = 10,
    bq_client = Depends(get_bigquery_client)
):
    """
    Get cached analysis results for a specific domain
    """
    try:
        df = bq_client.query_domain_results(domain, limit=limit)
        
        if df.empty:
            return []
        
        results = dataframe_to_analysis_result(df, from_cache=True)
        return results
        
    except Exception as e:
        logger.error(f"Error querying domain {domain}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


async def get_analysis_stats(
    bq_client = Depends(get_bigquery_client)
):
    """
    Get analysis statistics
    """
    try:
        stats = bq_client.get_analysis_stats()
        
        from models import StatsResponse
        return StatsResponse(
            total_analyses=stats.get('total_analyses', 0),
            unique_domains=stats.get('unique_domains', 0),
            avg_processing_time=stats.get('avg_processing_time', 0.0),
            avg_confidence_score=stats.get('avg_confidence_score', 0.0),
            successful_scrapes=stats.get('successful_scrapes', 0),
            failed_scrapes=stats.get('failed_scrapes', 0)
        )
        
    except Exception as e:
        logger.error(f"Error getting stats: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Stats query failed: {str(e)}")


async def get_recent_results(
    limit: int = 50,
    bq_client = Depends(get_bigquery_client)
):
    """
    Get recent analysis results
    """
    try:
        # Query recent results from BigQuery
        query = f"""
        SELECT *
        FROM `{bq_client.project_id}.{bq_client.dataset_id}.{bq_client.table_id}`
        ORDER BY created_at DESC
        LIMIT {limit}
        """
        
        query_job = bq_client.client.query(query)
        df = query_job.result().to_dataframe()
        
        if df.empty:
            return []
        
        results = dataframe_to_analysis_result(df, from_cache=True)
        return results
        
    except Exception as e:
        logger.error(f"Error getting recent results: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


async def health_check(
    analyzer = Depends(get_domain_analyzer),
    bq_client = Depends(get_bigquery_client)
):
    """
    Health check endpoint
    """
    try:
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "services": {
                "domain_analyzer": "ready",
                "bigquery": "ready"
            }
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {str(e)}")


def get_api_info():
    """
    Root endpoint with API information
    """
    return {
        "name": "Domain Analysis API",
        "version": "2.0.0",
        "description": "Advanced RAG pipeline with parallel processing and persistent jobs",
        "endpoints": {
            "analyze": "POST /analyze - Analyze single email",
            "batch_analyze": "POST /analyze/batch - Analyze multiple emails",
            "upload_csv": "POST /csv/upload - Upload CSV for persistent batch processing",
            "job_status": "GET /job/{job_id}/status - Check job status",
            "domain": "GET /domain/{domain} - Get cached domain results",
            "stats": "GET /stats - Get analysis statistics",
            "health": "GET /health - Health check"
        }
    }
    
"""
CSV upload endpoint for persistent job creation
Handles file validation, email cleaning, and job initialization
"""

import asyncio
import io
import logging
import pandas as pd

from fastapi import HTTPException, Depends, UploadFile, File, Form

# get_domain_analyzer and get_bigquery_client are defined above
# utility functions are defined above
# batch processing function needed

logger = logging.getLogger(__name__)


async def upload_csv_for_processing(
    session_id: str = Form(...),
    file: UploadFile = File(...),
    analyzer = Depends(get_domain_analyzer),
    bq_client = Depends(get_bigquery_client)
):
    """
    Upload CSV file and create persistent processing job
    This replaces the old chat/upload-csv endpoint for better job management
    """
    logger.info(f"CSV upload request for session: {session_id}, file: {file.filename}")
    
    try:
        # Validate file type
        if not validate_file_upload(file.filename):
            raise HTTPException(status_code=400, detail="Please upload a CSV file.")
        
        # Read CSV file
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))
        
        # Clean emails and check BigQuery duplicates
        valid_emails, cleaning_stats = clean_email_dataframe(df, bq_client)
        
        if not valid_emails:
            raise HTTPException(
                status_code=400,
                detail=f"No new email addresses found. {cleaning_stats['bigquery_duplicates']} already exist in database."
            )
        
        # Create persistent batch job
        job_id = bq_client.create_batch_job(session_id, valid_emails, file.filename)
        
        # Start processing in background (this will survive container restarts)
        asyncio.create_task(process_batch_job(job_id, bq_client, analyzer))
        
        logger.info(f"Created batch job {job_id} for {len(valid_emails)} emails")
        
        from models import JobCreateResponse
        return JobCreateResponse(
            job_id=job_id,
            message=f"Created batch job for {len(valid_emails)} emails. Processing started in background.",
            total_emails=len(valid_emails),
            cleaning_stats=cleaning_stats,
            status_url=f"/job/{job_id}/status"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading CSV: {str(e)}")
        raise HTTPException(status_code=500, detail=f"CSV upload failed: {str(e)}")
    
    
"""
Job status and results endpoints
Handles job status polling, results retrieval, and job history
"""

import logging
from typing import Optional

from fastapi import HTTPException, Depends
from google.cloud import bigquery

from config import get_bigquery_client
# dataframe_to_analysis_result is defined below

logger = logging.getLogger(__name__)


async def get_job_status_endpoint(
    job_id: str,
    bq_client = Depends(get_bigquery_client)
):
    """
    Get real-time job status - works across container restarts
    Frontend can poll this endpoint to track progress
    """
    try:
        job_status = bq_client.get_job_status(job_id)
        
        if not job_status:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        # Calculate progress percentage
        total = job_status['total_emails']
        processed = job_status['processed_emails']
        progress_percentage = (processed / total * 100) if total > 0 else 0
        
        from models import JobStatus
        return JobStatus(
            job_id=job_status['job_id'],
            session_id=job_status['session_id'],
            filename=job_status['filename'],
            status=job_status['status'],
            progress={
                "total": total,
                "processed": processed,
                "successful": job_status['successful_emails'],
                "failed": job_status['failed_emails'],
                "duplicates": job_status['duplicate_emails'],
                "percentage": round(progress_percentage, 1)
            },
            timestamps={
                "created_at": job_status['created_at'],
                "started_at": job_status['started_at'],
                "completed_at": job_status['completed_at'],
                "last_updated": job_status['last_updated']
            },
            error_message=job_status['error_message']
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting job status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get job status: {str(e)}")


async def get_job_results(
    job_id: str,
    offset: int = 0,
    limit: int = 100,
    bq_client = Depends(get_bigquery_client)
):
    """
    Get completed results from a batch job
    """
    try:
        # Verify job exists
        job_status = bq_client.get_job_status(job_id)
        if not job_status:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        # Query results from main analysis table joined with email queue
        query = f"""
        SELECT da.*
        FROM `{bq_client.project_id}.{bq_client.dataset_id}.email_domain_results` da
        JOIN `{bq_client.project_id}.{bq_client.dataset_id}.email_queue` eq
        ON da.original_email = eq.email
        WHERE eq.job_id = @job_id
        AND eq.status = 'completed'
        ORDER BY da.created_at DESC
        LIMIT {limit} OFFSET {offset}
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", job_id)]
        )
        
        query_job = bq_client.client.query(query, job_config=job_config)
        df = query_job.result().to_dataframe()
        
        if df.empty:
            return []
        
        # Convert to analysis results
        results = dataframe_to_analysis_result(df, from_cache=False)
        return results
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting job results: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get job results: {str(e)}")


async def get_recent_jobs(
    session_id: Optional[str] = None,
    limit: int = 20,
    bq_client = Depends(get_bigquery_client)
):
    """
    Get recent jobs for a session or all jobs
    """
    try:
        query = f"""
        SELECT *
        FROM `{bq_client.project_id}.{bq_client.dataset_id}.batch_jobs`
        """
        
        params = []
        if session_id:
            query += " WHERE session_id = @session_id"
            params.append(bigquery.ScalarQueryParameter("session_id", "STRING", session_id))
        
        query += f" ORDER BY created_at DESC LIMIT {limit}"
        
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        query_job = bq_client.client.query(query, job_config=job_config)
        results = query_job.result()
        
        jobs = []
        from models import JobStatus
        for row in results:
            progress_percentage = (row.processed_emails / row.total_emails * 100) if row.total_emails > 0 else 0
            
            jobs.append(JobStatus(
                job_id=row.job_id,
                session_id=row.session_id,
                filename=row.filename,
                status=row.status,
                progress={
                    "total": row.total_emails,
                    "processed": row.processed_emails or 0,
                    "successful": row.successful_emails or 0,
                    "failed": row.failed_emails or 0,
                    "duplicates": row.duplicate_emails or 0,
                    "percentage": round(progress_percentage, 1)
                },
                timestamps={
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                    "started_at": row.started_at.isoformat() if row.started_at else None,
                    "completed_at": row.completed_at.isoformat() if row.completed_at else None,
                    "last_updated": row.last_updated.isoformat() if row.last_updated else None
                },
                error_message=row.error_message
            ))
        
        return jobs
        
    except Exception as e:
        logger.error(f"Error getting recent jobs: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get recent jobs: {str(e)}")
    
    
"""
Job control operations endpoints
Handles job restart, cancellation, and management operations
"""

import asyncio
import logging

from fastapi import HTTPException, Depends
from google.cloud import bigquery

# get_domain_analyzer and get_bigquery_client are defined above
# batch processing function needed

logger = logging.getLogger(__name__)


async def restart_failed_job(
    job_id: str,
    bq_client = Depends(get_bigquery_client),
    analyzer = Depends(get_domain_analyzer)
):
    """
    Restart a failed job
    """
    try:
        # Verify job exists and is failed
        job_status = bq_client.get_job_status(job_id)
        if not job_status:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        if job_status['status'] not in ['failed', 'completed_with_errors']:
            raise HTTPException(
                status_code=400, 
                detail=f"Job {job_id} is not in a restartable state (current status: {job_status['status']})"
            )
        
        # Reset failed emails to pending
        query = f"""
        UPDATE `{bq_client.project_id}.{bq_client.dataset_id}.email_queue`
        SET status = 'pending', started_at = NULL, completed_at = NULL, error_message = NULL
        WHERE job_id = @job_id AND status = 'failed'
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", job_id)]
        )
        
        bq_client.client.query(query, job_config=job_config).result()
        
        # Update job status
        bq_client.update_job_status(job_id, "processing")
        
        # Restart processing
        asyncio.create_task(process_batch_job(job_id, bq_client, analyzer))
        
        return {"message": f"Job {job_id} restarted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error restarting job: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to restart job: {str(e)}")


async def cancel_job(
    job_id: str,
    bq_client = Depends(get_bigquery_client)
):
    """
    Cancel a running job
    """
    try:
        # Verify job exists
        job_status = bq_client.get_job_status(job_id)
        if not job_status:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        if job_status['status'] not in ['pending', 'processing']:
            raise HTTPException(
                status_code=400,
                detail=f"Job {job_id} cannot be cancelled (current status: {job_status['status']})"
            )
        
        # Update job status to cancelled
        bq_client.update_job_status(job_id, "cancelled")
        
        # Mark all pending emails as cancelled
        query = f"""
        UPDATE `{bq_client.project_id}.{bq_client.dataset_id}.email_queue`
        SET status = 'cancelled'
        WHERE job_id = @job_id AND status IN ('pending', 'processing')
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", job_id)]
        )
        
        bq_client.client.query(query, job_config=job_config).result()
        
        return {"message": f"Job {job_id} cancelled successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling job: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to cancel job: {str(e)}")


async def pause_job(
    job_id: str,
    bq_client = Depends(get_bigquery_client)
):
    """
    Pause a running job (marks as paused, processing will naturally stop when current batch completes)
    """
    try:
        # Verify job exists and is running
        job_status = bq_client.get_job_status(job_id)
        if not job_status:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        if job_status['status'] != 'processing':
            raise HTTPException(
                status_code=400,
                detail=f"Job {job_id} is not currently processing (current status: {job_status['status']})"
            )
        
        # Update job status to paused
        bq_client.update_job_status(job_id, "paused")
        
        return {"message": f"Job {job_id} paused successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error pausing job: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to pause job: {str(e)}")


async def resume_job(
    job_id: str,
    bq_client = Depends(get_bigquery_client),
    analyzer = Depends(get_domain_analyzer)
):
    """
    Resume a paused job
    """
    try:
        # Verify job exists and is paused
        job_status = bq_client.get_job_status(job_id)
        if not job_status:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        if job_status['status'] != 'paused':
            raise HTTPException(
                status_code=400,
                detail=f"Job {job_id} is not paused (current status: {job_status['status']})"
            )
        
        # Update job status to processing
        bq_client.update_job_status(job_id, "processing")
        
        # Resume processing
        asyncio.create_task(process_batch_job(job_id, bq_client, analyzer))
        
        return {"message": f"Job {job_id} resumed successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resuming job: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to resume job: {str(e)}")


async def delete_job(
    job_id: str,
    bq_client = Depends(get_bigquery_client)
):
    """
    Delete a completed or failed job and its associated data
    """
    try:
        # Verify job exists
        job_status = bq_client.get_job_status(job_id)
        if not job_status:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        # Only allow deletion of completed, failed, or cancelled jobs
        if job_status['status'] in ['pending', 'processing', 'paused']:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot delete active job {job_id} (current status: {job_status['status']}). Cancel or wait for completion first."
            )
        
        # Delete job progress entries
        progress_query = f"""
        DELETE FROM `{bq_client.project_id}.{bq_client.dataset_id}.job_progress`
        WHERE job_id = @job_id
        """
        
        # Delete email queue entries
        queue_query = f"""
        DELETE FROM `{bq_client.project_id}.{bq_client.dataset_id}.email_queue`
        WHERE job_id = @job_id
        """
        
        # Delete batch job record
        job_query = f"""
        DELETE FROM `{bq_client.project_id}.{bq_client.dataset_id}.batch_jobs`
        WHERE job_id = @job_id
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", job_id)]
        )
        
        # Execute deletions in order
        bq_client.client.query(progress_query, job_config=job_config).result()
        bq_client.client.query(queue_query, job_config=job_config).result()
        bq_client.client.query(job_query, job_config=job_config).result()
        
        return {"message": f"Job {job_id} and associated data deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting job: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete job: {str(e)}")
    
    
"""
WebSocket endpoints for real-time chat communication
Handles WebSocket connections, message routing, and real-time updates
"""

import logging
import uuid
from datetime import datetime

from fastapi import Depends, WebSocket, WebSocketDisconnect

# get_domain_analyzer and get_bigquery_client are defined above
# utility functions are defined above
# legacy processing functions needed

logger = logging.getLogger(__name__)


async def websocket_endpoint(websocket: WebSocket, session_id: str):
    """
    WebSocket endpoint for real-time chat updates
    """
    await websocket.accept()
    
    session = get_or_create_session(session_id)
    session.websocket = websocket
    
    # Send welcome message
    welcome_msg = "Welcome to Domain Analysis Chat! You can type email addresses or upload a CSV file."
    await send_chat_message(session_id, welcome_msg)
    
    try:
        while True:
            data = await websocket.receive_json()
            
            # Handle different message types
            if data.get("type") == "ping":
                await websocket.send_json({"type": "pong"})
            elif data.get("type") == "message":
                # Store user message
                session.add_message(data.get("content", ""), "user")
                
                # Process the message (will be handled by HTTP endpoints)
                await send_chat_message(session_id, "Message received. Processing...")
                
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for session {session_id}")
        # Clear websocket reference
        session.websocket = None


async def send_chat_message_endpoint(
    request,
    analyzer = Depends(get_domain_analyzer),
    bq_client = Depends(get_bigquery_client)
):
    """
    Handle chat messages (single email processing)
    """
    try:
        session = get_or_create_session(request.session_id)
        
        # Store user message
        session.add_message(request.message, request.message_type)
        
        # Extract email from message
        email = extract_email_from_chat_message(request.message)
        
        if email:
            # Process the email
            result = await process_single_chat_email(
                email=email,
                session_id=request.session_id,
                analyzer=analyzer,
                bq_client=bq_client
            )
            
            if result:
                response_content = format_single_email_result(result)
                metadata = {"analysis_result": result.dict()}
            else:
                response_content = f"Email {email} was already processed recently or failed to process."
                metadata = None
        else:
            response_content = "Please provide a valid email address or upload a CSV file for analysis."
            metadata = None
        
        from models import ChatResponse
        return ChatResponse(
            message_id=str(uuid.uuid4()),
            session_id=request.session_id,
            message_type="system",
            content=response_content,
            timestamp=datetime.utcnow(),
            metadata=metadata
        )
            
    except Exception as e:
        logger.error(f"Error processing chat message: {str(e)}")
        error_msg = f"Sorry, there was an error processing your request: {str(e)}"
        
        from models import ChatResponse
        return ChatResponse(
            message_id=str(uuid.uuid4()),
            session_id=request.session_id,
            message_type="system",
            content=error_msg,
            timestamp=datetime.utcnow()
        )


async def broadcast_message_to_all_sessions(message: str, message_type: str = "system"):
    """
    Broadcast a message to all active WebSocket sessions
    """
    # chat_sessions is defined above
    
    disconnected_sessions = []
    
    for session_id, session in chat_sessions.items():
        if session.websocket:
            try:
                await send_chat_message(session_id, message)
            except Exception as e:
                logger.error(f"Failed to send broadcast to session {session_id}: {e}")
                disconnected_sessions.append(session_id)
    
    # Clean up disconnected sessions
    for session_id in disconnected_sessions:
        if session_id in chat_sessions:
            chat_sessions[session_id].websocket = None
    
    return {
        "message": f"Broadcasted to {len(chat_sessions) - len(disconnected_sessions)} sessions",
        "disconnected": len(disconnected_sessions)
    }


def get_active_websocket_connections():
    """
    Get count of active WebSocket connections
    """
    # chat_sessions is defined above
    
    active_connections = 0
    for session in chat_sessions.values():
        if session.websocket:
            active_connections += 1
    
    return {
        "active_websockets": active_connections,
        "total_sessions": len(chat_sessions)
    }
    
"""
CSV processing endpoints for chat interface
Handles CSV file uploads, previews, and legacy batch processing for chat
"""

import asyncio
import io
import logging
import pandas as pd

from fastapi import Depends, UploadFile, File, Form

# get_domain_analyzer and get_bigquery_client are defined above
# utility functions are defined above
# legacy processing function needed

logger = logging.getLogger(__name__)


async def preview_csv_file(
    session_id: str = Form(...),
    file: UploadFile = File(...),
    bq_client = Depends(get_bigquery_client)
):
    """
    Preview CSV file with BigQuery duplicate checking before processing
    """
    logger.info(f"CSV preview request for session: {session_id}, file: {file.filename}")
    
    try:
        # Validate file type
        if not validate_file_upload(file.filename):
            return {"error": "Please upload a CSV file."}
        
        # Read CSV file
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))
        
        # Clean emails and check BigQuery duplicates
        valid_emails, cleaning_stats = clean_email_dataframe(df, bq_client)
        
        return {
            "valid_emails": valid_emails[:10],  # Preview first 10
            "total_count": cleaning_stats['new_emails'],
            "has_more": cleaning_stats['new_emails'] > 10,
            "stats": cleaning_stats
        }
        
    except Exception as e:
        logger.error(f"Error previewing CSV: {str(e)}")
        return {"error": f"Error processing CSV file: {str(e)}"}


async def upload_csv_file_legacy(
    session_id: str = Form(...),
    file: UploadFile = File(...),
    analyzer = Depends(get_domain_analyzer),
    bq_client = Depends(get_bigquery_client)
):
    """
    Handle CSV file upload and process emails in batch (legacy chat interface)
    """
    logger.info(f"Legacy CSV upload request for session: {session_id}, file: {file.filename}")
    
    try:
        session = get_or_create_session(session_id)
        
        # Validate file type
        if not validate_file_upload(file.filename):
            error_msg = "Please upload a CSV file."
            await send_chat_message(session_id, error_msg)
            return {"error": error_msg}
        
        # Read CSV file
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))
        
        # Enhanced email cleaning using new function with BigQuery duplicate checking
        valid_emails, cleaning_stats = clean_email_dataframe(df, bq_client)
        
        if not valid_emails:
            error_msg = """No new email addresses found in the CSV file.
            
📊 Processing Summary:
• Total rows: {total_rows}
• Email column used: '{email_column}'
• Valid emails found: {valid_emails}
• Invalid emails: {invalid_emails}
• CSV duplicates removed: {duplicates_removed}
• Already in database: {bigquery_duplicates}
• New emails to process: {new_emails}
• Empty rows: {empty_rows}

All valid emails are already processed in our database. Please check with new email addresses.""".format(
                total_rows=cleaning_stats['total_rows'],
                email_column=cleaning_stats['email_column'],
                valid_emails=cleaning_stats['valid_emails'],
                invalid_emails=cleaning_stats['invalid_emails'],
                duplicates_removed=cleaning_stats['duplicates_removed'],
                bigquery_duplicates=cleaning_stats['bigquery_duplicates'],
                new_emails=cleaning_stats['new_emails'],
                empty_rows=cleaning_stats['empty_rows']
            )
            await send_chat_message(session_id, error_msg)
            return {"error": error_msg}
        
        # Send detailed processing summary
        summary_msg = format_processing_summary(cleaning_stats)
        await send_chat_message(session_id, summary_msg)
        
        # Process emails in background
        logger.info(f"Starting background processing for {len(valid_emails)} emails")
        task = asyncio.create_task(process_csv_emails_background(
            session_id, valid_emails, analyzer, bq_client
        ))
        # Add exception handler to catch silent failures
        task.add_done_callback(
            lambda t: logger.error(f"Background task error: {t.exception()}") 
            if t.exception() else logger.info("Background task completed successfully")
        )
        
        return {
            "message": f"Processing {len(valid_emails)} emails. You'll receive updates every 10 completions.",
            "total_emails": len(valid_emails)
        }
        
    except Exception as e:
        logger.error(f"Error uploading CSV: {str(e)}")
        error_msg = f"Error processing CSV file: {str(e)}"
        await send_chat_message(session_id, error_msg)
        return {"error": error_msg}


async def get_csv_processing_history(
    session_id: str,
    limit: int = 10
):
    """
    Get CSV processing history for a chat session
    """
    try:
        session = get_or_create_session(session_id)
        
        # Filter messages that contain CSV processing info
        csv_messages = []
        for message in session.messages:
            if message.message_type == "system" and any(keyword in message.content.lower() 
                for keyword in ["csv", "processing", "batch", "emails processed"]):
                csv_messages.append({
                    "message_id": message.message_id,
                    "content": message.content,
                    "timestamp": message.timestamp.isoformat(),
                    "metadata": message.metadata
                })
        
        # Return most recent CSV-related messages
        return {
            "session_id": session_id,
            "csv_messages": csv_messages[-limit:] if csv_messages else [],
            "total_csv_messages": len(csv_messages)
        }
        
    except Exception as e:
        logger.error(f"Error getting CSV history: {str(e)}")
        return {"error": f"Failed to get CSV processing history: {str(e)}"}


def validate_csv_format(df: pd.DataFrame):
    """
    Validate CSV format and provide helpful feedback
    """
    validation_results = {
        "valid": True,
        "warnings": [],
        "errors": [],
        "suggestions": []
    }
    
    try:
        # Check if DataFrame is empty
        if df.empty:
            validation_results["valid"] = False
            validation_results["errors"].append("CSV file is empty")
            return validation_results
        
        # Check for potential email columns
        email_columns = []
        for col in df.columns:
            if any(keyword in col.lower() for keyword in ['email', 'e-mail', 'mail', 'address', 'contact']):
                email_columns.append(col)
        
        if not email_columns:
            validation_results["warnings"].append("No obvious email column found. Will use first column.")
            validation_results["suggestions"].append("Consider naming your email column 'email' for better detection.")
        
        # Check for common formatting issues
        first_col = df.columns[0]
        sample_values = df[first_col].dropna().head(5).astype(str)
        
        email_like_count = 0
        for value in sample_values:
            if '@' in value and '.' in value:
                email_like_count += 1
        
        if email_like_count == 0:
            validation_results["warnings"].append(f"First column '{first_col}' doesn't contain email-like values in sample data")
        
        # Check for reasonable row count
        if len(df) > 1000:
            validation_results["warnings"].append(f"Large file with {len(df)} rows. Processing may take significant time.")
        
        return validation_results
        
    except Exception as e:
        validation_results["valid"] = False
        validation_results["errors"].append(f"Error validating CSV: {str(e)}")
        return validation_results
        
"""
Session management endpoints for chat interface
Handles session information, cleanup, and management operations
"""

import logging
from datetime import datetime, timedelta

from fastapi import HTTPException

# utility functions are defined above

logger = logging.getLogger(__name__)


def get_chat_session_info(session_id: str):
    """
    Get information about a chat session
    """
    try:
        session = get_or_create_session(session_id)
        
        return {
            "session_id": session.session_id,
            "created_at": session.created_at.isoformat(),
            "last_activity": session.last_activity.isoformat(),
            "message_count": len(session.messages),
            "websocket_connected": session.websocket is not None
        }
        
    except Exception as e:
        logger.error(f"Error getting session info: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get session info: {str(e)}")


def clear_chat_session(session_id: str):
    """
    Clear chat session messages
    """
    try:
        session = get_or_create_session(session_id)
        message_count = len(session.messages)
        session.messages.clear()
        session.last_activity = datetime.utcnow()
        
        return {
            "message": f"Cleared {message_count} messages from session {session_id}",
            "session_id": session_id
        }
        
    except Exception as e:
        logger.error(f"Error clearing session: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to clear session: {str(e)}")


def get_all_chat_sessions():
    """
    Get information about all active chat sessions
    """
    try:
        # chat_sessions is defined above
        
        sessions_info = []
        for session_id, session in chat_sessions.items():
            sessions_info.append({
                "session_id": session.session_id,
                "created_at": session.created_at.isoformat(),
                "last_activity": session.last_activity.isoformat(),
                "message_count": len(session.messages),
                "websocket_connected": session.websocket is not None
            })
        
        return {
            "active_sessions": len(sessions_info),
            "sessions": sessions_info
        }
        
    except Exception as e:
        logger.error(f"Error getting all sessions: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get sessions: {str(e)}")


def cleanup_inactive_sessions(max_age_hours: int = 24):
    """
    Clean up inactive chat sessions older than specified hours
    """
    try:
        # chat_sessions is defined above
        
        cutoff_time = datetime.utcnow() - timedelta(hours=max_age_hours)
        sessions_to_remove = []
        
        for session_id, session in chat_sessions.items():
            if session.last_activity < cutoff_time and session.websocket is None:
                sessions_to_remove.append(session_id)
        
        # Remove inactive sessions
        for session_id in sessions_to_remove:
            del chat_sessions[session_id]
        
        return {
            "message": f"Cleaned up {len(sessions_to_remove)} inactive sessions",
            "removed_sessions": sessions_to_remove,
            "remaining_sessions": len(chat_sessions)
        }
        
    except Exception as e:
        logger.error(f"Error cleaning up sessions: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to cleanup sessions: {str(e)}")


def get_session_messages(session_id: str, limit: int = 50, message_type: str = None):
    """
    Get messages from a chat session with optional filtering
    """
    try:
        session = get_or_create_session(session_id)
        
        messages = session.messages
        
        # Filter by message type if specified
        if message_type:
            messages = [msg for msg in messages if msg.message_type == message_type]
        
        # Get recent messages
        recent_messages = messages[-limit:] if messages else []
        
        # Convert to dict format
        message_list = []
        for msg in recent_messages:
            message_list.append({
                "message_id": msg.message_id,
                "message_type": msg.message_type,
                "content": msg.content,
                "timestamp": msg.timestamp.isoformat(),
                "metadata": msg.metadata
            })
        
        return {
            "session_id": session_id,
            "total_messages": len(session.messages),
            "filtered_messages": len(messages),
            "returned_messages": len(message_list),
            "messages": message_list
        }
        
    except Exception as e:
        logger.error(f"Error getting session messages: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get session messages: {str(e)}")


def update_session_activity(session_id: str):
    """
    Update last activity timestamp for a session
    """
    try:
        session = get_or_create_session(session_id)
        session.last_activity = datetime.utcnow()
        
        return {
            "session_id": session_id,
            "last_activity": session.last_activity.isoformat(),
            "message": "Session activity updated"
        }
        
    except Exception as e:
        logger.error(f"Error updating session activity: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to update session activity: {str(e)}")


def get_session_statistics():
    """
    Get overall session statistics
    """
    try:
        # chat_sessions is defined above
        
        if not chat_sessions:
            return {
                "total_sessions": 0,
                "active_websockets": 0,
                "total_messages": 0,
                "avg_messages_per_session": 0,
                "oldest_session": None,
                "newest_session": None
            }
        
        total_messages = 0
        active_websockets = 0
        oldest_session = None
        newest_session = None
        
        for session in chat_sessions.values():
            total_messages += len(session.messages)
            
            if session.websocket:
                active_websockets += 1
            
            if oldest_session is None or session.created_at < oldest_session:
                oldest_session = session.created_at
            
            if newest_session is None or session.created_at > newest_session:
                newest_session = session.created_at
        
        avg_messages = total_messages / len(chat_sessions) if chat_sessions else 0
        
        return {
            "total_sessions": len(chat_sessions),
            "active_websockets": active_websockets,
            "total_messages": total_messages,
            "avg_messages_per_session": round(avg_messages, 2),
            "oldest_session": oldest_session.isoformat() if oldest_session else None,
            "newest_session": newest_session.isoformat() if newest_session else None
        }
        
    except Exception as e:
        logger.error(f"Error getting session statistics: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get session statistics: {str(e)}")


def delete_session(session_id: str):
    """
    Completely delete a chat session
    """
    try:
        # chat_sessions is defined above
        
        if session_id not in chat_sessions:
            raise HTTPException(status_code=404, detail=f"Session {session_id} not found")
        
        session = chat_sessions[session_id]
        message_count = len(session.messages)
        
        # Close WebSocket connection if active
        if session.websocket:
            try:
                session.websocket.close()
            except:
                pass  # Ignore close errors
        
        # Delete session
        del chat_sessions[session_id]
        
        return {
            "message": f"Deleted session {session_id} with {message_count} messages",
            "session_id": session_id,
            "deleted_messages": message_count
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting session: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete session: {str(e)}")
    
"""
FastAPI Application Assembly - Complete Modular Version
Main application file that brings together all 13 modules and defines routes
"""

import os
import logging
from fastapi import FastAPI, WebSocket, Form, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

# Import configuration and lifespan
from config import lifespan

# Import all endpoint handlers from modular files
from core_endpoints import (
    analyze_single_email, batch_analyze_emails, get_domain_results,
    get_analysis_stats, get_recent_results, health_check, get_api_info
)
from csv_upload_endpoint import upload_csv_for_processing
from job_status_endpoints import get_job_status_endpoint, get_job_results, get_recent_jobs
from job_control_endpoints import restart_failed_job, cancel_job
from websocket_endpoints import websocket_endpoint, send_chat_message_endpoint
from csv_chat_endpoints import preview_csv_file, upload_csv_file_legacy
from session_management_endpoints import get_chat_session_info, clear_chat_session

# Import models for response typing
from models import (
    AnalyzeEmailRequest, BatchAnalyzeRequest, AnalysisResult, BatchAnalysisResult,
    JobCreateResponse, JobStatus, ChatRequest, ChatResponse
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app with lifespan management
app = FastAPI(
    title="Domain Analysis API",
    description="Advanced RAG pipeline with parallel processing and persistent jobs",
    version="2.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://domain-analysis-frontend-456664817971.europe-west1.run.app",
        "http://localhost:3000",  # For local development
        "http://localhost:8080",  # For local testing
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Serve static files (React build)
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Serve favicon
@app.get("/favicon.ico")
async def favicon():
    """Serve favicon to prevent 404 errors"""
    return JSONResponse(status_code=204)


# CORE API ROUTES
@app.get("/")
async def root():
    """Root endpoint with API information"""
    return get_api_info()

@app.get("/health")
async def health():
    """Health check endpoint"""
    return await health_check()

@app.post("/analyze", response_model=AnalysisResult)
async def analyze(request: AnalyzeEmailRequest):
    """Analyze a single email address"""
    return await analyze_single_email(request)

@app.post("/analyze/batch", response_model=BatchAnalysisResult)
async def batch_analyze(request: BatchAnalyzeRequest):
    """Analyze multiple email addresses in batch"""
    return await batch_analyze_emails(request)

@app.get("/domain/{domain}")
async def domain_results(domain: str, limit: int = 10):
    """Get cached domain results"""
    return await get_domain_results(domain, limit)

@app.get("/stats")
async def stats():
    """Get analysis statistics"""
    return await get_analysis_stats()

@app.get("/recent")
async def recent_results(limit: int = 50):
    """Get recent analysis results"""
    return await get_recent_results(limit)


# JOB MANAGEMENT ROUTES
@app.post("/csv/upload", response_model=JobCreateResponse)
async def upload_csv(session_id: str = Form(...), file: UploadFile = File(...)):
    """Upload CSV for persistent batch processing"""
    return await upload_csv_for_processing(session_id, file)

@app.get("/job/{job_id}/status", response_model=JobStatus)
async def job_status(job_id: str):
    """Get job status"""
    return await get_job_status_endpoint(job_id)

@app.get("/job/{job_id}/results")
async def job_results(job_id: str, offset: int = 0, limit: int = 100):
    """Get job results"""
    return await get_job_results(job_id, offset, limit)

@app.get("/jobs/recent")
async def recent_jobs(session_id: str = None, limit: int = 20):
    """Get recent jobs"""
    return await get_recent_jobs(session_id, limit)

@app.post("/job/{job_id}/restart")
async def restart_job(job_id: str):
    """Restart a failed job"""
    return await restart_failed_job(job_id)

@app.post("/job/{job_id}/cancel")
async def cancel_job_endpoint(job_id: str):
    """Cancel a running job"""
    return await cancel_job(job_id)


# CHAT INTERFACE ROUTES (Legacy Support)
@app.websocket("/ws/{session_id}")
async def websocket(websocket: WebSocket, session_id: str):
    """WebSocket for real-time chat updates"""
    await websocket_endpoint(websocket, session_id)

@app.post("/chat/message", response_model=ChatResponse)
async def chat_message(request: ChatRequest):
    """Handle chat messages"""
    return await send_chat_message_endpoint(request)

@app.post("/chat/preview-csv")
async def preview_csv(session_id: str = Form(...), file: UploadFile = File(...)):
    """Preview CSV file"""
    return await preview_csv_file(session_id, file)

@app.post("/chat/upload-csv")
async def upload_csv_legacy(session_id: str = Form(...), file: UploadFile = File(...)):
    """Legacy CSV upload for chat interface"""
    return await upload_csv_file_legacy(session_id, file)

@app.get("/chat/session/{session_id}")
async def chat_session_info(session_id: str):
    """Get chat session information"""
    return get_chat_session_info(session_id)

@app.delete("/chat/session/{session_id}")
async def clear_session(session_id: str):
    """Clear chat session"""
    return clear_chat_session(session_id)


# ADDITIONAL UTILITY ROUTES
@app.get("/system/sessions")
async def system_sessions():
    """Get all chat sessions (admin endpoint)"""
    from session_management_endpoints import get_all_chat_sessions
    return get_all_chat_sessions()

@app.post("/system/cleanup")
async def system_cleanup(max_age_hours: int = 24):
    """Clean up inactive sessions (admin endpoint)"""
    from session_management_endpoints import cleanup_inactive_sessions
    return cleanup_inactive_sessions(max_age_hours)

@app.get("/system/stats")
async def system_stats():
    """Get system statistics (admin endpoint)"""
    from session_management_endpoints import get_session_statistics
    return get_session_statistics()


# Serve React app for all non-API routes
@app.get("/{path:path}")
async def serve_react_app(path: str = ""):
    """Serve React app for all non-API routes"""
    static_dir = os.path.join(os.path.dirname(__file__), "static")
    
    if os.path.exists(static_dir):
        index_file = os.path.join(static_dir, "index.html")
        if os.path.exists(index_file):
            return FileResponse(index_file)
    
    # Fallback if no static files
    return {"message": "Domain Analysis API - React frontend not available"}


# Error handlers
@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "error": str(exc)}
    )


if __name__ == "__main__":
    # For local development
    port = int(os.getenv("PORT", 8080))
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=True,
        log_level="info"
    )