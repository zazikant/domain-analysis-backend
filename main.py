"""
FastAPI Application for Domain Analysis
Cloud Run deployment with BigQuery integration
"""

import os
import logging
import asyncio
import uuid
import io
import re
from typing import List, Optional, Dict, Any
from datetime import datetime
from contextlib import asynccontextmanager

import pandas as pd
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, WebSocket, WebSocketDisconnect, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, EmailStr
import uvicorn

from domain_analyzer import DomainAnalyzer, BatchProcessingRequest, BatchStatus, QueueItem
from bigquery_client import BigQueryClient, create_bigquery_client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables for clients
domain_analyzer: Optional[DomainAnalyzer] = None
bigquery_client: Optional[BigQueryClient] = None


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
        "total_rows": int(len(df)),
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
    stats["empty_rows"] = int(emails_series.isna().sum())
    
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
    
    # Debug: Log invalid emails for troubleshooting
    invalid_emails = emails_series[~valid_mask].tolist()
    if invalid_emails:
        logger.info(f"Invalid emails found: {invalid_emails}")
    
    logger.info(f"Email validation: {len(emails_series)} total → {len(valid_emails)} valid, {len(invalid_emails)} invalid")
    
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
    
    # Check BigQuery for existing domains if client provided
    final_emails = unique_emails
    bigquery_duplicates = 0
    
    if bq_client and unique_emails:
        try:
            # Check which specific EMAIL ADDRESSES exist in BigQuery (exact match, not domain similarity)
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
    
    # Update stats - Convert to Python native types for JSON serialization
    stats["valid_emails"] = int(len(unique_emails))
    stats["invalid_emails"] = int(len(emails_series) - len(valid_emails))
    stats["duplicates_removed"] = int(duplicates)
    stats["bigquery_duplicates"] = int(bigquery_duplicates)
    stats["new_emails"] = int(len(final_emails))
    
    return final_emails, stats


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global domain_analyzer, bigquery_client
    
    # Startup - Initialize clients
    logger.info("Starting up Domain Analysis API...")
    
    # Get environment variables
    serper_api_key = os.getenv("SERPER_API_KEY")
    brightdata_api_token = os.getenv("BRIGHTDATA_API_TOKEN")
    google_api_key = os.getenv("GOOGLE_API_KEY")
    project_id = os.getenv("GCP_PROJECT_ID")
    
    if not all([serper_api_key, brightdata_api_token, google_api_key, project_id]):
        logger.warning("Missing required environment variables:")
        logger.warning(f"SERPER_API_KEY: {'✓' if serper_api_key else '✗'}")
        logger.warning(f"BRIGHTDATA_API_TOKEN: {'✓' if brightdata_api_token else '✗'}")
        logger.warning(f"GOOGLE_API_KEY: {'✓' if google_api_key else '✗'}")
        logger.warning(f"GCP_PROJECT_ID: {'✓' if project_id else '✗'}")
        
        # Don't fail startup - allow container to start in degraded mode
        logger.warning("Starting in degraded mode - API endpoints will return errors until env vars are set")
        return  # Skip client initialization but don't fail
    
    # Initialize Domain Analyzer
    domain_analyzer = DomainAnalyzer(
        serper_api_key=serper_api_key,
        brightdata_api_token=brightdata_api_token,
        google_api_key=google_api_key
    )
    
    # Initialize BigQuery Client with fast startup
    dataset_id = os.getenv("BIGQUERY_DATASET_ID", "advanced_csv_analysis") 
    table_id = os.getenv("BIGQUERY_TABLE_ID", "email_domain_results")
    try:
        logger.info("Initializing BigQuery client...")
        bigquery_client = create_bigquery_client(project_id, dataset_id, table_id)
        logger.info("✅ BigQuery client initialized successfully")
    except Exception as e:
        logger.warning(f"⚠️  BigQuery initialization delayed: {str(e)}")
        # Set to None for lazy initialization
        bigquery_client = None
    
    logger.info("🚀 Domain Analysis API started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Domain Analysis API...")


# Initialize FastAPI app
app = FastAPI(
    title="Domain Analysis API",
    description="Advanced RAG pipeline for email domain intelligence gathering",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Temporary wildcard for debugging
    allow_credentials=False,  # Must be False with wildcard origins
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# Serve static files (React build)
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Massive Batch Processing Endpoints

@app.post("/batch/upload-massive-csv", response_model=BatchStatus)
async def upload_massive_csv(
    file: UploadFile = File(...),
    session_id: str = Form(...),
    priority: int = Form(5),
    bq_client: BigQueryClient = Depends(get_bigquery_client)
):
    """
    Upload massive CSV files (unlimited size) with immediate queue processing
    """
    logger.info(f"Massive CSV upload request received for session: {session_id}, file: {file.filename}")
    try:
        # Generate unique batch ID
        batch_id = f"batch_{uuid.uuid4().hex[:12]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Validate file type
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="Please upload a CSV file.")
        
        # Read CSV file (no size limits)
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))
        
        # Enhanced email cleaning
        valid_emails, cleaning_stats = clean_email_dataframe(df, bq_client)
        
        if not valid_emails:
            raise HTTPException(
                status_code=400, 
                detail=f"No valid email addresses found. Processed {cleaning_stats['total_rows']} rows from column '{cleaning_stats['email_column']}'"
            )
        
        # Create batch record in BigQuery
        success = bq_client.create_batch_record(
            batch_id=batch_id,
            total_emails=len(valid_emails),
            user_session_id=session_id,
            filename=file.filename
        )
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to create batch record")
        
        # Add all emails to processing queue
        queue_success = bq_client.add_emails_to_queue(
            emails=valid_emails,
            batch_id=batch_id,
            priority=priority
        )
        
        if not queue_success:
            raise HTTPException(status_code=500, detail="Failed to add emails to processing queue")
        
        # Update batch status to processing
        bq_client.update_batch_progress(batch_id=batch_id, status="processing")
        
        # Start distributed background processing
        asyncio.create_task(process_massive_batch_background(batch_id, bq_client))
        
        logger.info(f"Successfully created massive batch {batch_id} with {len(valid_emails)} emails")
        
        # Return immediate batch status
        batch_status = bq_client.get_batch_status(batch_id)
        if batch_status:
            return BatchStatus(**batch_status)
        else:
            raise HTTPException(status_code=500, detail="Failed to retrieve batch status")
            
    except Exception as e:
        logger.error(f"Error in massive CSV upload: {str(e)}")
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/batch/status/{batch_id}", response_model=BatchStatus)
async def get_batch_status_endpoint(
    batch_id: str,
    bq_client: BigQueryClient = Depends(get_bigquery_client)
):
    """
    Get real-time batch processing status
    """
    try:
        batch_status = bq_client.get_batch_status(batch_id)
        
        if not batch_status:
            raise HTTPException(status_code=404, detail=f"Batch {batch_id} not found")
        
        return BatchStatus(**batch_status)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting batch status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/batch/results/{batch_id}", response_model=List[AnalysisResult])
async def get_batch_results(
    batch_id: str,
    offset: int = 0,
    limit: int = 100,
    bq_client: BigQueryClient = Depends(get_bigquery_client)
):
    """
    Get completed results from a batch
    """
    try:
        # Get batch info first to verify it exists
        batch_status = bq_client.get_batch_status(batch_id)
        if not batch_status:
            raise HTTPException(status_code=404, detail=f"Batch {batch_id} not found")
        
        # Query results from main analysis table
        query = f"""
        SELECT da.*
        FROM `{bq_client.project_id}.{bq_client.dataset_id}.domain_analysis` da
        JOIN `{bq_client.project_id}.{bq_client.dataset_id}.email_processing_queue` epq
        ON da.original_email = epq.email
        WHERE epq.batch_id = @batch_id
        AND epq.status = 'completed'
        ORDER BY da.created_at DESC
        LIMIT {limit} OFFSET {offset}
        """
        
        from google.cloud import bigquery
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id)]
        )
        
        query_job = bq_client.client.query(query, job_config=job_config)
        df = query_job.result().to_dataframe()
        
        if df.empty:
            return []
        
        # Convert to analysis results
        results = []
        for _, row in df.iterrows():
            result = AnalysisResult(
                original_email=row.get('original_email', ''),
                extracted_domain=row.get('extracted_domain', ''),
                selected_url=row.get('selected_url', ''),
                scraping_status=row.get('scraping_status', ''),
                website_summary=row.get('website_summary', ''),
                confidence_score=row.get('confidence_score', 0.0),
                selection_reasoning=row.get('selection_reasoning', ''),
                completed_timestamp=row.get('completed_timestamp', '').isoformat() if row.get('completed_timestamp') else None,
                processing_time_seconds=row.get('processing_time_seconds', 0.0),
                created_at=row.get('created_at', '').isoformat() if row.get('created_at') else None,
                from_cache=False,
                real_estate=row.get('real_estate', 'Can\'t Say'),
                infrastructure=row.get('infrastructure', 'Can\'t Say'),
                industrial=row.get('industrial', 'Can\'t Say'),
            )
            results.append(result)
        
        return results
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting batch results: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/batch/retry/{batch_id}")
async def retry_failed_batch_items(
    batch_id: str,
    bq_client: BigQueryClient = Depends(get_bigquery_client)
):
    """
    Retry failed items in a batch
    """
    try:
        # Reset failed items to pending
        from google.cloud import bigquery
        from datetime import datetime
        
        query = f"""
        UPDATE `{bq_client.project_id}.{bq_client.dataset_id}.email_processing_queue`
        SET status = 'pending',
            retry_count = retry_count + 1,
            error_message = NULL,
            started_at = NULL,
            completed_at = NULL
        WHERE batch_id = @batch_id 
        AND status = 'failed'
        AND retry_count < 3
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id)]
        )
        
        query_job = bq_client.client.query(query, job_config=job_config)
        query_job.result()
        
        # Update batch status back to processing
        bq_client.update_batch_progress(batch_id=batch_id, status="processing")
        
        # Restart background processing
        asyncio.create_task(process_massive_batch_background(batch_id, bq_client))
        
        return {"message": f"Retrying failed items for batch {batch_id}"}
        
    except Exception as e:
        logger.error(f"Error retrying batch: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# Serve favicon
@app.get("/favicon.ico")
async def favicon():
    """Serve favicon to prevent 404 errors"""
    return JSONResponse(content="", status_code=204)


# Request/Response Models
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


class AnalysisResult(BaseModel):
    original_email: str
    extracted_domain: str
    selected_url: Optional[str]
    scraping_status: str
    company_summary: Optional[str]
    confidence_score: Optional[float]
    selection_reasoning: Optional[str]
    completed_timestamp: Optional[str]
    processing_time_seconds: Optional[float]
    created_at: str
    from_cache: bool = Field(default=False, description="Whether result was retrieved from cache")
    # Sector classification fields
    real_estate: Optional[str] = Field(default="Can't Say", description="Real Estate sector classification")
    infrastructure: Optional[str] = Field(default="Can't Say", description="Infrastructure sector classification")
    industrial: Optional[str] = Field(default="Can't Say", description="Industrial sector classification")
    # New company information fields
    company_type: Optional[str] = Field(default="Can't Say", description="Company type: Developer, Contractor, or Consultant")
    company_name: Optional[str] = Field(default="Can't Say", description="Company name extracted from content")
    base_location: Optional[str] = Field(default="Can't Say", description="Head office or base location of company")


class BatchAnalysisResult(BaseModel):
    results: List[AnalysisResult]
    total_processed: int
    successful: int
    failed: int
    from_cache: int
    processing_time_seconds: float


class JobStatus(BaseModel):
    job_id: str
    status: str  # "pending", "processing", "completed", "failed"
    progress: Optional[int] = None
    total: Optional[int] = None
    message: Optional[str] = None
    result: Optional[AnalysisResult] = None


class StatsResponse(BaseModel):
    total_analyses: int
    unique_domains: int
    avg_processing_time: float
    avg_confidence_score: float
    successful_scrapes: int
    failed_scrapes: int


# Chat-specific models
class ChatMessage(BaseModel):
    message_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    session_id: str
    message_type: str = Field(description="'user' or 'system'")
    content: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metadata: Optional[Dict[str, Any]] = None


class ChatRequest(BaseModel):
    session_id: str
    message: str
    message_type: str = "user"


class ChatResponse(BaseModel):
    message_id: str
    session_id: str
    message_type: str = "system"
    content: str
    timestamp: datetime
    metadata: Optional[Dict[str, Any]] = None


class ProcessingStatus(BaseModel):
    session_id: str
    status: str  # "processing", "completed", "error"
    progress: int = Field(description="Number of emails processed")
    total: int = Field(description="Total emails to process")
    current_email: Optional[str] = None
    message: str
    results: Optional[List[AnalysisResult]] = None


# Session management
class ChatSession:
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.messages: List[ChatMessage] = []
        self.processing_status: Optional[ProcessingStatus] = None
        self.created_at = datetime.utcnow()
        self.last_activity = datetime.utcnow()
        self.websocket: Optional[WebSocket] = None
    
    def add_message(self, content: str, message_type: str, metadata: Optional[Dict] = None):
        message = ChatMessage(
            session_id=self.session_id,
            message_type=message_type,
            content=content,
            metadata=metadata
        )
        self.messages.append(message)
        self.last_activity = datetime.utcnow()
        return message
    
    def update_status(self, status: str, progress: int, total: int, message: str, current_email: Optional[str] = None):
        self.processing_status = ProcessingStatus(
            session_id=self.session_id,
            status=status,
            progress=progress,
            total=total,
            current_email=current_email,
            message=message
        )
        self.last_activity = datetime.utcnow()


# Global session storage (in production, use Redis or similar)
chat_sessions: Dict[str, ChatSession] = {}


# Dependency to get clients
def get_domain_analyzer() -> DomainAnalyzer:
    if domain_analyzer is None:
        raise HTTPException(
            status_code=503, 
            detail="Domain analyzer not initialized - check environment variables (SERPER_API_KEY, BRIGHTDATA_API_TOKEN, GOOGLE_API_KEY)"
        )
    return domain_analyzer


def get_bigquery_client() -> BigQueryClient:
    if bigquery_client is None:
        raise HTTPException(
            status_code=503, 
            detail="BigQuery client not initialized - check environment variables (GCP_PROJECT_ID)"
        )
    return bigquery_client


# Chat helper functions
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


# Utility functions
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
            # New company information fields
            company_type=row.get('company_type', "Can't Say"),
            company_name=row.get('company_name', "Can't Say"),
            base_location=row.get('base_location', "Can't Say")
        )
        results.append(result)
    
    return results


async def process_email_analysis(
    email: str, 
    analyzer: DomainAnalyzer, 
    bq_client: BigQueryClient,
    force_refresh: bool = False
) -> AnalysisResult:
    """Process single email analysis"""
    
    # Extract domain
    domain_output = analyzer.extract_domain_from_email(email)
    domain = domain_output.domain
    
    # Check if we have any previous results (unless force refresh)
    if not force_refresh and bq_client.check_domain_exists(domain, max_age_hours=525600):  # 10 years
        logger.info(f"Using cached result for domain: {domain}")
        
        cached_df = bq_client.query_domain_results(domain, limit=1)
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


# API Endpoints

@app.get("/", response_class=JSONResponse)
async def root():
    """Root endpoint with API information"""
    return {
        "name": "Domain Analysis API",
        "version": "1.0.0",
        "description": "Advanced RAG pipeline for email domain intelligence gathering",
        "endpoints": {
            "analyze": "POST /analyze - Analyze single email",
            "batch_analyze": "POST /analyze/batch - Analyze multiple emails",
            "domain": "GET /domain/{domain} - Get cached domain results",
            "stats": "GET /stats - Get analysis statistics",
            "health": "GET /health - Health check"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test connections
        analyzer = get_domain_analyzer()
        bq_client = get_bigquery_client()
        
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


@app.post("/analyze", response_model=AnalysisResult)
async def analyze_email(
    request: AnalyzeEmailRequest,
    analyzer: DomainAnalyzer = Depends(get_domain_analyzer),
    bq_client: BigQueryClient = Depends(get_bigquery_client)
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


@app.post("/analyze/batch", response_model=BatchAnalysisResult)
async def batch_analyze_emails(
    request: BatchAnalyzeRequest,
    analyzer: DomainAnalyzer = Depends(get_domain_analyzer),
    bq_client: BigQueryClient = Depends(get_bigquery_client)
):
    """
    Analyze multiple email addresses in batch
    """
    try:
        start_time = datetime.utcnow()
        results = []
        successful = 0
        failed = 0
        from_cache = 0
        
        # Process each email
        for email in request.emails:
            try:
                result = await process_email_analysis(
                    email=email,
                    analyzer=analyzer,
                    bq_client=bq_client,
                    force_refresh=request.force_refresh
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
                error_result = AnalysisResult(
                    original_email=email,
                    extracted_domain="error",
                    selected_url=None,
                    scraping_status="error",
                    company_summary=f"Error: {str(e)}",
                    confidence_score=0.0,
                    selection_reasoning="Processing failed",
                    completed_timestamp=datetime.utcnow().isoformat(),
                    processing_time_seconds=0.0,
                    created_at=datetime.utcnow().isoformat(),
                    from_cache=False,
                    # Sector classification fields (error state)
                    real_estate="Can't Say",
                    infrastructure="Can't Say",
                    industrial="Can't Say",
                    # New company information fields (error state)
                    company_type="Can't Say",
                    company_name="Can't Say",
                    base_location="Can't Say"
                )
                results.append(error_result)
        
        processing_time = (datetime.utcnow() - start_time).total_seconds()
        
        return BatchAnalysisResult(
            results=results,
            total_processed=len(request.emails),
            successful=successful,
            failed=failed,
            from_cache=from_cache,
            processing_time_seconds=processing_time
        )
        
    except Exception as e:
        logger.error(f"Error in batch analysis: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Batch analysis failed: {str(e)}")


@app.get("/domain/{domain}", response_model=List[AnalysisResult])
async def get_domain_results(
    domain: str,
    limit: int = 10,
    bq_client: BigQueryClient = Depends(get_bigquery_client)
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


@app.get("/stats", response_model=StatsResponse)
async def get_analysis_stats(
    bq_client: BigQueryClient = Depends(get_bigquery_client)
):
    """
    Get analysis statistics
    """
    try:
        stats = bq_client.get_analysis_stats()
        
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


@app.get("/recent", response_model=List[AnalysisResult])
async def get_recent_results(
    limit: int = 50,
    bq_client: BigQueryClient = Depends(get_bigquery_client)
):
    """
    Get recent analysis results
    """
    try:
        df = bq_client.get_recent_results(limit=limit)
        
        if df.empty:
            return []
        
        results = dataframe_to_analysis_result(df, from_cache=True)
        return results
        
    except Exception as e:
        logger.error(f"Error getting recent results: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


# Chat API Endpoints

@app.websocket("/ws/{session_id}")
async def websocket_endpoint(websocket: WebSocket, session_id: str):
    """WebSocket endpoint for real-time chat updates"""
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
        if session_id in chat_sessions:
            chat_sessions[session_id].websocket = None


@app.post("/chat/message", response_model=ChatResponse)
async def send_chat_message_endpoint(
    request: ChatRequest,
    analyzer: DomainAnalyzer = Depends(get_domain_analyzer),
    bq_client: BigQueryClient = Depends(get_bigquery_client)
):
    """Handle chat messages (single email processing)"""
    try:
        session = get_or_create_session(request.session_id)
        
        # Store user message
        session.add_message(request.message, request.message_type)
        
        # Check if message contains an email
        email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', request.message)
        
        if email_match:
            email = email_match.group().lower().strip()
            
            # Check for duplicates
            domain_output = analyzer.extract_domain_from_email(email)
            domain = domain_output.domain
            
            if bq_client.check_domain_exists(domain, max_age_hours=24):
                response_content = f"Email {email} was already processed recently. Please share another email address."
                await send_chat_message(request.session_id, response_content)
                
                return ChatResponse(
                    message_id=str(uuid.uuid4()),
                    session_id=request.session_id,
                    message_type="system",
                    content=response_content,
                    timestamp=datetime.utcnow()
                )
            
            # Process email
            await send_chat_message(request.session_id, f"Processing email: {email}...")
            
            # Run analysis
            result_df = await asyncio.get_event_loop().run_in_executor(
                None, analyzer.analyze_email_domain, email
            )
            
            # Save to BigQuery
            bq_client.insert_dataframe(result_df)
            
            # Convert to result object
            results = dataframe_to_analysis_result(result_df)
            result = results[0]
            
            # Create response message
            response_content = f"""Analysis complete for {email}!

**Domain**: {result.extracted_domain}
**Summary**: {result.company_summary}
**Confidence**: {result.confidence_score:.2f if result.confidence_score else 0}

**Company Information**:
- Company Name: {result.company_name}
- Company Type: {result.company_type}
- Base Location: {result.base_location}

**Sector Classifications**:
- Real Estate: {result.real_estate}
- Infrastructure: {result.infrastructure}  
- Industrial: {result.industrial}

Feel free to submit another email or upload a CSV file!"""

            await send_chat_message(request.session_id, response_content, {"analysis_result": result.dict()})
            
            return ChatResponse(
                message_id=str(uuid.uuid4()),
                session_id=request.session_id,
                message_type="system",
                content=response_content,
                timestamp=datetime.utcnow(),
                metadata={"analysis_result": result.dict()}
            )
        
        else:
            response_content = "Please provide a valid email address or upload a CSV file for analysis."
            await send_chat_message(request.session_id, response_content)
            
            return ChatResponse(
                message_id=str(uuid.uuid4()),
                session_id=request.session_id,
                message_type="system",
                content=response_content,
                timestamp=datetime.utcnow()
            )
            
    except Exception as e:
        logger.error(f"Error processing chat message: {str(e)}")
        error_msg = f"Sorry, there was an error processing your request: {str(e)}"
        await send_chat_message(request.session_id, error_msg)
        
        return ChatResponse(
            message_id=str(uuid.uuid4()),
            session_id=request.session_id,
            message_type="system",
            content=error_msg,
            timestamp=datetime.utcnow()
        )


@app.options("/chat/preview-csv")
async def preview_csv_options():
    """Handle CORS preflight for CSV preview endpoint"""
    return JSONResponse(
        content={"message": "OPTIONS OK"}, 
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "*",
        }
    )

@app.post("/chat/preview-csv")
async def preview_csv_file(
    file: UploadFile = File(...),
    session_id: str = Form(...),
    bq_client: BigQueryClient = Depends(get_bigquery_client)
):
    """Preview CSV file with BigQuery duplicate checking before processing"""
    logger.info(f"CSV preview request received for session: {session_id}, file: {file.filename}")
    try:
        # Validate file type
        if not file.filename.endswith('.csv'):
            return {"error": "Please upload a CSV file."}
        
        # Read CSV file
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))
        
        # Clean emails and check BigQuery duplicates
        valid_emails, cleaning_stats = clean_email_dataframe(df, bq_client)
        
        response_data = {
            "valid_emails": valid_emails[:10],  # Preview first 10
            "total_count": cleaning_stats['new_emails'],
            "has_more": cleaning_stats['new_emails'] > 10,
            "stats": cleaning_stats
        }
        
        return JSONResponse(
            content=response_data,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "POST, OPTIONS",
                "Access-Control-Allow-Headers": "*",
            }
        )
        
    except Exception as e:
        logger.error(f"Error previewing CSV: {str(e)}")
        return JSONResponse(
            content={"error": f"Error processing CSV file: {str(e)}"},
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "POST, OPTIONS", 
                "Access-Control-Allow-Headers": "*",
            }
        )


@app.options("/chat/upload-csv")
async def upload_csv_options():
    """Handle CORS preflight for CSV upload endpoint"""
    return JSONResponse(
        content={"message": "OK"}, 
        headers={
            "Access-Control-Allow-Origin": "https://domain-analysis-frontend-456664817971.europe-west1.run.app",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "*",
        }
    )

@app.post("/chat/upload-csv")
async def upload_csv_file(
    file: UploadFile = File(...),
    session_id: str = Form(...),
    analyzer: DomainAnalyzer = Depends(get_domain_analyzer),
    bq_client: BigQueryClient = Depends(get_bigquery_client)
):
    """Handle CSV file upload and process emails in batch"""
    logger.info(f"CSV upload request received for session: {session_id}, file: {file.filename}")
    try:
        session = get_or_create_session(session_id)
        
        # Validate file type
        if not file.filename.endswith('.csv'):
            error_msg = "Please upload a CSV file."
            await send_chat_message(session_id, error_msg)
            return {"error": error_msg}
        
        # Read CSV file
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))
        
        # Enhanced email cleaning using new function with BigQuery duplicate checking
        valid_emails, cleaning_stats = clean_email_dataframe(df, bq_client)
        
        if not valid_emails:
            error_msg = f"""No new email addresses found in the CSV file.
            
📊 Processing Summary:
• Total rows: {cleaning_stats['total_rows']}
• Email column used: '{cleaning_stats['email_column']}'
• Valid emails found: {cleaning_stats['valid_emails']}
• Invalid emails: {cleaning_stats['invalid_emails']}
• CSV duplicates removed: {cleaning_stats['duplicates_removed']}
• Already in database: {cleaning_stats['bigquery_duplicates']}
• New emails to process: {cleaning_stats['new_emails']}
• Empty rows: {cleaning_stats['empty_rows']}

All valid emails are already processed in our database. Please check with new email addresses."""
            await send_chat_message(session_id, error_msg)
            return {"error": error_msg}
        
        # Send detailed processing summary
        summary_msg = f"""📁 CSV file processed successfully!

📊 Processing Summary:
• Total rows: {cleaning_stats['total_rows']}
• Email column used: '{cleaning_stats['email_column']}'
• Valid emails found: {cleaning_stats['valid_emails']}
• Invalid emails: {cleaning_stats['invalid_emails']}
• CSV duplicates removed: {cleaning_stats['duplicates_removed']}
• Already in database: {cleaning_stats['bigquery_duplicates']} ⚡
• New emails to process: {cleaning_stats['new_emails']}
• Empty rows skipped: {cleaning_stats['empty_rows']}

🚀 Starting analysis for {len(valid_emails)} new emails..."""
        await send_chat_message(session_id, summary_msg)
        
        # Process emails in background
        logger.info(f"Starting background processing for {len(valid_emails)} emails")
        task = asyncio.create_task(process_csv_emails_background(
            session_id, valid_emails, analyzer, bq_client
        ))
        # Add exception handler to catch silent failures
        task.add_done_callback(lambda t: logger.error(f"Background task error: {t.exception()}") if t.exception() else logger.info("Background task completed successfully"))
        
        return {
            "message": f"Processing {len(valid_emails)} emails. You'll receive updates every 10 completions.",
            "total_emails": len(valid_emails)
        }
        
    except Exception as e:
        logger.error(f"Error uploading CSV: {str(e)}")
        error_msg = f"Error processing CSV file: {str(e)}"
        await send_chat_message(session_id, error_msg)
        return {"error": error_msg}


async def process_massive_batch_background(batch_id: str, bq_client: BigQueryClient):
    """
    Distributed background processing for massive batches
    Uses parallel workers to process queue items concurrently
    """
    logger.info(f"Starting distributed processing for massive batch {batch_id}")
    
    try:
        # Initialize analyzer
        analyzer = get_domain_analyzer()
        
        # Configuration for parallel processing
        MAX_CONCURRENT_WORKERS = 10  # Adjust based on API rate limits
        BATCH_SIZE = 50  # Process in chunks
        
        # Get batch info
        batch_info = bq_client.get_batch_status(batch_id)
        if not batch_info:
            logger.error(f"Batch {batch_id} not found")
            return
        
        total_emails = batch_info['total_emails']
        logger.info(f"Processing batch {batch_id} with {total_emails} emails using {MAX_CONCURRENT_WORKERS} workers")
        
        # Create semaphore to limit concurrent workers
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_WORKERS)
        
        # Statistics tracking
        stats = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'duplicates': 0
        }
        
        async def process_queue_batch():
            """Process a batch of queue items"""
            async with semaphore:
                try:
                    # Get pending items from queue
                    queue_items = bq_client.get_pending_queue_items(limit=BATCH_SIZE, priority_order=True)
                    
                    if not queue_items:
                        return False  # No more items to process
                    
                    # Filter items for this batch only
                    batch_items = [item for item in queue_items if item['batch_id'] == batch_id]
                    
                    if not batch_items:
                        return False  # No items for this batch
                    
                    # Process each item in this batch
                    for item in batch_items:
                        try:
                            queue_id = item['queue_id']
                            email = item['email']
                            
                            # Mark as processing
                            bq_client.update_queue_item_status(queue_id, "processing")
                            
                            # Check if already exists (duplicate handling)
                            domain = email.split('@')[-1] if '@' in email else None
                            if domain and bq_client.check_domain_exists(domain, max_age_hours=24):
                                logger.info(f"Domain {domain} already exists, marking as duplicate")
                                bq_client.update_queue_item_status(queue_id, "completed")
                                stats['duplicates'] += 1
                                stats['processed'] += 1
                                continue
                            
                            # Perform domain analysis
                            result = await asyncio.to_thread(analyzer.analyze_email_domain, email)
                            
                            if result and hasattr(result, 'scraping_status') and result.scraping_status == 'success':
                                # Insert successful result to main analysis table
                                result_dict = {
                                    'original_email': result.original_email,
                                    'extracted_domain': result.extracted_domain,
                                    'selected_url': result.selected_url,
                                    'scraping_status': result.scraping_status,
                                    'website_summary': result.website_summary,
                                    'confidence_score': result.confidence_score,
                                    'selection_reasoning': result.selection_reasoning,
                                    'completed_timestamp': result.completed_timestamp,
                                    'processing_time_seconds': result.processing_time_seconds,
                                    'created_at': datetime.utcnow().isoformat(),
                                    'real_estate': getattr(result, 'real_estate', 'Can\'t Say'),
                                    'infrastructure': getattr(result, 'infrastructure', 'Can\'t Say'),
                                    'industrial': getattr(result, 'industrial', 'Can\'t Say'),
                                }
                                
                                success = bq_client.insert_single_result(result_dict)
                                
                                if success:
                                    bq_client.update_queue_item_status(queue_id, "completed")
                                    stats['successful'] += 1
                                    logger.info(f"Successfully processed {email}")
                                else:
                                    bq_client.update_queue_item_status(queue_id, "failed", "Failed to insert result to BigQuery")
                                    stats['failed'] += 1
                                    logger.error(f"Failed to insert result for {email}")
                            else:
                                # Analysis failed
                                error_msg = f"Domain analysis failed for {email}"
                                bq_client.update_queue_item_status(queue_id, "failed", error_msg)
                                stats['failed'] += 1
                                logger.error(error_msg)
                                
                            stats['processed'] += 1
                            
                        except Exception as e:
                            # Handle individual item failures
                            error_msg = f"Processing error for {item.get('email', 'unknown')}: {str(e)}"
                            logger.error(error_msg)
                            bq_client.update_queue_item_status(item['queue_id'], "failed", str(e))
                            stats['failed'] += 1
                            stats['processed'] += 1
                    
                    return True  # Successfully processed batch
                    
                except Exception as e:
                    logger.error(f"Error in process_queue_batch: {str(e)}")
                    return False
        
        # Main processing loop
        active_batches = 0
        max_retries = 3
        retry_count = 0
        
        while True:
            try:
                # Create batch processing tasks
                tasks = []
                for _ in range(MAX_CONCURRENT_WORKERS):
                    task = asyncio.create_task(process_queue_batch())
                    tasks.append(task)
                
                # Wait for all batch tasks to complete
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Check if any batches processed items
                any_processed = any(r is True for r in results if not isinstance(r, Exception))
                
                if not any_processed:
                    retry_count += 1
                    if retry_count >= max_retries:
                        logger.info(f"No more items to process for batch {batch_id}, stopping")
                        break
                    else:
                        # Wait a bit before retrying
                        await asyncio.sleep(2)
                        continue
                else:
                    retry_count = 0  # Reset retry count on successful processing
                
                # Update batch progress every processing cycle
                bq_client.update_batch_progress(
                    batch_id=batch_id,
                    processed=stats['processed'],
                    successful=stats['successful'],
                    failed=stats['failed'],
                    duplicates=stats['duplicates']
                )
                
                # Log progress
                progress_pct = (stats['processed'] / total_emails * 100) if total_emails > 0 else 0
                logger.info(f"Batch {batch_id} progress: {stats['processed']}/{total_emails} ({progress_pct:.1f}%) - ✅ {stats['successful']} successful, ❌ {stats['failed']} failed, 🔄 {stats['duplicates']} duplicates")
                
                # Small delay to prevent overwhelming the APIs
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Error in main processing loop: {str(e)}")
                await asyncio.sleep(5)  # Wait longer on errors
        
        # Final status update
        final_status = "completed" if stats['failed'] == 0 else "completed_with_errors"
        bq_client.update_batch_progress(
            batch_id=batch_id,
            processed=stats['processed'],
            successful=stats['successful'],
            failed=stats['failed'],
            duplicates=stats['duplicates'],
            status=final_status
        )
        
        logger.info(f"Massive batch {batch_id} processing completed: {stats['processed']} processed, {stats['successful']} successful, {stats['failed']} failed, {stats['duplicates']} duplicates")
        
    except Exception as e:
        logger.error(f"Error in massive batch background processing: {str(e)}")
        # Update batch status to failed
        bq_client.update_batch_progress(batch_id=batch_id, status="failed")


async def process_csv_emails_background(
    session_id: str, 
    emails: List[str], 
    analyzer: DomainAnalyzer, 
    bq_client: BigQueryClient
):
    """Process emails in background with progress updates"""
    logger.info(f"Background processing started for session {session_id} with {len(emails)} emails")
    try:
        session = get_or_create_session(session_id)
        total_emails = len(emails)
        processed = 0
        successful = 0
        duplicates = 0
        failed = 0
        all_results = []
        
        for i, email in enumerate(emails):
            try:
                # Check for duplicates
                domain_output = analyzer.extract_domain_from_email(email)
                domain = domain_output.domain
                
                if bq_client.check_domain_exists(domain, max_age_hours=24):
                    duplicates += 1
                    processed += 1
                    continue
                
                # Process email
                result_df = await asyncio.get_event_loop().run_in_executor(
                    None, analyzer.analyze_email_domain, email
                )
                
                # Save to BigQuery
                bq_client.insert_dataframe(result_df)
                
                # Convert to result object
                results = dataframe_to_analysis_result(result_df)
                all_results.extend(results)
                
                if results[0].scraping_status in ['success', 'success_text', 'success_fallback']:
                    successful += 1
                else:
                    failed += 1
                
                processed += 1
                
                # Send progress update every 10 completions
                if processed % 10 == 0 or processed == total_emails:
                    progress_msg = f"Progress: {processed}/{total_emails} emails processed. "
                    progress_msg += f"✅ {successful} successful, ❌ {failed} failed, 🔄 {duplicates} duplicates."
                    
                    await send_chat_message(session_id, progress_msg)
                
            except Exception as e:
                logger.error(f"Error processing email {email}: {str(e)}")
                failed += 1
                processed += 1
        
        # Send final summary
        final_msg = f"""🎉 Batch processing complete!

📊 **Final Results:**
- Total processed: {processed}/{total_emails}
- ✅ Successful: {successful}
- ❌ Failed: {failed}  
- 🔄 Duplicates skipped: {duplicates}

All results have been saved to the database. You can now submit more emails or upload another CSV file!"""

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