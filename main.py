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
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, WebSocket, WebSocketDisconnect, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, EmailStr
import uvicorn

from domain_analyzer import DomainAnalyzer
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
    
    # Check BigQuery for existing domains if client provided
    final_emails = unique_emails
    bigquery_duplicates = 0
    
    if bq_client and unique_emails:
        try:
            # Extract domains from emails
            domains = [email.split('@')[1] for email in unique_emails]
            
            # Check which domains exist in BigQuery
            existing_domains = bq_client.check_multiple_domains_exist(domains)
            
            # Filter out emails with existing domains
            new_emails = []
            for email in unique_emails:
                domain = email.split('@')[1]
                if not existing_domains.get(domain, False):
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
    
    # Initialize BigQuery Client
    dataset_id = os.getenv("BIGQUERY_DATASET_ID", "advanced_csv_analysis")
    table_id = os.getenv("BIGQUERY_TABLE_ID", "email_domain_results")
    bigquery_client = create_bigquery_client(project_id, dataset_id, table_id)
    
    logger.info("Domain Analysis API started successfully")
    
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
    website_summary: Optional[str]
    confidence_score: Optional[float]
    selection_reasoning: Optional[str]
    completed_timestamp: Optional[str]
    processing_time_seconds: Optional[float]
    created_at: str
    from_cache: bool = Field(default=False, description="Whether result was retrieved from cache")
    # New sector classification fields
    real_estate: Optional[str] = Field(default="Can't Say", description="Real Estate sector classification")
    infrastructure: Optional[str] = Field(default="Can't Say", description="Infrastructure sector classification")
    industrial: Optional[str] = Field(default="Can't Say", description="Industrial sector classification")


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
        
        result = AnalysisResult(
            original_email=row.get('original_email', ''),
            extracted_domain=row.get('extracted_domain', ''),
            selected_url=row.get('selected_url'),
            scraping_status=row.get('scraping_status', ''),
            website_summary=row.get('website_summary'),
            confidence_score=row.get('confidence_score'),
            selection_reasoning=row.get('selection_reasoning'),
            completed_timestamp=completed_timestamp,
            processing_time_seconds=row.get('processing_time_seconds'),
            created_at=created_at or '',
            from_cache=from_cache,
            # New sector classification fields
            real_estate=row.get('real_estate', "Can't Say"),
            infrastructure=row.get('infrastructure', "Can't Say"),
            industrial=row.get('industrial', "Can't Say")
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
                    website_summary=f"Error: {str(e)}",
                    confidence_score=0.0,
                    selection_reasoning="Processing failed",
                    completed_timestamp=datetime.utcnow().isoformat(),
                    processing_time_seconds=0.0,
                    created_at=datetime.utcnow().isoformat(),
                    from_cache=False,
                    # New sector classification fields (error state)
                    real_estate="Can't Say",
                    infrastructure="Can't Say",
                    industrial="Can't Say"
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
**Summary**: {result.website_summary}
**Confidence**: {result.confidence_score:.2f if result.confidence_score else 0}

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


@app.post("/chat/preview-csv")
async def preview_csv_file(
    session_id: str,
    file: UploadFile = File(...),
    bq_client: BigQueryClient = Depends(get_bigquery_client)
):
    """Preview CSV file with BigQuery duplicate checking before processing"""
    try:
        # Validate file type
        if not file.filename.endswith('.csv'):
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


@app.post("/chat/upload-csv")
async def upload_csv_file(
    session_id: str,
    file: UploadFile = File(...),
    analyzer: DomainAnalyzer = Depends(get_domain_analyzer),
    bq_client: BigQueryClient = Depends(get_bigquery_client)
):
    """Handle CSV file upload and process emails in batch"""
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
        asyncio.create_task(process_csv_emails_background(
            session_id, valid_emails, analyzer, bq_client
        ))
        
        return {
            "message": f"Processing {len(valid_emails)} emails. You'll receive updates every 10 completions.",
            "total_emails": len(valid_emails)
        }
        
    except Exception as e:
        logger.error(f"Error uploading CSV: {str(e)}")
        error_msg = f"Error processing CSV file: {str(e)}"
        await send_chat_message(session_id, error_msg)
        return {"error": error_msg}


async def process_csv_emails_background(
    session_id: str, 
    emails: List[str], 
    analyzer: DomainAnalyzer, 
    bq_client: BigQueryClient
):
    """Process emails in background with progress updates"""
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
        error_msg = f"Batch processing failed: {str(e)}"
        await send_chat_message(session_id, error_msg)


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