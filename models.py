"""
Pydantic models for the Domain Analysis API
Contains data models for requests, responses, and internal data structures
"""

import uuid
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, EmailStr


class AnalysisResult(BaseModel):
    original_email: str
    extracted_domain: str
    selected_url: Optional[str]
    scraping_status: str
    company_summary: Optional[str]  # Updated field name
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

    # Company information fields
    company_type: Optional[str] = Field(default="Can't Say", description="Company type")
    company_name: Optional[str] = Field(default="Can't Say", description="Company name")
    base_location: Optional[str] = Field(default="Can't Say", description="Company base location")


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


# Session Management (simplified)
class ChatSession:
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.messages: List[ChatMessage] = []
        self.created_at = datetime.utcnow()
        self.last_activity = datetime.utcnow()
        self.websocket = None  # WebSocket connection

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

    def get_recent_messages(self, limit: int = 50) -> List[ChatMessage]:
        return self.messages[-limit:] if limit > 0 else self.messages

    def is_active(self, inactive_threshold_minutes: int = 60) -> bool:
        """Check if session has been active within threshold"""
        threshold = datetime.utcnow() - timedelta(minutes=inactive_threshold_minutes)
        return self.last_activity > threshold


class AnalysisRequest(BaseModel):
    email: EmailStr
    force_refresh: bool = Field(default=False, description="Force new analysis even if cached result exists")


class BatchAnalysisRequest(BaseModel):
    emails: List[EmailStr]
    force_refresh: bool = Field(default=False, description="Force new analysis even if cached results exist")


class BatchAnalysisResponse(BaseModel):
    results: List[AnalysisResult]
    total_processed: int
    successful: int
    failed: int
    duplicates: int
    processing_time_seconds: float


class JobStatus(BaseModel):
    job_id: str
    status: str  # pending, processing, completed, failed, cancelled, paused
    total_emails: int
    processed_emails: int
    successful_emails: int
    failed_emails: int
    duplicate_emails: int
    progress_percentage: float
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    last_updated: datetime
    error_message: Optional[str] = None


class JobResults(BaseModel):
    job_id: str
    results: List[AnalysisResult]
    total_count: int
    successful_count: int
    failed_count: int
    duplicate_count: int


class RecentJobsResponse(BaseModel):
    jobs: List[JobStatus]
    total_count: int


class ProgressUpdate(BaseModel):
    job_id: str
    progress_percentage: float
    processed_count: int
    total_count: int
    successful_count: int
    failed_count: int
    duplicate_count: int
    current_email: Optional[str] = None
    status: str
    message: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class SessionInfo(BaseModel):
    session_id: str
    message_count: int
    created_at: datetime
    last_activity: datetime
    is_active: bool


class SessionStats(BaseModel):
    total_sessions: int
    active_sessions: int
    total_messages: int
    avg_messages_per_session: float