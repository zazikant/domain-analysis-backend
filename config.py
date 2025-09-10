"""
Configuration management for the Domain Analysis API
Handles environment variables and settings
"""

import os
from typing import Optional
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # API Keys
    serper_api_key: str = Field(..., env="SERPER_API_KEY")
    brightdata_api_token: str = Field(..., env="BRIGHTDATA_API_TOKEN") 
    google_api_key: str = Field(..., env="GOOGLE_API_KEY")
    
    # Google Cloud Configuration
    gcp_project_id: str = Field(..., env="GCP_PROJECT_ID")
    
    # BigQuery Configuration
    bigquery_dataset_id: str = Field("domain_intelligence", env="BIGQUERY_DATASET_ID")
    bigquery_table_id: str = Field("domain_analysis", env="BIGQUERY_TABLE_ID")
    
    # Application Configuration
    port: int = Field(8080, env="PORT")
    environment: str = Field("production", env="ENV")
    
    # Logging Configuration
    log_level: str = Field("INFO", env="LOG_LEVEL")
    
    # Processing Configuration
    max_batch_size: int = Field(50, env="MAX_BATCH_SIZE")
    cache_ttl_hours: int = Field(24, env="CACHE_TTL_HOURS")
    request_timeout_seconds: int = Field(900, env="REQUEST_TIMEOUT_SECONDS")  # 15 minutes
    
    # Rate Limiting
    max_requests_per_minute: int = Field(30, env="MAX_REQUESTS_PER_MINUTE")
    
    class Config:
        env_file = ".env"
        case_sensitive = False


def get_settings() -> Settings:
    """Get application settings"""
    return Settings()


# Global settings instance
settings = get_settings()


def validate_environment():
    """Validate that all required environment variables are set"""
    required_vars = [
        "SERPER_API_KEY",
        "BRIGHTDATA_API_TOKEN", 
        "GOOGLE_API_KEY",
        "GCP_PROJECT_ID"
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )
    
    return True


if __name__ == "__main__":
    # Test configuration
    try:
        validate_environment()
        print("✅ All required environment variables are set")
        
        settings = get_settings()
        print(f"📊 Project ID: {settings.gcp_project_id}")
        print(f"📁 Dataset: {settings.bigquery_dataset_id}")
        print(f"📋 Table: {settings.bigquery_table_id}")
        print(f"🌐 Port: {settings.port}")
        print(f"🔧 Environment: {settings.environment}")
        
    except Exception as e:
        print(f"❌ Configuration error: {e}")