"""
Configuration management for the Domain Analysis API
Handles environment variables and settings
"""

import os
from typing import Optional


class Settings:
    """Application settings loaded from environment variables"""

    def __init__(self):
        # API Keys
        self.serper_api_key = os.getenv("SERPER_API_KEY")
        self.brightdata_api_token = os.getenv("BRIGHTDATA_API_TOKEN")
        self.google_api_key = os.getenv("GOOGLE_API_KEY")

        # Google Cloud Configuration
        self.gcp_project_id = os.getenv("GCP_PROJECT_ID")

        # BigQuery Configuration
        self.bigquery_dataset_id = os.getenv("BIGQUERY_DATASET_ID", "domain_intelligence")
        self.bigquery_table_id = os.getenv("BIGQUERY_TABLE_ID", "domain_analysis")

        # Application Configuration
        self.port = int(os.getenv("PORT", "8080"))
        self.environment = os.getenv("ENV", "production")

        # Logging Configuration
        self.log_level = os.getenv("LOG_LEVEL", "INFO")

        # Processing Configuration
        self.max_batch_size = int(os.getenv("MAX_BATCH_SIZE", "50"))
        self.cache_ttl_hours = int(os.getenv("CACHE_TTL_HOURS", "24"))
        self.request_timeout_seconds = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "900"))  # 15 minutes

        # Rate Limiting
        self.max_requests_per_minute = int(os.getenv("MAX_REQUESTS_PER_MINUTE", "30"))


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