"""
Simple FastAPI test application for Cloud Run
"""
import os
from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI(
    title="Domain Analysis API - Test Version",
    description="Simple test API for Cloud Run deployment",
    version="1.0.0"
)

@app.get("/")
async def root():
    return {
        "message": "Domain Analysis API is running!",
        "status": "success",
        "version": "1.0.0",
        "environment": {
            "port": os.getenv("PORT", "8080"),
            "has_serper_key": bool(os.getenv("SERPER_API_KEY")),
            "has_brightdata_token": bool(os.getenv("BRIGHTDATA_API_TOKEN")),
            "has_google_key": bool(os.getenv("GOOGLE_API_KEY")),
            "has_project_id": bool(os.getenv("GCP_PROJECT_ID"))
        }
    }

@app.get("/health")
async def health():
    return {"status": "healthy", "message": "Service is running"}

@app.get("/test")
async def test():
    return {
        "message": "Test endpoint working",
        "timestamp": "2025-09-09T06:30:00Z"
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    uvicorn.run("simple_main:app", host="0.0.0.0", port=port)