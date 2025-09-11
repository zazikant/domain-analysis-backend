#!/bin/bash

# Enhanced Domain Analysis API - Cloud Run Deployment Script
# This script builds and deploys the application to Google Cloud Run

set -e

# Configuration
PROJECT_ID="${GCP_PROJECT_ID:-feisty-outrider-471302-k6}"
SERVICE_NAME="domain-analysis-backend"
REGION="europe-west1"
IMAGE_NAME="gcr.io/$PROJECT_ID/$SERVICE_NAME"

echo "🚀 Starting deployment of Domain Analysis API..."
echo "📋 Configuration:"
echo "   Project ID: $PROJECT_ID"
echo "   Service: $SERVICE_NAME"
echo "   Region: $REGION"
echo "   Image: $IMAGE_NAME"

# Check if required environment variables are set
echo "🔍 Checking environment variables..."
required_vars=("SERPER_API_KEY" "BRIGHTDATA_API_TOKEN" "GOOGLE_API_KEY" "GCP_PROJECT_ID")
missing_vars=()

for var in "${required_vars[@]}"; do
    if [[ -z "${!var}" ]]; then
        missing_vars+=("$var")
    else
        echo "   ✓ $var is set"
    fi
done

if [[ ${#missing_vars[@]} -ne 0 ]]; then
    echo "❌ Missing required environment variables:"
    printf '   - %s\n' "${missing_vars[@]}"
    echo ""
    echo "💡 Please set these environment variables before running the deployment:"
    echo "   export SERPER_API_KEY='your-serper-key'"
    echo "   export BRIGHTDATA_API_TOKEN='your-brightdata-token'"
    echo "   export GOOGLE_API_KEY='your-google-api-key'"
    echo "   export GCP_PROJECT_ID='your-gcp-project-id'"
    exit 1
fi

# Set the active project
echo "🔧 Setting GCP project..."
gcloud config set project $PROJECT_ID

# Enable required APIs
echo "🔧 Enabling required APIs..."
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable bigquery.googleapis.com

# Build the Docker image
echo "🏗️  Building Docker image..."
docker build -t $IMAGE_NAME .

# Push the image to Google Container Registry
echo "📦 Pushing image to GCR..."
docker push $IMAGE_NAME

# Deploy to Cloud Run
echo "🚀 Deploying to Cloud Run..."
gcloud run deploy $SERVICE_NAME \
    --image $IMAGE_NAME \
    --platform managed \
    --region $REGION \
    --allow-unauthenticated \
    --memory 4Gi \
    --cpu 2 \
    --timeout 900 \
    --max-instances 10 \
    --min-instances 0 \
    --concurrency 1000 \
    --port 8080 \
    --set-env-vars "PORT=8080,GCP_PROJECT_ID=$GCP_PROJECT_ID,BIGQUERY_DATASET_ID=advanced_csv_analysis,BIGQUERY_TABLE_ID=email_domain_results" \
    --set-env-vars "SERPER_API_KEY=$SERPER_API_KEY,BRIGHTDATA_API_TOKEN=$BRIGHTDATA_API_TOKEN,GOOGLE_API_KEY=$GOOGLE_API_KEY"

# Get the service URL
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --platform managed --region $REGION --format 'value(status.url)')

echo ""
echo "🎉 Deployment successful!"
echo "🌐 Service URL: $SERVICE_URL"
echo "🏥 Health check: $SERVICE_URL/health"
echo "📚 API docs: $SERVICE_URL/docs"
echo ""
echo "💡 Test the API:"
echo "   curl -X POST '$SERVICE_URL/analyze' \\"
echo "        -H 'Content-Type: application/json' \\"
echo "        -d '{\"email\": \"contact@example.com\"}'"
echo ""
echo "🔍 View logs:"
echo "   gcloud logs tail $SERVICE_NAME --project=$PROJECT_ID"