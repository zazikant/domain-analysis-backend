# Domain Analysis API

Advanced RAG pipeline for email domain intelligence gathering, deployed on Google Cloud Run with BigQuery integration.

## Overview

This system analyzes email addresses to extract comprehensive company intelligence by:
1. Extracting domains from email addresses
2. Searching for official company websites
3. Scraping website content
4. Generating AI-powered summaries
5. Storing results in BigQuery with pandas DataFrames

## Features

- **FastAPI REST API** with async processing
- **Real-time BigQuery integration** - Every analysis is immediately stored
- **Pandas DataFrame output** with standardized column structure
- **Intelligent caching** - Avoids duplicate processing
- **Batch processing** support for multiple emails
- **Cloud Run deployment** with auto-scaling
- **Comprehensive error handling** and fallback mechanisms

## DataFrame Output Structure

Each analysis returns a pandas DataFrame with these exact columns:

```python
{
    'original_email': 'contact@example.com',
    'extracted_domain': 'example.com', 
    'selected_url': 'https://example.com',
    'scraping_status': 'success',
    'website_summary': 'Company providing X services...',
    'confidence_score': 0.95,
    'selection_reasoning': 'Official company website',
    'completed_timestamp': '2024-01-01T12:00:00',
    'processing_time_seconds': 120.5,
    'created_at': '2024-01-01T12:00:00'
}
```

## Quick Start

### 1. Environment Setup

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your API keys
SERPER_API_KEY=your_serper_key
BRIGHTDATA_API_TOKEN=your_brightdata_token  
GOOGLE_API_KEY=your_google_ai_key
GCP_PROJECT_ID=your_gcp_project
```

### 2. Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Test the pipeline
python test_pipeline.py

# Run locally
python main.py
```

### 3. Cloud Run Deployment

```bash
# Make deploy script executable
chmod +x deploy.sh

# Deploy to Cloud Run
./deploy.sh YOUR_PROJECT_ID us-central1
```

## API Endpoints

### Single Analysis
```bash
curl -X POST "https://your-service-url/analyze" \
  -H "Content-Type: application/json" \
  -d '{"email": "contact@example.com"}'
```

### Batch Analysis  
```bash
curl -X POST "https://your-service-url/analyze/batch" \
  -H "Content-Type: application/json" \
  -d '{"emails": ["contact@example.com", "info@company.com"]}'
```

### Get Cached Results
```bash
curl "https://your-service-url/domain/example.com"
```

### Statistics
```bash
curl "https://your-service-url/stats"
```

## Architecture

```
FastAPI App → Domain Analyzer → Search APIs → Web Scraping → AI Analysis → DataFrame → BigQuery
```

### Components

1. **Domain Analyzer** (`domain_analyzer.py`) - Core analysis logic
2. **BigQuery Client** (`bigquery_client.py`) - DataFrame to BigQuery integration
3. **FastAPI App** (`main.py`) - REST API endpoints
4. **Configuration** (`config.py`) - Environment management

### External APIs Used

- **Serper API** - Web search functionality
- **Bright Data API** - Advanced web scraping
- **Google Gemini** - AI analysis and summarization

## BigQuery Schema

The BigQuery table is automatically created with this schema:

```sql
CREATE TABLE domain_analysis (
  original_email STRING,
  extracted_domain STRING,
  selected_url STRING,
  scraping_status STRING,
  website_summary STRING,
  confidence_score FLOAT64,
  selection_reasoning STRING,
  completed_timestamp TIMESTAMP,
  processing_time_seconds FLOAT64,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
```

Features:
- **Partitioned** by `created_at` for performance
- **Clustered** by `extracted_domain` and `scraping_status`
- **Automatic schema** matching DataFrame structure

## Cloud Run Configuration

- **Memory**: 4GB
- **CPU**: 2 cores  
- **Timeout**: 15 minutes
- **Concurrency**: 1000 requests
- **Scaling**: 0-100 instances
- **Health checks**: `/health` endpoint

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SERPER_API_KEY` | Yes | Serper.dev API key |
| `BRIGHTDATA_API_TOKEN` | Yes | Bright Data API token |
| `GOOGLE_API_KEY` | Yes | Google AI API key |
| `GCP_PROJECT_ID` | Yes | Google Cloud project ID |
| `BIGQUERY_DATASET_ID` | No | BigQuery dataset (default: domain_intelligence) |
| `BIGQUERY_TABLE_ID` | No | BigQuery table (default: domain_analysis) |

## Processing Flow

1. **Email Input** → Extract domain using regex
2. **Search Query Generation** → AI creates optimized search query
3. **Web Search** → Serper API returns top 5 results
4. **URL Selection** → AI selects best official website URL
5. **Content Scraping** → Bright Data scrapes website (with polling)
6. **Summary Generation** → AI creates one-line company description
7. **DataFrame Creation** → Structure results in pandas DataFrame
8. **BigQuery Insert** → Immediately push to BigQuery table
9. **Response** → Return structured results

## Performance

- **Processing Time**: 3-5 minutes per email (due to web scraping)
- **Caching**: 24-hour TTL for duplicate domains
- **Batch Processing**: Up to 50 emails per request
- **Throughput**: ~10-20 analyses per hour per instance

## Error Handling

- **Fallback scraping** if Bright Data fails
- **Graceful degradation** for API failures  
- **Comprehensive logging** with correlation IDs
- **Retry logic** with exponential backoff
- **Circuit breaker patterns** for external APIs

## Testing

```bash
# Run all tests
python test_pipeline.py

# Test specific functionality
python -c "from domain_analyzer import DomainAnalyzer; print('Import successful')"

# Test BigQuery connection
python -c "from bigquery_client import BigQueryClient; print('BigQuery client ready')"
```

## Monitoring

- **Health Check**: `GET /health`
- **Statistics**: `GET /stats` 
- **Recent Results**: `GET /recent`
- **Cloud Logging** integration
- **Error tracking** and alerting

## Cost Optimization

- **Intelligent caching** reduces API calls
- **Batch processing** improves efficiency
- **Serverless scaling** with Cloud Run
- **BigQuery partitioning** reduces query costs
- **Rate limiting** prevents runaway costs

## Security

- **API keys** stored in Google Secret Manager
- **Non-root container** user
- **Input validation** with Pydantic
- **HTTPS-only** communication
- **IAM permissions** following least privilege

## Deployment

The deployment script handles:
- Docker image building and pushing
- Secret creation for API keys
- BigQuery dataset creation
- Cloud Run service deployment
- Health check configuration

## Troubleshooting

### Common Issues

1. **Missing API keys** - Check Secret Manager configuration
2. **BigQuery permissions** - Ensure service account has BigQuery admin role
3. **Timeout errors** - Increase Cloud Run timeout for web scraping
4. **Memory issues** - Monitor memory usage and adjust limits

### Logs

```bash
# View Cloud Run logs
gcloud logs read --service=domain-analysis-api --limit=100

# Real-time logs
gcloud logs tail --service=domain-analysis-api
```

## Contributing

1. Test changes locally with `python test_pipeline.py`
2. Update documentation if adding new features
3. Ensure all environment variables are documented
4. Test deployment with `./deploy.sh`

## License

Private project - All rights reserved