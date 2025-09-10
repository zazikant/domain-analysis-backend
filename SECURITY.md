# Security Notice

## 🔐 API Key Security

**IMPORTANT**: This project requires API keys that must be kept secure.

### ⚠️ Never Commit API Keys to GitHub

- API keys are stored in `.env` file (gitignored)
- Use `.env.example` as a template
- Never hardcode API keys in source code

### Required API Keys

1. **SERPER_API_KEY** - Get from [serper.dev](https://serper.dev)
2. **BRIGHTDATA_API_TOKEN** - Get from [brightdata.com](https://brightdata.com)
3. **GOOGLE_API_KEY** - Get from [Google AI Studio](https://aistudio.google.com)
4. **GCP_PROJECT_ID** - Your Google Cloud project ID

### Setup Instructions

1. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` with your actual API keys:
   ```
   SERPER_API_KEY=your_actual_serper_key
   BRIGHTDATA_API_TOKEN=your_actual_brightdata_token
   GOOGLE_API_KEY=your_actual_google_key
   GCP_PROJECT_ID=your_gcp_project_id
   ```

3. The `.env` file is automatically ignored by git

### Production Deployment

For Cloud Run deployment:
- Use Google Secret Manager for API keys
- Environment variables are set via Cloud Run service configuration
- Never store secrets in container images

### Security Best Practices

- ✅ Use environment variables for all secrets
- ✅ Use Google Secret Manager in production
- ✅ Rotate API keys regularly
- ✅ Monitor API usage for unusual activity
- ❌ Never commit `.env` files
- ❌ Never hardcode API keys in source code
- ❌ Never share API keys via email/chat

## 🚨 If API Keys Are Compromised

1. Immediately revoke the compromised keys
2. Generate new API keys
3. Update all deployments with new keys
4. Review access logs for unauthorized usage