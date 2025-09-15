"""
Domain Analysis Engine
Extracted from Jupyter notebook for production deployment
"""

import os
import re
import json
import requests
import time
from typing import List, Optional, Dict, Any
from datetime import datetime
from urllib.parse import urlparse
import pandas as pd

from langchain.chains import SequentialChain, LLMChain
from langchain.prompts import PromptTemplate
from langchain.llms.base import LLM
from langchain.schema import BaseOutputParser
from langchain.chains.base import Chain
from typing import ClassVar

from pydantic import BaseModel, Field
import google.generativeai as genai


# Pydantic Models for Structured Outputs
class DomainOutput(BaseModel):
    domain: str = Field(description="Extracted domain from email")
    original_email: str = Field(description="Original email address")

class SearchQueryOutput(BaseModel):
    search_query: str = Field(description="Optimized search query for the domain")
    domain: str = Field(description="Domain being searched")

class SearchResult(BaseModel):
    title: str = Field(description="Title of the search result")
    url: str = Field(description="URL of the search result")
    snippet: str = Field(description="Description/snippet of the search result")

class SearchResultsOutput(BaseModel):
    results: List[SearchResult] = Field(description="List of top 5 search results")
    query_used: str = Field(description="Search query that was used")

class URLSelectionOutput(BaseModel):
    selected_url: str = Field(description="The best URL selected by Gemini")
    reasoning: str = Field(description="Reasoning for URL selection")
    confidence_score: float = Field(description="Confidence score (0-1)")

class ScrapedContentOutput(BaseModel):
    url: str = Field(description="URL that was scraped")
    html_content: str = Field(description="Raw HTML content from the page")
    scrape_status: str = Field(description="Status of scraping operation")

class FinalSummaryOutput(BaseModel):
    summary: str = Field(description="One-line summary of the website")
    url: str = Field(description="URL of the summarized website")
    domain: str = Field(description="Domain of the website")
    timestamp: str = Field(description="When the analysis was completed")
    # Sector classification fields
    real_estate: str = Field(description="Real Estate sector classification")
    infrastructure: str = Field(description="Infrastructure sector classification")
    industrial: str = Field(description="Industrial sector classification")
    # New company information fields
    company_type: str = Field(description="Company type: Developer, Contractor, or Consultant")
    company_name: str = Field(description="Company name extracted from content")
    base_location: str = Field(description="Head office or base location of company")

# New models for batch processing
# MASSIVE BATCH PROCESSING MODELS - COMMENTED OUT
# class BatchProcessingRequest(BaseModel):
#     emails: List[str] = Field(description="List of email addresses to process")
#     batch_id: str = Field(description="Unique batch identifier")
#     priority: int = Field(default=5, description="Processing priority (1=highest, 10=lowest)")
#     user_session_id: str = Field(description="User session identifier")

# class BatchStatus(BaseModel):
#     batch_id: str = Field(description="Batch identifier")
#     total_emails: int = Field(description="Total number of emails in batch")
#     processed_emails: int = Field(description="Number of processed emails")
#     successful_emails: int = Field(description="Number of successful analyses")
#     failed_emails: int = Field(description="Number of failed analyses")
#     duplicate_emails: int = Field(description="Number of duplicate emails found")
#     status: str = Field(description="Current batch status")
#     progress_percentage: float = Field(description="Progress percentage (0-100)")
#     created_at: Optional[str] = Field(description="Batch creation timestamp")
#     started_at: Optional[str] = Field(description="Processing start timestamp")
#     completed_at: Optional[str] = Field(description="Processing completion timestamp")
#     last_updated: Optional[str] = Field(description="Last update timestamp")

# class QueueItem(BaseModel):
#     queue_id: str = Field(description="Queue item identifier")
#     batch_id: str = Field(description="Batch identifier")
#     email: str = Field(description="Email address to process")
#     extracted_domain: str = Field(description="Extracted domain")
#     status: str = Field(description="Queue item status")
#     priority: int = Field(description="Processing priority")
#     created_at: str = Field(description="Creation timestamp")


# Custom Gemini LLM Wrapper
class GeminiLLM(LLM):
    model_name: str = "gemini-1.5-flash"
    temperature: float = 0.1

    @property
    def _llm_type(self) -> str:
        return "gemini"

    def _call(self, prompt: str, stop: Optional[List[str]] = None) -> str:
        try:
            model = genai.GenerativeModel(self.model_name)
            response = model.generate_content(prompt)
            return response.text
        except Exception as e:
            return f"Error: {str(e)}"


# Custom Output Parsers
class DomainOutputParser(BaseOutputParser):
    def parse(self, text: str) -> DomainOutput:
        try:
            import json
            parsed = json.loads(text)
            return DomainOutput(**parsed)
        except:
            domain_match = re.search(r'"domain":\s*"([^"]+)"', text)
            email_match = re.search(r'"original_email":\s*"([^"]+)"', text)
            domain = domain_match.group(1) if domain_match else "unknown"
            email = email_match.group(1) if email_match else "unknown"
            return DomainOutput(domain=domain, original_email=email)

class SearchQueryOutputParser(BaseOutputParser):
    def parse(self, text: str) -> SearchQueryOutput:
        try:
            import json
            parsed = json.loads(text)
            return SearchQueryOutput(**parsed)
        except:
            query_match = re.search(r'"search_query":\s*"([^"]+)"', text)
            domain_match = re.search(r'"domain":\s*"([^"]+)"', text)
            query = query_match.group(1) if query_match else text.strip()
            domain = domain_match.group(1) if domain_match else "unknown"
            return SearchQueryOutput(search_query=query, domain=domain)

class URLSelectionOutputParser(BaseOutputParser):
    def parse(self, text: str) -> URLSelectionOutput:
        try:
            import json
            parsed = json.loads(text)
            return URLSelectionOutput(**parsed)
        except:
            url_match = re.search(r'"selected_url":\s*"([^"]+)"', text)
            reasoning_match = re.search(r'"reasoning":\s*"([^"]+)"', text)
            confidence_match = re.search(r'"confidence_score":\s*([0-9.]+)', text)
            
            url = url_match.group(1) if url_match else "unknown"
            reasoning = reasoning_match.group(1) if reasoning_match else "No reasoning provided"
            confidence = float(confidence_match.group(1)) if confidence_match else 0.5
            
            return URLSelectionOutput(
                selected_url=url,
                reasoning=reasoning,
                confidence_score=confidence
            )

class FinalSummaryOutputParser(BaseOutputParser):
    def parse(self, text: str) -> FinalSummaryOutput:
        try:
            import json
            import re
            
            # Strip markdown code blocks if present
            clean_text = text.strip()
            if clean_text.startswith('```json') and clean_text.endswith('```'):
                clean_text = clean_text[7:-3].strip()
            elif clean_text.startswith('```') and clean_text.endswith('```'):
                clean_text = clean_text[3:-3].strip()
            
            # Try to extract JSON from text if it contains markdown
            json_match = re.search(r'\{[\s\S]*\}', clean_text)
            if json_match:
                clean_text = json_match.group()
            
            parsed = json.loads(clean_text)
            return FinalSummaryOutput(**parsed)
        except Exception as e:
            # Log parsing error for debugging
            print(f"Failed to parse summary output: {e}")
            print(f"Raw text: {text[:200]}...")
            return FinalSummaryOutput(
                summary=text.strip(),
                url="unknown",
                domain="unknown",
                timestamp=datetime.now().isoformat(),
                real_estate="Can't Say",
                infrastructure="Can't Say",
                industrial="Can't Say",
                company_type="Can't Say",
                company_name="Can't Say",
                base_location="Can't Say"
            )


class DomainAnalyzer:
    """Main class for domain analysis functionality"""
    
    def __init__(self, serper_api_key: str, brightdata_api_token: str, google_api_key: str):
        self.serper_api_key = serper_api_key
        self.brightdata_api_token = brightdata_api_token
        
        # Configure Google AI
        genai.configure(api_key=google_api_key)
        self.gemini_llm = GeminiLLM(temperature=0.1)
        
        # Initialize chains
        self._setup_chains()
    
    def _setup_chains(self):
        """Initialize LangChain components"""
        
        # Search Query Builder
        search_query_prompt = PromptTemplate(
            input_variables=["domain"],
            template="""
You are tasked with creating an optimal search query to find the official website for a company domain.

Domain: {domain}

Create a search query that will help find the official company website. Consider:
- The domain name itself
- Adding terms like "official website" if helpful
- Avoiding overly complex queries

Return your response in this exact JSON format:
{{
    "search_query": "your optimized search query here",
    "domain": "{domain}"
}}
"""
        )
        
        self.search_query_chain = LLMChain(
            llm=self.gemini_llm,
            prompt=search_query_prompt,
            output_parser=SearchQueryOutputParser(),
            output_key="search_query_output"
        )
        
        # URL Selection Chain
        url_selection_prompt = PromptTemplate(
            input_variables=["search_results"],
            template="""
You are an expert at identifying official company websites from search results.

Search Results:
{search_results}

Analyze these search results and select the BEST URL that represents the official company website. Consider:
- Official company domains vs third-party sites
- Homepage vs subpages
- Credibility and authority of the source
- Relevance to the original domain

Return your response in this exact JSON format:
{{
    "selected_url": "the best URL from the results",
    "reasoning": "brief explanation of why you chose this URL",
    "confidence_score": 0.95
}}
"""
        )
        
        self.url_selection_chain = LLMChain(
            llm=self.gemini_llm,
            prompt=url_selection_prompt,
            output_parser=URLSelectionOutputParser(),
            output_key="url_selection_output"
        )
        
        # Summary Generation Chain
        summary_prompt = PromptTemplate(
            input_variables=["scraped_content", "url", "domain"],
            template="""
You are tasked with creating a concise, one-line summary of a website and classifying it into business sectors and company information based on semantic similarity.

Website URL: {url}
Domain: {domain}
Scraped Content: {scraped_content}

TASKS:
1. Create a single, clear sentence that describes what this company/website does
2. Classify the company into these THREE sectors based on semantic similarity:

REAL ESTATE SECTOR OPTIONS:
- Commercial, Data Center, Educational, Residential, Hospitality
- Use "Can't Say" if no semantic match

INFRASTRUCTURE SECTOR OPTIONS:
- Airport, Bridges, Hydro, Highway, Marine, Power, Railways
- Use "Can't Say" if no semantic match

INDUSTRIAL SECTOR OPTIONS:
- Aerospace, Warehouse
- Use "Can't Say" if no semantic match

3. Determine the COMPANY TYPE based on their business activities:
- Developer: Companies that develop, create, or build projects (real estate developers, software developers, etc.)
- Contractor: Companies that execute work for others (construction contractors, service contractors, etc.)
- Consultant: Companies that provide advice, supervision, reporting, or consulting services
- Use "Can't Say" if unclear or doesn't fit these categories

4. Extract the COMPANY NAME from the content (the actual business name, not domain)
5. Identify the BASE LOCATION (head office, headquarters, or primary business location)

CLASSIFICATION RULES:
- Multiple selections allowed for sectors (comma-separated if multiple match)
- Base decisions on semantic similarity to company's actual business
- Use "Can't Say" when uncertain or no clear match
- Consider the company's primary business activities

Return your response in this exact JSON format:
{{
    "summary": "One clear sentence describing what this company/website does",
    "url": "{url}",
    "domain": "{domain}",
    "timestamp": "{timestamp}",
    "real_estate": "your classification or Can't Say",
    "infrastructure": "your classification or Can't Say", 
    "industrial": "your classification or Can't Say",
    "company_type": "Developer or Contractor or Consultant or Can't Say",
    "company_name": "Actual company name or Can't Say",
    "base_location": "Head office location or Can't Say"
}}
"""
        )
        
        self.summary_chain = LLMChain(
            llm=self.gemini_llm,
            prompt=summary_prompt,
            output_parser=FinalSummaryOutputParser(),
            output_key="final_summary"
        )
    
    def extract_domain_from_email(self, email: str) -> DomainOutput:
        """Extract domain from email address"""
        try:
            domain_match = re.search(r'@([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', email)
            if domain_match:
                domain = domain_match.group(1)
                return DomainOutput(domain=domain, original_email=email)
            else:
                return DomainOutput(domain="invalid", original_email=email)
        except Exception as e:
            return DomainOutput(domain="error", original_email=email)
    
    def call_serper_api(self, query: str) -> SearchResultsOutput:
        """Call Serper API to get search results"""
        url = "https://google.serper.dev/search"
        
        headers = {
            "X-API-KEY": self.serper_api_key,
            "Content-Type": "application/json"
        }
        
        payload = {
            "q": query,
            "num": 5
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                organic_results = data.get('organic', [])[:5]
                
                results = []
                for result in organic_results:
                    results.append(SearchResult(
                        title=result.get('title', ''),
                        url=result.get('link', ''),
                        snippet=result.get('snippet', '')
                    ))
                
                return SearchResultsOutput(results=results, query_used=query)
            else:
                return SearchResultsOutput(results=[], query_used=query)
        
        except Exception as e:
            return SearchResultsOutput(results=[], query_used=query)
    
    def call_brightdata_api(self, url: str, dataset_id: str = "gd_m6gjtfmeh43we6cqc") -> ScrapedContentOutput:
        """Call Bright Data API with two-phase workflow"""
        
        # Phase 1: Trigger the scraping job
        trigger_url = "https://api.brightdata.com/datasets/v3/trigger"
        
        headers = {
            "Authorization": f"Bearer {self.brightdata_api_token}",
            "Content-Type": "application/json"
        }
        
        params = {
            'dataset_id': dataset_id,
            'format': 'json'
        }
        
        payload = [{"url": url}]
        
        try:
            response = requests.post(
                trigger_url,
                headers=headers,
                params=params,
                json=payload,
                timeout=30
            )
            
            if not response.ok:
                return self.simple_scrape(url)
            
            result = response.json()
            
            if 'snapshot_id' not in result:
                return self.simple_scrape(url)
            
            snapshot_id = result['snapshot_id']
            
            # Phase 2: Poll and retrieve results
            return self.poll_and_retrieve_results(snapshot_id, url)
        
        except Exception as e:
            return self.simple_scrape(url)
    
    def poll_and_retrieve_results(self, snapshot_id: str, original_url: str, max_wait_minutes: int = 3) -> ScrapedContentOutput:
        """Poll Bright Data for results and retrieve when ready"""
        
        headers = {
            "Authorization": f"Bearer {self.brightdata_api_token}",
            "Content-Type": "application/json"
        }
        
        progress_url = f"https://api.brightdata.com/datasets/v3/progress/{snapshot_id}"
        start_time = time.time()
        max_wait_seconds = max_wait_minutes * 60
        poll_count = 0
        
        while True:
            elapsed_time = time.time() - start_time
            poll_count += 1
            
            if elapsed_time > max_wait_seconds:
                return ScrapedContentOutput(
                    url=original_url,
                    html_content="",
                    scrape_status=f"timeout_after_{max_wait_minutes}min"
                )
            
            try:
                response = requests.get(progress_url, headers=headers, timeout=30)
                
                if response.ok:
                    status_data = response.json()
                    current_status = status_data.get('status', 'unknown')
                    
                    if current_status == 'done' or current_status == 'ready':
                        return self.download_scraped_results(snapshot_id, original_url)
                    
                    elif current_status == 'failed':
                        return ScrapedContentOutput(
                            url=original_url,
                            html_content="",
                            scrape_status="failed"
                        )
                    
                    elif current_status == 'running':
                        time.sleep(30)
                        continue
                    
                    else:
                        time.sleep(30)
                        continue
                
                else:
                    if poll_count >= 3:
                        break
                    time.sleep(30)
                    continue
            
            except Exception as e:
                if poll_count >= 3:
                    break
                time.sleep(30)
                continue
        
        return self.simple_scrape(original_url)
    
    def download_scraped_results(self, snapshot_id: str, original_url: str) -> ScrapedContentOutput:
        """Download the actual scraped content from Bright Data"""
        
        headers = {
            "Authorization": f"Bearer {self.brightdata_api_token}",
            "Content-Type": "application/json"
        }
        
        download_urls = [
            f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}?format=json",
            f"https://api.brightdata.com/datasets/v3/download/{snapshot_id}",
            f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}"
        ]
        
        for download_url in download_urls:
            try:
                response = requests.get(download_url, headers=headers, timeout=60)
                
                if response.ok:
                    try:
                        scraped_data = response.json()
                        html_content = self.extract_html_from_response(scraped_data)
                        
                        return ScrapedContentOutput(
                            url=original_url,
                            html_content=html_content,
                            scrape_status="success"
                        )
                    
                    except json.JSONDecodeError:
                        return ScrapedContentOutput(
                            url=original_url,
                            html_content=response.text,
                            scrape_status="success_text"
                        )
                
                else:
                    continue
            
            except Exception as e:
                continue
        
        return ScrapedContentOutput(
            url=original_url,
            html_content="",
            scrape_status="download_failed"
        )
    
    def extract_html_from_response(self, scraped_data) -> str:
        """Extract HTML content from Bright Data response structure"""
        
        try:
            if isinstance(scraped_data, list) and scraped_data:
                item = scraped_data[0]
                
                html_fields = ['html', 'page_html', 'content', 'body', 'raw_html']
                for field in html_fields:
                    if isinstance(item, dict) and field in item:
                        return str(item[field])
                
                return str(item)
            
            elif isinstance(scraped_data, dict):
                html_fields = ['html', 'page_html', 'content', 'body', 'raw_html', 'data']
                for field in html_fields:
                    if field in scraped_data:
                        content = scraped_data[field]
                        if isinstance(content, list) and content:
                            return str(content[0])
                        return str(content)
                
                return str(scraped_data)
            
            else:
                return str(scraped_data)
        
        except Exception as e:
            return str(scraped_data)
    
    def simple_scrape(self, url: str) -> ScrapedContentOutput:
        """Fallback scraping using requests"""
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                return ScrapedContentOutput(
                    url=url,
                    html_content=response.text[:5000],
                    scrape_status="success_fallback"
                )
            else:
                return ScrapedContentOutput(
                    url=url,
                    html_content="",
                    scrape_status=f"failed_{response.status_code}"
                )
        
        except Exception as e:
            return ScrapedContentOutput(
                url=url,
                html_content="",
                scrape_status=f"error_{str(e)}"
            )
    
    def format_search_results_for_prompt(self, search_results_output):
        """Format search results for LLM processing"""
        
        results_text = ""
        for i, result in enumerate(search_results_output.results, 1):
            results_text += f"{i}. Title: {result.title}\n"
            results_text += f"   URL: {result.url}\n"
            results_text += f"   Snippet: {result.snippet}\n\n"
        return results_text
    
    def analyze_email_domain(self, email: str) -> pd.DataFrame:
        """
        Main function to analyze an email domain and return results as pandas DataFrame
        
        Args:
            email (str): Email address to analyze
            
        Returns:
            pd.DataFrame: Analysis results with standardized columns
        """
        
        start_time = time.time()
        
        try:
            # Step 1: Domain Extraction
            domain_output = self.extract_domain_from_email(email)
            domain = domain_output.domain
            
            # Step 2: Search Query Generation
            search_query_result = self.search_query_chain({"domain": domain})
            search_query_output = search_query_result['search_query_output']
            
            # Step 3: Web Search
            search_results = self.call_serper_api(search_query_output.search_query)
            
            # Step 4: URL Selection
            formatted_results = self.format_search_results_for_prompt(search_results)
            url_selection_result = self.url_selection_chain({"search_results": formatted_results})
            url_selection_output = url_selection_result['url_selection_output']
            
            # Step 5: Content Scraping
            selected_url = url_selection_output.selected_url
            try:
                parsed = urlparse(selected_url)
                root_url = f"{parsed.scheme}://{parsed.netloc}"
            except:
                root_url = selected_url
            
            scraped_content = self.call_brightdata_api(root_url)
            
            # Step 6: Summary Generation
            content = scraped_content.html_content[:2000] + "..." if len(scraped_content.html_content) > 2000 else scraped_content.html_content
            
            summary_result = self.summary_chain({
                'scraped_content': content,
                'url': url_selection_output.selected_url,
                'domain': domain,
                'timestamp': datetime.now().isoformat()
            })
            
            final_summary = summary_result['final_summary']
            
            # Calculate processing time
            processing_time = time.time() - start_time
            
            # Create pandas DataFrame with exact column structure
            df = pd.DataFrame({
                'original_email': [email],
                'extracted_domain': [domain],
                'selected_url': [url_selection_output.selected_url],
                'scraping_status': [scraped_content.scrape_status],
                'company_summary': [final_summary.summary],
                'confidence_score': [url_selection_output.confidence_score],
                'selection_reasoning': [url_selection_output.reasoning],
                'completed_timestamp': [final_summary.timestamp],
                'processing_time_seconds': [processing_time],
                'created_at': [datetime.utcnow().isoformat()],
                # Sector classification columns
                'real_estate': [final_summary.real_estate],
                'infrastructure': [final_summary.infrastructure],
                'industrial': [final_summary.industrial],
                # New company information columns
                'company_type': [final_summary.company_type],
                'company_name': [final_summary.company_name],
                'base_location': [final_summary.base_location]
            })
            
            return df
        
        except Exception as e:
            # Return error DataFrame
            processing_time = time.time() - start_time
            
            df = pd.DataFrame({
                'original_email': [email],
                'extracted_domain': ['error'],
                'selected_url': [''],
                'scraping_status': ['error'],
                'company_summary': [f'Error: {str(e)}'],
                'confidence_score': [0.0],
                'selection_reasoning': ['Processing failed'],
                'completed_timestamp': [datetime.now().isoformat()],
                'processing_time_seconds': [processing_time],
                'created_at': [datetime.utcnow().isoformat()],
                # Sector classification columns (error state)
                'real_estate': ["Can't Say"],
                'infrastructure': ["Can't Say"],
                'industrial': ["Can't Say"],
                # New company information columns (error state)
                'company_type': ["Can't Say"],
                'company_name': ["Can't Say"],
                'base_location': ["Can't Say"]
            })
            
            return df
    
    def analyze_multiple_emails(self, emails: List[str]) -> pd.DataFrame:
        """
        Analyze multiple email addresses and return combined DataFrame
        
        Args:
            emails (List[str]): List of email addresses to analyze
            
        Returns:
            pd.DataFrame: Combined analysis results
        """
        
        all_results = []
        
        for email in emails:
            result_df = self.analyze_email_domain(email)
            all_results.append(result_df)
            
            # Add small delay between requests
            time.sleep(1)
        
        # Combine all DataFrames
        combined_df = pd.concat(all_results, ignore_index=True)
        return combined_df