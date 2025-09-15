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
from bs4 import BeautifulSoup

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



# Custom Gemini LLM Wrapper
class GeminiLLM(LLM):
    model_name: str = "gemini-2.5-flash"
    temperature: float = 0.0

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

        # Initialize search metadata storage
        self._last_search_metadata = {}

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

IMPORTANT: Only select URLs that are complete and valid (starting with http:// or https://)
If no valid URLs are available, use "https://www.example.com" as a fallback.

Return your response in this exact JSON format:
{{
    "selected_url": "the best complete URL from the results",
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
            input_variables=["scraped_content", "search_results", "url", "domain"],
            template="""
Analyze this company website and extract business information. Be thorough and consistent.

URL: {url}
Domain: {domain}

COMPANY DATA FROM WEB:
{search_results}

WEBSITE CONTENT:
{scraped_content}

EXTRACT THE FOLLOWING:

1. BUSINESS SUMMARY: One clear sentence about what this company does.

2. SECTOR CLASSIFICATIONS (multiple allowed per category):

   REAL ESTATE: Commercial, Residential, Data Center, Educational, Hospitality
   INFRASTRUCTURE: Airport, Bridges, Hydro, Highway, Marine, Power, Railways
   INDUSTRIAL: Aerospace, Warehouse

   Rules:
   - List ALL relevant sectors per category (e.g., "Commercial, Residential")
   - Use "Can't Say" only if absolutely no match
   - Look for keywords like "construction", "development", "projects"

3. COMPANY TYPE (construction/civil focus):
   Developer: Develops/builds real estate, infrastructure projects
   Contractor: Executes construction work, builds for others
   Consultant: Engineering/design consulting, advisory services
   Can't Say: Non-construction businesses

4. COMPANY NAME: Extract from website title, headers, or search results

5. LOCATION EXTRACTION (Check these sections in order):

   STEP 1 - FOOTER SECTIONS: Search for addresses in footer/bottom areas
   - Look for: "Head Office:", "Corporate Office:", "Registered Office:"
   - Address patterns: "Mumbai - 400001, Maharashtra, India"
   - Contact footers: "123 Street Name, City, Country"
   - Copyright notices: "© 2025 Company, Mumbai, India"

   STEP 2 - HEADER/NAVIGATION: Check top sections
   - Company taglines: "Mumbai-based construction leader"
   - Navigation menus: "Our Offices" → city listings
   - Contact headers with location info

   STEP 3 - PHONE CODE HINTS: Indian phone patterns
   - +91-22 = Mumbai, +91-11 = Delhi, +91-80 = Bangalore
   - +91-40 = Hyderabad, +91-44 = Chennai, +91-79 = Ahmedabad

   STEP 4 - CONTACT/ABOUT PAGES: Look for office addresses
   - "Headquartered in", "Based in", "Located in"
   - Multiple office listings with cities

   FORMAT: Return as "City, Country" (e.g., "Mumbai, India", "London, UK")
   If multiple offices, prioritize HEAD/CORPORATE office location.

PRIORITY: Search FOOTER FIRST, then HEADER, then rest of content for accuracy.

EXAMPLE LOCATION EXTRACTIONS:
- "Head Office: Shapoorji Pallonji Tower, Mumbai 400001" → "Mumbai, India"
- "© 2025 Larsen & Toubro Limited, Chennai" → "Chennai, India"
- "Corporate Office: Gurgaon - 122001, Haryana" → "Gurgaon, India"
- Phone: "+91-22-6615-8888" → "Mumbai, India" (based on +91-22 code)

Return JSON format:
{{
    "summary": "One sentence about company business",
    "url": "{url}",
    "domain": "{domain}",
    "timestamp": "{timestamp}",
    "real_estate": "Commercial, Residential OR Can't Say",
    "infrastructure": "Power, Hydro OR Can't Say",
    "industrial": "Aerospace OR Can't Say",
    "company_type": "Developer OR Contractor OR Consultant OR Can't Say",
    "company_name": "Company Name OR Can't Say",
    "base_location": "City, Country OR Can't Say"
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
            "num": 5,
            "gl": "us",  # Global location for better results
            "hl": "en"   # Language for results
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

                # Store additional data for enhanced analysis (knowledge graph, etc.)
                self._last_search_metadata = {
                    'knowledge_graph': data.get('knowledgeGraph', {}),
                    'answer_box': data.get('answerBox', {}),
                    'people_also_ask': data.get('peopleAlsoAsk', []),
                    'related_searches': data.get('relatedSearches', [])
                }

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
        """Format search results for LLM processing with enhanced metadata"""

        results_text = ""

        # Add knowledge graph information if available (optimized)
        if hasattr(self, '_last_search_metadata') and self._last_search_metadata.get('knowledge_graph'):
            kg = self._last_search_metadata['knowledge_graph']
            if kg.get('title'):
                results_text += f"COMPANY: {kg['title'][:100]}\n"  # Truncate for speed
            if kg.get('description'):
                results_text += f"DESC: {kg['description'][:200]}\n"  # Truncate for speed

        # Skip answer box for performance (usually redundant with organic results)

        # Add organic search results (optimized for speed)
        results_text += "SEARCH RESULTS:\n"
        for i, result in enumerate(search_results_output.results[:3], 1):  # Limit to top 3 for speed
            results_text += f"{i}. {result.title[:80]}\n"  # Truncate titles
            results_text += f"   URL: {result.url}\n"  # Keep URLs for URL selection
            results_text += f"   {result.snippet[:120]}\n\n"  # Truncate snippets

        return results_text

    def extract_priority_content(self, html_content: str) -> str:
        """
        Extract and prioritize content from HTML for comprehensive business analysis
        Helps Gemini get better results for ALL fields: location, company name, sectors, etc.
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Remove script, style, and other non-content elements
            for element in soup(['script', 'style', 'meta', 'link', 'noscript']):
                element.decompose()

            priority_content = []

            # PRIORITY 1: Page Title and Main Headings (company name, business description)
            title = soup.find('title')
            if title:
                priority_content.append(f"=== PAGE TITLE ===\n{title.get_text(strip=True)}")

            # Main headings (often contain company name and business focus)
            headings = soup.find_all(['h1', 'h2', 'h3'], limit=5)
            heading_texts = [h.get_text(strip=True) for h in headings if len(h.get_text(strip=True)) > 10]
            if heading_texts:
                priority_content.append("=== MAIN HEADINGS ===\n" + "\n".join(heading_texts))

            # PRIORITY 2: Footer sections (best for addresses, company info)
            footer_selectors = ['footer', '.footer', '#footer', '.contact-info', '.contact-footer',
                              '.site-footer', '.page-footer', '.footer-content', '.contact-details']
            footer_content = []
            for selector in footer_selectors:
                elements = soup.select(selector)
                for elem in elements:
                    text = elem.get_text(strip=True)
                    if len(text) > 20:  # Skip very short elements
                        footer_content.append(text)

            if footer_content:
                priority_content.append("=== FOOTER SECTIONS ===\n" + "\n".join(footer_content[:3]))

            # PRIORITY 3: About/Services sections (business activities, sectors)
            about_selectors = ['.about', '#about', '.services', '#services', '.what-we-do',
                             '.business', '.company-profile', '.overview', '.industries',
                             '.sectors', '.capabilities', '.expertise']
            about_content = []
            for selector in about_selectors:
                elements = soup.select(selector)
                for elem in elements:
                    text = elem.get_text(strip=True)
                    if len(text) > 50:  # Only substantial content
                        about_content.append(text)

            if about_content:
                priority_content.append("=== BUSINESS/ABOUT SECTIONS ===\n" + "\n".join(about_content[:3]))

            # PRIORITY 4: Header/Navigation (company taglines, business focus)
            header_selectors = ['header', '.header', '#header', 'nav', '.navbar', '.navigation',
                              '.hero', '.banner', '.intro', '.tagline', '.company-intro']
            header_content = []
            for selector in header_selectors:
                elements = soup.select(selector)
                for elem in elements:
                    text = elem.get_text(strip=True)
                    if len(text) > 30 and len(text) < 500:  # Skip very short/long navigation
                        header_content.append(text)

            if header_content:
                priority_content.append("=== HEADER/HERO SECTIONS ===\n" + "\n".join(header_content[:2]))

            # PRIORITY 5: Project/Portfolio sections (sector evidence)
            project_selectors = ['.projects', '#projects', '.portfolio', '.work', '.case-studies',
                               '.developments', '.construction', '.real-estate', '.infrastructure']
            project_content = []
            for selector in project_selectors:
                elements = soup.select(selector)
                for elem in elements:
                    text = elem.get_text(strip=True)
                    if len(text) > 50:
                        project_content.append(text)

            if project_content:
                priority_content.append("=== PROJECTS/PORTFOLIO SECTIONS ===\n" + "\n".join(project_content[:2]))

            # PRIORITY 6: Copyright and legal text (location hints)
            copyright_text = soup.find_all(text=re.compile(r'copyright|©|®|pvt|ltd|limited|inc|corp', re.IGNORECASE))
            copyright_content = [text.strip() for text in copyright_text if len(text.strip()) > 10][:3]
            if copyright_content:
                priority_content.append("=== COPYRIGHT/LEGAL INFO ===\n" + "\n".join(copyright_content))

            # Combine all priority content
            extracted_content = "\n\n".join(priority_content)

            # Add broader page content for context (more generous now)
            if extracted_content:
                # Clean general text from entire page
                general_text = soup.get_text(separator=' ')
                general_text = re.sub(r'\s+', ' ', general_text).strip()

                # More generous content limit since we have structured sections above
                if len(general_text) > 2000:
                    general_text = general_text[:2000] + "..."
                extracted_content += f"\n\n=== ADDITIONAL PAGE CONTENT ===\n{general_text}"
            else:
                # Fallback: clean general content if no structured sections found
                general_text = soup.get_text(separator=' ')
                general_text = re.sub(r'\s+', ' ', general_text).strip()
                extracted_content = general_text[:4000] + "..." if len(general_text) > 4000 else general_text

            return extracted_content

        except Exception as e:
            # Fallback to original content if HTML parsing fails
            clean_text = re.sub(r'<[^>]+>', '', html_content)  # Strip HTML tags
            clean_text = re.sub(r'\s+', ' ', clean_text).strip()
            return clean_text[:4000] + "..." if len(clean_text) > 4000 else clean_text

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
            
            # Step 5: Content Scraping with URL validation
            selected_url = url_selection_output.selected_url

            # Validate URL format
            if not selected_url or not selected_url.startswith(('http://', 'https://')):
                logger.warning(f"Invalid URL received: '{selected_url}', using fallback")
                selected_url = f"https://www.{domain}"  # Fallback to domain

            try:
                parsed = urlparse(selected_url)
                if not parsed.scheme or not parsed.netloc:
                    raise ValueError("Invalid URL structure")
                root_url = f"{parsed.scheme}://{parsed.netloc}"
            except Exception as e:
                logger.error(f"URL parsing failed for {selected_url}: {e}")
                root_url = f"https://www.{domain}"  # Fallback
            
            scraped_content = self.call_brightdata_api(root_url)
            
            # Step 6: Enhanced Content Processing with Priority Section Extraction
            # Use new method to extract and prioritize footer/header content for better location extraction
            content = self.extract_priority_content(scraped_content.html_content)

            # Include search results for enhanced company name and location extraction (limited)
            search_results_text = self.format_search_results_for_prompt(search_results)[:2000]  # Reduced for more focus

            summary_result = self.summary_chain({
                'scraped_content': content,
                'search_results': search_results_text,
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