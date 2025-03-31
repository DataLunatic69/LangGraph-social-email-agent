import os
import re
import csv
import time
import requests
import threading
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from urllib.parse import urlparse, urljoin
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_community.tools.tavily_search import TavilySearchResults
from dotenv import load_dotenv
from typing import Optional


load_dotenv()

# Configuration
CONFIG = {
    'output_file': 'global_marketing_leads.csv',
    'max_threads': 5,
    'request_timeout': 20,
    'delay_between_requests': 2,
    'max_emails_per_company': 5,
    'contact_keywords': ['contact', 'about', 'team', 'connect', 'reach', 'sales'],
    'email_domains': ['com', 'io', 'ai', 'co', 'org', 'net', 'us', 'uk', 'ca', 'de', 'fr', 'au'],
    'ignore_email_patterns': ['noreply', 'no-reply', 'support', 'info', 'hello', 'mailer'],
    'user_agent': UserAgent(),
    'search_depth': 'advanced',
}

# Service Definitions
SERVICES = {
    "Advanced Analytics": {
        "description": "Uncover actionable insights and track conversions effectively with our advanced analytics solutions. Harness the power of data to optimize your marketing strategies and drive conversions.",
        "keywords": ["analytics", "data", "tracking", "metrics", "conversion", "optimization"]
    },
    "Account Based Marketing": {
        "description": "Tailor your marketing efforts precisely to high-value accounts with our strategic account-based marketing approach. Engage your target audience on a personalized level and maximize your ROI.",
        "keywords": ["ABM", "account based", "targeted marketing", "enterprise marketing", "personalization"]
    },
    "Sales & Marketing Automation": {
        "description": "Streamline your sales and marketing processes with our automation solutions. From lead nurturing to email campaigns, automate repetitive tasks and focus on building meaningful connections.",
        "keywords": ["automation", "marketing ops", "lead nurturing", "email campaigns", "workflow"]
    },
    "HubSpot Excellence": {
        "description": "Unlock the full potential of HubSpot with our expert guidance and support. From implementation to optimization, we ensure you get the most out of HubSpot's powerful features.",
        "keywords": ["HubSpot", "CRM implementation", "marketing automation", "sales enablement"]
    },
    "CRM Migration": {
        "description": "Seamlessly transition your CRM system with our migration services. Our experienced team ensures a smooth migration process, preserving data integrity and minimizing disruptions.",
        "keywords": ["CRM migration", "data transfer", "system migration", "platform change"]
    },
    "CRM Strategy": {
        "description": "Develop a comprehensive CRM strategy tailored to your business goals with our expertise. From lead management to customer retention, we help you maximize your CRM investment.",
        "keywords": ["CRM strategy", "customer relationship", "lead management", "retention"]
    }
}

class GlobalLeadGenerator:
    def __init__(self, config, services):
        self.config = config
        self.services = services
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': config['user_agent'].random})
        self.lock = threading.Lock()
        self.processed_urls = set()
        
        # Initialize AI components
        self.llm = ChatGroq(
            groq_api_key=os.getenv("GROQ_API_KEY"),
            model_name="llama-3.3-70b-versatile",
            temperature=0.7
        )
        self.search_tool = TavilySearchResults(
            max_results=10,
            search_depth=config['search_depth']
        )
        
        # Precompile regex patterns
        self.email_regex = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b')
        self.phone_regex = re.compile(r'(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}')

    async def ai_suggest_companies(self, service_name: str) -> list[str]:
        """Use AI to suggest global companies that would benefit from this service"""
        prompt = ChatPromptTemplate.from_messages([
            ("system", """You are an expert in B2B marketing. Suggest specific companies worldwide that would benefit from:
            Service: {service_name}
            Description: {service_description}
            
            Focus on established companies with digital marketing needs.
            Return ONLY a JSON list of company names."""),
            ("human", "Please suggest global companies for {service_name}")
        ])
        
        chain = prompt | self.llm | JsonOutputParser()
        
        try:
            companies = await chain.ainvoke({
                "service_name": service_name,
                "service_description": self.services[service_name]["description"]
            })
            return list(set(companies))  # Remove duplicates
        except Exception as e:
            self.log_error(f"AI suggestion failed for {service_name}: {str(e)}")
            return []

    async def ai_enhance_company_info(self, company_name: str, service_name: str) -> dict:
        """Use AI to analyze why this company needs our service"""
        prompt = ChatPromptTemplate.from_messages([
            ("system", """Analyze why {company_name} would need our service:
            Service: {service_name}
            Description: {service_description}
            
            Provide:
            - 3 key reasons they would benefit
            - Their likely current solutions
            - Estimated potential value
            
            Return concise JSON."""),
            ("human", "Analyze {company_name} for {service_name}")
        ])
        
        chain = prompt | self.llm | JsonOutputParser()
        
        try:
            insights = await chain.ainvoke({
                "company_name": company_name,
                "service_name": service_name,
                "service_description": self.services[service_name]["description"]
            })
            return insights
        except:
            return {}

    async def search_emails_with_api(self, company_name: str, domain: str = None) -> list[str]:
        """Find professional emails using search APIs"""
        if not domain:
            domain = self.get_company_domain(company_name)
            if not domain:
                return []
        
        try:
            query = f"email contacts @{urlparse(domain).netloc} -@gmail.com -@yahoo.com -@outlook.com"
            results = await self.search_tool.ainvoke({"query": query})
            
            emails = set()
            for result in results:
                if 'content' in result:
                    found_emails = self.email_regex.findall(result['content'])
                    emails.update(
                        e.lower() for e in found_emails 
                        if not any(p in e.lower() for p in self.config['ignore_email_patterns'])
                    )
            
            return list(emails)[:self.config['max_emails_per_company']]
        except Exception as e:
            self.log_error(f"Email search API failed for {company_name}: {str(e)}")
            return []

    def get_company_domain(self, company_name: str) -> Optional[str]:
        """Find company domain with multiple fallback methods"""
        # Try common domain patterns first
        name_variations = [
            company_name.lower().replace(' ', ''),
            company_name.lower().replace(' ', '-'),
            company_name.lower().replace(' ', '').replace('&', 'and'),
            ''.join([word[0] for word in company_name.split()]).lower()
        ]
        
        for name in name_variations:
            for domain in self.config['email_domains']:
                for prefix in ['', 'www.']:
                    url = f"https://{prefix}{name}.{domain}"
                    if self.check_url_exists(url):
                        return url
        
        # Fallback to search API
        try:
            results = self.search_tool.invoke({
                "query": f"{company_name} official website",
                "include_raw_content": True
            })
            
            for result in results:
                if 'url' in result:
                    url = result['url']
                    if any(domain in url for domain in self.config['email_domains']):
                        return url
        except:
            pass
        
        return None

    def check_url_exists(self, url: str) -> bool:
        """Check if URL exists without downloading full content"""
        try:
            with self.lock:
                if url in self.processed_urls:
                    return False
                self.processed_urls.add(url)
            
            response = requests.head(
                url,
                headers={'User-Agent': self.config['user_agent'].random},
                timeout=5,
                allow_redirects=True
            )
            return response.status_code == 200
        except:
            return False

    async def get_company_info(self, company_name: str, service_name: str) -> dict:
        """Get comprehensive company information with AI insights"""
        self.log_info(f"\n🔍 Processing {company_name} for {service_name}...")
        
        # Step 1: Find domain
        website = self.get_company_domain(company_name)
        if not website:
            self.log_warning(f"Could not find website for {company_name}")
            return None
        
        # Step 2: Get AI insights
        ai_insights = await self.ai_enhance_company_info(company_name, service_name)
        
        # Step 3: Find emails using multiple methods
        emails = set()
        
        # Method 1: Search API
        api_emails = await self.search_emails_with_api(company_name, website)
        emails.update(api_emails)
        
        # Method 2: Website scraping (fallback)
        if len(emails) < self.config['max_emails_per_company']:
            contact_page = self.find_contact_page(website)
            if contact_page:
                scraped_emails = self.extract_emails_from_page(contact_page)
                emails.update(scraped_emails)
        
        # Prepare results
        result = {
            'company': company_name,
            'website': website,
            'service': service_name,
            'service_description': self.services[service_name]["description"],
            'emails': ', '.join(list(emails)[:self.config['max_emails_per_company']]),
            'email_count': len(emails),
            'date_collected': datetime.now().strftime('%Y-%m-%d %H:%M'),
            'status': 'Success' if emails else 'Partial',
            **ai_insights
        }
        
        if emails:
            self.log_success(f"✅ Found {len(emails)} contacts for {company_name}")
        else:
            self.log_warning(f"⚠️ No emails found for {company_name}")
        
        time.sleep(self.config['delay_between_requests'])
        return result

    async def generate_leads(self) -> list[dict]:
        """Generate leads for all services globally"""
        all_leads = []
        
        for service_name in self.services:
            self.log_info(f"\n=== Generating {service_name} leads ===")
            
            # Get AI-suggested companies
            companies = await self.ai_suggest_companies(service_name)
            self.log_info(f"AI suggested {len(companies)} companies for {service_name}")
            
            # Process top companies (limit to 15 per service for demo)
            for company in companies[:15]:
                lead = await self.get_company_info(company, service_name)
                if lead:
                    all_leads.append(lead)
        
        return all_leads

    def find_contact_page(self, base_url: str) -> Optional[str]:
        """Find contact page using intelligent search"""
        try:
            # Try common contact paths
            common_paths = ['/contact', '/contact-us', '/about/contact', '/connect']
            for path in common_paths:
                contact_url = urljoin(base_url, path)
                if self.check_url_exists(contact_url):
                    return contact_url
            
            # Analyze homepage for contact links
            response = self.session.get(base_url, timeout=self.config['request_timeout'])
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find likely contact links
            contact_links = []
            for link in soup.find_all('a', href=True):
                text = link.get_text().lower()
                href = link['href'].lower()
                
                if any(kw in text or kw in href for kw in self.config['contact_keywords']):
                    if not href.startswith(('mailto:', 'tel:', 'javascript:')):
                        contact_links.append(link['href'])
            
            # Check found links
            for link in set(contact_links):  # Deduplicate
                if not link.startswith('http'):
                    link = urljoin(base_url, link)
                if self.check_url_exists(link):
                    return link
            
            return None
        except Exception as e:
            self.log_error(f"Contact page search failed for {base_url}: {str(e)}")
            return None

    def extract_emails_from_page(self, url: str) -> list[str]:
        """Extract emails from webpage with multiple techniques"""
        try:
            response = self.session.get(url, timeout=self.config['request_timeout'])
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Remove unwanted elements
            for element in soup(['script', 'style', 'noscript', 'footer', 'header']):
                element.decompose()
            
            # Method 1: Text extraction
            text = soup.get_text()
            emails = set(re.findall(self.email_regex, text))
            
            # Method 2: Mailto links
            for link in soup.select('a[href^="mailto:"]'):
                email = link['href'][7:].split('?')[0]
                if re.match(self.email_regex, email):
                    emails.add(email)
            
            # Method 3: Contact forms
            for form in soup.select('form'):
                form_text = form.get_text()
                form_emails = re.findall(self.email_regex, form_text)
                emails.update(form_emails)
            
            # Filter unwanted patterns
            filtered_emails = [
                e.lower() for e in emails
                if not any(p in e.lower() for p in self.config['ignore_email_patterns'])
            ]
            
            return filtered_emails[:self.config['max_emails_per_company']]
        except Exception as e:
            self.log_error(f"Email extraction failed for {url}: {str(e)}")
            return []

    def save_leads_to_csv(self, leads: list[dict]) -> bool:
        """Save enhanced lead data to CSV"""
        if not leads:
            self.log_warning("No leads to save")
            return False
        
        # Dynamically determine all fields
        fieldnames = set()
        for lead in leads:
            fieldnames.update(lead.keys())
        
        try:
            with open(self.config['output_file'], 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=sorted(fieldnames))
                writer.writeheader()
                writer.writerows(leads)
            
            self.log_success(f"💾 Saved {len(leads)} leads to {self.config['output_file']}")
            return True
        except Exception as e:
            self.log_error(f"Failed to save CSV: {str(e)}")
            return False

    # Logging methods
    def log_info(self, message):
        print(message)
    
    def log_success(self, message):
        print(f"\033[92m{message}\033[0m")  # Green
    
    def log_warning(self, message):
        print(f"\033[93m{message}\033[0m")  # Yellow
    
    def log_error(self, message):
        print(f"\033[91m{message}\033[0m")  # Red

async def main():
    print("🚀 Global Marketing Services Lead Generator")
    print("="*50)
    print("Services:")
    for service, data in SERVICES.items():
        print(f"\n🔹 {service}")
        print(f"   {data['description']}")
    
    # Initialize generator
    lead_gen = GlobalLeadGenerator(CONFIG, SERVICES)
    
    # Generate leads
    leads = await lead_gen.generate_leads()
    
    # Save results
    lead_gen.save_leads_to_csv(leads)
    
    # Print summary
    print("\n🎉 Lead generation complete!")
    print(f"Total companies processed: {len(leads)}")
    print(f"Successful profiles: {len([l for l in leads if l['status'] == 'Success'])}")
    print(f"Total emails collected: {sum(l['email_count'] for l in leads)}")
    
    # Service breakdown
    print("\nService Breakdown:")
    for service in SERVICES:
        count = len([l for l in leads if l['service'] == service])
        print(f"- {service}: {count} leads")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())