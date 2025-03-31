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

# Configuration
CONFIG = {
    'output_file': 'ziellabs_leads.csv',
    'max_threads': 3,  # Be gentle with servers
    'request_timeout': 15,
    'delay_between_requests': 3,
    'max_emails_per_company': 5,
    'contact_keywords': ['contact', 'about', 'team', 'connect', 'reach', 'sales', 'support', 'help'],
    'email_domains': ['com', 'io', 'ai', 'co', 'org', 'net', 'us', 'uk', 'ca'],
    'ignore_email_patterns': ['noreply', 'no-reply', 'support', 'info', 'hello', 'mailer', 'notification'],
    'user_agent': UserAgent(),
}

# Target Companies by Service Category
COMPANY_LISTS = {
    "analytics": [
        "Mixpanel", "Amplitude", "Heap", "Segment", "Google Analytics",
        "Adobe Analytics", "Matomo", "Pendo", "Looker", "Tableau"
    ],
    "abm": [
        "Terminus", "Demandbase", "6sense", "Triblio", "Engagio",
        "RollWorks", "Madison Logic", "Kwanzoo", "Metadata.io", "Uberflip"
    ],
    "automation": [
        "HubSpot", "Marketo", "Pardot", "Salesforce Marketing Cloud",
        "ActiveCampaign", "Keap", "Customer.io", "Autopilot", "Drip", "Omnisend"
    ]
}

class LeadGenerator:
    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': config['user_agent'].random})
        self.lock = threading.Lock()
        self.processed_urls = set()

    def get_company_domain(self, company_name):
        """Find company website using multiple techniques"""
        # Try common domain patterns first
        variations = set()
        base_names = [
            company_name.lower().replace(' ', ''),
            company_name.lower().replace(' ', '-'),
            ''.join([word[0] for word in company_name.split()]).lower()
        ]
        
        for name in base_names:
            for domain in self.config['email_domains']:
                variations.add(f"https://{name}.{domain}")
                variations.add(f"https://www.{name}.{domain}")
        
        # Check all variations
        for url in variations:
            if self.check_url_exists(url):
                return url
        
        # Fallback to Google search
        try:
            google_url = f"https://www.google.com/search?q={company_name.replace(' ', '+')}+official+website"
            response = self.session.get(google_url, timeout=self.config['request_timeout'])
            soup = BeautifulSoup(response.text, 'html.parser')
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                if 'url?q=' in href and not any(x in href for x in ['google.com', 'webcache']):
                    potential_url = href.split('url?q=')[1].split('&')[0]
                    if self.check_url_exists(potential_url):
                        return potential_url
        except Exception as e:
            self.log_error(f"Google search failed for {company_name}: {str(e)}")
        
        return None

    def check_url_exists(self, url):
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

    def extract_emails_from_page(self, url):
        """Extract emails from webpage using multiple techniques"""
        try:
            response = self.session.get(url, timeout=self.config['request_timeout'])
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Remove unwanted elements
            for element in soup(['script', 'style', 'noscript', 'footer', 'header']):
                element.decompose()
            
            # Method 1: Direct text extraction
            text = soup.get_text()
            emails = set(re.findall(self.config['email_regex'], text))
            
            # Method 2: Mailto links
            for link in soup.select('a[href^="mailto:"]'):
                email = link['href'][7:].split('?')[0]  # Remove mailto: and parameters
                if re.match(self.config['email_regex'], email):
                    emails.add(email)
            
            # Method 3: Contact forms
            for form in soup.select('form'):
                form_text = form.get_text()
                form_emails = re.findall(self.config['email_regex'], form_text)
                emails.update(form_emails)
            
            # Method 4: Specific elements likely to contain emails
            for element in soup.select('.email, [class*="contact"], [id*="contact"], .team-member'):
                element_emails = re.findall(self.config['email_regex'], element.get_text())
                emails.update(element_emails)
            
            # Filter out unwanted email patterns
            filtered_emails = [
                email for email in emails
                if not any(pattern in email.lower() for pattern in self.config['ignore_email_patterns'])
            ]
            
            return filtered_emails[:self.config['max_emails_per_company']]
        except Exception as e:
            self.log_error(f"Failed to extract emails from {url}: {str(e)}")
            return []

    def find_contact_page(self, base_url):
        """Find contact page using multiple approaches"""
        # Try common contact page paths
        common_paths = [
            '/contact', '/contact-us', '/contact.html',
            '/about/contact', '/connect', '/get-in-touch',
            '/contact-sales', '/demo-request', '/support',
            '/help', '/contacto', '/about-us', '/team'
        ]
        
        for path in common_paths:
            contact_url = urljoin(base_url, path)
            if self.check_url_exists(contact_url):
                return contact_url
        
        # Try to find contact links in page navigation
        try:
            response = self.session.get(base_url, timeout=self.config['request_timeout'])
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all links that might lead to contact pages
            possible_links = []
            for link in soup.find_all('a', href=True):
                text = link.get_text().lower()
                href = link['href'].lower()
                
                if any(keyword in text or keyword in href for keyword in self.config['contact_keywords']):
                    possible_links.append(link['href'])
            
            # Check each possible link
            for link in set(possible_links):  # Deduplicate
                if not link.startswith('http'):
                    link = urljoin(base_url, link)
                if self.check_url_exists(link):
                    return link
        except Exception as e:
            self.log_error(f"Failed to find contact page for {base_url}: {str(e)}")
        
        return None

    def find_linkedin_profile(self, company_name):
        """Find LinkedIn profile using Google search"""
        try:
            google_url = f"https://www.google.com/search?q={company_name.replace(' ', '+')}+LinkedIn"
            response = self.session.get(
                google_url,
                headers={'User-Agent': self.config['user_agent'].random},
                timeout=self.config['request_timeout']
            )
            soup = BeautifulSoup(response.text, 'html.parser')
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                if 'linkedin.com/company/' in href and not any(x in href for x in ['/posts', '/jobs']):
                    return href.split('&')[0]  # Remove Google tracking parameters
            
            # Second try with site:linkedin.com search
            google_url = f"https://www.google.com/search?q=site:linkedin.com/company+{company_name.replace(' ', '+')}"
            response = self.session.get(google_url, timeout=self.config['request_timeout'])
            soup = BeautifulSoup(response.text, 'html.parser')
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                if 'linkedin.com/company/' in href:
                    return href.split('&')[0]
        except Exception as e:
            self.log_error(f"Failed to find LinkedIn profile for {company_name}: {str(e)}")
        
        return None

    def get_company_info(self, company_name, service_category):
        """Get comprehensive company information"""
        self.log_info(f"\n🔍 Processing {company_name}...")
        
        # Step 1: Find company website
        website = self.get_company_domain(company_name)
        if not website:
            self.log_warning(f"Could not find website for {company_name}")
            return None
        
        self.log_info(f"🌐 Website found: {website}")
        
        # Step 2: Find LinkedIn profile
        linkedin = self.find_linkedin_profile(company_name)
        if linkedin:
            self.log_info(f"🔗 LinkedIn profile: {linkedin}")
        
        # Step 3: Find contact page
        contact_page = self.find_contact_page(website)
        if contact_page:
            self.log_info(f"📞 Contact page: {contact_page}")
        
        # Step 4: Extract emails from multiple sources
        email_sources = [website]
        if contact_page and contact_page != website:
            email_sources.append(contact_page)
        
        all_emails = []
        for source in email_sources:
            emails = self.extract_emails_from_page(source)
            if emails:
                self.log_info(f"✉️ Found {len(emails)} emails on {source}")
                all_emails.extend(emails)
        
        # Deduplicate emails
        all_emails = list(set(all_emails))
        
        # Step 5: Try harder if no emails found
        if not all_emails:
            self.log_info("⚠️ No emails found, trying deeper search...")
            # Check additional pages that might contain contacts
            deeper_pages = ['/about', '/team', '/company', '/leadership', '/people']
            for page in deeper_pages:
                deeper_url = urljoin(website, page)
                if self.check_url_exists(deeper_url):
                    deeper_emails = self.extract_emails_from_page(deeper_url)
                    if deeper_emails:
                        self.log_info(f"✉️ Found {len(deeper_emails)} emails on {deeper_url}")
                        all_emails.extend(deeper_emails)
                        break  # Stop after first successful deeper search
        
        # Prepare results
        result = {
            'company': company_name,
            'website': website,
            'linkedin': linkedin or '',
            'service_category': service_category,
            'emails': ', '.join(all_emails),
            'email_count': len(all_emails),
            'contact_page': contact_page or '',
            'date_collected': datetime.now().strftime('%Y-%m-%d %H:%M'),
            'status': 'Success' if all_emails else 'No emails found'
        }
        
        if all_emails:
            self.log_success(f"✅ Found {len(all_emails)} emails for {company_name}")
        else:
            self.log_warning(f"⚠️ No valid emails found for {company_name}")
        
        time.sleep(self.config['delay_between_requests'])
        return result

    def generate_leads(self, company_lists):
        """Generate leads using multithreading"""
        all_leads = []
        
        with ThreadPoolExecutor(max_workers=self.config['max_threads']) as executor:
            futures = []
            
            for category, companies in company_lists.items():
                service_map = {
                    "analytics": "analytics and conversion tracking",
                    "abm": "account based marketing",
                    "automation": "sales and marketing automation"
                }
                service = service_map.get(category, category)
                
                self.log_info(f"\n=== Processing {service} companies ===")
                
                for company in companies:
                    futures.append(
                        executor.submit(
                            self.get_company_info,
                            company,
                            service
                        )
                    )
            
            for future in as_completed(futures):
                result = future.result()
                if result:
                    all_leads.append(result)
        
        return all_leads

    def save_to_csv(self, leads, filename):
        """Save leads to CSV with proper formatting"""
        if not leads:
            self.log_warning("No leads to save")
            return False
        
        fieldnames = [
            'company', 'website', 'linkedin', 'service_category',
            'emails', 'email_count', 'contact_page', 'date_collected', 'status'
        ]
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(leads)
            
            self.log_success(f"\n💾 Successfully saved {len(leads)} leads to {filename}")
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

def main():
    # Initialize configuration
    CONFIG['email_regex'] = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
    
    print("🚀 Starting Ziellabs Lead Generation System")
    print("="*50)
    
    # Initialize lead generator
    lead_gen = LeadGenerator(CONFIG)
    
    # Generate leads
    leads = lead_gen.generate_leads(COMPANY_LISTS)
    
    # Save results
    lead_gen.save_to_csv(leads, CONFIG['output_file'])
    
    print("\n🎉 Lead generation complete!")
    print(f"Total leads processed: {len(leads)}")
    print(f"Leads with emails: {len([l for l in leads if l['email_count'] > 0])}")

if __name__ == "__main__":
    main()