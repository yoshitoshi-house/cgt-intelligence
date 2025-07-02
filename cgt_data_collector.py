# CGT Data Collection Backend - Complete Fixed Version
# Save as: cgt_data_collector.py

import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
import json
import re
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
import logging
from dataclasses import dataclass
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Company:
    symbol: str
    name: str
    website: Optional[str] = None
    market_cap: Optional[str] = None
    xbi_weight: Optional[float] = None
    nbi_weight: Optional[float] = None

class ETFHoldingsScraper:
    """Scrape XBI and NBI ETF holdings to get comprehensive biotech company list"""
    
    def __init__(self):
        self.session = None
        
    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            headers={'User-Agent': 'CGT Research Bot 1.0'}
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def get_xbi_holdings(self) -> List[Company]:
        """Get XBI (SPDR S&P Biotech ETF) holdings"""
        try:
            # XBI holdings API endpoint
            url = "https://www.ssga.com/bin/v1/ssmp/fund/fundfinder/1464253357/holdings/1464253357-fund-holdings.json"
            
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    companies = []
                    holdings = data.get('fund', {}).get('priceDate', {}).get('holding', [])
                    
                    for holding in holdings:
                        if float(holding.get('percentWeight', 0)) > 0.1:  # Only significant holdings
                            company = Company(
                                symbol=holding.get('identifier', '').strip(),
                                name=holding.get('name', '').strip(),
                                xbi_weight=float(holding.get('percentWeight', 0)),
                                market_cap=self._format_market_value(holding.get('marketValue'))
                            )
                            companies.append(company)
                    
                    logger.info(f"Scraped {len(companies)} companies from XBI")
                    return companies
                    
        except Exception as e:
            logger.error(f"Error scraping XBI holdings: {e}")
            return []
    
    async def get_nbi_holdings(self) -> List[Company]:
        """Get NBI/IBB (Invesco Nasdaq Biotechnology ETF) holdings"""
        try:
            # IBB holdings API endpoint
            url = "https://www.invesco.com/us/rest/contenthandlers/fund-data-handler/fund-holdings"
            params = {
                'fundId': 'IBB',
                'audienceType': 'Investor'
            }
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    companies = []
                    holdings = data.get('holdings', [])
                    
                    for holding in holdings:
                        if float(holding.get('percentOfNetAssets', 0)) > 0.1:
                            company = Company(
                                symbol=holding.get('ticker', '').strip(),
                                name=holding.get('securityName', '').strip(),
                                nbi_weight=float(holding.get('percentOfNetAssets', 0)),
                                market_cap=self._format_market_value(holding.get('marketValue'))
                            )
                            companies.append(company)
                    
                    logger.info(f"Scraped {len(companies)} companies from IBB/NBI")
                    return companies
                    
        except Exception as e:
            logger.error(f"Error scraping IBB holdings: {e}")
            return []
    
    def _format_market_value(self, value) -> str:
        """Format market value for display"""
        if not value:
            return "N/A"
        try:
            num_value = float(value)
            if num_value >= 1e9:
                return f"${num_value/1e9:.1f}B"
            elif num_value >= 1e6:
                return f"${num_value/1e6:.1f}M"
            else:
                return f"${num_value:,.0f}"
        except:
            return str(value)
    
    async def get_combined_holdings(self) -> List[Company]:
        """Get combined and deduplicated holdings from both ETFs"""
        xbi_companies = await self.get_xbi_holdings()
        nbi_companies = await self.get_nbi_holdings()
        
        # Combine and deduplicate by symbol
        company_dict = {}
        
        for company in xbi_companies:
            company_dict[company.symbol] = company
            
        for company in nbi_companies:
            if company.symbol in company_dict:
                # Merge data for companies in both ETFs
                existing = company_dict[company.symbol]
                existing.nbi_weight = company.nbi_weight
                if not existing.market_cap and company.market_cap:
                    existing.market_cap = company.market_cap
            else:
                company_dict[company.symbol] = company
        
        companies = list(company_dict.values())
        logger.info(f"Combined total: {len(companies)} unique biotech companies")
        return companies

class FDADataCollector:
    """Collect data from FDA OpenFDA API - completely free"""
    
    def __init__(self):
        self.base_url = "https://api.fda.gov"
        self.session = None
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def get_recent_drug_approvals(self, days_back: int = 90) -> List[Dict]:
        """Get recent drug approvals from FDA"""
        endpoint = f"{self.base_url}/drug/drugsfda.json"
        
        # Calculate date range
        recent_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y%m%d')
        
        params = {
            'search': f'submissions.submission_status_date:[{recent_date} TO 20991231]',
            'limit': 100
        }
        
        try:
            async with self.session.get(endpoint, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._process_drug_approvals(data)
                else:
                    logger.error(f"FDA API error: {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Error fetching FDA approvals: {e}")
            return []
    
    async def search_company_drugs(self, company_name: str) -> List[Dict]:
        """Search for all drugs by a specific company"""
        endpoint = f"{self.base_url}/drug/drugsfda.json"
        
        params = {
            'search': f'sponsor_name:"{company_name}"',
            'limit': 50
        }
        
        try:
            async with self.session.get(endpoint, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._process_drug_approvals(data)
        except Exception as e:
            logger.error(f"Error searching company drugs for {company_name}: {e}")
            return []
    
    def _process_drug_approvals(self, fda_data: Dict) -> List[Dict]:
        """Process FDA drug approval data"""
        approvals = []
        
        for drug in fda_data.get('results', []):
            try:
                # Extract submission data
                submissions = drug.get('submissions', [])
                latest_submission = submissions[0] if submissions else {}
                
                # Extract product data
                products = drug.get('products', [])
                main_product = products[0] if products else {}
                
                approval = {
                    'drug_name': main_product.get('brand_name', 'Unknown'),
                    'generic_name': self._extract_generic_name(main_product),
                    'company': drug.get('sponsor_name', 'Unknown'),
                    'application_number': drug.get('application_number'),
                    'approval_date': latest_submission.get('submission_status_date'),
                    'application_type': latest_submission.get('submission_type'),
                    'submission_status': latest_submission.get('submission_status'),
                    'marketing_status': main_product.get('marketing_status'),
                    'route': main_product.get('route', []),
                    'dosage_form': main_product.get('dosage_form')
                }
                approvals.append(approval)
                
            except Exception as e:
                logger.error(f"Error processing drug data: {e}")
                continue
        
        return approvals
    
    def _extract_generic_name(self, product: Dict) -> str:
        """Extract generic drug name from product data"""
        ingredients = product.get('active_ingredients', [])
        if ingredients and isinstance(ingredients, list):
            return ingredients[0].get('name', 'Unknown')
        return 'Unknown'

class ClinicalTrialsCollector:
    """Collect data from ClinicalTrials.gov API - free"""
    
    def __init__(self):
        self.base_url = "https://clinicaltrials.gov/api/query/study_fields"
    
    async def search_company_trials(self, company_name: str) -> List[Dict]:
        """Search clinical trials for a company"""
        fields = [
            'NCTId', 'BriefTitle', 'OverallStatus', 'Phase', 
            'StudyType', 'Condition', 'InterventionName',
            'PrimaryCompletionDate', 'StudyFirstPostDate',
            'LeadSponsorName', 'SecondaryId', 'LocationCountry'
        ]
        
        params = {
            'expr': f'"{company_name}"',
            'fields': ','.join(fields),
            'fmt': 'json',
            'min_rnk': 1,
            'max_rnk': 50
        }
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(self.base_url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return self._process_trials(data)
            except Exception as e:
                logger.error(f"Error fetching trials for {company_name}: {e}")
                return []
    
    def _process_trials(self, ct_data: Dict) -> List[Dict]:
        """Process clinical trials data"""
        trials = []
        
        study_fields = ct_data.get('StudyFieldsResponse', {}).get('StudyFields', [])
        
        for trial in study_fields:
            try:
                processed_trial = {
                    'nct_id': self._safe_get(trial, 'NCTId', 0),
                    'title': self._safe_get(trial, 'BriefTitle', 0),
                    'status': self._safe_get(trial, 'OverallStatus', 0),
                    'phase': self._safe_get(trial, 'Phase', 0),
                    'study_type': self._safe_get(trial, 'StudyType', 0),
                    'condition': ', '.join(trial.get('Condition', [])),
                    'intervention': ', '.join(trial.get('InterventionName', [])),
                    'completion_date': self._safe_get(trial, 'PrimaryCompletionDate', 0),
                    'start_date': self._safe_get(trial, 'StudyFirstPostDate', 0),
                    'sponsor': self._safe_get(trial, 'LeadSponsorName', 0),
                    'countries': ', '.join(trial.get('LocationCountry', []))
                }
                trials.append(processed_trial)
            except Exception as e:
                logger.error(f"Error processing trial: {e}")
                continue
        
        return trials
    
    def _safe_get(self, data: Dict, key: str, index: int) -> str:
        """Safely get value from trial data"""
        try:
            value = data.get(key, [''])
            return value[index] if isinstance(value, list) and len(value) > index else ''
        except:
            return ''

class CompanyWebsiteScraper:
    """Scrape company websites for additional data"""
    
    def __init__(self):
        self.session = None
        
    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=20)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def get_company_website(self, symbol: str) -> Optional[str]:
        """Get company website from Yahoo Finance"""
        url = f"https://finance.yahoo.com/quote/{symbol}/profile"
        
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    soup = BeautifulSoup(await response.text(), 'html.parser')
                    
                    # Look for website link
                    website_elem = soup.find('a', {'data-test': 'website-link'})
                    if website_elem and website_elem.get('href'):
                        return website_elem['href']
                    
                    # Alternative: look for website in text
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        if any(domain in href.lower() for domain in ['.com', '.org', '.net']) and 'mailto:' not in href:
                            if not any(exclude in href.lower() for exclude in ['yahoo', 'sec.gov', 'edgar']):
                                return href
                                
        except Exception as e:
            logger.error(f"Error getting website for {symbol}: {e}")
        
        return None
    
    async def scrape_basic_company_data(self, symbol: str, website: str) -> Dict:
        """Scrape basic company information from website"""
        try:
            async with self.session.get(website) as response:
                if response.status == 200:
                    soup = BeautifulSoup(await response.text(), 'html.parser')
                    
                    return {
                        'symbol': symbol,
                        'website': website,
                        'title': soup.title.string if soup.title else '',
                        'description': self._extract_description(soup),
                        'has_pipeline_page': await self._check_pipeline_page(website),
                        'has_investor_page': await self._check_investor_page(website),
                        'last_scraped': datetime.now().isoformat()
                    }
        except Exception as e:
            logger.error(f"Error scraping {website}: {e}")
            
        return {'symbol': symbol, 'error': 'Failed to scrape'}
    
    def _extract_description(self, soup: BeautifulSoup) -> str:
        """Extract company description from meta tags or content"""
        # Try meta description first
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            return meta_desc['content'][:500]
        
        # Try og:description
        og_desc = soup.find('meta', attrs={'property': 'og:description'})
        if og_desc and og_desc.get('content'):
            return og_desc['content'][:500]
        
        return ''
    
    async def _check_pipeline_page(self, website: str) -> bool:
        """Check if company has a pipeline page"""
        pipeline_urls = [
            f"{website}/pipeline",
            f"{website}/product-pipeline", 
            f"{website}/research/pipeline"
        ]
        
        for url in pipeline_urls:
            try:
                async with self.session.get(url) as response:
                    if response.status == 200:
                        return True
            except:
                continue
        return False
    
    async def _check_investor_page(self, website: str) -> bool:
        """Check if company has an investor relations page"""
        ir_urls = [
            f"{website}/investors",
            f"{website}/investor-relations",
            f"{website}/ir"
        ]
        
        for url in ir_urls:
            try:
                async with self.session.get(url) as response:
                    if response.status == 200:
                        return True
            except:
                continue
        return False

class CGTDataOrchestrator:
    """Main orchestrator for collecting all CGT data"""
    
    def __init__(self):
        self.data = {
            'companies': [],
            'fda_approvals': [],
            'clinical_trials': [],
            'company_websites': [],
            'collection_timestamp': None
        }
    
    async def collect_all_data(self, max_companies: int = 50) -> Dict:
        """Collect all free tier data"""
        logger.info("Starting comprehensive CGT data collection...")
        start_time = time.time()
        
        # Step 1: Get ETF holdings
        logger.info("Step 1: Collecting ETF holdings...")
        async with ETFHoldingsScraper() as etf_scraper:
            companies = await etf_scraper.get_combined_holdings()
        
        self.data['companies'] = [
            {
                'symbol': c.symbol,
                'name': c.name,
                'website': c.website,
                'market_cap': c.market_cap,
                'xbi_weight': c.xbi_weight,
                'nbi_weight': c.nbi_weight
            } for c in companies[:max_companies]
        ]
        
        # Step 2: Get FDA approvals
        logger.info("Step 2: Collecting FDA approvals...")
        async with FDADataCollector() as fda_collector:
            fda_approvals = await fda_collector.get_recent_drug_approvals(days_back=180)
        
        self.data['fda_approvals'] = fda_approvals
        
        # Step 3: Get clinical trials for top companies
        logger.info("Step 3: Collecting clinical trials...")
        ct_collector = ClinicalTrialsCollector()
        all_trials = []
        
        for company in companies[:10]:  # Top 10 companies by ETF weight
            trials = await ct_collector.search_company_trials(company.name)
            all_trials.extend(trials)
            await asyncio.sleep(1)  # Be respectful to the API
        
        self.data['clinical_trials'] = all_trials
        
        # Step 4: Scrape company websites
        logger.info("Step 4: Scraping company websites...")
        async with CompanyWebsiteScraper() as web_scraper:
            website_data = []
            
            for company in companies[:20]:  # Top 20 companies
                # Get website URL
                website = await web_scraper.get_company_website(company.symbol)
                if website:
                    # Scrape basic data
                    company_data = await web_scraper.scrape_basic_company_data(company.symbol, website)
                    website_data.append(company_data)
                    await asyncio.sleep(2)  # Be respectful
        
        self.data['company_websites'] = website_data
        
        # Finalize
        self.data['collection_timestamp'] = datetime.now().isoformat()
        
        elapsed_time = time.time() - start_time
        logger.info(f"Data collection completed in {elapsed_time:.1f} seconds")
        
        return self.data
    
    def save_to_files(self, output_dir: str = "data"):
        """Save collected data to JSON files"""
        import os
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Save each dataset
        for key, data in self.data.items():
            if key != 'collection_timestamp':
                filename = f"{output_dir}/{key}_{datetime.now().strftime('%Y%m%d')}.json"
                with open(filename, 'w') as f:
                    json.dump(data, f, indent=2, default=str)
                logger.info(f"Saved {len(data)} records to {filename}")
        
        # Save combined data
        combined_file = f"{output_dir}/cgt_data_combined_{datetime.now().strftime('%Y%m%d')}.json"
        with open(combined_file, 'w') as f:
            json.dump(self.data, f, indent=2, default=str)
        logger.info(f"Saved combined data to {combined_file}")
    
    def to_dataframes(self) -> Dict[str, pd.DataFrame]:
        """Convert collected data to pandas DataFrames for analysis"""
        dfs = {}
        
        for key, data in self.data.items():
            if key != 'collection_timestamp' and data:
                try:
                    dfs[key] = pd.DataFrame(data)
                except Exception as e:
                    logger.error(f"Error creating DataFrame for {key}: {e}")
        
        return dfs

# FastAPI Web Server for real-time dashboard
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn

app = FastAPI(title="CGT Data Collection API", version="1.0.0")

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global data store
global_data_store = {}

@app.get("/")
async def root():
    return {"message": "CGT Data Collection API is running"}

@app.get("/api/companies")
async def get_companies():
    """Get all ETF companies"""
    return global_data_store.get('companies', [])

@app.get("/api/fda-approvals")
async def get_fda_approvals():
    """Get recent FDA approvals"""
    return global_data_store.get('fda_approvals', [])

@app.get("/api/clinical-trials")
async def get_clinical_trials():
    """Get clinical trials data"""
    return global_data_store.get('clinical_trials', [])

@app.get("/api/company-websites")
async def get_company_websites():
    """Get scraped company website data"""
    return global_data_store.get('company_websites', [])

@app.get("/api/stats")
async def get_stats():
    """Get collection statistics"""
    return {
        'companies_count': len(global_data_store.get('companies', [])),
        'fda_approvals_count': len(global_data_store.get('fda_approvals', [])),
        'clinical_trials_count': len(global_data_store.get('clinical_trials', [])),
        'websites_scraped': len(global_data_store.get('company_websites', [])),
        'last_updated': global_data_store.get('collection_timestamp')
    }

@app.post("/api/collect")
async def trigger_collection():
    """Trigger new data collection"""
    try:
        orchestrator = CGTDataOrchestrator()
        data = await orchestrator.collect_all_data(max_companies=30)
        
        # Update global store
        global_data_store.update(data)
        
        return {"status": "success", "message": "Data collection completed"}
    
    except Exception as e:
        logger.error(f"Collection failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/company/{symbol}")
async def get_company_details(symbol: str):
    """Get detailed information for a specific company"""
    companies = global_data_store.get('companies', [])
    company = next((c for c in companies if c['symbol'] == symbol.upper()), None)
    
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    
    # Get related data
    fda_data = [d for d in global_data_store.get('fda_approvals', []) 
                if d.get('company', '').lower() in company['name'].lower()]
    
    trials = [t for t in global_data_store.get('clinical_trials', [])
              if company['name'].lower() in t.get('sponsor', '').lower()]
    
    website_data = next((w for w in global_data_store.get('company_websites', [])
                        if w['symbol'] == symbol.upper()), {})
    
    return {
        'company': company,
        'fda_approvals': fda_data,
        'clinical_trials': trials,
        'website_data': website_data
    }

# Background task to collect data periodically
async def periodic_collection():
    """Run data collection every 4 hours"""
    while True:
        try:
            logger.info("Starting periodic data collection...")
            orchestrator = CGTDataOrchestrator()
            data = await orchestrator.collect_all_data(max_companies=50)
            global_data_store.update(data)
            logger.info("Periodic collection completed")
            
        except Exception as e:
            logger.error(f"Periodic collection failed: {e}")
        
        # Wait 4 hours
        await asyncio.sleep(4 * 60 * 60)

@app.on_event("startup")
async def startup_event():
    """Run initial data collection on startup"""
    # Run initial collection
    try:
        logger.info("Running initial data collection...")
        orchestrator = CGTDataOrchestrator()
        data = await orchestrator.collect_all_data(max_companies=30)
        global_data_store.update(data)
        logger.info("Initial collection completed")
    except Exception as e:
        logger.error(f"Initial collection failed: {e}")
    
    # Start background collection task
    asyncio.create_task(periodic_collection())

# Command-line interface
async def main():
    """Main function for command-line usage"""
    print("🧬 CGT Data Collector - Free Tier Implementation")
    print("=" * 50)
    
    orchestrator = CGTDataOrchestrator()
    
    try:
        # Collect all data
        data = await orchestrator.collect_all_data(max_companies=50)
        
        # Print summary
        print(f"\n📊 Collection Summary:")
        print(f"   Companies: {len(data['companies'])}")
        print(f"   FDA Approvals: {len(data['fda_approvals'])}")
        print(f"   Clinical Trials: {len(data['clinical_trials'])}")
        print(f"   Websites Scraped: {len(data['company_websites'])}")
        
        # Save to files
        orchestrator.save_to_files()
        print(f"\n💾 Data saved to 'data/' directory")
        
        # Convert to DataFrames for analysis
        dfs = orchestrator.to_dataframes()
        print(f"\n📈 DataFrames created: {list(dfs.keys())}")
        
        # Show sample data
        if 'companies' in dfs and not dfs['companies'].empty:
            print(f"\n🏢 Top 10 Companies by XBI Weight:")
            top_companies = dfs['companies'].nlargest(10, 'xbi_weight')[['symbol', 'name', 'xbi_weight', 'market_cap']]
            print(top_companies.to_string(index=False))
        
        if 'fda_approvals' in dfs and not dfs['fda_approvals'].empty:
            print(f"\n💊 Recent FDA Approvals:")
            recent_approvals = dfs['fda_approvals'][['drug_name', 'company', 'approval_date']].head(5)
            print(recent_approvals.to_string(index=False))
        
        print(f"\n✅ Collection completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error during collection: {e}")
        logger.error(f"Main execution failed: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "server":
        # Run as web server
        uvicorn.run(app, host="0.0.0.0", port=8000)
    else:
        # Run as command-line script
        asyncio.run(main())
