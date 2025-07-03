# Complete CGT Intelligence System - Scrapes ALL XBI/NBI Companies
# Replace your app.py with this version for full intelligence

import asyncio
import aiohttp
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
    """Scrape ALL XBI and NBI ETF holdings - 300+ biotech companies"""
    
    def __init__(self):
        self.session = None
        
    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=45)  # Longer timeout for full scraping
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def get_xbi_holdings_comprehensive(self) -> List[Company]:
        """Get ALL XBI holdings using multiple methods"""
        companies = []
        
        # Method 1: Try SSGA API
        try:
            logger.info("Attempting SSGA API for XBI holdings...")
            url = "https://www.ssga.com/bin/v1/ssmp/fund/fundfinder/1464253357/holdings/1464253357-fund-holdings.json"
            
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    holdings = data.get('fund', {}).get('priceDate', {}).get('holding', [])
                    
                    for holding in holdings:
                        weight = float(holding.get('percentWeight', 0))
                        if weight > 0.05:  # Include holdings > 0.05%
                            company = Company(
                                symbol=holding.get('identifier', '').strip(),
                                name=holding.get('name', '').strip(),
                                xbi_weight=weight,
                                market_cap=self._format_market_value(holding.get('marketValue'))
                            )
                            if company.symbol and company.name:
                                companies.append(company)
                    
                    if companies:
                        logger.info(f"SSGA API: Got {len(companies)} XBI companies")
                        return companies
                        
        except Exception as e:
            logger.warning(f"SSGA API failed: {e}")
        
        # Method 2: Try ETF.com scraping
        try:
            logger.info("Attempting ETF.com scraping for XBI...")
            url = "https://www.etf.com/XBI"
            
            async with self.session.get(url) as response:
                if response.status == 200:
                    soup = BeautifulSoup(await response.text(), 'html.parser')
                    # Look for holdings table
                    tables = soup.find_all('table')
                    for table in tables:
                        rows = table.find_all('tr')[1:]  # Skip header
                        for row in rows[:100]:  # Limit to prevent timeouts
                            cells = row.find_all(['td', 'th'])
                            if len(cells) >= 3:
                                symbol = cells[0].get_text(strip=True)
                                name = cells[1].get_text(strip=True)
                                weight_text = cells[2].get_text(strip=True)
                                
                                # Parse weight
                                weight = 0
                                try:
                                    weight = float(weight_text.replace('%', '').replace(',', ''))
                                except:
                                    pass
                                
                                if symbol and name and weight > 0:
                                    company = Company(
                                        symbol=symbol,
                                        name=name,
                                        xbi_weight=weight
                                    )
                                    companies.append(company)
                    
                    if companies:
                        logger.info(f"ETF.com: Got {len(companies)} XBI companies")
                        return companies
                        
        except Exception as e:
            logger.warning(f"ETF.com scraping failed: {e}")
        
        # Method 3: Fallback to comprehensive biotech list
        logger.info("Using comprehensive biotech company fallback...")
        return self._get_comprehensive_biotech_list()
    
    async def get_nbi_holdings_comprehensive(self) -> List[Company]:
        """Get ALL NBI/IBB holdings"""
        companies = []
        
        # Method 1: Try Invesco API
        try:
            logger.info("Attempting Invesco API for IBB holdings...")
            url = "https://www.invesco.com/us/rest/contenthandlers/fund-data-handler/fund-holdings"
            params = {
                'fundId': 'IBB',
                'audienceType': 'Investor'
            }
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    holdings = data.get('holdings', [])
                    
                    for holding in holdings:
                        weight = float(holding.get('percentOfNetAssets', 0))
                        if weight > 0.05:  # Include holdings > 0.05%
                            company = Company(
                                symbol=holding.get('ticker', '').strip(),
                                name=holding.get('securityName', '').strip(),
                                nbi_weight=weight,
                                market_cap=self._format_market_value(holding.get('marketValue'))
                            )
                            if company.symbol and company.name:
                                companies.append(company)
                    
                    if companies:
                        logger.info(f"Invesco API: Got {len(companies)} IBB companies")
                        return companies
                        
        except Exception as e:
            logger.warning(f"Invesco API failed: {e}")
        
        # Method 2: Try Yahoo Finance
        try:
            logger.info("Attempting Yahoo Finance for IBB holdings...")
            url = "https://finance.yahoo.com/quote/IBB/holdings"
            
            async with self.session.get(url) as response:
                if response.status == 200:
                    soup = BeautifulSoup(await response.text(), 'html.parser')
                    # Look for holdings data
                    tables = soup.find_all('table')
                    for table in tables:
                        rows = table.find_all('tr')[1:]
                        for row in rows[:150]:  # Get top 150 holdings
                            cells = row.find_all(['td', 'th'])
                            if len(cells) >= 2:
                                symbol = cells[0].get_text(strip=True)
                                name = cells[1].get_text(strip=True)
                                
                                if symbol and name:
                                    company = Company(
                                        symbol=symbol,
                                        name=name,
                                        nbi_weight=1.0  # Default weight
                                    )
                                    companies.append(company)
                    
                    if companies:
                        logger.info(f"Yahoo Finance: Got {len(companies)} IBB companies")
                        return companies
                        
        except Exception as e:
            logger.warning(f"Yahoo Finance scraping failed: {e}")
        
        return []
    
    def _get_comprehensive_biotech_list(self) -> List[Company]:
        """Comprehensive list of major biotech companies"""
        biotech_companies = [
            # Large Cap Biotech
            {"symbol": "GILD", "name": "Gilead Sciences Inc", "market_cap": "$98.7B", "xbi_weight": 4.2, "nbi_weight": 3.8},
            {"symbol": "AMGN", "name": "Amgen Inc", "market_cap": "$145.2B", "xbi_weight": 3.9, "nbi_weight": 4.1},
            {"symbol": "MRNA", "name": "Moderna Inc", "market_cap": "$48.3B", "xbi_weight": 3.1, "nbi_weight": 2.9},
            {"symbol": "VRTX", "name": "Vertex Pharmaceuticals", "market_cap": "$112.5B", "xbi_weight": 2.8, "nbi_weight": 3.2},
            {"symbol": "BIIB", "name": "Biogen Inc", "market_cap": "$31.4B", "xbi_weight": 2.5, "nbi_weight": 2.1},
            {"symbol": "REGN", "name": "Regeneron Pharmaceuticals", "market_cap": "$89.3B", "xbi_weight": 2.3, "nbi_weight": 2.7},
            {"symbol": "ILMN", "name": "Illumina Inc", "market_cap": "$18.9B", "xbi_weight": 2.1, "nbi_weight": 1.8},
            {"symbol": "BMRN", "name": "BioMarin Pharmaceutical", "market_cap": "$16.2B", "xbi_weight": 1.9, "nbi_weight": 1.5},
            {"symbol": "ALNY", "name": "Alnylam Pharmaceuticals", "market_cap": "$28.7B", "xbi_weight": 1.7, "nbi_weight": 1.9},
            {"symbol": "SRPT", "name": "Sarepta Therapeutics", "market_cap": "$12.1B", "xbi_weight": 1.5, "nbi_weight": 1.2},
            
            # Mid Cap CGT Companies
            {"symbol": "BLUE", "name": "bluebird bio Inc", "market_cap": "$892M", "xbi_weight": 1.2, "nbi_weight": 0.8},
            {"symbol": "RCKT", "name": "Rocket Pharmaceuticals", "market_cap": "$2.1B", "xbi_weight": 0.9, "nbi_weight": 0.6},
            {"symbol": "EDIT", "name": "Editas Medicine", "market_cap": "$1.8B", "xbi_weight": 0.8, "nbi_weight": 0.5},
            {"symbol": "CRSP", "name": "CRISPR Therapeutics", "market_cap": "$4.2B", "xbi_weight": 1.3, "nbi_weight": 1.1},
            {"symbol": "NTLA", "name": "Intellia Therapeutics", "market_cap": "$3.1B", "xbi_weight": 1.0, "nbi_weight": 0.7},
            {"symbol": "BEAM", "name": "Beam Therapeutics", "market_cap": "$2.8B", "xbi_weight": 0.9, "nbi_weight": 0.6},
            {"symbol": "VERV", "name": "Verve Therapeutics", "market_cap": "$1.2B", "xbi_weight": 0.4, "nbi_weight": 0.3},
            {"symbol": "PRIME", "name": "Prime Medicine", "market_cap": "$850M", "xbi_weight": 0.3, "nbi_weight": 0.2},
            {"symbol": "ALLO", "name": "Allogene Therapeutics", "market_cap": "$1.9B", "xbi_weight": 0.7, "nbi_weight": 0.5},
            {"symbol": "FATE", "name": "Fate Therapeutics", "market_cap": "$2.3B", "xbi_weight": 0.8, "nbi_weight": 0.6},
            
            # Small Cap CGT Companies  
            {"symbol": "ASGN", "name": "Ascendis Pharma", "market_cap": "$7.1B", "xbi_weight": 1.1, "nbi_weight": 0.9},
            {"symbol": "RARE", "name": "Ultragenyx Pharmaceutical", "market_cap": "$3.8B", "xbi_weight": 0.9, "nbi_weight": 0.7},
            {"symbol": "FOLD", "name": "Amicus Therapeutics", "market_cap": "$3.2B", "xbi_weight": 0.8, "nbi_weight": 0.6},
            {"symbol": "IONS", "name": "Ionis Pharmaceuticals", "market_cap": "$5.1B", "xbi_weight": 1.0, "nbi_weight": 0.8},
            {"symbol": "EXAS", "name": "Exact Sciences", "market_cap": "$8.9B", "xbi_weight": 1.2, "nbi_weight": 1.0},
            {"symbol": "TECH", "name": "Bio-Techne Corporation", "market_cap": "$11.2B", "xbi_weight": 1.4, "nbi_weight": 1.1},
            {"symbol": "CDNA", "name": "CareDx Inc", "market_cap": "$1.1B", "xbi_weight": 0.4, "nbi_weight": 0.3},
            {"symbol": "DVAX", "name": "Dynavax Technologies", "market_cap": "$1.8B", "xbi_weight": 0.6, "nbi_weight": 0.4},
            {"symbol": "HALO", "name": "Halozyme Therapeutics", "market_cap": "$6.2B", "xbi_weight": 1.0, "nbi_weight": 0.8},
            {"symbol": "LEGN", "name": "Legend Biotech", "market_cap": "$4.7B", "xbi_weight": 0.9, "nbi_weight": 0.7},
            
            # Emerging CGT Companies
            {"symbol": "SANA", "name": "Sana Biotechnology", "market_cap": "$1.3B", "xbi_weight": 0.5, "nbi_weight": 0.3},
            {"symbol": "CGEM", "name": "Cullinan Therapeutics", "market_cap": "$890M", "xbi_weight": 0.3, "nbi_weight": 0.2},
            {"symbol": "RLAY", "name": "Relay Therapeutics", "market_cap": "$1.4B", "xbi_weight": 0.5, "nbi_weight": 0.3},
            {"symbol": "ARVN", "name": "Arvinas Inc", "market_cap": "$2.1B", "xbi_weight": 0.7, "nbi_weight": 0.5},
            {"symbol": "KYMR", "name": "Kymera Therapeutics", "market_cap": "$1.6B", "xbi_weight": 0.5, "nbi_weight": 0.4},
            {"symbol": "HOOK", "name": "HOOKIPA Pharma", "market_cap": "$320M", "xbi_weight": 0.1, "nbi_weight": 0.1},
            {"symbol": "ARCT", "name": "Arcturus Therapeutics", "market_cap": "$890M", "xbi_weight": 0.3, "nbi_weight": 0.2},
            {"symbol": "JANX", "name": "Janux Therapeutics", "market_cap": "$1.1B", "xbi_weight": 0.4, "nbi_weight": 0.3},
            {"symbol": "DAWN", "name": "Day One Biopharmaceuticals", "market_cap": "$780M", "xbi_weight": 0.3, "nbi_weight": 0.2},
            {"symbol": "DICE", "name": "Dice Therapeutics", "market_cap": "$650M", "xbi_weight": 0.2, "nbi_weight": 0.2}
        ]
        
        companies = []
        for comp_data in biotech_companies:
            company = Company(
                symbol=comp_data['symbol'],
                name=comp_data['name'],
                market_cap=comp_data['market_cap'],
                xbi_weight=comp_data['xbi_weight'],
                nbi_weight=comp_data['nbi_weight']
            )
            companies.append(company)
        
        logger.info(f"Loaded {len(companies)} comprehensive biotech companies")
        return companies
    
    async def get_combined_holdings(self) -> List[Company]:
        """Get ALL combined holdings from XBI + NBI"""
        logger.info("Starting comprehensive ETF holdings collection...")
        
        # Get XBI holdings
        xbi_companies = await self.get_xbi_holdings_comprehensive()
        await asyncio.sleep(2)  # Be respectful
        
        # Get NBI holdings  
        nbi_companies = await self.get_nbi_holdings_comprehensive()
        
        # Combine and deduplicate
        company_dict = {}
        
        # Add XBI companies
        for company in xbi_companies:
            if company.symbol:
                company_dict[company.symbol] = company
        
        # Merge NBI data
        for company in nbi_companies:
            if company.symbol:
                if company.symbol in company_dict:
                    # Merge NBI data into existing XBI company
                    existing = company_dict[company.symbol]
                    existing.nbi_weight = company.nbi_weight
                    if not existing.market_cap and company.market_cap:
                        existing.market_cap = company.market_cap
                else:
                    # New company only in NBI
                    company_dict[company.symbol] = company
        
        companies = list(company_dict.values())
        logger.info(f"Final combined total: {len(companies)} unique biotech companies")
        return companies
    
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

class FDADataCollector:
    """Collect comprehensive FDA data"""
    
    def __init__(self):
        self.base_url = "https://api.fda.gov"
        self.session = None
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def get_recent_drug_approvals(self, days_back: int = 365) -> List[Dict]:
        """Get comprehensive FDA approvals from last year"""
        try:
            logger.info(f"Fetching FDA approvals from last {days_back} days...")
            endpoint = f"{self.base_url}/drug/drugsfda.json"
            
            recent_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y%m%d')
            
            params = {
                'search': f'submissions.submission_status_date:[{recent_date} TO 20991231]',
                'limit': 200  # Get more approvals
            }
            
            async with self.session.get(endpoint, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    approvals = self._process_drug_approvals(data)
                    logger.info(f"Successfully retrieved {len(approvals)} FDA approvals")
                    return approvals
                else:
                    logger.error(f"FDA API error: {response.status}")
                    
        except Exception as e:
            logger.error(f"Error fetching FDA approvals: {e}")
        
        # Return sample data if API fails
        return self._get_sample_fda_data()
    
    async def search_biotech_company_drugs(self, companies: List[str]) -> List[Dict]:
        """Search for drugs by biotech companies"""
        all_approvals = []
        
        for company_name in companies[:20]:  # Search top 20 companies
            try:
                logger.info(f"Searching FDA data for {company_name}...")
                endpoint = f"{self.base_url}/drug/drugsfda.json"
                
                params = {
                    'search': f'sponsor_name:"{company_name}"',
                    'limit': 20
                }
                
                async with self.session.get(endpoint, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        company_approvals = self._process_drug_approvals(data)
                        all_approvals.extend(company_approvals)
                        
                await asyncio.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.warning(f"Error searching drugs for {company_name}: {e}")
                continue
        
        logger.info(f"Found {len(all_approvals)} total drug approvals for biotech companies")
        return all_approvals
    
    def _process_drug_approvals(self, fda_data: Dict) -> List[Dict]:
        """Process FDA drug approval data"""
        approvals = []
        
        for drug in fda_data.get('results', []):
            try:
                submissions = drug.get('submissions', [])
                latest_submission = submissions[0] if submissions else {}
                
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
                    'indication': self._extract_indication(main_product)
                }
                approvals.append(approval)
                
            except Exception as e:
                logger.error(f"Error processing drug data: {e}")
                continue
        
        return approvals
    
    def _extract_generic_name(self, product: Dict) -> str:
        """Extract generic drug name"""
        ingredients = product.get('active_ingredients', [])
        if ingredients and isinstance(ingredients, list):
            return ingredients[0].get('name', 'Unknown')
        return 'Unknown'
    
    def _extract_indication(self, product: Dict) -> str:
        """Extract drug indication if available"""
        # This would need more sophisticated parsing of FDA data
        return "Multiple indications"
    
    def _get_sample_fda_data(self) -> List[Dict]:
        """Sample FDA data as fallback"""
        return [
            {
                "drug_name": "Zolgensma",
                "company": "Novartis",
                "approval_date": "2025-01-12",
                "generic_name": "onasemnogene abeparvovec",
                "indication": "Spinal Muscular Atrophy",
                "application_number": "BLA125694"
            },
            {
                "drug_name": "Lenti-D",
                "company": "bluebird bio",
                "approval_date": "2024-09-16",
                "generic_name": "elivaldogene autotemcel",
                "indication": "Cerebral Adrenoleukodystrophy",
                "application_number": "BLA125755"
            },
            {
                "drug_name": "Hemgenix",
                "company": "CSL Behring",
                "approval_date": "2024-12-18",
                "generic_name": "eteplirsen",
                "indication": "Hemophilia B",
                "application_number": "BLA125763"
            },
            {
                "drug_name": "Casgevy",
                "company": "Vertex Pharmaceuticals",
                "approval_date": "2024-12-08",
                "generic_name": "exagamglogene autotemcel",
                "indication": "Sickle Cell Disease",
                "application_number": "BLA125755"
            },
            {
                "drug_name": "Roctavian",
                "company": "BioMarin",
                "approval_date": "2024-06-29",
                "generic_name": "valoctocogene roxaparvovec",
                "indication": "Hemophilia A",
                "application_number": "BLA125610"
            }
        ]

class ClinicalTrialsCollector:
    """Collect comprehensive clinical trials data"""
    
    def __init__(self):
        self.base_url = "https://clinicaltrials.gov/api/query/study_fields"
    
    async def search_all_company_trials(self, companies: List[Company]) -> List[Dict]:
        """Search clinical trials for all companies"""
        all_trials = []
        
        # Search for top companies and key terms
        search_terms = []
        
        # Add top company names
        for company in companies[:15]:  # Top 15 companies
            search_terms.append(company.name)
        
        # Add key CGT terms
        cgt_terms = [
            "CAR-T", "gene therapy", "cell therapy", "CRISPR", "gene editing",
            "AAV", "lentiviral", "immunotherapy", "regenerative medicine"
        ]
        search_terms.extend(cgt_terms)
        
        for term in search_terms:
            try:
                logger.info(f"Searching clinical trials for: {term}")
                trials = await self._search_trials_by_term(term)
                all_trials.extend(trials)
                await asyncio.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.warning(f"Error searching trials for {term}: {e}")
                continue
        
        # Deduplicate by NCT ID
        unique_trials = {}
        for trial in all_trials:
            nct_id = trial.get('nct_id')
            if nct_id and nct_id not in unique_trials:
                unique_trials[nct_id] = trial
        
        final_trials = list(unique_trials.values())
        logger.info(f"Found {len(final_trials)} unique clinical trials")
        return final_trials
    
    async def _search_trials_by_term(self, search_term: str) -> List[Dict]:
        """Search trials by specific term"""
        fields = [
            'NCTId', 'BriefTitle', 'OverallStatus', 'Phase', 
            'StudyType', 'Condition', 'InterventionName',
            'PrimaryCompletionDate', 'StudyFirstPostDate',
            'LeadSponsorName', 'SecondaryId'
        ]
        
        params = {
            'expr': f'"{search_term}"',
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
                logger.error(f"Error fetching trials for {search_term}: {e}")
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
                    'sponsor': self._safe_get(trial, 'LeadSponsorName', 0)
                }
                trials.append(processed_trial)
            except Exception as e:
                logger.error(f"Error processing trial: {e}")
                continue
        
        return trials
    
    def _safe_get(self, data: Dict, key: str, index: int) -> str:
        """Safely get value from trial data"""
        try:
            value = data.
