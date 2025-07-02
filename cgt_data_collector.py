# Complete Full-Stack CGT Intelligence App
# Save as: app.py

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
                    'marketing_status': main_product.get('marketing_status')
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
            'LeadSponsorName'
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
            value = data.get(key, [''])
            return value[index] if isinstance(value, list) and len(value) > index else ''
        except:
            return ''

class CGTDataOrchestrator:
    """Main orchestrator for collecting all CGT data"""
    
    def __init__(self):
        self.data = {
            'companies': [],
            'fda_approvals': [],
            'clinical_trials': [],
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
                'xbi_weight': c.xbi_weight or 0,
                'nbi_weight': c.nbi_weight or 0
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
        
        for company in companies[:5]:  # Top 5 companies by ETF weight
            if company and company.name:  # Add safety check
                trials = await ct_collector.search_company_trials(company.name)
                all_trials.extend(trials)
                await asyncio.sleep(1)  # Be respectful to the API
        
        self.data['clinical_trials'] = all_trials
        
        # Finalize
        self.data['collection_timestamp'] = datetime.now().isoformat()
        
        elapsed_time = time.time() - start_time
        logger.info(f"Data collection completed in {elapsed_time:.1f} seconds")
        
        return self.data

# FastAPI with HTML Frontend
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

app = FastAPI(title="CGT Intelligence Dashboard", version="1.0.0")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global data store
global_data_store = {}

# HTML Template for the dashboard
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CGT Intelligence Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        .loading { animation: spin 1s linear infinite; }
        @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
    </style>
</head>
<body class="bg-gray-50">
    <!-- Header -->
    <div class="bg-white shadow-sm border-b">
        <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div class="flex justify-between items-center py-4">
                <div class="flex items-center space-x-2">
                    <div class="w-8 h-8 bg-blue-600 rounded-full flex items-center justify-center">
                        <span class="text-white font-bold">🧬</span>
                    </div>
                    <h1 class="text-2xl font-bold text-gray-900">CGT Intelligence</h1>
                </div>
                <div class="flex items-center space-x-4">
                    <button onclick="collectData()" class="bg-blue-600 text-white px-4 py-2 rounded-md hover:bg-blue-700">
                        Refresh Data
                    </button>
                    <div id="lastUpdated" class="text-sm text-gray-500"></div>
                </div>
            </div>
        </div>
    </div>

    <!-- Main Content -->
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
        <!-- Stats Cards -->
        <div class="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
            <div class="bg-white rounded-lg shadow p-6">
                <div class="flex items-center">
                    <div class="flex-shrink-0">
                        <div class="w-8 h-8 bg-green-600 rounded-full flex items-center justify-center">📈</div>
                    </div>
                    <div class="ml-4">
                        <p class="text-sm font-medium text-gray-500">Companies Tracked</p>
                        <p id="companiesCount" class="text-2xl font-semibold text-gray-900">0</p>
                    </div>
                </div>
            </div>
            
            <div class="bg-white rounded-lg shadow p-6">
                <div class="flex items-center">
                    <div class="flex-shrink-0">
                        <div class="w-8 h-8 bg-blue-600 rounded-full flex items-center justify-center">💊</div>
                    </div>
                    <div class="ml-4">
                        <p class="text-sm font-medium text-gray-500">FDA Approvals</p>
                        <p id="fdaCount" class="text-2xl font-semibold text-gray-900">0</p>
                    </div>
                </div>
            </div>
            
            <div class="bg-white rounded-lg shadow p-6">
                <div class="flex items-center">
                    <div class="flex-shrink-0">
                        <div class="w-8 h-8 bg-purple-600 rounded-full flex items-center justify-center">🔬</div>
                    </div>
                    <div class="ml-4">
                        <p class="text-sm font-medium text-gray-500">Clinical Trials</p>
                        <p id="trialsCount" class="text-2xl font-semibold text-gray-900">0</p>
                    </div>
                </div>
            </div>
            
            <div class="bg-white rounded-lg shadow p-6">
                <div class="flex items-center">
                    <div class="flex-shrink-0">
                        <div class="w-8 h-8 bg-orange-600 rounded-full flex items-center justify-center">⚡</div>
                    </div>
                    <div class="ml-4">
                        <p class="text-sm font-medium text-gray-500">Status</p>
                        <p id="systemStatus" class="text-2xl font-semibold text-gray-900">Ready</p>
                    </div>
                </div>
            </div>
        </div>

        <!-- Loading indicator -->
        <div id="loadingIndicator" class="hidden text-center py-8">
            <div class="loading w-8 h-8 border-4 border-blue-600 border-t-transparent rounded-full mx-auto"></div>
            <p class="mt-2 text-gray-600">Collecting biotech intelligence data...</p>
        </div>

        <!-- Companies Table -->
        <div class="bg-white rounded-lg shadow mb-8">
            <div class="px-6 py-4 border-b border-gray-200">
                <h3 class="text-lg font-medium text-gray-900">Top Biotech Companies (XBI/NBI ETFs)</h3>
            </div>
            <div class="overflow-x-auto">
                <table class="min-w-full divide-y divide-gray-200">
                    <thead class="bg-gray-50">
                        <tr>
                            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Symbol</th>
                            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Company</th>
                            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Market Cap</th>
                            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">XBI Weight</th>
                            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">NBI Weight</th>
                        </tr>
                    </thead>
                    <tbody id="companiesTable" class="bg-white divide-y divide-gray-200">
                    </tbody>
                </table>
            </div>
        </div>

        <!-- FDA Approvals -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div class="bg-white rounded-lg shadow">
                <div class="px-6 py-4 border-b border-gray-200">
                    <h3 class="text-lg font-medium text-gray-900">Recent FDA Approvals</h3>
                </div>
                <div id="fdaApprovals" class="p-6">
                </div>
            </div>

            <div class="bg-white rounded-lg shadow">
                <div class="px-6 py-4 border-b border-gray-200">
                    <h3 class="text-lg font-medium text-gray-900">Active Clinical Trials</h3>
                </div>
                <div id="clinicalTrials" class="p-6">
                </div>
            </div>
        </div>
    </div>

    <script>
        let isCollecting = false;

        async function loadDashboard() {
            try {
                const statsResponse = await fetch('/api/stats');
                const stats = await statsResponse.json();
                
                // Update stats
                document.getElementById('companiesCount').textContent = stats.companies_count || 0;
                document.getElementById('fdaCount').textContent = stats.fda_approvals_count || 0;
                document.getElementById('trialsCount').textContent = stats.clinical_trials_count || 0;
                document.getElementById('systemStatus').textContent = stats.companies_count > 0 ? 'Active' : 'Ready';
                
                if (stats.last_updated) {
                    const lastUpdated = new Date(stats.last_updated).toLocaleString();
                    document.getElementById('lastUpdated').textContent = `Updated: ${lastUpdated}`;
                }

                // Load companies if available
                if (stats.companies_count > 0) {
                    await loadCompanies();
                    await loadFDAApprovals();
                    await loadClinicalTrials();
                }
            } catch (error) {
                console.error('Error loading dashboard:', error);
            }
        }

        async function loadCompanies() {
            try {
                const response = await fetch('/api/companies');
                const companies = await response.json();
                
                const tableBody = document.getElementById('companiesTable');
                tableBody.innerHTML = '';
                
                companies.slice(0, 20).forEach(company => {
                    const row = `
                        <tr>
                            <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">${company.symbol}</td>
                            <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">${company.name}</td>
                            <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">${company.market_cap || 'N/A'}</td>
                            <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">${(company.xbi_weight || 0).toFixed(2)}%</td>
                            <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">${(company.nbi_weight || 0).toFixed(2)}%</td>
                        </tr>
                    `;
                    tableBody.innerHTML += row;
                });
            } catch (error) {
                console.error('Error loading companies:', error);
            }
        }

        async function loadFDAApprovals() {
            try {
                const response = await fetch('/api/fda-approvals');
                const approvals = await response.json();
                
                const container = document.getElementById('fdaApprovals');
                container.innerHTML = '';
                
                if (approvals.length === 0) {
                    container.innerHTML = '<p class="text-gray-500">No recent FDA approvals found.</p>';
                    return;
                }
                
                approvals.slice(0, 5).forEach(approval => {
                    const item = `
                        <div class="mb-4 p-3 border border-gray-200 rounded-lg">
                            <h4 class="font-medium text-gray-900">${approval.drug_name}</h4>
                            <p class="text-sm text-gray-600">${approval.company}</p>
                            <p class="text-xs text-gray-500">${approval.approval_date || 'Date pending'}</p>
                        </div>
                    `;
                    container.innerHTML += item;
                });
            } catch (error) {
                console.error('Error loading FDA approvals:', error);
            }
        }

        async function loadClinicalTrials() {
            try {
                const response = await fetch('/api/clinical-trials');
                const trials = await response.json();
                
                const container = document.getElementById('clinicalTrials');
                container.innerHTML = '';
                
                if (trials.length === 0) {
                    container.innerHTML = '<p class="text-gray-500">No clinical trials data available.</p>';
                    return;
                }
                
                trials.slice(0, 5).forEach(trial => {
                    const item = `
                        <div class="mb-4 p-3 border border-gray-200 rounded-lg">
                            <h4 class="font-medium text-gray-900">${trial.title}</h4>
                            <p class="text-sm text-gray-600">${trial.sponsor}</p>
                            <div class="flex space-x-2 mt-1">
                                <span class="bg-blue-100 text-blue-800 text-xs px-2 py-1 rounded">${trial.phase || 'N/A'}</span>
                                <span class="bg-gray-100 text-gray-800 text-xs px-2 py-1 rounded">${trial.status}</span>
                            </div>
                        </div>
                    `;
                    container.innerHTML += item;
                });
            } catch (error) {
                console.error('Error loading clinical trials:', error);
            }
        }

        async function collectData() {
            if (isCollecting) return;
            
            isCollecting = true;
            document.getElementById('loadingIndicator').classList.remove('hidden');
            document.getElementById('systemStatus').textContent = 'Collecting...';
            
            try {
                const response = await fetch('/api/collect', { method: 'POST' });
                const result = await response.json();
                
                if (result.status === 'success') {
                    await loadDashboard();
                }
            } catch (error) {
                console.error('Error collecting data:', error);
                alert('Error collecting data. Please try again.');
            } finally {
                isCollecting = false;
                document.getElementById('loadingIndicator').classList.add('hidden');
            }
        }

        // Initialize dashboard
        loadDashboard();
        
        // Auto-refresh every 5 minutes
        setInterval(loadDashboard, 5 * 60 * 1000);
    </script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the HTML dashboard"""
    return HTML_TEMPLATE

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

@app.get("/api/stats")
async def get_stats():
    """Get collection statistics"""
    return {
        'companies_count': len(global_data_store.get('companies', [])),
        'fda_approvals_count': len(global_data_store.get('fda_approvals', [])),
        'clinical_trials_count': len(global_data_store.get('clinical_trials', [])),
        'last_updated': global_data_store.get('collection_timestamp'),
        'status': 'active'
    }

@app.post("/api/collect")
async def trigger_collection():
    """Trigger new data collection"""
    try:
        orchestrator = CGTDataOrchestrator()
        data = await orchestrator.collect_all_data(max_companies=30)
        
        # Update global store
        global_data_store.update(data)
        
        return {"status": "success", "message": "Data collection completed", "timestamp": data['collection_timestamp']}
    
    except Exception as e:
        logger.error(f"Collection failed: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/api/company/{symbol}")
async def get_company_details(symbol: str):
    """Get detailed information for a specific company"""
    companies = global_data_store.get('companies', [])
    company = next((c for c in companies if c['symbol'] == symbol.upper()), None)
    
    if not company:
        return {"error": "Company not found"}
    
    # Get related data
    fda_data = [d for d in global_data_store.get('fda_approvals', []) 
                if d.get('company', '').lower() in company['name'].lower()]
    
    trials = [t for t in global_data_store.get('clinical_trials', [])
              if company['name'].lower() in t.get('sponsor', '').lower()]
    
    return {
        'company': company,
        'fda_approvals': fda_data,
        'clinical_trials': trials
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
    # Start background collection task
    asyncio.create_task(periodic_collection())

if __name__ == "__main__":
    import sys
    
    # Run as web server
    uvicorn.run(app, host="0.0.0.0", port=8000)
