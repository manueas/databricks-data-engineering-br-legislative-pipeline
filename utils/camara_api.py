import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import logging

logger = logging.getLogger(__name__)

def _get_resilient_session() -> requests.Session:
    """
    Creates a requests Session with an exponential backoff retry strategy.
    This protects the pipeline against public API instabilities (5xx errors).
    """
    session = requests.Session()
    
    # Configure retry logic: 5 attempts, backoff factor of 1 
    # (waits 0s, 2s, 4s, 8s, 16s between retries)
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def fetch_paginated_data(
    endpoint: str, 
    base_url: str,
    params: dict = None, 
    required_params: list = None,
    paginated: bool = True
) -> list:
    """
    Generic function to fetch paginated data from the Camara API.
    Handles standard pagination, rate limiting, and error logging.
    
    Args:
        endpoint (str): The API endpoint (e.g., 'deputados' or 'proposicoes').
        base_url (str): Base URL for the API (provided from config).
        params (dict): Query parameters to pass to the API.
        required_params (list): List of required parameter names for validation.
        paginated (bool): Whether the endpoint supports pagination. If False, makes a single request without any params.
        
    Returns:
        list: A list containing all fetched records across all pages.
    """

    if params is None:
        params = {}
        
    # --- Contract Validation ---
    if required_params:
        missing_params = [p for p in required_params if p not in params]
        if missing_params:
            raise ValueError(f"CRITICAL: Missing required parameters for endpoint '{endpoint}': {missing_params}")
    
    # Log key parameters at start
    param_summary = {k: v for k, v in params.items() if k not in ['formato', 'pagina']}
    if param_summary:
        logger.info(f"Starting fetch for '{endpoint}' with params: {param_summary}")
    else:
        logger.info(f"Starting fetch for '{endpoint}' (no filters)")
    
    # For non-paginated endpoints, make a single request WITHOUT any parameters
    if not paginated:
        url = f"{base_url}/{endpoint}" if endpoint else base_url
        session = _get_resilient_session()
        
        try:
            # timeout=(connect_timeout, read_timeout)
            # DO NOT send any params - individual resource endpoints don't accept them
            response = session.get(url, timeout=(10, 60))
            
            if response.status_code != 200:
                logger.error(f"API Error at {url} | Status: {response.status_code} | Reason: {response.text}")
                return []
            
            data = response.json()
            
            # Handle different response structures
            if isinstance(data, dict):
                # Try to extract from 'dados' wrapper if present
                data = data.get('dados', data)
                # If still a dict, wrap it in a list (single record response)
                if isinstance(data, dict):
                    return [data]
            
            return data if isinstance(data, list) else []
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed at {url}: {str(e)}")
            return []
        
    # --- Paginated flow (original logic) ---
    # Ensure format is always JSON and starting page is set
    params['formato'] = 'json'
    if 'pagina' not in params:
        params['pagina'] = 1
        
    all_data = []

    # Instantiate the resilient session once per pipeline run
    session = _get_resilient_session()
    
    while True:
        # Construct URL from base_url + endpoint
        url = f"{base_url}/{endpoint}" if endpoint else base_url

        try:
            # timeout=(connect_timeout, read_timeout)
            # 10s to connect, 60s to read response
            response = session.get(url, params=params, timeout=(10, 60))

            if response.status_code != 200:
                logger.error(f"API Error at {url} | Status: {response.status_code} | Reason: {response.text}")
                break
                
            data = response.json().get('dados', [])
            
            if not data:
                # Empty list means no more pages
                break
                
            all_data.extend(data)
            logger.info(f"Fetched {len(data)} records from '{endpoint}' (Page {params['pagina']})")
            
            # Pagination control and rate limit
            params['pagina'] += 1
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            # Catches catastrophic network failures that bypass the Retry adapter
            logger.error(f"Catastrophic network failure at {url}: {str(e)}")
            break
        
    return all_data
