import json
import logging
import os
import concurrent.futures
from abc import ABC, abstractmethod
from typing import List, Any, Callable, Dict
from pyspark.sql import SparkSession

from utils.model import IngestionConfig
from utils.camara_api import fetch_paginated_data
from utils.storage import save_to_volume
from utils.file_handler import download_raw_file, download_and_extract_zip
from utils.path_utils import find_project_root, make_absolute_path

logger = logging.getLogger(__name__)


class BaseBronzeIngestion(ABC):
    """
    Framework-level Base Class for API Ingestions.
    Orchestrates the lifecycle from config loading to storage, providing multiple extraction engines.
    """

    def __init__(self, 
                 spark: SparkSession, 
                 entity_name: str):
        """
        Initializes the ingestion base class with Spark session.

        Args:
            spark (SparkSession): The active Spark session.
            entity_name (str): The entity name (e.g., 'deputados', 'despesas'). 
                              Used to automatically locate 'configs/{entity_name}.json'.
        
        Example:
            ingestion = DeputadosIngestion(spark=spark, entity_name='deputados')
        """
        self.project_root = find_project_root()
        self.entity_name = entity_name
        
        # Construct paths automatically from entity name
        self.generic_path = os.path.join(self.project_root, 'configs', 'generic.json')
        self.entity_path = os.path.join(self.project_root, 'configs', f'{entity_name}.json')
        
        self.spark = spark
        self._load_configs()

    def _load_configs(self) -> None:
        """Loads and merges the generic and entity JSON files."""
        
        logger.info(f"Loading Global Config: {self.generic_path}")
        with open(self.generic_path, 'r', encoding='utf-8') as f:
            self.generic_config = json.load(f)
            
        logger.info(f"Loading Entity Config: {self.entity_path}")
        with open(self.entity_path, 'r', encoding='utf-8') as f:
            self.entity_config = json.load(f)
            
        # Create IngestionConfig with only the fields it accepts
        self.config = IngestionConfig(
            entity=self.entity_config['entity'],
            catalog=self.generic_config.get('catalog', 'workspace'),
            layer=self.generic_config.get('layer', 'bronze')
        )

    @abstractmethod
    def fetch_data(self) -> List[Dict[str, Any]]:
        """
        Abstract method to be implemented by child classes. 
        Defines the specific logic for data extraction (Standard, Multi-threaded, or Fallback).

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing the raw data.
        """
        pass

    def _fetch_with_fallback(self, 
                            fetch_strategy: Callable[[], List[Dict[str, Any]]], 
                            strategy_name: str = "API",
                            current_params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Wrapper method that tries a fetch strategy and automatically falls back to file download if it fails.
        
        Args:
            fetch_strategy (Callable): The primary fetch strategy function to execute.
            strategy_name (str): Name of the strategy for logging purposes.
            current_params (Dict[str, Any]): Current parameters for placeholder substitution in fallback URLs.
        
        Returns:
            List[Dict[str, Any]]: The fetched data from either the strategy or fallback.
        """
        # Try primary strategy first
        try:
            logger.info(f"Attempting {strategy_name} strategy for {self.config.entity}...")
            data = fetch_strategy()
            if data:
                logger.info(f"{strategy_name} strategy succeeded: {len(data)} records fetched")
                return data
            else:
                logger.warning(f"WARNING: {strategy_name} strategy returned no data")
        except Exception as e:
            logger.error(f"ERROR: {strategy_name} strategy failed: {str(e)}")
        
        # Fallback: Check if file download is configured
        fallback_url = self._get_fallback_url(current_params or {})
        if not fallback_url:
            logger.error(f"No fallback configured for {self.config.entity}. Exiting.")
            return []
        
        logger.warning(f"Activating fallback: downloading from {fallback_url}")
        return self._execute_file_fallback(fallback_url)

    def _get_fallback_url(self, current_params: Dict[str, Any] = None) -> str:
        """
        Gets the fallback URL from entity config, supporting dynamic parameter substitution.
        
        Args:
            current_params (Dict[str, Any]): Current parameters to use for placeholder substitution.
        
        Returns:
            str: The fallback URL with parameters substituted, or empty string if not configured.
        """
        if current_params is None:
            current_params = {}
        
        # Enrich params with extracted year if dataInicio is present
        params_with_year = current_params.copy()
        if 'dataInicio' in params_with_year and 'ano' not in params_with_year:
            # Extract year from dataInicio (format: YYYY-MM-DD)
            data_inicio = params_with_year['dataInicio']
            if isinstance(data_inicio, str) and len(data_inicio) >= 4:
                params_with_year['ano'] = data_inicio[:4]
            
        # Helper function to substitute placeholders
        def substitute_placeholders(url: str) -> str:
            for key, value in params_with_year.items():
                placeholder = f"{{{key}}}"
                if placeholder in url:
                    # Handle list params (take first value)
                    actual_value = value[0] if isinstance(value, list) else value
                    url = url.replace(placeholder, str(actual_value))
            return url
        
        # Check for file_url (direct JSON/ZIP download)
        if 'file_url' in self.entity_config:
            return substitute_placeholders(self.entity_config['file_url'])
        
        # Check for file_path (may contain placeholders like {ano})
        if 'file_path' in self.entity_config:
            return substitute_placeholders(self.entity_config['file_path'])
        
        return ""

    def _execute_file_fallback(self, url: str) -> List[Dict[str, Any]]:
        """
        Executes the file download fallback and parses the data.
        
        Args:
            url (str): The URL to download from.
        
        Returns:
            List[Dict[str, Any]]: Parsed data from the downloaded file.
        """
        is_zip = url.endswith('.zip')
        landing_dir = self.config.landing_path
        
        try:
            # Download file
            if is_zip:
                logger.info(f"Downloading and extracting ZIP from {url}")
                extracted_files = download_and_extract_zip(url=url, extract_dir=landing_dir)
                if not extracted_files:
                    logger.error("No files extracted from ZIP")
                    return []
                file_path = extracted_files[0]  # Use first file
            else:
                logger.info(f"Downloading JSON from {url}")
                file_name = f"{self.config.entity}_fallback.json"
                target_path = os.path.join(landing_dir, file_name)
                file_path = download_raw_file(url=url, target_path=target_path)
            
            # Parse JSON file
            logger.info(f"Reading downloaded file: {file_path}")
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Handle different JSON structures
            if isinstance(data, dict):
                # Check for common API response structures
                if 'dados' in data:
                    data = data['dados']
                elif 'data' in data:
                    data = data['data']
            
            if not isinstance(data, list):
                logger.error(f"Unexpected data format: {type(data)}")
                return []
            
            logger.info(f"Fallback successful: {len(data)} records loaded from file")
            return data
            
        except Exception as e:
            logger.error(f"ERROR: Fallback download failed: {str(e)}")
            return []

    def _fetch_standard(self, params: Dict[str, Any], required_params: List[str] = None, paginated: bool = True) -> List[Dict[str, Any]]:
        """
        Internal Engine 1: Standard paginated extraction for root entities.
        Now wrapped with automatic fallback on failure.

        Args:
            params (Dict[str, Any]): Dictionary of API query parameters.
            required_params (List[str], optional): List of parameters required by the endpoint. Defaults to None.
            paginated (bool): Whether the endpoint supports pagination. Defaults to True.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing the raw data fetched.
        """
        def fetch_strategy():
            return fetch_paginated_data(
                endpoint=self.config.entity,
                base_url=self.generic_config.get('base_url'),
                params=params,
                required_params=required_params,
                paginated=paginated
            )
        
        return self._fetch_with_fallback(fetch_strategy, strategy_name="Standard API", current_params=params)

    def _fetch_multithreaded(self, 
                             source_table: str, 
                             id_column: str, 
                             endpoint_builder: Callable[[int], str], 
                             foreign_key: str, 
                             params: Dict[str, Any],
                             paginated: bool = True) -> List[Dict[str, Any]]:
        """
        Internal Engine 2: Parallel extraction for child endpoints requiring parent IDs.
        Now wrapped with automatic fallback on failure.

        Args:
            source_table (str): The Delta table name to retrieve parent IDs from.
            id_column (str): The column name containing the parent IDs.
            endpoint_builder (Callable[[int], str]): Lambda or function to build the specific API endpoint.
            foreign_key (str): The key name to inject the parent ID into each record.
            params (Dict[str, Any]): Dictionary of API query parameters.
            paginated (bool): Whether the endpoint supports pagination. Defaults to True.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing the consolidated raw data.
        """
        def fetch_strategy():
            try:
                df_source = self.spark.read.table(source_table)
                parent_ids = [row[id_column] for row in df_source.select(id_column).distinct().collect()]
                logger.info(f"Retrieved {len(parent_ids)} IDs from {source_table}")
            except Exception as e:
                logger.error(f"Failed to read source table {source_table}: {e}")
                raise

            all_data = []

            def worker(parent_id: int):
                full_endpoint = endpoint_builder(parent_id)
                
                # Each thread needs its own params dict for independent pagination
                thread_params = params.copy()
                
                data = fetch_paginated_data(
                    endpoint=full_endpoint, 
                    base_url=self.generic_config.get('base_url'),
                    params=thread_params,
                    paginated=paginated
                )
                if data:
                    for record in data:
                        record[foreign_key] = parent_id
                return data

            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                results = executor.map(worker, parent_ids)
                for res_list in results:
                    if res_list:
                        all_data.extend(res_list)
            return all_data
        
        return self._fetch_with_fallback(fetch_strategy, strategy_name="Multithreaded API", current_params=params)

    def save(self, 
             data: List[Dict[str, Any]],
             save_mode: str="append") -> None:
        """
        Concrete method that encapsulates the storage logic to save data to the Landing Zone and Bronze layer.

        Args:
            data (List[Dict[str, Any]]): The raw data list to be processed and saved.
            save_mode (str): The save mode for the Delta table (append or overwrite). Defaults to "append".
        """
        save_to_volume(
            config=self.config,
            data=data,
            spark=self.spark,
            save_mode=save_mode,
        )

    def execute(self, save_mode: str="append") -> None:
        """
        The Template Method that defines the immutable ingestion lifecycle: 
        data extraction and storage orchestration.
        """
        logger.info(f"Starting extraction for {self.config.entity}...")
        
        raw_data = self.fetch_data()
        
        if not raw_data:
            logger.warning(f"No data retrieved for {self.config.entity}. Exiting.")
            return

        self.save(raw_data, save_mode)
        logger.info(f"Pipeline for {self.config.entity} completed successfully.")
