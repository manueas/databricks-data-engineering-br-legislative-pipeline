import json
import logging
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp

from utils.model import IngestionConfig

logger = logging.getLogger(__name__)

def save_to_volume(config: IngestionConfig, 
                   data: list, 
                   spark: SparkSession,
                   save_mode: str="append") -> None:
    """
    Centralized function to save raw API data to the Landing Zone (JSON)
    and append/overwrite it into the Bronze layer (Delta Table).
    
    Uses Python native file operations for Unity Catalog Volumes (POSIX/FUSE).
    
    Args:
        config (IngestionConfig): The configuration object for the entity.
        data (list): The list of dictionaries (JSON data) fetched from the API.
        spark (SparkSession): The active Spark session from the notebook.
        save_mode (str): The save mode for the Delta table (append or overwrite).
    """
    if not data:
        logger.warning(f"No data provided to save for {config.entity}.")
        return

    # 1. Save to Landing Zone (Raw JSON)
    os.makedirs(config.landing_path, exist_ok=True)
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_path = f"{config.landing_path}/{config.entity}_{timestamp_str}.json"
    
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    logger.info(f"Raw data saved to {file_path}")
    
    # 2. Identify fields that are 100% null across all records
    field_has_value = {}
    for record in data:
        for key, value in record.items():
            if value is not None:
                field_has_value[key] = True
    
    # Remove fields that are 100% null from all records
    all_fields = set(data[0].keys()) if data else set()
    null_fields = all_fields - set(field_has_value.keys())
    
    if null_fields:
        logger.warning(f"Removing fields with 100% null values: {null_fields}")
        cleaned_data = [{k: v for k, v in record.items() if k not in null_fields} for record in data]
    else:
        cleaned_data = data
    
    # 3. Save to Bronze Layer (Delta Table)
    temp_json_path = f"{file_path}.jsonl"
    with open(temp_json_path, 'w', encoding='utf-8') as f:
        for record in cleaned_data:
            f.write(json.dumps(record, ensure_ascii=False) + '\n')
    
    df = spark.read.json(temp_json_path)
    df = df.withColumn("source_file", lit(file_path)) \
           .withColumn("ingestion_timestamp", current_timestamp())
           
    df.write.format("delta").mode(save_mode).saveAsTable(config.table_name)
    logger.info(f"Success! {len(data)} records saved to {config.table_name} (mode: {save_mode})")
    
    # Clean up temp file
    if os.path.exists(temp_json_path):
        os.remove(temp_json_path)