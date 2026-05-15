from dataclasses import dataclass, field
from typing import Any, Dict, List

@dataclass
class IngestionConfig:
    """
    Configuration data class for the data pipeline.
    Handles dynamic schema and path generation based on the Medallion architecture.
    """
    entity: str  
    layer: str = "bronze"      # Target architectural layer (e.g., bronze, silver, gold)
    domain: str = "camara"     # Domain prefix for schema naming
    catalog: str = "workspace"
    volume: str = "landing_zone"
    base_url: str = ""
    api_params: Dict[str, Any] = field(default_factory=dict)
    required_params: List[str] = field(default_factory=list)
    
    @property
    def schema(self) -> str:
        """Builds the schema name dynamically (e.g., camara_bronze)."""
        return f"{self.domain}_{self.layer}"
    
    @property
    def landing_path(self) -> str:
        """Unity Catalog Volume path for raw JSON data landing."""
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/{self.entity}"
    
    @property
    def table_name(self) -> str:
        """Fully qualified Delta table name (e.g., workspace.camara_bronze.deputados)."""
        return f"{self.catalog}.{self.schema}.{self.entity}"