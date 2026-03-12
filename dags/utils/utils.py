from typing import Dict, Any
from datetime import datetime

from config.config import PROJECT_NAME

def build_gcs_path(source_name: str, frequency: str, extension: str, extend:str=None) -> str:

    VALID_FREQUENCIES = {"hourly", "daily", "weekly", "quarterly","monthly", "yearly"}
    VALID_EXTENSIONS = {"json", "csv", "parquet"}
    
    if frequency not in VALID_FREQUENCIES:
        raise ValueError(f"Fréquence '{frequency}' invalide. Valeurs acceptées: {VALID_FREQUENCIES}")
    
    if extension not in VALID_EXTENSIONS:
        raise ValueError(f"Extension '{extension}' invalide. Valeurs acceptées: {VALID_EXTENSIONS}")
    
    # Mapping fréquence -> (partition_path, filename)
    if extend:
        extend_label=f"_{extend}"
    else:
        extend_label=""
        
    return f"raw/{frequency}/{source_name}{extend_label}.{extension}"

def get_collected_tags(collected_name:str, frequency:str="days"):
    all_collected_tags = {
        "hour": [
            PROJECT_NAME,
            collected_name,
            "frequency:hourly",
            "schedule:every-hour",
            "offset-1",
            "retention:7d",
            "priority:normal"
        ],
        "days": [
            PROJECT_NAME,
            collected_name, 
            "frequency:daily",
            "schedule:daily",
            "offset-1",
            "retention:90d",
            "priority:high"
        ],
        "weekly": [
            PROJECT_NAME,
            collected_name, 
            "frequency:weekly",
            "schedule:weekly",
            "offset-1",
            "retention:1y",
            "priority:low"
        ],
        "monthly": [
            PROJECT_NAME,
            collected_name, 
            "frequency:monthly",
            "schedule:monthly",
            "offset-1",
            "retention:3y",
            "priority:high"
        ],
        "quarterly": [
            PROJECT_NAME,
            collected_name, 
            "frequency:quarterly",
            "schedule:quarterly",
            "offset-1",
            "retention:6y",
            "priority:low"
        ],
        "yearly": [
            PROJECT_NAME,
            collected_name, 
            "frequency:yearly",
            "schedule:yearly",
            "offset-1",
            "retention:10y",
            "priority:low"
        ],
    }

    if frequency not in all_collected_tags.keys():
        raise ValueError(
            f"Fréquence '{frequency}' invalide."
            f"Valeurs acceptées: {list(all_collected_tags.keys())}"
        )
    return all_collected_tags[frequency]

#    base = all_collected_tags[frequency]
#    if isinstance(collected_name, list):
#        return base + collected_name
#    else:
#        return base + [collected_name]
