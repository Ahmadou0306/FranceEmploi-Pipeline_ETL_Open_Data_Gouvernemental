from typing import Optional
from datetime import datetime
import json
from config.config import PROJECT_NAME
import requests
import time

VALID_FREQUENCIES = {"hourly", "daily", "weekly", "monthly", "quarterly", "yearly"}
VALID_EXTENSIONS  = {"json", "csv", "parquet"}

FREQUENCY_PARTITION = {
    "hourly":    lambda dt: f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/hour={dt.hour:02d}",
    "daily":     lambda dt: f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}",
    "weekly":    lambda dt: f"year={dt.year}/week={dt.isocalendar()[1]:02d}",
    "monthly":   lambda dt: f"year={dt.year}/month={dt.month:02d}",
    "quarterly": lambda dt: f"year={dt.year}/quarter=Q{((dt.month - 1) // 3) + 1}",
    "yearly":    lambda dt: f"year={dt.year}",
}

FREQUENCY_FILENAME_FORMAT = {
    "hourly":    "%Y%m%d_%H",
    "daily":     "%Y%m%d",
    "weekly":    "%Y_W%W",
    "monthly":   "%Y%m",
    "quarterly": "%Y_Q{quarter}",
    "yearly":    "%Y",
}

_TAGS_TEMPLATE: dict[str, list[str]] = {
    "hourly": [
        "frequency:hourly",
        "schedule:every-hour",
        "retention:7d",
        "priority:normal",
    ],
    "daily": [
        "frequency:daily",
        "schedule:daily",
        "retention:90d",
        "priority:high",
    ],
    "weekly": [
        "frequency:weekly",
        "schedule:weekly",
        "retention:1y",
        "priority:low",
    ],
    "monthly": [
        "frequency:monthly",
        "schedule:monthly",
        "retention:3y",
        "priority:high",
    ],
    "quarterly": [
        "frequency:quarterly",
        "schedule:quarterly",
        "retention:6y",
        "priority:low",
    ],
    "yearly": [
        "frequency:yearly",
        "schedule:yearly",
        "retention:10y",
        "priority:low",
    ],
}


def __format_filename_date__(dt: datetime, frequency: str) -> str:
    """Formate la date dans le nom de fichier selon la fréquence."""
    if frequency == "quarterly":
        quarter = ((dt.month - 1) // 3) + 1
        return f"{dt.year}_Q{quarter}"
    return dt.strftime(FREQUENCY_FILENAME_FORMAT[frequency])


def build_gcs_current_path(source_name: str, extension: str) -> str:
    return f"raw/{source_name}/current/{source_name}.{extension}"


def build_gcs_archive_path(
    source_name: str,
    extension:   str,
    frequency:   str,
    dt:          Optional[datetime] = None,
) -> str:
    if frequency not in VALID_FREQUENCIES:
        raise ValueError(
            f"Fréquence '{frequency}' invalide. Valeurs acceptées : {sorted(VALID_FREQUENCIES)}"
        )
    dt        = dt or datetime.now()
    partition = FREQUENCY_PARTITION[frequency](dt)
    label     = __format_filename_date__(dt, frequency)  # ✅ utilise la fonction dédiée
    filename  = f"{source_name}_{label}.{extension}"

    return f"raw/{source_name}/archive/{partition}/{filename}"



def get_collected_tags(collected_name: str, frequency: str = "daily") -> list[str]:
    if frequency not in _TAGS_TEMPLATE:
        raise ValueError(
            f"Fréquence '{frequency}' invalide. "
            f"Valeurs acceptées : {sorted(_TAGS_TEMPLATE.keys())}"
        )

    return [
        PROJECT_NAME,
        collected_name,
        f"source:{collected_name}",
        "offset:1",             
        *_TAGS_TEMPLATE[frequency],
    ]

#    base = all_collected_tags[frequency]
#    if isinstance(collected_name, list):
#        return base + collected_name
#    else:
#        return base + [collected_name]


def convert_to_ndjson(data):
    if not isinstance(data, list):
        return data
    return '\n'.join(json.dumps(record) for record in data)



def fetch_xml_json_with_retry(url: str, params: dict, logger, max_retries:int=3) -> dict:
    """Effectue un GET avec retry exponentiel. Lève une exception après max_retries échecs."""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()  # lève HTTPError si 4xx/5xx
            return response.json()

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout (tentative {attempt + 1}/{max_retries})")

        except requests.exceptions.HTTPError as e:
            logger.error(f"Erreur HTTP : {e}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur réseau : {e}")

        if attempt < max_retries - 1:
            time.sleep(2 ** attempt)

    raise Exception(f"Echec apres {max_retries} tentatives sur {url} avec {params}")

