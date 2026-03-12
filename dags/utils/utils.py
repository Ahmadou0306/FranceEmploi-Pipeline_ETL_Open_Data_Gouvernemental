from typing import Optional
from datetime import datetime
import json
from config.config import PROJECT_NAME

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


def build_gcs_path(
    source_name: str,
    frequency:   str,
    extension:   str,
    dt:          Optional[datetime] = None,
) -> str:
    # --- Validation ---
    if frequency not in VALID_FREQUENCIES:
        raise ValueError(
            f"Fréquence '{frequency}' invalide. Valeurs acceptées : {sorted(VALID_FREQUENCIES)}"
        )
    if extension not in VALID_EXTENSIONS:
        raise ValueError(
            f"Extension '{extension}' invalide. Valeurs acceptées : {sorted(VALID_EXTENSIONS)}"
        )

    dt = dt or datetime.now()

    partition  = FREQUENCY_PARTITION[frequency](dt)
    date_label = __format_filename_date__(dt, frequency)
    filename   = f"{source_name}_{date_label}.{extension}"

    return f"raw/{frequency}/{source_name}/{partition}/{filename}"






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