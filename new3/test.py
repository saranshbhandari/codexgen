from io import StringIO
import csv
from typing import Any, Dict, List


def flatten_dict(data: Dict[str, Any], parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
    items: Dict[str, Any] = {}

    for key, value in data.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key

        if isinstance(value, dict):
            items.update(flatten_dict(value, new_key, sep=sep))
        elif isinstance(value, list):
            # store list as string for now
            items[new_key] = str(value)
        else:
            items[new_key] = value

    return items


def dict_to_csv_string(flattened: Dict[str, Any]) -> str:
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=list(flattened.keys()))
    writer.writeheader()
    writer.writerow(flattened)
    return output.getvalue()
