"""
Minimal Confluent Schema Registry HTTP client (no caching - callers cache).
fetch_latest_schema: a subject's latest schema JSON (reader schema).
fetch_all_schemas: every version + its immutable schema id (writer schemas).
"""

from __future__ import annotations

import json
import urllib.request
from typing import List, Tuple

from .logger import get_logger

logger = get_logger(__name__)


def _get_json(url: str) -> object:
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.load(response)


def fetch_latest_schema(registry_url: str, subject: str) -> str:
    """Return the literal Avro schema JSON registered for `subject`."""
    base = registry_url.rstrip("/")
    logger.info(f"Fetching latest Avro schema for subject '{subject}' from {registry_url}")
    return _get_json(f"{base}/subjects/{subject}/versions/latest")["schema"]


def fetch_all_schemas(registry_url: str, subject: str) -> List[Tuple[int, str]]:
    """`(schema_id, schema_json)` per version, ascending (last = latest)."""
    # ids are immutable - decode each record by its own id, cache freely
    base = registry_url.rstrip("/")
    versions = _get_json(f"{base}/subjects/{subject}/versions")
    logger.info(
        f"Fetching {len(versions)} registered schema version(s) for subject "
        f"'{subject}' from {registry_url}"
    )
    result = []
    for version in versions:
        payload = _get_json(f"{base}/subjects/{subject}/versions/{version}")
        result.append((payload["id"], payload["schema"]))
    return result
