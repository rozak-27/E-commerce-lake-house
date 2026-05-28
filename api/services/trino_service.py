"""
Trino Service
=============
Handle koneksi dan query ke Trino
"""
import trino
import os
from typing import Any

TRINO_HOST    = os.getenv("TRINO_HOST",    "trino")
TRINO_PORT    = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER    = os.getenv("TRINO_USER",    "admin")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA  = os.getenv("TRINO_SCHEMA",  "gold")

def get_connection():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )

def execute_query(sql: str) -> list[dict[str, Any]]:
    """Jalankan SQL ke Trino, return list of dict"""
    conn   = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    columns = [desc[0] for desc in cursor.description]
    rows    = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]