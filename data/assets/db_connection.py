import os
from dotenv import load_dotenv
from typing import Optional

import duckdb


def connect(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    load_dotenv()
    DUCKDB_PATH: Optional[str] = os.getenv("DUCKDB_PATH")

    print(f"DEBUG: DUCKDB_PATH = {DUCKDB_PATH}")  
    print(f"DEBUG: .env loaded from: {os.getcwd()}") 

    return duckdb.connect(database=DUCKDB_PATH, read_only=read_only)