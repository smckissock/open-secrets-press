import duckdb
from pathlib import Path

db_path = "db/data.duckdb"

script_dir = Path(__file__).parent.parent
csv_path = script_dir / "setup" / "media_outlets.csv"

con = duckdb.connect(str(db_path))
con.execute(f"""
    CREATE OR REPLACE TABLE stage_media_outlet AS 
    SELECT * FROM read_csv_auto('{csv_path}')
""")

result = con.execute("SELECT COUNT(*) FROM stage_media_outlet").fetchone()
print(f"Loaded {result[0]} rows into stage_media_outlets")

con.close()