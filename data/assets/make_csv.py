import duckdb
import pandas as pd
from pathlib import Path


def export_stories_to_csv(
    db_path="db/data.duckdb", 
    csv_path="web/data/stories.csv"
):
    ##Path(csv_path).parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(db_path)
    df = conn.execute("SELECT * FROM stories").fetchdf()
    conn.close()

    df.to_csv(csv_path, index=False)
    print(f"Exported {len(df)} stories to {csv_path}")


if __name__ == "__main__":
    export_stories_to_csv()
