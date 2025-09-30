import duckdb
from pathlib import Path

def to_camel_case(snake_str):
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])

def export_stories_to_csv(
    db_path="db/data.duckdb",
    csv_path="web/data/stories.csv"
):
    conn = duckdb.connect(db_path)
    df = conn.execute("""
        SELECT * FROM story_web_view 
        WHERE image <> '' AND sentence <> ''
    """).fetchdf()
    conn.close()

    df.columns = [to_camel_case(col) for col in df.columns]
    
    df.to_csv(csv_path, index=False)
    print(f"Exported {len(df)} stories to {csv_path}")

if __name__ == "__main__":
    export_stories_to_csv()