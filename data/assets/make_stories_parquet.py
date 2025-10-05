import duckdb
from pathlib import Path

from .db_connection import connect


def to_camel_case(snake_str):
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])


def make_stories_parquet(
    csv_path = "web/data/stories.csv",
    parquet_path = "web/data/stories.parquet" 
):
    conn = connect()
    df = conn.execute("""
        SELECT * FROM story_web_view WHERE media_outlet <> 'Unspecified'
    """).fetchdf()
    conn.close()
    story_count = len(df)  

    df.columns = [to_camel_case(col) for col in df.columns]
    
    df.to_csv(csv_path, index=False)
    print(f"Exported {story_count} stories to {csv_path}")

    df.to_parquet(parquet_path, index=False)
    print(f"Exported {story_count} stories to {parquet_path}")

    return story_count

if __name__ == "__main__":
    make_stories_parquet()