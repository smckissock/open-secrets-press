import duckdb
from pathlib import Path
import brotli
import gzip

from .db_connection import connect  # dagster
#from db_connection import connect   # vscode


def to_camel_case(snake_str):
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])


def make_stories_parquet(
    csv_path = "web/data/stories.csv",
    parquet_path = "web/data/stories.parquet",
    brotli_path = "web/data/stories.csv.br",
    gzip_path = "web/data/stories.csv.gz"
):
    conn = connect()
    df = conn.execute("""
        SELECT * FROM story_web_view WHERE media_outlet <> 'Unspecified'
    """).fetchdf()
    conn.close()
    story_count = len(df)  

    df.columns = [to_camel_case(col) for col in df.columns]
    
    # Export CSV
    df.to_csv(csv_path, index=False)
    csv_size = Path(csv_path).stat().st_size
    print(f"Exported {story_count} stories to {csv_path}")

    # Export Parquet
    df.to_parquet(parquet_path, index=False)
    parquet_size = Path(parquet_path).stat().st_size
    print(f"Exported {story_count} stories to {parquet_path}")

    # Read CSV data once for compression
    with open(csv_path, 'rb') as f_in:
        csv_data = f_in.read()

    # Export Brotli-compressed CSV
    compressed_br = brotli.compress(csv_data, quality=11)
    with open(brotli_path, 'wb') as f_out:
        f_out.write(compressed_br)
    
    br_ratio = (1 - len(compressed_br) / len(csv_data)) * 100
    print(f"Exported {story_count} stories to {brotli_path}")
    print(f"Brotli compression: {len(csv_data):,} bytes -> {len(compressed_br):,} bytes ({br_ratio:.1f}% reduction)")

    # Export Gzip-compressed CSV
    with gzip.open(gzip_path, 'wb', compresslevel=9) as f_out:
        f_out.write(csv_data)
    
    gz_size = Path(gzip_path).stat().st_size
    gz_ratio = (1 - gz_size / len(csv_data)) * 100
    print(f"Exported {story_count} stories to {gzip_path}")
    print(f"Gzip compression: {len(csv_data):,} bytes -> {gz_size:,} bytes ({gz_ratio:.1f}% reduction)")

    # Summary comparison
    print("\n=== Compression Summary ===")
    print(f"Original CSV:    {csv_size:,} bytes")
    print(f"Parquet:         {parquet_size:,} bytes ({(1-parquet_size/csv_size)*100:.1f}% reduction)")
    print(f"Brotli (.br):    {len(compressed_br):,} bytes ({br_ratio:.1f}% reduction)")
    print(f"Gzip (.gz):      {gz_size:,} bytes ({gz_ratio:.1f}% reduction)")
    print(f"Brotli vs Gzip:  {((gz_size - len(compressed_br)) / gz_size * 100):.1f}% smaller")

    return story_count

if __name__ == "__main__":
    make_stories_parquet()