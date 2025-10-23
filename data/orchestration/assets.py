from dagster import asset, AssetExecutionContext, MaterializeResult
from pathlib import Path

from data.assets.get_media_cloud import get_media_cloud
from data.assets.get_newspaper import get_newspaper
from data.assets.get_sentence import get_sentence
from data.assets.make_stories_parquet import make_stories_parquet

from data.assets.db_connection import connect


@asset (
    description = "Gets latest stories using the Media Cloud api",
    group_name="Import"
)
def media_cloud_asset(context: AssetExecutionContext):
    stories_added = get_media_cloud()
    return MaterializeResult(
        metadata = {"New Media Cloud stories added": stories_added}
    )
    

@asset (
    deps=[media_cloud_asset],
    description = "Visit story to get body and metadata using the newspaper3k",
    group_name="Transform"
)
def newspaper_asset(context: AssetExecutionContext):
    results = get_newspaper()
    return MaterializeResult(
        metadata = {
            "Stories visited by newspaper": results["stories_found"],
            "Stories not visited":          results["stories_not_found"]
        }
    )
    

@asset (
    deps=[newspaper_asset],
    description = 'Find the longest sentence that has the text "OpenSecrets"',
    group_name="Transform"
    
)
def sentence_asset(context: AssetExecutionContext):
    get_sentence()
    

@asset (
    deps=[sentence_asset],
    description = "Run sql script to drop/create story table and populate it",
    group_name="Transform"
    
)
def story_asset(context: AssetExecutionContext):
    sql_file = Path(__file__).parent.parent / "assets" / "scripts" / "populate_story.sql"
    print(f"DEBUG: Executing SQL file at {sql_file}")
    sql = sql_file.read_text()
    
    conn = connect()
    conn.execute(sql)

    story_count = conn.execute("SELECT COUNT(*) FROM story").fetchone()[0]

    conn.close()    
    context.log.info(f"Executed {sql_file.name}")

    return MaterializeResult(
        metadata = {"Stories in story table": story_count}
    )


@asset (
    deps=[story_asset],
    description = "Write stories.parquet and stories.csv from story_web_view",
    group_name="Export"
)
def parquet_asset(context: AssetExecutionContext):
    story_count = make_stories_parquet()
    return MaterializeResult(
        metadata = {"Stories in stories.parquet": story_count}
    )