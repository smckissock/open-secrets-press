from dagster import Definitions
from .assets import (
    media_cloud_asset,
    newspaper_asset,
    sentence_asset,
    story_asset,
    parquet_asset
)

defs = Definitions(
    assets = [
        media_cloud_asset,
        newspaper_asset,
        sentence_asset,
        story_asset,
        parquet_asset
    ]
)


