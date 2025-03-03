from dagster_embedded_elt.sling import (
    SlingResource,
    sling_assets,
)
from dagster_embedded_elt.sling.dagster_sling_translator import DagsterSlingTranslator
from dagster import AssetKey
from typing import Any, Iterable, Mapping
from pathlib import Path
from dagster import load_assets_from_package_module

class CustomSlingTranslator(DagsterSlingTranslator):
    def __init__(self, prefix: str):
        super().__init__()
        self.prefix = prefix

    def get_asset_key(self, stream_definition: Mapping[str, Any]) -> AssetKey:
        stream_asset_key = super().get_asset_key(stream_definition)
        return AssetKey([self.prefix, *stream_asset_key.path])

    def get_group_name(self, stream_definition):
        return f"extractions_{self.prefix}"

@sling_assets(
    replication_config=Path(__file__).parent / "extractions.yaml",
    dagster_sling_translator=CustomSlingTranslator(prefix="primary"),
    op_tags={"dagster/priority": 2}
)
def primary_assets(context, embedded_elt: SlingResource) -> Iterable[Any]:
    yield from embedded_elt.replicate(context=context)

@sling_assets(
    replication_config=Path(__file__).parent / "extractions2.yaml",
    dagster_sling_translator=CustomSlingTranslator(prefix="secondary")
)
def secondary_assets(context, embedded_elt: SlingResource) -> Iterable[Any]:
    yield from embedded_elt.replicate(context=context)
