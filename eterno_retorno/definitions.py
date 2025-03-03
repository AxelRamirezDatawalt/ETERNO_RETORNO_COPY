from dagster import Definitions

from eterno_retorno.resources.slingConnection import embedded_elt_resource
from eterno_retorno.assets.sling import sling_assets
from eterno_retorno.jobs.sling import sling_assets_job, sling_assets_schedule, sling_assets_secondary_job, sling_assets_secondary_schedule
from dagster_gcp_pandas import BigQueryPandasIOManager
# from eterno_retorno.jobs.runslogs import runs_logs_job, runs_logs_schedule
from eterno_retorno.assets.analytics_logs import runs_logs_asset

# Definición de Resources
all_resources = {
    "embedded_elt": embedded_elt_resource,
    "logs_io_manager": BigQueryPandasIOManager(
        project="eternoretorno", 
        dataset="Eterno_Retorno"
    )
}

# Definición de Jobs
all_jobs = [
    sling_assets_job,
    sling_assets_secondary_job,
    # runs_logs_job
]

# Definición de Schedules
all_schedules = [
    sling_assets_schedule,
    sling_assets_secondary_schedule,
    # runs_logs_schedule
]

#sling assets
all_sling_assets = [
    *sling_assets
]

# Definición principal de Dagster
defs = Definitions(
    assets=[*all_sling_assets, runs_logs_asset],
    resources=all_resources,
    schedules=all_schedules,
    jobs=all_jobs
)