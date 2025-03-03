from dagster import (
    ScheduleDefinition, 
    define_asset_job, 
    multi_or_in_process_executor,
    Executor,
    RetryPolicy
)
from ..assets.sling import primary_assets, secondary_assets
from ..assets.analytics_logs import runs_logs_asset

sling_assets_job = define_asset_job(    
    name="sling_assets_job",
    selection=[primary_assets, runs_logs_asset],
    description="Job to process all sling assets",
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 1,  # Ensure strictly sequential execution
                    "retries": {
                        "enabled": {},  # Enable retries
                    }
                }
            }
        }
    }
)

sling_assets_schedule = ScheduleDefinition(
    name="sling_assets_daily",
    cron_schedule="10 4 * * *",
    execution_timezone="America/Lima",
    job_name="sling_assets_job",
    description="Schedule to process sling assets daily, primary"
)

sling_assets_secondary_job = define_asset_job(
    name="sling_assets_secondary_job",
    selection=[secondary_assets],
    description="Job to process all sling assets secondary",
)

sling_assets_secondary_schedule = ScheduleDefinition(
    name="sling_assets_secondary_daily",
    cron_schedule="0 4 * * *",
    execution_timezone="America/Lima",
    job_name="sling_assets_secondary_job",
    description="Schedule to process sling assets secondary daily, secondary"
)
