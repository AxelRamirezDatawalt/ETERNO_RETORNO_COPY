# from ..assets.analytics_logs import runs_logs_asset 
# from dagster import ScheduleDefinition, define_asset_job, multi_or_in_process_executor

# # Analytics Logs job
# runs_logs_job = define_asset_job(
#     name="runs_logs_job",
#     selection=[
#         runs_logs_asset
#     ],
#     description="Job to process Runs Logs",
# )

# # Analytics Logs schedule
# runs_logs_schedule = ScheduleDefinition(
#     name="runs_logs_daily",
#     cron_schedule="55 7 * * *",  # Runs at 7:00 AM
#     execution_timezone="America/Santiago",
#     job_name="runs_logs_job",
#     description="Schedule to process Runs Logs daily",
# )
