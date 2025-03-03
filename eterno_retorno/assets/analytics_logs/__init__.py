from dagster import asset, Output
from dagster_gcp import BigQueryResource
import requests
import json
import pandas as pd
import logging
import os
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def query_runs(endpoint_url, cursor=None):
    # GraphQL query
    query = """
    query PaginatedRunsQuery($cursor: String) {
        runsOrError(
            cursor: $cursor
            limit: 200
        ) {
            __typename
            ... on Runs {
                results {
                    runId
                    jobName
                    status
                    startTime
                    endTime
                  stepStats {
                    stepKey
                    status
                    startTime
                    endTime
                  }
                }
            }
        }
    }
    """
    
    # Variables for the query
    variables = {
        "cursor": cursor
    }
    
    # Headers
    headers = {
        'Content-Type': 'application/json',
    }
    
    # Prepare the request payload
    payload = {
        'query': query,
        'variables': variables
    }
    
    # Make the request
    response = requests.post(endpoint_url, 
                           json=payload,
                           headers=headers)
    
    # Check if request was successful
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Query failed with status code: {response.status_code}\n{response.text}")

@asset(
    name=f"run_{os.environ['MSSQL_DATABASE']}",
    io_manager_key="logs_io_manager",
    key_prefix=['analytics'],
    group_name="analytics",
    op_tags={"dagster/priority": 1}
)
def runs_logs_asset():
    logger.info("Starting analytics logs asset")
    try:
        # Initial query without cursor
        result = query_runs("http://localhost:3000/graphql")
        
        # Process the results
        if result.get('data') and result['data'].get('runsOrError'):
            runs = result['data']['runsOrError']
            if runs['__typename'] == 'Runs' and runs.get('results'):
                # First create DataFrame with run-level data
                run_data = []
                for run in runs['results']:
                    # Extract step stats
                    step_stats = run.pop('stepStats', [])
                    
                    # Create base run record
                    base_run = {
                        'runId': run['runId'],
                        'jobName': run['jobName'],
                        'status': run['status'],
                        'startTime': run['startTime'],
                        'endTime': run['endTime']
                    }
                    
                    # If there are no steps, add the run with null step data
                    if not step_stats:
                        run_data.append({
                            **base_run,
                            'stepKey': None,
                            'stepStatus': None,
                            'stepStartTime': None,
                            'stepEndTime': None
                        })
                    else:
                        # Add a record for each step
                        for step in step_stats:
                            run_data.append({
                                **base_run,
                                'stepKey': step.get('stepKey'),
                                'stepStatus': step.get('status'),
                                'stepStartTime': step.get('startTime'),
                                'stepEndTime': step.get('endTime')
                            })
                
                # Convert to DataFrame
                df = pd.DataFrame(run_data)
                #add column with database name from env
                df['database'] = os.getenv("DATABASE_INFOREST")
                
                # Output to BigQuery
                yield Output(
                    value=df,
                    metadata={
                        "table": "runs_logs",
                        "records": len(df),
                        "schema": list(df.columns)
                    }
                )
                logger.info(f"Data sent to BigQuery: {len(df)} records")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise
