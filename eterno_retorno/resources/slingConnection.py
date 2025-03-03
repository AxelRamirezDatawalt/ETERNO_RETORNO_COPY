from dagster import ConfigurableResource, EnvVar
from dagster_embedded_elt.sling.resources import SlingConnectionResource, SlingResource
from dagster import EnvVar
import os

embedded_elt_resource = SlingResource(
            connections=[
                SlingConnectionResource(
                    name="MSSQL_INFOREST",
                    type="sqlserver", 
                    host=os.getenv("MSSQL_HOST"),
                    user=os.getenv("MSSQL_USER"),
                    database=os.getenv("DATABASE_INFOREST"),
                    password=os.getenv("MSSQL_PASSWORD"),
                    encrypt="disable"
                ),
                SlingConnectionResource(
                    name="MSSQL_INFOREST2",
                    type="sqlserver", 
                    host=EnvVar("MSSQL_HOST2"),
                    user=EnvVar("MSSQL_USER2"),
                    database=EnvVar("DATABASE_INFOREST2"),
                    password=EnvVar("MSSQL_PASSWORD2"),
                    encrypt="disable"
                ),
                SlingConnectionResource(
                    name="BIGQUERY_INFOREST",
                    type="bigquery",
                    project="eternoretorno",  
                    dataset="Eterno_Retorno",  
                ),
            ]
    )
    