import os
from datetime import datetime, timedelta
from typing import Dict, Any
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import great_expectations as gx
import logging
import pandas as pd
import numpy as np

DEFAULT_ARGS = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'de_spotify_silver',
    default_args=DEFAULT_ARGS,
    description='Transform bronze.spotify_tracks to silver.spotify_tracks_silver with imputation, deduplication, and validation',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['etl', 'mysql', 'spotify', 'silver']
) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    @task(task_id='get_dag_configuration')
    def get_dag_configuration(**context) -> Dict[str, Any]:
        """Get DAG configuration for Silver layer (no CSV, no batch_id)"""
        load_type = Variable.get('silver_load_type', default_var='full')
        run_type = context['dag_run'].run_type
        logging.info(f"Load Type: {load_type}, Run Type: {run_type}")
        return {
            'load_type': load_type,
            'run_type': run_type,
            'mysql_table_name': 'spotify_tracks_silver'
        }

    @task(task_id='compute_medians_modes')
    def compute_medians_modes(config: Dict[str, Any], **context) -> Dict[str, Any]:
        """Compute medians for numeric columns and modes for categorical columns from spotify_tracks."""
        mysql_hook = MySqlHook(mysql_conn_id='mysql_northwind')
        df = mysql_hook.get_pandas_df('SELECT * FROM spotify_tracks')
        medians = {}
        modes = {}
        # Numeric columns for median
        numeric_cols = [
            'popularity', 'duration_ms', 'danceability', 'energy', 'loudness',
            'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo'
        ]
        for col in numeric_cols:
            if col in df.columns:
                medians[col] = float(df[col].median()) if not df[col].dropna().empty else None
        # Categorical columns for mode
        categorical_cols = ['artists', 'album_name', 'track_name', 'track_genre']
        for col in categorical_cols:
            if col in df.columns:
                mode_val = df[col].mode(dropna=True)
                modes[col] = mode_val.iloc[0] if not mode_val.empty else None
        return {'medians': medians, 'modes': modes}

    config = get_dag_configuration()
    medians_modes = compute_medians_modes(config)

    transform_to_silver = SQLExecuteQueryOperator(
        task_id='transform_to_silver',
        conn_id='mysql_northwind',
        sql='sql/de_spotify_silver.sql',
        split_statements=True
    )

    @task(task_id='validate_load_with_gx')
    def validate_load_with_gx(config: Dict[str, Any], **context) -> None:
        table_name = config['mysql_table_name']
        gx_context = gx.get_context(mode='file')
        target_hook = MySqlHook(mysql_conn_id='mysql_northwind')
        data_source_name = f"ds_sql_spotify_tracks_silver"
        try:
            data_source = gx_context.data_sources.get(data_source_name)
        except:
            data_source = gx_context.data_sources.add_sql(
                name=data_source_name,
                connection_string=target_hook.get_connection('mysql_northwind').get_uri()
            )
        data_asset_name = f"da_sql_spotify_tracks_silver"
        try:
            data_asset = data_source.get_asset(data_asset_name)
        except:
            data_asset = data_source.add_query_asset(
                query=f"SELECT * FROM {table_name}",
                name=data_asset_name
            )
        batch_definition_name = f"bd_sql_spotify_tracks_silver"
        try:
            batch_definition = data_asset.get_batch_definition(batch_definition_name)
        except:
            batch_definition = data_asset.add_batch_definition_whole_table(name=batch_definition_name)
        suite_name = f"suite_sql_spotify_tracks_silver"
        try:
            suite = gx_context.suites.get(name=suite_name)
            logging.info(f"Using existing expectation suite: {suite_name}")
        except:
            suite = gx.ExpectationSuite(name=suite_name)
            suite = gx_context.suites.add(suite)
            logging.info(f"Created new expectation suite: {suite_name}")
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeUnique(column="track_id")
            )
            suite.add_expectation(
                gx.expectations.ExpectTableRowCountToBeGreaterThan(
                    value=0
                )
            )
            for col in ['track_id', 'artists', 'album_name', 'track_name', 'popularity', 'duration_ms', 'explicit', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature', 'track_genre']:
                suite.add_expectation(
                    gx.expectations.ExpectColumnValuesToNotBeNull(column=col)
                )
            for col in ['popularity', 'duration_ms', 'mode', 'time_signature']:
                suite.add_expectation(
                    gx.expectations.ExpectColumnValuesToBeOfType(
                        column=col,
                        type_="INTEGER"
                    )
                )
            for col in ['loudness', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'danceability', 'energy']:
                suite.add_expectation(
                    gx.expectations.ExpectColumnValuesToBeOfType(
                        column=col,
                        type_="FLOAT"
                    )
                )
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="popularity",
                    min_value=0,
                    max_value=100
                )
            )
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="danceability",
                    min_value=0.0,
                    max_value=1.0
                )
            )
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="energy",
                    min_value=0.0,
                    max_value=1.0
                )
            )
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="acousticness",
                    min_value=0.0,
                    max_value=1.0
                )
            )
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="instrumentalness",
                    min_value=0.0,
                    max_value=1.0
                )
            )
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="liveness",
                    min_value=0.0,
                    max_value=1.0
                )
            )
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="valence",
                    min_value=0.0,
                    max_value=1.0
                )
            )
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="tempo",
                    min_value=0.0
                )
            )
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="loudness",
                    min_value=-60,
                    max_value=0
                )
            )
        definition_name = f"vd_sql_spotify_tracks_silver"
        try:
            validation_definition = gx_context.validation_definitions.get(definition_name)
        except:
            validation_definition = gx.ValidationDefinition(
                data=batch_definition, suite=suite, name=definition_name
            )
            validation_definition = gx_context.validation_definitions.add(validation_definition)
        validation_results = validation_definition.run(result_format="BASIC")
        if not validation_results.success:
            logging.error(f"Data validation failed for {data_asset_name}")
            logging.error(f"Validation result: {validation_results}")
            raise ValueError(f"Data validation failed for {data_asset_name}")
        else:
            logging.info(f"Data validation passed for {data_asset_name}")

    validate_task = validate_load_with_gx(config)

    start >> config >> medians_modes >> transform_to_silver >> validate_task >> end 