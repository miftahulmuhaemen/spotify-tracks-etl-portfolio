import os
import logging
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any
from pathlib import Path

from airflow import DAG
from airflow.models import Variable, Connection
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

import great_expectations as gx
from great_expectations import expectations as gxe

import pandas as pd
import numpy as np

# DAG configuration
DEFAULT_ARGS = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Source Dataset
# https://www.kaggle.com/datasets/melissamonfared/spotify-tracks-attributes-and-popularity

# DAG definition
with DAG(
    'csv_to_mysql_etl',
    default_args=DEFAULT_ARGS,
    description='Extract CSV data and load into MySQL with metadata',
    schedule_interval='@once',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['etl', 'csv', 'mysql', 'spotify']
) as dag:

    @task(task_id='get_dag_configuration')
    def get_dag_configuration(**context) -> Dict[str, Any]:
        """Get DAG configuration and determine load type"""
        # Get load type from Airflow Variable or default to 'full'
        load_type = Variable.get('csv_load_type', default_var='full')
        
        # Get run type from context
        run_type = context['dag_run'].run_type
        
        logging.info(f"Load Type: {load_type}, Run Type: {run_type}")
        
        # Validate configuration
        if run_type == "scheduled" and load_type == 'full':
            raise ValueError("Full load cannot be scheduled! Aborting...")
        
        # Generate batch identifier
        batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Get the path to the CSV file relative to the DAG file
        dag_file_path = Path(__file__).resolve()
        csv_file_path = dag_file_path.parent / 'csv' / 'dataset.csv'
        
        return {
            'load_type': load_type,
            'run_type': run_type,
            'batch_id': batch_id,
            'source_identifier': 'CSV',
            'csv_file_path': f"{csv_file_path}",
            'mysql_table_name': 'spotify_tracks'
        }
    
    # Get configuration
    config = get_dag_configuration()

    @task(task_id='extract_csv_data')
    def extract_csv_data(config: Dict[str, Any], **context) -> Dict[str, Any]:
        """Extract data from CSV file using Pandas"""
        csv_path = config['csv_file_path']
        
        logging.info(f"Extracting data from {csv_path}")
        
        try:
            # Read CSV file
            df = pd.read_csv(csv_path)
            
            # Add metadata columns
            current_timestamp = datetime.now()
            
            df['ingestion_timestamp'] = current_timestamp.isoformat()  # Convert to string
            df['source_identifier'] = config['source_identifier']
            df['batch_identifier'] = config['batch_id']
            
            # Log extraction statistics
            # logging.info(f"Extracted {len(df)} rows from CSV")
            logging.info(f"Columns: {list(df.columns)}")
            
            return {
                'row_count': len(df),
                'columns': list(df.columns),
                'dataframe_dict': df.to_dict('records')
            }
            
        except Exception as e:
            logging.error(f"Error extracting CSV data: {str(e)}")
            raise

    @task(task_id='validate_data')
    def validate_data(extraction_result: Dict[str, Any], **context) -> Dict[str, Any]:
        """Validate extracted data"""
        df_dict = extraction_result['dataframe_dict']
        df = pd.DataFrame(df_dict)
        
        validation_results = {
            'total_rows': len(df),
            'null_counts': {},
            'data_types': {},
            'validation_passed': True,
            'validation_errors': []
        }
        
        # Check for null values in key columns
        key_columns = ['track_id', 'track_name', 'artists']
        for col in key_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                validation_results['null_counts'][col] = null_count
                
                if null_count > 0:
                    validation_results['validation_errors'].append(
                        f"Column {col} has {null_count} null values"
                    )
        
        # Check data types
        for col in df.columns:
            validation_results['data_types'][col] = str(df[col].dtype)
        
        # Validate numeric columns
        numeric_columns = ['popularity', 'duration_ms', 'danceability', 'energy']
        for col in numeric_columns:
            if col in df.columns:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    validation_results['validation_errors'].append(
                        f"Column {col} should be numeric but is {df[col].dtype}"
                    )
        
        # Check if validation passed
        if validation_results['validation_errors']:
            validation_results['validation_passed'] = False
            logging.warning(f"Data validation failed: {validation_results['validation_errors']}")
        else:
            logging.info("Data validation passed successfully")
        
        return validation_results

    prep_table = SQLExecuteQueryOperator(
        task_id='prep_table',
        conn_id='mysql_northwind',
        sql='sql/de_spotify_create_table.sql',
        split_statements=True,
    )

    @task(task_id='load_data_to_mysql')
    def load_data_to_mysql(
        extraction_result: Dict[str, Any],
        config: Dict[str, Any],
        validation_result: Dict[str, Any],
        **context
    ) -> Dict[str, Any]:
        """Load data into MySQL database"""
        
        # Check if validation passed
        # if not validation_result['validation_passed']:
        #     logging.error("Data validation failed. Aborting load.")
            # raise ValueError("Data validation failed")
        
        mysql_hook = MySqlHook(mysql_conn_id='mysql_northwind')
        table_name = config['mysql_table_name']
        load_type = config['load_type']
        
        # Convert back to DataFrame
        df = pd.DataFrame(extraction_result['dataframe_dict'])
        
        # Handle NaN values - replace with None (which becomes NULL in MySQL)
        df = df.replace([np.nan, pd.NaT], None)
        
        try:
            if load_type == 'full':
                # For full load, truncate table first
                logging.info(f"Performing full load - truncating table {table_name}")
                mysql_hook.run(f"TRUNCATE TABLE {table_name}")
                
            elif load_type == 'batch':
                # For batch load, insert new records
                logging.info(f"Performing batch load to table {table_name}")
                
            # Load data
            # Handle reserved keywords by wrapping column names in backticks
            target_fields = [f"`{col}`" if col in ['index', 'key'] else col for col in df.columns.tolist()]
            
            mysql_hook.insert_rows(
                table=table_name,
                rows=df.values.tolist(),
                target_fields=target_fields
            )
            
            # Get final row count
            result = mysql_hook.get_records(f"SELECT COUNT(*) as count FROM {table_name}")
            final_count = result[0][0] if result else 0
            
            logging.info(f"Successfully loaded {len(df)} rows to {table_name}")
            logging.info(f"Total rows in table: {final_count}")
            
            return {
                'rows_loaded': len(df),
                'total_rows_in_table': final_count,
                'load_type': load_type,
                'table_name': table_name
            }
            
        except Exception as e:
            logging.error(f"Error loading data to MySQL: {str(e)}")
            raise

    @task(task_id='validate_load_with_gx')
    def validate_load_with_gx(config: Dict[str, Any], **context) -> Dict[str, Any]:
        """Validate extracted data"""

        table_name = config['mysql_table_name']
        gx_context = gx.get_context(mode='file')
        target_hook = MySqlHook(mysql_conn_id='mysql_northwind')

        # Define the Data Source name
        data_source_name = f"ds_sql_spotify_tracks"
        try:
            data_source = gx_context.data_sources.get(data_source_name)
        except:
            data_source = gx_context.data_sources.add_sql(
                name=data_source_name,
                connection_string=target_hook.get_connection('mysql_northwind').get_uri()
            )

        # Define the Data Asset name
        data_asset_name = f"da_sql_spotify_tracks"
        try:
            data_asset = data_source.get_asset(data_asset_name)
        except:
            data_asset = data_source.add_query_asset(
                query=f"SELECT * FROM {table_name}",
                name=data_asset_name
            )

        # Define the Batch Definition name
        batch_definition_name = f"bd_csv_spotify_tracks"
        try:
            batch_definition = data_asset.get_batch_definition(batch_definition_name)
        except:
            batch_definition = data_asset.add_batch_definition_whole_table(name=batch_definition_name)

        # Create or get expectation suite
        suite_name = f"suite_csv_spotify_tracks"
        try:
            # Try to get existing suite
            suite = gx_context.suites.get(name=suite_name)
            logging.info(f"Using existing expectation suite: {suite_name}")
        except:
            # Create new suite if it doesn't exist
            suite = gx.ExpectationSuite(name=suite_name)
            suite = gx_context.suites.add(suite)
            logging.info(f"Created new expectation suite: {suite_name}")

            # Check Unique Compound
            suite.add_expectation(
                gx.expectations.ExpectCompoundColumnsToBeUnique(column_list=["id", "updated_at"])
            )

            # Check for Volume
            load_data_to_mysql_result = context['task_instance'].xcom_pull(task_ids='load_data_to_mysql')
            total_rows_in_table = load_data_to_mysql_result['total_rows_in_table']
            suite.add_expectation(
                gx.expectations.ExpectTableRowCountToEqual(
                    value=total_rows_in_table
                )
            )

            # Check for null values in key columns
            for col in ['id', 'track_id', 'artists', 'album_name', 'track_name', 'popularity', 'duration_ms', 'explicit', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature', 'track_genre']:
                suite.add_expectation(
                    gx.expectations.ExpectColumnValuesToNotBeNull(column=col)
                )

            # Check for numeric columns
            for col in ['popularity', 'duration_ms', 'mode', 'time_signature']:
                suite.add_expectation(
                    gx.expectations.ExpectColumnValuesToBeOfType(
                        column=col,
                        type_="INTEGER"
                    )
                )
            
            # Check for double columns
            for col in ['loudness', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo']:
                suite.add_expectation(
                    gx.expectations.ExpectColumnValuesToBeOfType(
                        column=col,
                        type_="FLOAT"
                    )
                )

            # Check Validity
            suite.add_expectation(
                gx.expectations.ExpectColumnValueLengthsToBeBetween(
                    column="acousticness",
                    min_value=0.0,
                    max_value=1.0
                )
            )
            suite.add_expectation(
                gx.expectations.ExpectColumnValueLengthsToBeBetween(
                    column="danceability",
                    min_value=0.0,
                    max_value=1.0
                )
            )
            suite.add_expectation(
                gx.expectations.ExpectColumnValueLengthsToBeBetween(
                    column="instrumentalness",
                    min_value=0.0,
                    max_value=1.0
                )
            )
            suite.add_expectation(
                gx.expectations.ExpectColumnValueLengthsToBeBetween(
                    column="loudness",
                    min_value=-60,
                    max_value=0
                )
            )
        
        # Add the Validation Definition to the Data Context
        definition_name = f"vd_csv_spotify_tracks"
        try:
            validation_definition = gx_context.validation_definitions.get(definition_name)
        except:
            validation_definition = gx.ValidationDefinition(
                data=batch_definition, suite=suite, name=definition_name
            )
            validation_definition = gx_context.validation_definitions.add(validation_definition)

        validation_results = validation_definition.run(result_format="BASIC")
        
        if not validation_results.success:
            logging.warning(f"Data validation failed for {data_asset_name}")
            logging.warning(f"Validation result: {validation_results}")
        else:
            logging.info(f"Data validation passed for {data_asset_name}")

    @task(task_id='generate_load_report')
    def generate_load_report(
        extraction_result: Dict[str, Any],
        load_result: Dict[str, Any],
        config: Dict[str, Any],
        **context
    ) -> Dict[str, Any]:
        """Generate load report"""
        
        report = {
            'load_summary': {
                'batch_id': config['batch_id'],
                'source_identifier': config['source_identifier'],
                'load_type': config['load_type'],
                'extraction_timestamp': datetime.now().isoformat(),
                'rows_extracted': extraction_result['row_count'],
                'rows_loaded': load_result['rows_loaded'],
                'total_rows_in_table': load_result['total_rows_in_table'],
                'table_name': load_result['table_name']
            },
            'metadata': {
                'dag_run_id': context['dag_run'].run_id,
                'task_instance_key': context['task_instance_key_str'],
                'execution_date': context['execution_date'].isoformat()
            }
        }
        
        logging.info(f"Load Report: {report}")
        
        return report

    # Define task dependencies
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule=TriggerRule.ALL_DONE)
    
    # Extract data
    extraction_result = extract_csv_data(config)
    
    # Validate data
    validation_result = validate_data(extraction_result)
    
    # Load data to MySQL
    load_result = load_data_to_mysql(extraction_result, config, validation_result)
    
    # Generate report
    report = generate_load_report(extraction_result, load_result, config)
    
    # Set up task dependencies
    start >> config >> extraction_result >> validation_result >> prep_table >> load_result >> validate_load_with_gx(config) >> report >> end
