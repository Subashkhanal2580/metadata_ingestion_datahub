from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import requests
import psycopg2
from sqlalchemy import create_engine

# === DataHub Imports ===
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    MetadataChangeProposalClass,
    SchemaMetadataClass,
    DatasetPropertiesClass,
    AuditStampClass,
    SchemaFieldClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    Status,
    TimeStamp
)
from datahub.utilities.urns.data_platform_urn import DataPlatformUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn
import json
import time

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2024, 1, 1),
}

# Function to create output directory
def create_output_dir():
    output_dir = '/tmp/spark_output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    return output_dir

# Function to download CSV from GitHub
def download_csv(**kwargs):
    url = "https://raw.githubusercontent.com/Subashkhanal2580/metadata_test/main/employee_data.csv"
    local_path = "/tmp/employee_data.csv"
    
    response = requests.get(url)
    with open(local_path, 'wb') as f:
        f.write(response.content)
    
    print(f"Downloaded CSV from {url} to {local_path}")
    
    df = pd.read_csv(local_path)
    print(f"CSV has {len(df)} rows and {len(df.columns)} columns")
    print(f"Sample data:\n{df.head(3)}")
    
    return local_path

# Function to transform data
def transform_data(**kwargs):
    input_file = kwargs['ti'].xcom_pull(task_ids='download_csv')
    output_file = "/tmp/transformed_data.csv"
    
    df = pd.read_csv(input_file)
    print(f"Original data shape: {df.shape}")
    
    df['age'] = df['age'].fillna(0).astype(int)
    df['name'] = df['name'].fillna('Unknown')
    df['salary'] = df['salary'].fillna(0).astype(float)
    
    df = df[df['id'].notna()]
    df['id'] = df['id'].astype(int)
    
    print(f"Transformed data shape: {df.shape}")
    print(f"Sample transformed data:\n{df.head(3)}")
    
    df.to_csv(output_file, index=False)
    print(f"Transformed data saved to {output_file}")
    
    return output_file

# Function to load data to PostgreSQL
def load_to_postgres(**kwargs):
    transformed_file = kwargs['ti'].xcom_pull(task_ids='transform_data')
    df = pd.read_csv(transformed_file)
    print(f"Loading {len(df)} rows to PostgreSQL")
    
    conn_str = "postgresql+psycopg2://airflow:airflow@postgres:5432/etl_pipeline"
    engine = create_engine(conn_str)
    
    try:
        with engine.connect() as connection:
            connection.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                id INTEGER PRIMARY KEY,
                age INTEGER,
                name VARCHAR(255),
                salary DECIMAL(10, 2)
            )
            """)
            connection.execute("TRUNCATE TABLE employees")
        
        df.to_sql('employees', engine, if_exists='append', index=False)
        print("Data successfully loaded to PostgreSQL")
        
        with engine.connect() as connection:
            result = connection.execute("SELECT COUNT(*) FROM employees")
            count = result.fetchone()[0]
            print(f"Verified {count} rows in the employees table")
        
        return count
        
    except Exception as e:
        print(f"Error loading data to PostgreSQL: {str(e)}")
        raise

# Function to emit metadata to DataHub
def emit_to_datahub(**kwargs):
    """
    Emit metadata to DataHub using direct REST API call to the correct endpoints.
    """
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,etl_pipeline.employees,PROD)"
    gms_endpoint = "http://datahub-gms:8080"
    
    print(f"Using dataset URN: {dataset_urn}")
    
    # Create the dataset description properties
    print(f"Sending dataset properties to {gms_endpoint}")
    dataset_properties = {
        "description": "Employee dataset that contains processed information about employees",
        "customProperties": {
            "pipeline": "etl_pipeline",
            "team": "Data Team",
            "update_frequency": "daily"
        }
    }
    
    # Prepare the request payloads for each aspect
    aspect_requests = [
        # Dataset properties
        {
            "proposal": {
                "entityType": "dataset",
                "entityUrn": dataset_urn,
                "changeType": "UPSERT",
                "aspectName": "datasetProperties",
                "aspect": {
                    "value": json.dumps(dataset_properties),
                    "contentType": "application/json"
                }
            }
        },
        
        # Schema metadata
        {
            "proposal": {
                "entityType": "dataset",
                "entityUrn": dataset_urn,
                "changeType": "UPSERT",
                "aspectName": "schemaMetadata",
                "aspect": {
                    "value": json.dumps({
                        "schemaName": "employees_schema",
                        "platform": "urn:li:dataPlatform:postgres",
                        "version": 1,
                        "created": {
                            "time": int(time.time() * 1000),
                            "actor": "urn:li:corpuser:etl"
                        },
                        "lastModified": {
                            "time": int(time.time() * 1000),
                            "actor": "urn:li:corpuser:etl"
                        },
                        "hash": "",
                        "platformSchema": {
                            "com.linkedin.schema.MySqlDDL": {
                                "tableSchema": "CREATE TABLE employees (id INT, name VARCHAR(100), department VARCHAR(100));"
                            }
                        },
                        "fields": [
                            {
                                "fieldPath": "id",
                                "description": "Employee ID",
                                "type": {"type": {"com.linkedin.schema.NumberType": {}}},
                                "nativeDataType": "INTEGER"
                            },
                            {
                                "fieldPath": "name",
                                "description": "Employee Name",
                                "type": {"type": {"com.linkedin.schema.StringType": {}}},
                                "nativeDataType": "VARCHAR"
                            },
                            {
                                "fieldPath": "department",
                                "description": "Employee Department",
                                "type": {"type": {"com.linkedin.schema.StringType": {}}},
                                "nativeDataType": "VARCHAR"
                            }
                        ]
                    }),
                    "contentType": "application/json"
                }
            }
        },
        
        # Ownership
        {
            "proposal": {
                "entityType": "dataset",
                "entityUrn": dataset_urn,
                "changeType": "UPSERT",
                "aspectName": "ownership",
                "aspect": {
                    "value": json.dumps({
                        "owners": [
                            {
                                "owner": "urn:li:corpuser:admin",
                                "type": "DATAOWNER"
                            }
                        ],
                        "lastModified": {
                            "time": int(time.time() * 1000),
                            "actor": "urn:li:corpuser:etl"
                        }
                    }),
                    "contentType": "application/json"
                }
            }
        }
    ]
    
    success_count = 0
    for request_data in aspect_requests:
        aspect_name = request_data["proposal"]["aspectName"]
        print(f"Sending {aspect_name} to DataHub...")
        
        try:
            # Use the correct ingestProposal endpoint
            response = requests.post(
                f"{gms_endpoint}/aspects?action=ingestProposal",
                headers={
                    "Content-Type": "application/json",
                    "X-RestLi-Protocol-Version": "2.0.0"
                },
                json=request_data
            )
            
            if response.status_code == 200:
                print(f"Successfully emitted {aspect_name}: {response.json()}")
                success_count += 1
            else:
                print(f"Failed to emit {aspect_name}: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"Error during {aspect_name} emission: {str(e)}")
    
    if success_count != len(aspect_requests):
        raise Exception(f"Failed to emit all metadata to DataHub. Only {success_count}/{len(aspect_requests)} aspects were successful.")
    
    print(f"Successfully emitted {success_count} aspects to DataHub.")
    return "Metadata ingestion completed"

# Function to verify data in PostgreSQL
def verify_data(**kwargs):
    conn_str = "postgresql+psycopg2://airflow:airflow@postgres:5432/etl_pipeline"
    engine = create_engine(conn_str)
    
    try:
        with engine.connect() as connection:
            result = connection.execute("SELECT COUNT(*) FROM employees")
            count = result.fetchone()[0]
            
            if count == 0:
                raise ValueError("No data loaded into the employees table")
                
            print(f"Found {count} rows in the employees table")
            
            result = connection.execute("SELECT COUNT(*) FROM employees WHERE id IS NULL")
            null_count = result.fetchone()[0]
            if null_count > 0:
                print(f"WARNING: Found {null_count} rows with NULL IDs")
            
            result = connection.execute("SELECT * FROM employees LIMIT 5")
            rows = result.fetchall()
            
            print("Sample data from employees table:")
            for row in rows:
                print(row)
                
            return count
            
    except Exception as e:
        print(f"Error verifying data: {str(e)}")
        raise

# Define DAG
with DAG(
    'csv_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline to extract data from GitHub, transform, and load to PostgreSQL with DataHub metadata emission',
    schedule_interval='@once',
    catchup=False
) as dag:
    
    install_deps = BashOperator(
        task_id='install_dependencies',
        bash_command='''pip install pandas sqlalchemy psycopg2-binary requests && echo "Dependencies installed"''',
    )
    
    create_dir = PythonOperator(
        task_id='create_output_directory',
        python_callable=create_output_dir,
    )
    
    download_csv_task = PythonOperator(
        task_id='download_csv',
        python_callable=download_csv,
    )
    
    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )
    
    load_data_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )
    
    verify_data_task = PythonOperator(
        task_id='verify_data',
        python_callable=verify_data,
    )
    
    emit_datahub_task = PythonOperator(
        task_id='emit_to_datahub',
        python_callable=emit_to_datahub,
    )

    # Set task dependencies
    install_deps >> create_dir >> download_csv_task >> transform_data_task >> load_data_task >> verify_data_task >> emit_datahub_task
