from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'tillstream_lakehouse_maintenance',
    default_args=default_args,
    description='Automated maintenance for Apache Iceberg Lakehouse',
    schedule_interval='@daily',
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=['lakehouse', 'iceberg', 'maintenance'],
) as dag:

    # Task 1: Compact small Parquet files in Apache Iceberg to a 128MB target size
    compact_iceberg_tables = SparkSubmitOperator(
        task_id='compact_iceberg_tables',
        application='/app/lakehouse/spark/maintenance/compact_tables.py',
        name='iceberg-compaction',
        conf={
            'spark.sql.catalog.lakehouse': 'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.lakehouse.type': 'hadoop',
            'spark.sql.catalog.lakehouse.warehouse': 's3a://lakehouse/warehouse',
        },
        executor_memory='4G',
        executor_cores=2,
    )

    # Task 2: Expire old Iceberg snapshots and remove orphan files (Aligns with 7-day Kafka retention)
    expire_snapshots = SparkSubmitOperator(
        task_id='expire_iceberg_snapshots',
        application='/app/lakehouse/spark/maintenance/expire_snapshots.py',
        name='iceberg-snapshot-expiration',
        application_args=['--retention-days', '7'],
    )

    # Define DAG Dependencies (Workflow Order)
    compact_iceberg_tables >> expire_snapshots
