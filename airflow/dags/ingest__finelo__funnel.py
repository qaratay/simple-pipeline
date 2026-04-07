from datetime import datetime, timezone, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


GCS_ORDERS_URI = "gs://simple-pipeline-bucket/order_api_data.csv"
GCS_EVENTS_URI = "gs://simple-pipeline-bucket/raw_events.csv"


def get_bq_client():
    from google.cloud import bigquery
    from google.oauth2 import service_account
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection("bigquery")
    project_id = conn.extra_dejson.get("project")
    key_path = conn.extra_dejson.get("key_path", "/opt/airflow/gcp-key.json")
    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(project=project_id, credentials=credentials), project_id


def ingest_orders():
    from google.cloud import bigquery

    bq_client, project_id = get_bq_client()

    raw_table = f"{project_id}.raw.finelo_orders"
    staging_table = f"{project_id}.raw.finelo_orders_staging"

    schema = [
        bigquery.SchemaField("order_id", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("amount", "FLOAT64"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("mid", "STRING"),
        bigquery.SchemaField("processing_amount", "FLOAT64"),
        bigquery.SchemaField("processing_currency", "STRING"),
        bigquery.SchemaField("customer_account_id", "STRING"),
        bigquery.SchemaField("geo_country", "STRING"),
        bigquery.SchemaField("error_code", "FLOAT64"),
        bigquery.SchemaField("platform", "STRING"),
        bigquery.SchemaField("fraudulent", "BOOL"),
        bigquery.SchemaField("is_secured", "BOOL"),
        bigquery.SchemaField("created_at", "STRING"),
        bigquery.SchemaField("updated_at", "STRING"),
        bigquery.SchemaField("routing", "STRING"),
    ]

    print(f"Loading {GCS_ORDERS_URI} into staging")
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
    )

    staging_table_obj = bigquery.Table(staging_table, schema=schema)
    staging_table_obj.expires = datetime.now(timezone.utc) + timedelta(hours=1)
    bq_client.delete_table(staging_table, not_found_ok=True)
    bq_client.create_table(staging_table_obj)

    job = bq_client.load_table_from_uri(
        GCS_ORDERS_URI, staging_table, job_config=job_config
    )
    job.result()
    print(f"Loaded {job.output_rows} rows into staging")

    print("Running MERGE staging -> raw")
    merge_query = f"""
        MERGE {raw_table} AS t
        USING {staging_table} AS s
        ON t.order_id = s.order_id 
        AND DATE(t.created_at) = DATE(cast(s.created_at as timestamp))
        WHEN MATCHED THEN UPDATE SET
            status = s.status,
            type = s.type,
            amount = s.amount,
            currency = s.currency,
            mid = s.mid,
            processing_amount = s.processing_amount,
            processing_currency = s.processing_currency,
            customer_account_id = s.customer_account_id,
            geo_country = s.geo_country,
            error_code = s.error_code,
            platform = s.platform,
            fraudulent = s.fraudulent,
            is_secured = s.is_secured,
            updated_at = cast(s.updated_at as timestamp),
            routing = s.routing
        WHEN NOT MATCHED THEN INSERT
            (order_id, status, type, amount, currency, mid, processing_amount, processing_currency,
            customer_account_id, geo_country, error_code, platform, fraudulent, is_secured,
            created_at, updated_at, routing)
        VALUES 
            (s.order_id, s.status, s.type, s.amount, s.currency, s.mid, s.processing_amount, s.processing_currency,
            s.customer_account_id, s.geo_country, s.error_code, s.platform, s.fraudulent, s.is_secured,
            cast(s.created_at as timestamp), cast(s.updated_at as timestamp), s.routing);
    """
    bq_client.query(merge_query).result()
    print("MERGE completed successfully")


def ingest_events():
    from google.cloud import bigquery

    bq_client, project_id = get_bq_client()

    raw_table = f"{project_id}.raw.finelo_funnel_events"
    staging_table = f"{project_id}.raw.finelo_funnel_events_staging"

    schema = [
        bigquery.SchemaField("customer_account_id", "STRING"),
        bigquery.SchemaField("event_timestamp", "STRING"),
        bigquery.SchemaField("event_name", "STRING"),
    ]

    print(f"Loading {GCS_EVENTS_URI} into staging")
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
    )

    staging_table_obj = bigquery.Table(staging_table, schema=schema)
    staging_table_obj.expires = datetime.now(timezone.utc) + timedelta(hours=1)
    bq_client.delete_table(staging_table, not_found_ok=True)
    bq_client.create_table(staging_table_obj)

    job = bq_client.load_table_from_uri(
        GCS_EVENTS_URI, staging_table, job_config=job_config
    )
    job.result()
    print(f"Loaded {job.output_rows} rows into staging")

    print("Running DELETE from raw")
    delete_query = f"""
        DELETE
        FROM {raw_table}
        where event_timestamp >= '1970-01-01';
    """
    bq_client.query(delete_query).result()

    print("Running INSERT staging -> raw")
    insert_query = f"""
        INSERT INTO {raw_table} (customer_account_id, event_timestamp, event_name)
        SELECT  customer_account_id, cast(event_timestamp as timestamp), event_name
        FROM {staging_table}
    """
    bq_client.query(insert_query).result()
    print("INSERT completed successfully")


with DAG(
    dag_id="ingest__finelo__funnel",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    ingest_orders_task = PythonOperator(
        task_id="ingest_orders",
        python_callable=ingest_orders,
    )

    ingest_events_task = PythonOperator(
        task_id="ingest_events",
        python_callable=ingest_events,
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_build",
        trigger_dag_id="dbt_build",
        wait_for_completion=False,
    )

    [ingest_orders_task, ingest_events_task] >> trigger_dbt
