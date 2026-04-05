from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


with DAG(
    dag_id="test_bigquery_connection",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    BigQueryInsertJobOperator(
        task_id="orders",
        gcp_conn_id="bigquery",
        configuration={
            "query": {
                "query": """
                    CREATE TABLE IF NOT EXISTS raw.finelo_orders
                    (
                        order_id            STRING,
                        status              STRING,
                        type                STRING,
                        amount              FLOAT64,
                        currency            STRING,
                        mid                 STRING,
                        processing_amount   FLOAT64,
                        processing_currency STRING,
                        customer_account_id STRING,
                        geo_country         STRING,
                        error_code          FLOAT64,
                        platform            STRING,
                        fraudulent          BOOL,
                        is_secured          BOOL,
                        created_at          TIMESTAMP,
                        updated_at          TIMESTAMP,
                        routing             JSON
                    )
                    PARTITION BY DATE (created_at);
                """,
                "useLegacySql": False,
            }
        },
    )

    BigQueryInsertJobOperator(
        task_id="events",
        gcp_conn_id="bigquery",
        configuration={
            "query": {
                "query": """
                    CREATE TABLE IF NOT EXISTS raw.finelo_funnel_events
                    (
                        customer_account_id STRING,
                        event_timestamp     TIMESTAMP,
                        event_name          STRING
                    )
                        PARTITION BY DATE (event_timestamp);
                """,
                "useLegacySql": False,
            }
        },
    )
