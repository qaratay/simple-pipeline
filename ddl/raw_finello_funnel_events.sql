CREATE TABLE raw.finelo_funnel_events
(
    customer_account_id STRING,
    event_timestamp     TIMESTAMP,
    event_name          STRING
)
    PARTITION BY DATE (event_timestamp);