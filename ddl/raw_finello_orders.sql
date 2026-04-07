CREATE TABLE raw.finelo_orders
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
    routing             STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
)
    PARTITION BY DATE (created_at)
    CLUSTER BY status, type, geo_country;
