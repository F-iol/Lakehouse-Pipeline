{{ config (
    materialized= 'table',
    table_type='iceberg',
    format = "parquet",
    partitioned_by=['category']
)}}

with raw_source as (
    select * from {{source('lakehouse_source','bronze_orders')}}
),
cte as (
    select 
        order_id,
        user_id,
        product_id,
        product_name,
        category,
        cast(price as double) as price,
        cast(quantity as integer) as quantity,
        cast(total_amount as double) as total_amount,
        upper(trim(payment_method)) as payment_method,
        from_iso8601_timestamp(timestamp) as order_timestamp,
        ingested_at,
        row_number() over(partition by order_id order by ingested_at desc) as row_number
    from raw_source
    where order_id is not null and total_amount > 0
)
select 
    order_id,
    user_id,
    product_id,
    product_name,
    category,
    price,
    quantity,
    total_amount,
    payment_method,
    order_timestamp,
    ingested_at
from cte
where row_number = 1