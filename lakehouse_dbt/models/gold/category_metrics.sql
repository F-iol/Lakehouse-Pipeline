{{ config(
    materialized='table',
    table_type='iceberg',
    s3_data_dir='s3://lakehouse-project-gold-fiol/',
    format='parquet'
)}}


select 
    category,
    count(distinct( order_id)) as total_orders,
    count(distinct(user_id)) as total_users,
    sum(quantity) as total_items_sold,
    round(sum(total_amount),2) as revenue,
    round(avg(total_amount),2) as avg_order_value

from {{ref('silver_orders')}}
group by category
