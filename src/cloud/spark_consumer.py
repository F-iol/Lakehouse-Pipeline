import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE_BUCKET = 'lakehouse-project-bronze-fiol'
CATALOG_NAME = 'lakehouse_project_catalog_fiol'
REGION = 'eu-central-1'

__schema = 'order_id string,user_id string,product_id string,product_name string,category string,price double,quantity int,total_amount double,payment_method string,timestamp string'

load_dotenv()

spark = (SparkSession.builder
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,"
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
        "org.apache.iceberg:iceberg-aws-bundle:1.5.2,"
        "org.apache.hadoop:hadoop-aws:3.3.4"
    )
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{BRONZE_BUCKET}/")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_catalog.client.region",REGION)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


kafka_raw_df = (spark.readStream.format('kafka')
                .option('kafka.bootstrap.servers','localhost:19092')
                .option('subscribe','raw_orders')
                .option('startingOffsets','earliest')
                .option('failOnDataLoss','false')
                .load())

parsed_orders_df = (
                    kafka_raw_df
                    .selectExpr('Cast(value as string) as json_payload')
                    .select(F.from_json(F.col('json_payload'),__schema).alias('data'))
                    .select('data.*')
                    .withColumn('ingested_at',F.current_timestamp())
                    )


target_table = f"glue_catalog.{CATALOG_NAME}.bronze_orders"

spark.sql(f"""
        create table if not exists {target_table} (
        order_id string,
        user_id string,
        product_id string,
        product_name string,
        category string,
        price double,
        quantity int,
        total_amount double,
        payment_method string,
        timestamp string,
        ingested_at timestamp
        )
        using iceberg
        partitioned by (category)
        location 's3://{BRONZE_BUCKET}/bronze_orders'

""")

checkpoint_path = f"s3a://{BRONZE_BUCKET}/checkpoints/bronze_orders"

query =parsed_orders_df.writeStream.format('iceberg').outputMode('append').trigger(processingTime='10 seconds').option('checkpointLocation',checkpoint_path).toTable(target_table)
print('streaming active')
query.awaitTermination()
