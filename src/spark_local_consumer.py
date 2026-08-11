import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F



__schema = 'order_id string,user_id string,product_id string,product_name string,category string,price double,quantity int,total_amount double,payment_method string,timestamp string'


spark = (SparkSession.builder
        .appName('KafkaStreaming')
        .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"
    )
        .config('spark.sql.extenstions','org.apache.iceberg.spark.extenstions.IcebergSparkSessionExtensions')
        .config('spark.sql.catalog.local','org.apache.iceberg.spark.SparkCatalog')
        .config('spark.sql.catalog.local.type','hadoop')
        .config('spark.sql.catalog.local.warehouse','./warehouse')
        .getOrCreate()
        )

spark.sparkContext.setLogLevel('WARN')

kafka_raw_df = (
    spark.readStream
    .format('kafka')
    .option('kafka.bootstrap.servers','localhost:19092')
    .option('subscribe','raw_orders')
    .option('startingOffsets','earliest')
    .load()
)

parsed_orders_df = (
    kafka_raw_df
    .selectExpr('CAST(value as STRING) as json_payload')
    .select(F.from_json(F.col('json_payload'),__schema).alias('data'))
    .select("data.*")
    .withColumn('ingested_at',F.current_timestamp())
)


spark.sql("""
    create table if not exists local.db.bronze_orders (
        order_id STRING,
        user_id STRING,
        product_id STRING,
        product_name STRING,
        category STRING,
        price DOUBLE,
        quantity INT,
        total_amount DOUBLE,
        payment_method STRING,
        timestamp STRING,
        ingested_at TIMESTAMP
    ) using iceberg
    partitioned by (category)

""")


query = (
    parsed_orders_df.writeStream
    .format('iceberg')
    .outputMode('append')
    .trigger(processingTime='5 seconds')
    .option('checkpointLocation','./checkpoint/bronze_orders')
    .toTable('local.db.bronze_orders')
    )

query.awaitTermination()