from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Query")
    .config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"
    )
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "./warehouse")
    .getOrCreate()
)

spark.sparkContext.setLogLevel('ERROR')

spark.sql('select * from local.db.bronze_orders limit 10').show(truncate=False)

