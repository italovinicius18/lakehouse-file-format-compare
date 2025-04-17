from airflow.decorators import dag, task
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sisvan", "bronze", "delta"]
)
def bigquery_to_bronze():

    @task.pyspark(
        conn_id="spark_default",
        config_kwargs={
            # Connectors and packages: BigQuery, Delta, Hadoop AWS
            "spark.jars.packages": ",".join([
                "com.google.cloud.spark:spark-3.5-bigquery:0.42.1",
                "io.delta:delta-core_2.12:2.4.0",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.0"
            ]),
            # MinIO (S3) settings
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
        }
    )
    def extract_and_load(spark: SparkSession, sc: SparkContext):
        # Configure Delta on the Spark builder
        builder = spark.builder.appName("SisvanBigQueryToBronze") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        # Read SISVAN data from BigQuery
        sisvan_df = spark.read.format('bigquery') \
            .option("parentProject", "mdm-2025-1") \
            .option('credentialsFile', './include/gcp/service-account.json') \
            .option('table', 'basedosdados.br_ms_sisvan.microdados') \
            .load()

        # Write into Delta on MinIO bronze bucket partitioned
        sisvan_df.write \
            .format('delta') \
            .mode('overwrite') \
            .partitionBy('ano', 'mes', 'sigla_uf') \
            .save('s3a://bronze/sisvan_delta')

    extract_and_load()

# instantiate the DAG
bigquery_to_bronze()
