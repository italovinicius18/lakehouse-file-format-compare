from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sisvan", "gold", "iceberg"]
)
def silver_to_gold():

    @task.pyspark(
        conn_id="spark_default",
        config_kwargs={
            # Connectors and packages: BigQuery, Delta, Hadoop AWS
            "spark.jars.packages": ",".join([
                # "com.google.cloud.spark:spark-3.5-bigquery:0.42.1",
                "io.delta:delta-core_2.12:2.4.0",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.0"
            ]),
            # MinIO (S3) settings
            "spark.hadoop.fs.s3a.endpoint": Variable.get("MINIO_ENDPOINT"),
            "spark.hadoop.fs.s3a.access.key": Variable.get("MINIO_ACCESS_KEY"),
            "spark.hadoop.fs.s3a.secret.key": Variable.get("MINIO_SECRET_KEY"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
        }
    )
    def transform_to_gold(spark: SparkSession, sc: SparkContext):
        builder = spark.builder.appName("SisvanBigQueryToBronze") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        # # Create sample DataFrame
        # data = [
        #     (2024, 1, "SP", "sample_data_1"),
        #     (2024, 2, "RJ", "sample_data_2"),
        #     (2024, 3, "MG", "sample_data_3")
        # ]

        # columns = ["ano", "mes", "sigla_uf", "dados"]
        # df = spark.createDataFrame(data, columns)
        
        # df.show()
        
        # df.write \
        #   .format("delta") \
        #   .mode("overwrite") \
        #   .option("path", "s3a://gold/teste") \
        #   .saveAsTable("prod_gold.teste")

    transform_to_gold()

# instancia o DAG
silver_to_gold()
