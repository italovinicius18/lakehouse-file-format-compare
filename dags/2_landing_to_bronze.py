from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
from pyspark.sql import SparkSession

@dag(start_date=datetime(2024, 1, 1), schedule=None, catchup=False, tags=["sisvan", "bronze", "delta"])
def landing_to_bronze():

    @task.pyspark(
        conn_id="spark_default",
        config_kwargs={
            "spark.jars.packages": ",".join([
                "io.delta:delta-spark_2.12:3.1.0",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.0"
            ]),
            "spark.hadoop.fs.s3a.endpoint": Variable.get("MINIO_ENDPOINT"),
            "spark.hadoop.fs.s3a.access.key": Variable.get("MINIO_ACCESS_KEY"),
            "spark.hadoop.fs.s3a.secret.key": Variable.get("MINIO_SECRET_KEY"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.catalogImplementation": "hive",
            "hive.metastore.uris": "thrift://hive-metastore:9083"
        }
    )
    def extract_and_load(spark: SparkSession, sc):
        # Leitura da tabela Delta na camada landing
        df = spark.read.format("delta").load("s3a://landing/sisvan")

        print("Esquema do DataFrame:")
        df.printSchema()
        df.show(5)

        # Criação do schema bronze (caso não exista)
        spark.sql("""
            CREATE DATABASE IF NOT EXISTS bronze
            LOCATION 's3a://bronze'
        """)

        # Escrita da tabela Delta no metastore Hive
        df.write \
          .format("delta") \
          .mode("overwrite") \
          .partitionBy("ano", "mes", "sigla_uf") \
          .saveAsTable("bronze.sisvan")

    extract_and_load()

landing_to_bronze()
