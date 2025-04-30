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
    tags=["sisvan", "landing", "delta"],
)
def bigquery_to_landing():

    @task.pyspark(
        conn_id="spark_default",
        config_kwargs={
            "spark.jars.packages": ",".join(
                [
                    "com.google.cloud.spark:spark-3.5-bigquery:0.42.1",
                    "io.delta:delta-spark_2.12:3.1.0",
                    "org.apache.hadoop:hadoop-aws:3.3.4",
                    "com.amazonaws:aws-java-sdk-bundle:1.12.0",
                ]
            ),
            "spark.hadoop.fs.s3a.endpoint": Variable.get("MINIO_ENDPOINT"),
            "spark.hadoop.fs.s3a.access.key": Variable.get("MINIO_ACCESS_KEY"),
            "spark.hadoop.fs.s3a.secret.key": Variable.get("MINIO_SECRET_KEY"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
    )
    def extract_and_load(spark: SparkSession, sc: SparkContext):
        print("🔧 Configurando Spark com Delta Lake extensions...")

        builder = (
            spark.builder.appName("SisvanBigQueryToLanding")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        print("✅ Spark configurado com Delta Lake.")

        print("🚀 Iniciando leitura dos dados do BigQuery...")
        sisvan_df = (
            spark.read.format("bigquery")
            .option("parentProject", "mdm-2025-1")
            .option("credentialsFile", "./include/gcp/service-account.json")
            .option("table", "basedosdados.br_ms_sisvan.microdados")
            .load()
        )
        print("✅ Dados lidos com sucesso.")

        print("🧾 Esquema do DataFrame:")
        sisvan_df.printSchema()

        print("🔍 Primeiras linhas do DataFrame:")
        sisvan_df.show(5)

        print("💾 Salvando os dados em Delta format no bucket 'landing/sisvan'...")
        sisvan_df.write.format("delta").mode("overwrite").partitionBy(
            "ano", "mes", "sigla_uf"
        ).save("s3a://landing/sisvan")

        print("✅ Dados salvos com sucesso no formato Delta.")

    extract_and_load()


bigquery_to_landing()
