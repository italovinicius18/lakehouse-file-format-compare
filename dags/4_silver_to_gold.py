from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import count, col
import os
from pathlib import Path
import glob


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sisvan", "gold", "delta"],
)
def silver_to_gold():

    @task.pyspark(
        conn_id="spark_default",
        config_kwargs={
            "spark.jars.packages": ",".join(
                [
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
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.catalogImplementation": "hive",
            "hive.metastore.uris": "thrift://hive-metastore:9083",
        },
    )
    def aggregate_to_gold(spark: SparkSession, sc: SparkContext):
        print("🔄 Iniciando agregações da camada Silver para Gold...")

        print("📥 Lendo tabela 'silver.sisvan' do Hive Metastore...")
        df = spark.read.table("silver.sisvan")
        df.printSchema()
        print()
        df.show(10)

        df.createOrReplaceTempView("silver_sisvan")

        print("📁 Criando schema 'gold' no metastore Hive (caso não exista)...")
        spark.sql("CREATE DATABASE IF NOT EXISTS gold LOCATION 's3a://gold'")

        # Loop over "./include/queries/*.sql" files
        print("📁 Lendo arquivos SQL da pasta './include/queries/'...")
        sql_files = glob.glob("./include/queries/*.sql")

        print(sql_files)
        for sql_file in sql_files:
            with open(sql_file, "r") as file:

                table_name = os.path.splitext(os.path.basename(sql_file))[0]
                print(f"🧹 Limpando arquivos antigos da tabela gold.{table_name}...")
                spark.sql(f"DROP TABLE IF EXISTS gold.{table_name}")
                print(f"✅ Tabela gold.{table_name} limpa com sucesso!")

                sql_query = file.read()
                print(f"📄 Executando consulta SQL: {sql_file}")
                df = spark.sql(sql_query)

                print(f"📊 Criando tabela 'gold.{table_name}'...")
                df.write.format("delta").mode("overwrite").saveAsTable(
                    f"gold.{table_name}"
                )
                print(f"✅ Tabela 'gold.{table_name}' criada com sucesso!")

        print("✅ Tabelas da camada gold criadas com sucesso!")

    aggregate_to_gold()


silver_to_gold()
