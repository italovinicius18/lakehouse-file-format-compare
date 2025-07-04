from airflow.decorators import dag, task
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from airflow.models import Variable

@dag(
    start_date=datetime(2025, 6, 28),
    schedule=None,
    catchup=False,
    tags=["iceberg", "upsert", "historical"]
)
def bronze_to_silver():

    @task.pyspark(
        conn_id="spark_default",
        config_kwargs={
            # jars Iceberg + Hadoop-AWS + AWS SDK
            "spark.jars.packages": ",".join([
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.0",
            ]),
            # extensões Iceberg
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            # catálogo Silver Iceberg
            "spark.sql.catalog.silver_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.silver_catalog.type": "hadoop",
            "spark.sql.catalog.silver_catalog.warehouse": "s3a://silver",
            # S3A / MinIO
            "spark.hadoop.fs.s3a.endpoint":    Variable.get("MINIO_ENDPOINT"),
            "spark.hadoop.fs.s3a.access.key":  Variable.get("MINIO_ACCESS_KEY"),
            "spark.hadoop.fs.s3a.secret.key":  Variable.get("MINIO_SECRET_KEY"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl":        "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        }
    )
    def upsert_iceberg(spark: SparkSession, sc: SparkContext):
        # listas de tabelas bronze (pastas) e formatos Iceberg
        bronze_tables = [
            "d_products",
            "d_transaction_types",
            "d_customers",
            "f_contracts",
            "f_transactions",
        ]
        formats = ["parquet", "orc", "avro"]

        for table in bronze_tables:
            # 1) lê todos os CSVs em bronze/<table>/
            path = f"s3a://bronze/{table}/*.csv"
            df = (
                spark.read
                     .option("header", "true")
                     .option("inferSchema", "true")
                     .csv(path)
            )
            # assume chave única de negócio: todas as colunas que terminam em "_id"
            key_cols = [c for c in df.columns if c.endswith("_id")]

            # registra staging view
            df.createOrReplaceTempView("staging")

            for fmt in formats:
                tgt = f"silver_catalog.default.{table}_{fmt}"

                # 2) cria tabela Iceberg se não existir
                if not spark.catalog.tableExists(tgt):
                    # usar DataFrameWriterV2 para criar
                    (
                        df.writeTo(tgt)
                          .using("iceberg")
                          .tableProperty("write.format.default", fmt)
                          .create()
                    )

                # 3) merge (upsert histórico)
                join_cond = " AND ".join([f"t.{c}=s.{c}" for c in key_cols])
                spark.sql(f"""
                    MERGE INTO {tgt} AS t
                    USING staging AS s
                      ON {join_cond}
                    WHEN MATCHED THEN
                      UPDATE SET *
                    WHEN NOT MATCHED THEN
                      INSERT *
                """)

            # limpa a view temporária
            spark.catalog.dropTempView("staging")

    upsert_iceberg()

bronze_to_silver()
