from airflow.decorators import dag, task
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from airflow.models import Variable

@dag(
    start_date=datetime(2025,6,28),
    schedule=None,
    catchup=False,
    tags=["iceberg","gold","monthly_balance"],
)
def silver_to_gold():

    @task.pyspark(
        conn_id="spark_default",
        config_kwargs={
            # 1) dependências Iceberg + Hadoop-AWS + AWS SDK
            "spark.jars.packages": ",".join([
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.0",
            ]),
            # 2) habilita extensões do Iceberg
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            # 3) catálogos Silver e Gold
            "spark.sql.catalog.silver_catalog":      "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.silver_catalog.type": "hadoop",
            "spark.sql.catalog.silver_catalog.warehouse": "s3a://silver",
            "spark.sql.catalog.gold_catalog":        "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.gold_catalog.type":   "hadoop",
            "spark.sql.catalog.gold_catalog.warehouse":   "s3a://gold",
            # 4) S3A / MinIO
            "spark.hadoop.fs.s3a.endpoint":    Variable.get("MINIO_ENDPOINT"),
            "spark.hadoop.fs.s3a.access.key":  Variable.get("MINIO_ACCESS_KEY"),
            "spark.hadoop.fs.s3a.secret.key":  Variable.get("MINIO_SECRET_KEY"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl":        "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        }
    )
    def build_monthly_balance(spark: SparkSession, sc: SparkContext):
        formats = ["parquet", "orc", "avro"]
        silver_table_base = "silver_catalog.default.f_transactions_{}"

        for fmt in formats:
            silver_table = silver_table_base.format(fmt)
            gold_table = f"gold_catalog.default.monthly_balance_{fmt}"

            # 1) Cria view staging a partir da tabela Silver
            df = spark.read.table(silver_table)
            df.createOrReplaceTempView("staging")

            # 2) Cria tabela Gold particionada, usando CTAS e formato correto
            spark.sql(f"DROP TABLE IF EXISTS {gold_table}")
            spark.sql(f"""
              CREATE TABLE {gold_table}
              USING ICEBERG
              PARTITIONED BY (ano, mes)
              TBLPROPERTIES ('write.format.default' = '{fmt}')
              AS
              SELECT
                year(transaction_timestamp) AS ano,
                month(transaction_timestamp) AS mes,
                contract_id AS account,
                SUM(CASE WHEN amount >= 0 THEN amount ELSE 0 END) AS entrou,
                SUM(CASE WHEN amount < 0 THEN -amount ELSE 0 END)  AS saiu,
                SUM(amount) AS diferenca
              FROM staging
              GROUP BY ano, mes, contract_id
            """)

            spark.catalog.dropTempView("staging")

    build_monthly_balance()

silver_to_gold()
