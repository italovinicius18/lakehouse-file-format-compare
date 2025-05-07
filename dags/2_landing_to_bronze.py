from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sisvan", "bronze", "delta"],
)
def landing_to_bronze():

    def remove_outliers_by_percentile(df, col_name, lower=0.01, upper=0.99):
        print(f"📊 Calculando percentis para coluna '{col_name}'...")
        bounds = df.approxQuantile(col_name, [lower, upper], 0.07)
        lower_bound, upper_bound = bounds
        print(f"➖ Removendo outliers: {col_name} < {lower_bound} ou > {upper_bound}")
        return df.filter(
            (col(col_name) >= lower_bound) & (col(col_name) <= upper_bound)
        )

    def clean_landing_data(df):
        print("🧼 Iniciando tratamento dos dados da landing...")

        df = df.replace("NULL", None)

        # Remove linhas sem ano, mês ou sigla_uf (necessárias para particionamento)
        df = df.filter(
            col("ano").isNotNull()
            & col("mes").isNotNull()
            & col("sigla_uf").isNotNull()
        )

        # Remove outliers por percentis
        for col_name in ["peso", "altura", "imc", "idade"]:
            if col_name in df.columns:
                df = remove_outliers_by_percentile(df, col_name)

        print("✅ Tratamento concluído.")
        return df

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
            "spark.driver.memory":   "2g",
            "spark.executor.memory": "3g",
            "spark.executor.cores":  "2",
        },
    )
    def extract_and_load(spark: SparkSession, sc):
        print("🚀 Iniciando leitura da camada landing (s3a://landing/sisvan)...")
        df = spark.read.format("delta").load("s3a://landing/sisvan")

        print("✅ Dados lidos com sucesso!")
        df.printSchema()
        df.show(5)

        # Count rows
        row_count = df.count()
        print(f"📊 Total de linhas lidas inicialmente: {row_count}")

        df = clean_landing_data(df)

        row_count = df.count()
        print(f"📊 Total de linhas após tratamento: {row_count}")

        print("📁 Criando schema 'bronze' no metastore Hive (caso não exista)...")
        spark.sql("CREATE DATABASE IF NOT EXISTS bronze LOCATION 's3a://bronze'")

        print("💾 Gravando dados em 'bronze.sisvan' com particionamento por ano, mês e UF...")
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("ano", "mes", "sigla_uf") \
            .saveAsTable("bronze.sisvan")

        print("✅ Tabela bronze.sisvan criada com sucesso no Hive Metastore!")

    extract_and_load()


landing_to_bronze()
