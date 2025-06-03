from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

# Aponta para o bucket “bronze” no MinIO
BUCKET_BRONZE = "s3a://bronze"

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sisvan", "bronze", "iceberg"],
)
def test_fake_to_bronze():

    @task.pyspark(
        conn_id="spark_default",
        config_kwargs={

            #
            # 1) Dependências do Iceberg + Hadoop-AWS + AWS SDK (MinIO)
            #
            "spark.jars.packages": ",".join([
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.0",
            ]),

            #
            # 2) Habilita Iceberg Extensions
            #
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",

            #
            # 3) Configura um HadoopCatalog chamado "bronze_catalog" para ficar em s3a://bronze
            #
            "spark.sql.catalog.bronze_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.bronze_catalog.type": "hadoop",
            "spark.sql.catalog.bronze_catalog.warehouse": BUCKET_BRONZE,

            #
            # 4) Para que writeTo e read.table() usem bronze_catalog por padrão:
            #
            "spark.sql.defaultCatalog": "bronze_catalog",

            #
            # 5) Configurações de acesso S3A/MinIO
            #
            "spark.hadoop.fs.s3a.endpoint": Variable.get("MINIO_ENDPOINT"),
            "spark.hadoop.fs.s3a.access.key": Variable.get("MINIO_ACCESS_KEY"),
            "spark.hadoop.fs.s3a.secret.key": Variable.get("MINIO_SECRET_KEY"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
    )
    def transform(spark: SparkSession, sc: SparkContext):
        #
        # 1) Só para garantir: estamos usando o catálogo “bronze_catalog”
        #
        print(f"⭐ Current catalog: {spark.catalog.currentCatalog()}  (deve ser bronze_catalog)")

        #
        # 2) Gera alguns dados falsos para sisvan
        #
        sample_rows = [
            (2024,  5,  "DF", 70.0, 1.75, 22.9, 30, "1", "7",  "6", "1",  "21"),
            (2024,  4,  "SP", 80.0, 1.80, 24.7, 45, "2", "8",  "5", "2",  "3"),
            (2023, 12,  "RJ", 60.0, 1.65, 22.0, 25, "4", "7", "11", "1", "12"),
        ]
        columns = [
            "ano", "mes", "sigla_uf",
            "peso", "altura", "imc",
            "idade", "raca_cor",
            "fase_vida", "escolaridade",
            "sistema_origem", "povo_comunidade"
        ]
        fake_data = sample_rows * 1000
        sisvan_df = spark.createDataFrame(fake_data, schema=columns)

        print("🧾 Esquema do DataFrame fake:")
        sisvan_df.printSchema()
        print("🔍 Algumas linhas iniciais:")
        sisvan_df.show(5)

        #
        # 3) Garante que o namespace “default” exista (no Iceberg/HadoopCatalog, o namespace padrão é 'default')
        #    e que sobrescrevamos qualquer tabela anterior com o mesmo nome:
        #
        spark.sql("DROP TABLE IF EXISTS default.sisvan")

        #
        # 4) Vamos gravar em iceberg → será escrito em s3a://bronze/sisvan
        #
        start_time = time.time()
        (
            sisvan_df
                .writeTo("default.sisvan")      # equivale a bronze_catalog.default.sisvan
                .using("iceberg")
                .tableProperty("write.format.default", "parquet")
                .partitionedBy(col("ano"), col("mes"), col("sigla_uf"))
                .createOrReplace()
        )
        elapsed = time.time() - start_time
        print(f"✅ Tabela Bronze 'default.sisvan' criada em {elapsed:.2f} s.")

        #
        # 5) Pega o caminho físico em MinIO e imprime contagem de arquivos/tamanho
        #
        location_uri = f"{BUCKET_BRONZE}/sisvan"
        print(f"➡️ Location físico Bronze: {location_uri}")

        jpath = spark._jvm.org.apache.hadoop.fs.Path(location_uri)
        jfs   = jpath.getFileSystem(spark._jsc.hadoopConfiguration())
        summary = jfs.getContentSummary(jpath)
        file_count = summary.getFileCount()
        size_bytes = summary.getLength()
        print(f"📦 Bronze: #Arquivos = {file_count}, Tamanho total (bytes) = {size_bytes}")

    transform()

test_fake_to_bronze()
