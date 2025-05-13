from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

ROWS = 4_000_000

config = {
    "default": {
        "spark.driver.memory": "1g",
        "spark.executor.memory": "1g",
        "spark.executor.cores": "1",
    },
    "all": {
        "spark.driver.memory": "2g",
        "spark.executor.memory": "2g",
        "spark.executor.cores": "2",
    },
}

config_kwargs = {
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
}

config_kwargs_default = config_kwargs.copy()

config_kwargs_default["spark.driver.memory"] = config["default"]["spark.driver.memory"]
config_kwargs_default["spark.executor.memory"] = config["default"][
    "spark.executor.memory"
]
config_kwargs_default["spark.executor.cores"] = config["default"][
    "spark.executor.cores"
]

config_kwargs_all = config_kwargs.copy()

config_kwargs_all["spark.driver.memory"] = config["all"]["spark.driver.memory"]
config_kwargs_all["spark.executor.memory"] = config["all"]["spark.executor.memory"]
config_kwargs_all["spark.executor.cores"] = config["all"]["spark.executor.cores"]


def create_data(spark: SparkSession, sc: SparkContext):
    print("🔧 Configurando Spark com Delta Lake extensions...")

    builder = (
        spark.builder.appName("SisvanBigQueryTobronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    print("✅ Spark configurado com Delta Lake.")

    print("🧹 Limpando arquivos antigos da tabela bronze.example...")
    spark.sql("DROP TABLE IF EXISTS bronze.example")
    print("✅ Tabela bronze.example limpa com sucesso!")

    print("🚀 Criando dataset de exemplo...")
    df = spark.createDataFrame(
        [
            (
                i,
                i + 1,
                i + 2,
                i + 3,
                i + 4,
                i + 5,
                i + 6,
                i + 7,
                i + 8,
                i + 9,
                i + 10,
                i + 11,
            )
            for i in range(ROWS)
        ],
        [
            "col1",
            "col2",
            "col3",
            "col4",
            "col5",
            "col6",
            "col7",
            "col8",
            "col9",
            "col10",
            "col11",
            "col12",
        ],
    )
    print("✅ Dataset criado com sucesso.")

    print("🚀 Iniciando escrita dos dados no Delta Lake...")

    # Create a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("bronze.example")

    print("✅ Dados escritos com sucesso.")


def process_data(spark: SparkSession, sc: SparkContext):
    print("🚀 Iniciando leitura da camada silver (s3a://silver/example)...")
    # Read example table from silver
    df = spark.read.table("bronze.example")
    df.createOrReplaceTempView("example")

    df = spark.sql(
        """
            SELECT *
            FROM example
            WHERE col1 % 2 == 0
        """
    )

    print("📁 Criando schema 'silver' no metastore Hive (caso não exista)...")
    spark.sql("CREATE DATABASE IF NOT EXISTS silver LOCATION 's3a://silver'")

    print("🧹 Limpando arquivos antigos da tabela silver.example...")
    spark.sql("DROP TABLE IF EXISTS silver.example")
    print("✅ Tabela silver.example limpa com sucesso!")

    print(
        "💾 Gravando dados em 'silver.example' com particionamento por ano, mês e UF..."
    )
    df.write.format("delta").mode("overwrite").partitionBy("col1", "col2").saveAsTable(
        "silver.example"
    )

    print("✅ Tabela silver.example criada com sucesso no Hive Metastore!")


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def example():

    @task.pyspark(conn_id="spark_default", config_kwargs=config_kwargs_default)
    def default_configs(spark: SparkSession, sc: SparkContext):
        create_data(spark, sc)

    @task.pyspark(conn_id="spark_default", config_kwargs=config_kwargs_default)
    def all_resources_config(spark: SparkSession, sc: SparkContext):
        create_data(spark, sc)

    @task.pyspark(conn_id="spark_default", config_kwargs=config_kwargs_all)
    def bronze_to_silver_default(spark: SparkSession, sc):
        process_data(spark, sc)

    @task.pyspark(conn_id="spark_default", config_kwargs=config_kwargs_all)
    def bronze_to_silver_all_resources(spark: SparkSession, sc):
        process_data(spark, sc)

    (
        default_configs()
        >> bronze_to_silver_default()
        >> all_resources_config()
        >> bronze_to_silver_all_resources()
    )


example()
