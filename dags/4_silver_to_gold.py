from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import count, col

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
            "hive.metastore.uris": "thrift://hive-metastore:9083",
        },
    )
    def aggregate_to_gold(spark: SparkSession, sc: SparkContext):
        print("🔄 Iniciando agregações da camada Silver para Gold...")

        print("📥 Lendo tabela 'silver.sisvan' do Hive Metastore...")
        df = spark.read.table("silver.sisvan")
        df.createOrReplaceTempView("silver_sisvan")

        print("📁 Criando schema 'gold' no metastore Hive (caso não exista)...")
        spark.sql("CREATE DATABASE IF NOT EXISTS gold LOCATION 's3a://gold'")

        # 1. Estado nutricional por faixa de vida e sexo
        print("📊 Gerando 'gold.estado_nutricional_por_faixa_etaria'...")
        df_estado = spark.sql("""
            SELECT
                fase_vida,
                sexo,
                estado_nutricional_adulto AS estado_nutricional,
                COUNT(*) AS total
            FROM silver_sisvan
            WHERE estado_nutricional_adulto IS NOT NULL
            GROUP BY fase_vida, sexo, estado_nutricional_adulto
        """)
        df_estado.write.format("delta").mode("overwrite").saveAsTable("gold.estado_nutricional_por_faixa_etaria")

        # 2. Total de acompanhamentos mensais por UF
        print("📊 Gerando 'gold.acompanhamentos_mensais'...")
        df_mensal = spark.sql("""
            SELECT
                ano,
                mes,
                sigla_uf,
                COUNT(*) AS total_acompanhamentos
            FROM silver_sisvan
            GROUP BY ano, mes, sigla_uf
        """)
        df_mensal.write.format("delta").mode("overwrite").saveAsTable("gold.acompanhamentos_mensais")

        # 3. Distribuição por sistema de origem
        print("📊 Gerando 'gold.distribuicao_sistema_origem'...")
        df_sistema = spark.sql("""
            SELECT sistema_origem, COUNT(*) AS total
            FROM silver_sisvan
            GROUP BY sistema_origem
        """)
        df_sistema.write.format("delta").mode("overwrite").saveAsTable("gold.distribuicao_sistema_origem")

        # 4. Total de indivíduos únicos por ano
        print("📊 Gerando 'gold.usuarios_unicos_por_ano'...")
        df_unicos = spark.sql("""
            SELECT ano, COUNT(DISTINCT id_individuo) AS total_unicos
            FROM silver_sisvan
            GROUP BY ano
        """)
        df_unicos.write.format("delta").mode("overwrite").saveAsTable("gold.usuarios_unicos_por_ano")

        print("✅ Tabelas da camada gold criadas com sucesso!")

    aggregate_to_gold()

silver_to_gold()
