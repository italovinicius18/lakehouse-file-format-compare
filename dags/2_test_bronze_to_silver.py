from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

# Raiz dos nossos buckets MinIO para as camadas “bronze” e “silver”
BUCKET_BRONZE = "s3a://bronze"
BUCKET_SILVER = "s3a://silver"

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sisvan", "silver", "iceberg"],
)
def test_bronze_to_silver():

    def remove_outliers_by_percentile(df, col_name, lower=0.01, upper=0.99):
        bounds = df.approxQuantile(col_name, [lower, upper], 0.07)
        lower_bound, upper_bound = bounds
        return df.filter(
            (col(col_name) >= lower_bound) & (col(col_name) <= upper_bound)
        )

    def clean_and_map(df):
        # 1) Filtra linhas com ano, mês e UF nulos
        df = df.filter(
            col("ano").isNotNull() &
            col("mes").isNotNull() &
            col("sigla_uf").isNotNull()
        )

        # 2) Remove outliers por percentil nas colunas numéricas
        for numeric_col in ["peso", "altura", "imc", "idade"]:
            if numeric_col in df.columns:
                df = remove_outliers_by_percentile(df, numeric_col)

        # 3) Aplica mapeamentos semânticos
        df = df.replace({
            "5": "Indigena",
            "3": "Amarela",
            "X": "Invalido",
            "1": "Branca",
            "4": "Parda",
            "2": "Preta",
            "99": "Sem informacao",
        }, subset=["raca_cor"]) \
        .replace({
            "6": "Adolescente",
            "3": "Entre 2 anos a 5 anos",
            "7": "Adulto",
            "4": "Entre 5 anos a 7 anos",
            "8": "Idoso",
            "1": "Menor de 6 meses",
            "2": "Entre 6 meses a 2 anos",
            "5": "Entre 7 anos a 10 anos",
        }, subset=["fase_vida"]) \
        .replace({
            "1": "Creche",
            "5": "Ensino fundamental 5ª a 8ª séries",
            "99": "Sem informação",
            "11": "Ensino médio especial",
            "14": "Alfabetização para adultos (mobral, etc)",
            "6": "Ensino fundamental completo",
            "2": "Pré-escola (exceto ca)",
            "10": "Ensino médio, médio2º ciclo (científico,técnico e etc)",
            "15": "Nenhum",
            "9": "Ensino fundamental eja - séries iniciais (supletivo 5ª a 8ª)",
            "13": "Superior, aperfeiçoamento, especialização, mestrado, doutorado",
            "3": "Classe alfabetizada - ca",
            "4": "Ensino fundamental 1ª a 4ª séries",
            "12": "Ensino médio eja(supletivo)",
            "7": "Ensino fundamental especial",
            "8": "Ensino fundamental eja - séries iniciais (supletivo 1ª a 4ª)",
        }, subset=["escolaridade"]) \
        .replace({"4": "E-sus ab", "1": "Sisvan web", "2": "Auxilio brasil"}, subset=["sistema_origem"]) \
        .replace({
            "19": "Seringueiros", "15": "Povos de terreiro", "6": "Comunidades do cerrado",
            "2": "Agroextrativistas", "4": "Caiçaras", "12": "Pescadores artesanais",
            "21": "Outros", "1": "Povos quilombolas", "14": "Povos ciganos",
            "20": "Vazanteiros", "5": "Comunidades de fundo e fecho de pasto",
            "17": "Retireiros", "7": "Extrativistas", "3": "Caatingueiros",
            "13": "Pomeranos", "10": "Marisqueiros", "9": "Geraizeiros",
            "8": "Faxinalenses", "18": "Ribeirinhos", "11": "Pantaneiros",
            "16": "Quebradeiras de coco-de-babaçu",
        }, subset=["povo_comunidade"])

        return df

    @task.pyspark(
        conn_id="spark_default",
        config_kwargs={

            # 1) Dependências do Iceberg + Hadoop-AWS + AWS SDK (MinIO)
            "spark.jars.packages": ",".join([
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.0",
            ]),

            # 2) Habilita as extensões do Iceberg
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",

            #
            # 3) Configura dois HadoopCatalogs:
            #    – bronze_catalog: Iceberg → s3a://bronze
            #    – silver_catalog: Iceberg → s3a://silver
            #
            "spark.sql.catalog.bronze_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.bronze_catalog.type": "hadoop",
            "spark.sql.catalog.bronze_catalog.warehouse": BUCKET_BRONZE,

            "spark.sql.catalog.silver_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.silver_catalog.type": "hadoop",
            "spark.sql.catalog.silver_catalog.warehouse": BUCKET_SILVER,

            # 4) Configurações de acesso S3A/MinIO
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
        # ——————————————————————————————————————————————
        # 1) LEITURA da tabela Bronze Iceberg:
        #    – Catálogo: bronze_catalog
        #    – Namespace (schema): default
        #    – Tabela: sisvan
        #    Fisicamente fica em: s3a://bronze/sisvan
        # ——————————————————————————————————————————————
        print(f"🌐 Spark version: {spark.version}")

        # Em vez de usar "USE CATALOG", basta QUALIFICAR o nome da tabela:
        #   bronze_catalog.default.sisvan
        df_bronze = spark.sql("""
            SELECT * FROM bronze_catalog.default.sisvan
        """)

        print("📖 Esquema do DataFrame Bronze:")
        df_bronze.printSchema()
        print("🔍 Primeiras linhas do DataFrame Bronze:")
        df_bronze.show()

        # ——————————————————————————————————————————————
        # 2) LIMPEZA / MAPEAMENTO
        # ——————————————————————————————————————————————
        df_clean = clean_and_map(df_bronze)
        print("🧹 DataFrame amenizado (clean_and_map):")
        df_clean.show()

        # ——————————————————————————————————————————————
        # 3) ESCRITA na camada Silver:
        #    – Catálogo silver_catalog
        #    – Cria namespace “silver” se não existir
        #    – Gera três tabelas Iceberg: sisvan_parquet, sisvan_orc e sisvan_avro
        # ——————————————————————————————————————————————
        # Cria o namespace “silver” (= schema) dentro do catálogo silver_catalog:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS silver_catalog.default")

        results = []
        for fmt in ["parquet", "orc", "avro"]:
            #
            # Monta o nome qualificado da tabela Iceberg:
            #   silver_catalog.default.sisvan_<fmt>
            #
            table_name = f"silver_catalog.default.sisvan_{fmt}"

            # 3a) Se ela já existia, drope primeiro
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

            # 3b) Grava no Iceberg -> s3a://silver/silver/sisvan_<fmt>
            start_time = time.time()
            (
                df_clean
                    .writeTo(table_name)                       # → silver_catalog.default.sisvan_<fmt>
                    .using("iceberg")
                    .tableProperty("write.format.default", fmt)
                    .partitionedBy(col("ano"), col("mes"), col("sigla_uf"))
                    .createOrReplace()
            )
            elapsed = time.time() - start_time
            print(f"✅ Tabela Iceberg {table_name} criada em {elapsed:.2f} s.")

            # 4) Local físico no MinIO:
            #     s3a://silver/default/sisvan_<fmt>
            location_uri = f"{BUCKET_SILVER}/default/sisvan_{fmt}"
            print(f"➡️ Location físico de {table_name}: {location_uri}")

            # 5) Conta arquivos & bytes
            jpath = spark._jvm.org.apache.hadoop.fs.Path(location_uri)
            jfs   = jpath.getFileSystem(spark._jsc.hadoopConfiguration())
            summary = jfs.getContentSummary(jpath)
            file_count = summary.getFileCount()
            size_bytes = summary.getLength()

            results.append((fmt, elapsed, file_count, size_bytes))

        # ——————————————————————————————————————————————
        # 4) Imprime relatório final no log
        # ——————————————————————————————————————————————
        print()
        print("Formato | Tempo (s) | #Arquivos | Tamanho (bytes)")
        for fmt, dur, cnt, sz in results:
            print(f"{fmt:7} | {dur:8.2f} | {cnt:9d} | {sz:13d}")

    transform()

test_bronze_to_silver()
