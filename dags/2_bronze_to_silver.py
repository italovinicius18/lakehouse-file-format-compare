from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sisvan", "silver", "delta"],
)
def bronze_to_silver():

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
            "spark.jars.packages": ",".join([
                "io.delta:delta-spark_2.12:3.1.0",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.0",
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
    def transform(spark: SparkSession, sc: SparkContext):
        # 1) Leitura dos dados já em bronze
        df = spark.read.table("bronze.sisvan")

        # 2) Limpeza e mapeamento
        df_clean = clean_and_map(df)

        # 3) Prepara a camada silver no metastore
        spark.sql("CREATE DATABASE IF NOT EXISTS silver LOCATION 's3a://silver'")
        spark.sql("DROP TABLE IF EXISTS silver.sisvan")

        # 4) Escrita em Delta particionado
        df_clean.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("ano", "mes", "sigla_uf") \
            .saveAsTable("silver.sisvan")

    transform()

bronze_to_silver()
