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
    tags=["sisvan", "silver", "delta"]
)
def bronze_to_silver():

    @task.pyspark(
        conn_id="spark_default",
        config_kwargs={
            # Pacotes necessários: Delta + Hadoop AWS/MinIO
            "spark.jars.packages": ",".join([
                "io.delta:delta-core_2.12:2.4.0",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.0"
            ]),
            # Configurações de S3 (MinIO)
            "spark.hadoop.fs.s3a.endpoint": Variable.get("MINIO_ENDPOINT"),
            "spark.hadoop.fs.s3a.access.key": Variable.get("MINIO_ACCESS_KEY"),
            "spark.hadoop.fs.s3a.secret.key": Variable.get("MINIO_SECRET_KEY"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
        }
    )
    def transform_to_silver(spark: SparkSession, sc: SparkContext):
        # Configura Delta no builder
        builder = spark.builder.appName("BronzeToSilver") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        # Leitura da camada Bronze
        df = spark.read.format("delta").load("s3a://bronze/sisvan")

        # Mapas de substituição
        raca_cor_map = {
            "5": "Indigena", "3": "Amarela", "X": "Invalido",
            "1": "Branca", "4": "Parda", "2": "Preta", "99": "Sem informacao"
        }
        fase_vida_map = {
            "6": "Adolescente", "3": "Entre 2 anos a 5 anos", "7": "Adulto",
            "4": "Entre 5 anos a 7 anos", "8": "Idoso", "1": "Menor de 6 meses",
            "2": "Entre 6 meses a 2 anos", "5": "Entre 7 anos a 10 anos"
        }
        escolaridade_map = {
            "1": "Creche", "5": "Ensino fundamental 5ª a 8ª séries", "99": "Sem informação",
            "11": "Ensino médio especial", "14": "Alfabetização para adultos (mobral, etc)",
            "6": "Ensino fundamental completo", "2": "Pré-escola (exceto ca)",
            "10": "Ensino médio, médio2º ciclo (científico,técnico e etc)", "15": "Nenhum",
            "9": "Ensino fundamental eja - séries iniciais (supletivo 5ª a 8ª)",
            "13": "Superior, aperfeiçoamento, especialização, mestrado, doutorado",
            "3": "Classe alfabetizada - ca", "4": "Ensino fundamental 1ª a 4ª séries",
            "12": "Ensino médio eja(supletivo)", "7": "Ensino fundamental especial",
            "8": "Ensino fundamental eja - séries iniciais (supletivo 1ª a 4ª)"
        }
        sistema_origem_map = {
            "4": "E-sus ab", "1": "Sisvan web", "2": "Auxilio brasil"
        }
        povo_comunidade_map = {
            "19": "Seringueiros", "15": "Povos de terreiro", "6": "Comunidades do cerrado",
            "2": "Agroextrativistas", "4": "Caiçaras", "12": "Pescadores artesanais",
            "21": "Outros", "1": "Povos quilombolas", "14": "Povos ciganos",
            "20": "Vazanteiros", "5": "Comunidades de fundo e fecho de pasto",
            "17": "Retireiros", "7": "Extrativistas", "3": "Caatingueiros",
            "13": "Pomeranos", "10": "Marisqueiros", "9": "Geraizeiros",
            "8": "Faxinalenses", "18": "Ribeirinhos", "11": "Pantaneiros",
            "16": "Quebradeiras de coco-de-babaçu"
        }

        # Aplica mapeamentos
        df2 = df.replace(raca_cor_map, subset=["raca_cor"]) \
                .replace(fase_vida_map, subset=["fase_vida"]) \
                .replace(escolaridade_map, subset=["escolaridade"]) \
                .replace(sistema_origem_map, subset=["sistema_origem"]) \
                .replace(povo_comunidade_map, subset=["povo_comunidade"])

        # Grava na camada Silver
        df2.write \
           .format("delta") \
           .mode("overwrite") \
           .partitionBy("ano", "mes", "sigla_uf") \
           .save("s3a://silver/sisvan")

    transform_to_silver()

# Instancia o DAG
bronze_to_silver()
