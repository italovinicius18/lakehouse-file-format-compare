from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

BUCKET_BRONZE = "s3a://bronze"

@dag(
    dag_id="generate_time_dimensions",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Executar manualmente ou uma vez
    catchup=False,
    tags=["dimensions", "time", "static"],
)
def generate_time_dimensions():
    
    @task.pyspark(
        conn_id="spark_default",
        config_kwargs={
            "spark.jars.packages": ",".join([
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.0"
            ]),
            "spark.hadoop.fs.s3a.endpoint": Variable.get("MINIO_ENDPOINT"),
            "spark.hadoop.fs.s3a.access.key": Variable.get("MINIO_ACCESS_KEY"),
            "spark.hadoop.fs.s3a.secret.key": Variable.get("MINIO_SECRET_KEY"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
    )
    def generate_time_dims(spark: SparkSession):
        """Gera dimensões de tempo estáticas"""
        
        print("⏰ Gerando dimensões de tempo...")
        
        def write_csv(df, table_name: str):
            path = f"{BUCKET_BRONZE}/{table_name}"
            df.write.format("csv") \
              .mode("overwrite") \
              .option("header", "true") \
              .save(path)
            print(f"✅ Gravado: {table_name} ({df.count()} registros)")

        # 1. Dimensão Year (2020-2030)
        years_data = []
        for year in range(2020, 2031):
            years_data.append({
                "year_id": year,
                "action_year": year
            })
        
        df_years = spark.createDataFrame(years_data) \
            .withColumn("ingestion_timestamp", current_timestamp())
        write_csv(df_years, "d_year")

        # 2. Dimensão Month (1-12)
        months_data = []
        month_names = [
            "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
            "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
        ]
        
        for month in range(1, 13):
            months_data.append({
                "month_id": month,
                "action_month": month,
                "month_name": month_names[month-1]
            })
        
        df_months = spark.createDataFrame(months_data) \
            .withColumn("ingestion_timestamp", current_timestamp())
        write_csv(df_months, "d_month")

        # 3. Dimensão Week (1-53)
        weeks_data = []
        for week in range(1, 54):
            weeks_data.append({
                "week_id": week,
                "action_week": week
            })
        
        df_weeks = spark.createDataFrame(weeks_data) \
            .withColumn("ingestion_timestamp", current_timestamp())
        write_csv(df_weeks, "d_week")

        # 4. Dimensão Weekday (1-7)
        weekdays_data = [
            {"weekday_id": 1, "action_weekday": "Segunda-feira"},
            {"weekday_id": 2, "action_weekday": "Terça-feira"},
            {"weekday_id": 3, "action_weekday": "Quarta-feira"},
            {"weekday_id": 4, "action_weekday": "Quinta-feira"},
            {"weekday_id": 5, "action_weekday": "Sexta-feira"},
            {"weekday_id": 6, "action_weekday": "Sábado"},
            {"weekday_id": 7, "action_weekday": "Domingo"},
        ]
        
        df_weekdays = spark.createDataFrame(weekdays_data) \
            .withColumn("ingestion_timestamp", current_timestamp())
        write_csv(df_weekdays, "d_weekday")

        print("🎉 Dimensões de tempo geradas com sucesso!")

    generate_time_dims()

generate_time_dimensions()
