from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from faker import Faker
import random

# Constantes
BUCKET_BRONZE = "s3a://bronze"
fake = Faker()

# Dados estáticos
PRODUCTS = [
    {"product_id": 1, "product_name": "Credit Card", "product_category": "Credit"},
    {"product_id": 2, "product_name": "Personal Loan", "product_category": "Credit"},
    {"product_id": 3, "product_name": "NuInvest Stocks", "product_category": "Investment"},
    {"product_id": 4, "product_name": "Savings Account", "product_category": "Banking"},
]
TRANSACTION_TYPES = [
    {"transaction_type_id": 101, "transaction_type_name": "CREDIT_PURCHASE", "is_financial": True},
    {"transaction_type_id": 102, "transaction_type_name": "INVESTMENT_BUY",   "is_financial": True},
    {"transaction_type_id": 103, "transaction_type_name": "INVESTMENT_SELL",  "is_financial": True},
    {"transaction_type_id": 104, "transaction_type_name": "LOAN_PAYMENT",     "is_financial": True},
    {"transaction_type_id": 201, "transaction_type_name": "UPDATE_ADDRESS",   "is_financial": False},
]

@dag(
    start_date=datetime(2025, 6, 28),
    catchup=False,
    tags=["synthetic", "bronze", "csv"],
)
def generate_bronze():
    @task.pyspark(
        conn_id="spark_default",
        config_kwargs={
            "spark.jars.packages": ",".join([
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.0",
            ]),
            "spark.hadoop.fs.s3a.endpoint": Variable.get("MINIO_ENDPOINT"),
            "spark.hadoop.fs.s3a.access.key": Variable.get("MINIO_ACCESS_KEY"),
            "spark.hadoop.fs.s3a.secret.key": Variable.get("MINIO_SECRET_KEY"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
    )
    def ingest(spark: SparkSession):
        # utilitário para escrever CSV
        def write_csv(df, path, mode="append"):
            df.write.format("csv") \
              .mode(mode) \
              .option("header", "true") \
              .save(path)

        # 1) Dimensões estáticas (sempre overwrite)
        for name, data in [
            ("d_products", PRODUCTS),
            ("d_transaction_types", TRANSACTION_TYPES)
        ]:
            path = f"{BUCKET_BRONZE}/{name}"
            df_static = spark.createDataFrame(data) \
                .withColumn("ingestion_timestamp", current_timestamp())
            write_csv(df_static, path, mode="overwrite")

        # 2) Carrega IDs existentes
        def load_ids(table: str, id_col: str):
            try:
                df = spark.read.format("csv") \
                    .option("header", "true") \
                    .load(f"{BUCKET_BRONZE}/{table}")
                return [int(row[id_col]) for row in df.select(id_col).collect()]
            except Exception:
                return []

        customer_ids = load_ids("d_customers", "customer_id")
        contract_ids = load_ids("f_contracts",  "contract_id")

        # 3) Gera e grava novos clientes
        new_customers = []
        base = max(customer_ids) if customer_ids else 0
        for i in range(random.randint(1, 5)):
            new_customers.append({
                "customer_id": base + i + 1,
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
            })
        if new_customers:
            df_c = spark.createDataFrame(new_customers) \
                .withColumn("ingestion_timestamp", current_timestamp())
            write_csv(df_c, f"{BUCKET_BRONZE}/d_customers")
            customer_ids += [c["customer_id"] for c in new_customers]

        # 4) Gera e grava novos contratos
        new_contracts = []
        base_c = max(contract_ids) if contract_ids else 0
        for i in range(random.randint(1, 3)):
            start = fake.date_time_this_decade()
            new_contracts.append({
                "contract_id": base_c + i + 1,
                "customer_id": random.choice(customer_ids),
                "product_id": random.choice(PRODUCTS)["product_id"],
                "contract_status": random.choice(["ACTIVE","BLOCKED","CANCELLED"]),
                "start_date": start,
                "end_date": start + timedelta(days=365*5) if random.random()>0.2 else None,
            })
        if new_contracts:
            df_k = spark.createDataFrame(new_contracts) \
                .withColumn("ingestion_timestamp", current_timestamp())
            write_csv(df_k, f"{BUCKET_BRONZE}/f_contracts")
            contract_ids += [c["contract_id"] for c in new_contracts]

        # 5) Gera e grava novas transações
        new_tx = []
        for i in range(random.randint(5, 20)):
            new_tx.append({
                "transaction_id": int(datetime.now().timestamp() * 1e6) + i,
                "contract_id": random.choice(contract_ids),
                "transaction_type_id": random.choice(TRANSACTION_TYPES)["transaction_type_id"],
                "transaction_status": random.choice(["COMPLETED","PENDING","FAILED"]),
                "amount": round(random.uniform(5.0,1000.0),2),
                "currency": "BRL",
                "transaction_timestamp": fake.date_time_this_year(),
            })
        if new_tx:
            df_t = spark.createDataFrame(new_tx) \
                .withColumn("ingestion_timestamp", current_timestamp())
            write_csv(df_t, f"{BUCKET_BRONZE}/f_transactions")

    ingest()

generate_bronze()
