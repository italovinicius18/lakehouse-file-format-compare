from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col
from faker import Faker
import random
import uuid
from typing import List, Dict

# Configuração para ingestão em lote
BATCH_SIZE_CUSTOMERS = random.randint(100, 500)  # 100-500 novos clientes por batch
BATCH_SIZE_CONTRACTS = random.randint(200, 800)  # 200-800 novos contratos por batch
BATCH_SIZE_TRANSACTIONS = random.randint(1000, 5000)  # 1k-5k transações por batch
BATCH_SIZE_ATTRIBUTES = random.randint(300, 1200)  # 300-1.2k atributos por batch

BUCKET_BRONZE = "s3a://bronze"
fake = Faker('pt_BR')  # Usar dados brasileiros

# Dados estáticos globais
COUNTRIES = [
    {"country_id": 1, "country_name": "Brasil"},
    {"country_id": 2, "country_name": "Argentina"},
    {"country_id": 3, "country_name": "Chile"},
    {"country_id": 4, "country_name": "Colombia"},
]

STATES_BRAZIL = [
    {"state_id": 1, "state_name": "São Paulo", "country_id": 1},
    {"state_id": 2, "state_name": "Rio de Janeiro", "country_id": 1},
    {"state_id": 3, "state_name": "Minas Gerais", "country_id": 1},
    {"state_id": 4, "state_name": "Bahia", "country_id": 1},
    {"state_id": 5, "state_name": "Paraná", "country_id": 1},
    {"state_id": 6, "state_name": "Rio Grande do Sul", "country_id": 1},
    {"state_id": 7, "state_name": "Pernambuco", "country_id": 1},
    {"state_id": 8, "state_name": "Ceará", "country_id": 1},
]

CITIES = [
    {"city_id": 1, "city_name": "São Paulo", "state_id": 1},
    {"city_id": 2, "city_name": "Campinas", "state_id": 1},
    {"city_id": 3, "city_name": "Santos", "state_id": 1},
    {"city_id": 4, "city_name": "Rio de Janeiro", "state_id": 2},
    {"city_id": 5, "city_name": "Niterói", "state_id": 2},
    {"city_id": 6, "city_name": "Belo Horizonte", "state_id": 3},
    {"city_id": 7, "city_name": "Salvador", "state_id": 4},
    {"city_id": 8, "city_name": "Curitiba", "state_id": 5},
    {"city_id": 9, "city_name": "Porto Alegre", "state_id": 6},
    {"city_id": 10, "city_name": "Recife", "state_id": 7},
    {"city_id": 11, "city_name": "Fortaleza", "state_id": 8},
]

PRODUCTS = [
    {"product_id": 1, "product_name": "NuCard Credit", "product_category": "Credit"},
    {"product_id": 2, "product_name": "NuConta", "product_category": "Banking"},
    {"product_id": 3, "product_name": "NuInvest Stocks", "product_category": "Investment"},
    {"product_id": 4, "product_name": "NuInvest Funds", "product_category": "Investment"},
    {"product_id": 5, "product_name": "Personal Loan", "product_category": "Credit"},
    {"product_id": 6, "product_name": "Nubank Rewards", "product_category": "Rewards"},
    {"product_id": 7, "product_name": "NuSeguro Auto", "product_category": "Insurance"},
    {"product_id": 8, "product_name": "NuSeguro Vida", "product_category": "Insurance"},
]

TRANSACTION_TYPES = [
    {"transaction_type_id": 101, "transaction_type_name": "CREDIT_PURCHASE", "is_financial": True},
    {"transaction_type_id": 102, "transaction_type_name": "CREDIT_PAYMENT", "is_financial": True},
    {"transaction_type_id": 103, "transaction_type_name": "INVESTMENT_BUY", "is_financial": True},
    {"transaction_type_id": 104, "transaction_type_name": "INVESTMENT_SELL", "is_financial": True},
    {"transaction_type_id": 105, "transaction_type_name": "TRANSFER_IN", "is_financial": True},
    {"transaction_type_id": 106, "transaction_type_name": "TRANSFER_OUT", "is_financial": True},
    {"transaction_type_id": 107, "transaction_type_name": "PIX_SENT", "is_financial": True},
    {"transaction_type_id": 108, "transaction_type_name": "PIX_RECEIVED", "is_financial": True},
    {"transaction_type_id": 109, "transaction_type_name": "LOAN_DISBURSEMENT", "is_financial": True},
    {"transaction_type_id": 110, "transaction_type_name": "LOAN_PAYMENT", "is_financial": True},
    {"transaction_type_id": 201, "transaction_type_name": "UPDATE_ADDRESS", "is_financial": False},
    {"transaction_type_id": 202, "transaction_type_name": "UPDATE_PHONE", "is_financial": False},
    {"transaction_type_id": 203, "transaction_type_name": "UPDATE_EMAIL", "is_financial": False},
    {"transaction_type_id": 204, "transaction_type_name": "CARD_ACTIVATION", "is_financial": False},
    {"transaction_type_id": 205, "transaction_type_name": "CARD_BLOCK", "is_financial": False},
]

# Dimensões de tempo - geramos para 2024-2025
TIME_DIMENSIONS = []

@dag(
    dag_id="generate_bronze_batch",
    start_date=datetime(2025, 1, 1),
    schedule="*/10 * * * *",  # A cada 10 minutos
    catchup=False,
    tags=["synthetic", "bronze", "batch", "high-volume"],
    max_active_runs=1,  # Evita sobreposição
)
def generate_bronze_batch():
    
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
            "spark.executor.memory": "2g",
            "spark.driver.memory": "2g",
        },
    )
    def generate_batch_data(spark: SparkSession):
        """Gera um lote grande de dados sintéticos"""
        
        batch_timestamp = datetime.now()
        batch_id = batch_timestamp.strftime("%Y%m%d_%H%M%S")
        
        print(f"🚀 Iniciando geração de batch {batch_id}")
        
        def write_csv_with_partition(df, table_name: str):
            """Escreve CSV com particionamento por batch_id"""
            path = f"{BUCKET_BRONZE}/{table_name}/batch_id={batch_id}"
            df.write.format("csv") \
              .mode("overwrite") \
              .option("header", "true") \
              .save(path)
            print(f"✅ Gravado: {table_name} ({df.count()} registros)")

        # 1. Gerar dimensões estáticas (apenas uma vez por batch)
        print("📋 Gerando dimensões estáticas...")
        
        # Countries
        df_countries = spark.createDataFrame(COUNTRIES) \
            .withColumn("ingestion_timestamp", current_timestamp())
        write_csv_with_partition(df_countries, "d_country")
        
        # States  
        df_states = spark.createDataFrame(STATES_BRAZIL) \
            .withColumn("ingestion_timestamp", current_timestamp())
        write_csv_with_partition(df_states, "d_state")
        
        # Cities
        df_cities = spark.createDataFrame(CITIES) \
            .withColumn("ingestion_timestamp", current_timestamp())
        write_csv_with_partition(df_cities, "d_city")
        
        # Products
        df_products = spark.createDataFrame(PRODUCTS) \
            .withColumn("ingestion_timestamp", current_timestamp())
        write_csv_with_partition(df_products, "d_products")
        
        # Transaction Types
        df_transaction_types = spark.createDataFrame(TRANSACTION_TYPES) \
            .withColumn("ingestion_timestamp", current_timestamp())
        write_csv_with_partition(df_transaction_types, "d_transaction_types")

        # 2. Gerar dimensões de tempo
        print("⏰ Gerando dimensões de tempo...")
        time_data = []
        
        # Gerar para os próximos 30 dias
        start_date = batch_timestamp.date()
        for i in range(30):
            date = start_date + timedelta(days=i)
            dt = datetime.combine(date, datetime.min.time())
            
            time_data.append({
                "time_id": int(dt.timestamp()),
                "full_timestamp": dt,
                "year_id": dt.year,
                "month_id": dt.month,
                "week_id": dt.isocalendar()[1],
                "weekday_id": dt.weekday() + 1
            })
        
        if time_data:
            df_time = spark.createDataFrame(time_data) \
                .withColumn("ingestion_timestamp", current_timestamp())
            write_csv_with_partition(df_time, "d_time")
        
        # 3. Gerar customers em larga escala
        print(f"👥 Gerando {BATCH_SIZE_CUSTOMERS} customers...")
        
        # Buscar IDs existentes para continuidade
        existing_customer_ids = []
        try:
            existing_df = spark.read.format("csv") \
                .option("header", "true") \
                .load(f"{BUCKET_BRONZE}/d_customers/*/")
            existing_customer_ids = [row.customer_id for row in existing_df.select("customer_id").distinct().collect()]
        except:
            pass
        
        base_customer_id = max([int(x) for x in existing_customer_ids], default=0)
        
        customers_data = []
        for i in range(BATCH_SIZE_CUSTOMERS):
            customers_data.append({
                "customer_id": base_customer_id + i + 1,
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
            })
        
        df_customers = spark.createDataFrame(customers_data) \
            .withColumn("ingestion_timestamp", current_timestamp())
        write_csv_with_partition(df_customers, "d_customers")
        
        new_customer_ids = [c["customer_id"] for c in customers_data]

        # 4. Gerar customer identifiers
        print(f"📄 Gerando customer identifiers...")
        
        identifiers_data = []
        identifier_id = 1
        
        for customer_id in new_customer_ids:
            # CPF para todos os clientes brasileiros
            identifiers_data.append({
                "identifier_id": identifier_id,
                "customer_id": customer_id,
                "identifier_type": "CPF",
                "identifier_value": fake.cpf(),
                "country_id": 1  # Brasil
            })
            identifier_id += 1
            
            # Alguns clientes também têm RG
            if random.random() < 0.7:
                identifiers_data.append({
                    "identifier_id": identifier_id, 
                    "customer_id": customer_id,
                    "identifier_type": "RG",
                    "identifier_value": fake.rg(),
                    "country_id": 1
                })
                identifier_id += 1
        
        if identifiers_data:
            df_identifiers = spark.createDataFrame(identifiers_data) \
                .withColumn("ingestion_timestamp", current_timestamp())
            write_csv_with_partition(df_identifiers, "d_customer_identifiers")

        # 5. Gerar contracts em larga escala
        print(f"📋 Gerando {BATCH_SIZE_CONTRACTS} contracts...")
        
        existing_contract_ids = []
        try:
            existing_df = spark.read.format("csv") \
                .option("header", "true") \
                .load(f"{BUCKET_BRONZE}/f_contracts/*/")
            existing_contract_ids = [row.contract_id for row in existing_df.select("contract_id").distinct().collect()]
        except:
            pass
        
        base_contract_id = max([int(x) for x in existing_contract_ids], default=0)
        
        # Buscar todos os customer IDs disponíveis
        all_customer_ids = []
        try:
            all_customers_df = spark.read.format("csv") \
                .option("header", "true") \
                .load(f"{BUCKET_BRONZE}/d_customers/*/")
            all_customer_ids = [int(row.customer_id) for row in all_customers_df.select("customer_id").collect()]
        except:
            all_customer_ids = new_customer_ids
        
        contracts_data = []
        for i in range(BATCH_SIZE_CONTRACTS):
            start_date = fake.date_time_between(start_date='-2y', end_date='now')
            end_date = None
            if random.random() < 0.1:  # 10% dos contratos têm data fim
                end_date = start_date + timedelta(days=random.randint(365, 1825))
            
            contracts_data.append({
                "contract_id": base_contract_id + i + 1,
                "customer_id": random.choice(all_customer_ids),
                "product_id": random.choice(PRODUCTS)["product_id"],
                "contract_status": random.choices(
                    ["ACTIVE", "BLOCKED", "CANCELLED", "PENDING"],
                    weights=[70, 15, 10, 5]
                )[0],
                "start_date": start_date,
                "end_date": end_date,
            })
        
        df_contracts = spark.createDataFrame(contracts_data) \
            .withColumn("ingestion_timestamp", current_timestamp())
        write_csv_with_partition(df_contracts, "f_contracts")
        
        new_contract_ids = [c["contract_id"] for c in contracts_data]

        # 6. Gerar contract attributes
        print(f"⚙️ Gerando {BATCH_SIZE_ATTRIBUTES} contract attributes...")
        
        attributes_data = []
        attribute_id = 1
        
        attribute_names = [
            "credit_limit", "interest_rate", "annual_fee", "reward_points",
            "minimum_balance", "overdraft_limit", "insurance_coverage",
            "monthly_fee", "transaction_limit", "daily_limit"
        ]
        
        for _ in range(BATCH_SIZE_ATTRIBUTES):
            contract_id = random.choice(new_contract_ids)
            attr_name = random.choice(attribute_names)
            
            # Gerar valor baseado no tipo de atributo
            if "limit" in attr_name or "balance" in attr_name:
                attr_value = str(random.randint(1000, 50000))
            elif "rate" in attr_name:
                attr_value = str(round(random.uniform(0.5, 15.0), 2))
            elif "fee" in attr_name:
                attr_value = str(random.randint(0, 500))
            elif "points" in attr_name:
                attr_value = str(random.randint(0, 10000))
            else:
                attr_value = str(random.randint(100, 10000))
            
            valid_from = fake.date_time_between(start_date='-1y', end_date='now')
            valid_to = None
            if random.random() < 0.2:  # 20% têm data de validade
                valid_to = valid_from + timedelta(days=random.randint(30, 365))
            
            attributes_data.append({
                "attribute_id": attribute_id,
                "contract_id": contract_id,
                "attribute_name": attr_name,
                "attribute_value": attr_value,
                "valid_from": valid_from,
                "valid_to": valid_to,
            })
            attribute_id += 1
        
        if attributes_data:
            df_attributes = spark.createDataFrame(attributes_data) \
                .withColumn("ingestion_timestamp", current_timestamp())
            write_csv_with_partition(df_attributes, "f_contract_attributes")

        # 7. Gerar transactions em larga escala
        print(f"💳 Gerando {BATCH_SIZE_TRANSACTIONS} transactions...")
        
        # Buscar todos os contract IDs disponíveis
        all_contract_ids = []
        try:
            all_contracts_df = spark.read.format("csv") \
                .option("header", "true") \
                .load(f"{BUCKET_BRONZE}/f_contracts/*/")
            all_contract_ids = [int(row.contract_id) for row in all_contracts_df.select("contract_id").collect()]
        except:
            all_contract_ids = new_contract_ids
        
        transactions_data = []
        for i in range(BATCH_SIZE_TRANSACTIONS):
            transaction_type = random.choice(TRANSACTION_TYPES)
            
            # Gerar valores realistas baseados no tipo de transação
            if "INVESTMENT" in transaction_type["transaction_type_name"]:
                amount = round(random.uniform(100.0, 10000.0), 2)
            elif "PIX" in transaction_type["transaction_type_name"]:
                amount = round(random.uniform(10.0, 2000.0), 2)
            elif "CREDIT" in transaction_type["transaction_type_name"]:
                amount = round(random.uniform(20.0, 5000.0), 2)
            elif "LOAN" in transaction_type["transaction_type_name"]:
                amount = round(random.uniform(1000.0, 50000.0), 2)
            else:
                amount = round(random.uniform(5.0, 1000.0), 2)
            
            # Para transações de saída, tornar valor negativo
            if any(x in transaction_type["transaction_type_name"] for x in ["OUT", "SENT", "PAYMENT", "BUY"]):
                amount = -abs(amount)
            
            requested_time = fake.date_time_between(start_date='-30d', end_date='now')
            completed_time = requested_time + timedelta(seconds=random.randint(1, 300))
            
            transactions_data.append({
                "transaction_id": int(batch_timestamp.timestamp() * 1e6) + i,
                "contract_id": random.choice(all_contract_ids),
                "transaction_type_id": transaction_type["transaction_type_id"],
                "requested_at_time_id": int(requested_time.timestamp()),
                "completed_at_time_id": int(completed_time.timestamp()),
                "transaction_status": random.choices(
                    ["COMPLETED", "PENDING", "FAILED", "CANCELLED"],
                    weights=[85, 8, 5, 2]
                )[0],
                "amount": amount,
                "currency": "BRL",
            })
        
        df_transactions = spark.createDataFrame(transactions_data) \
            .withColumn("ingestion_timestamp", current_timestamp())
        write_csv_with_partition(df_transactions, "f_transactions")

        print(f"🎉 Batch {batch_id} concluído!")
        print(f"📊 Estatísticas do batch:")
        print(f"   - Customers: {len(customers_data)}")
        print(f"   - Identifiers: {len(identifiers_data)}")
        print(f"   - Contracts: {len(contracts_data)}")
        print(f"   - Attributes: {len(attributes_data)}")
        print(f"   - Transactions: {len(transactions_data)}")

    generate_batch_data()

generate_bronze_batch()
