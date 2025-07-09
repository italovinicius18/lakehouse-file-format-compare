from airflow.decorators import dag, task
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, lit
from airflow.models import Variable

@dag(
    dag_id="bronze_to_silver_batch",
    start_date=datetime(2025, 6, 28),
    schedule="*/12 * * * *",  # A cada 12 minutos (2 min após o bronze)
    catchup=False,
    tags=["iceberg", "upsert", "silver", "batch"],
    max_active_runs=1,
)
def bronze_to_silver_batch():

    @task.pyspark(
        conn_id="spark_default",
        config_kwargs={
            # jars Iceberg + Hadoop-AWS + AWS SDK
            "spark.jars.packages": ",".join([
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.0",
            ]),
            # extensões Iceberg
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            
            # CONFIGURAÇÃO CORRIGIDA: Usar Hive Metastore como catálogo
            "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
            "spark.sql.catalog.spark_catalog.type": "hive",
            "spark.sql.catalog.spark_catalog.uri": "thrift://hive-metastore:9083",
            "spark.sql.catalog.spark_catalog.warehouse": "s3a://silver",
            
            # Catálogo específico para silver (usando mesmo Hive Metastore)
            "spark.sql.catalog.silver_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.silver_catalog.type": "hive",
            "spark.sql.catalog.silver_catalog.uri": "thrift://hive-metastore:9083",
            "spark.sql.catalog.silver_catalog.warehouse": "s3a://silver",
            
            # Configuração Hive Metastore
            "spark.sql.catalogImplementation": "hive",
            "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore:9083",
            
            # S3A / MinIO
            "spark.hadoop.fs.s3a.endpoint": Variable.get("MINIO_ENDPOINT"),
            "spark.hadoop.fs.s3a.access.key": Variable.get("MINIO_ACCESS_KEY"),
            "spark.hadoop.fs.s3a.secret.key": Variable.get("MINIO_SECRET_KEY"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            
            # Configurações de performance para grandes volumes
            "spark.executor.memory": "3g",
            "spark.driver.memory": "2g",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        }
    )
    def upsert_iceberg_batch(spark: SparkSession, sc: SparkContext):
        """Processa dados em batches do bronze para silver usando Iceberg"""
        
        print("🚀 Iniciando processamento Bronze → Silver")
        
        # Criar database se não existir
        spark.sql("CREATE DATABASE IF NOT EXISTS silver_catalog.silver")
        
        # Definir tabelas e suas configurações
        table_configs = [
            {
                "name": "d_country",
                "key_cols": ["country_id"],
                "is_dimension": True,
                "description": "Países"
            },
            {
                "name": "d_state", 
                "key_cols": ["state_id"],
                "is_dimension": True,
                "description": "Estados"
            },
            {
                "name": "d_city",
                "key_cols": ["city_id"], 
                "is_dimension": True,
                "description": "Cidades"
            },
            {
                "name": "d_year",
                "key_cols": ["year_id"],
                "is_dimension": True,
                "description": "Anos"
            },
            {
                "name": "d_month",
                "key_cols": ["month_id"],
                "is_dimension": True, 
                "description": "Meses"
            },
            {
                "name": "d_week",
                "key_cols": ["week_id"],
                "is_dimension": True,
                "description": "Semanas"
            },
            {
                "name": "d_weekday",
                "key_cols": ["weekday_id"],
                "is_dimension": True,
                "description": "Dias da semana"
            },
            {
                "name": "d_time",
                "key_cols": ["time_id"],
                "is_dimension": True,
                "description": "Dimensão tempo"
            },
            {
                "name": "d_products",
                "key_cols": ["product_id"],
                "is_dimension": True,
                "description": "Produtos"
            },
            {
                "name": "d_transaction_types",
                "key_cols": ["transaction_type_id"],
                "is_dimension": True,
                "description": "Tipos de transação"
            },
            {
                "name": "d_customers", 
                "key_cols": ["customer_id"],
                "is_dimension": False,  # SCD Type 1 - pode ter updates
                "description": "Clientes"
            },
            {
                "name": "d_customer_identifiers",
                "key_cols": ["identifier_id"],
                "is_dimension": False,
                "description": "Identificadores de clientes"
            },
            {
                "name": "f_contracts",
                "key_cols": ["contract_id"],
                "is_dimension": False,
                "description": "Contratos"
            },
            {
                "name": "f_contract_attributes",
                "key_cols": ["attribute_id"],
                "is_dimension": False,
                "description": "Atributos de contratos"
            },
            {
                "name": "f_transactions",
                "key_cols": ["transaction_id"],
                "is_dimension": False,
                "description": "Transações"
            },
        ]
        
        # Formatos Iceberg para teste
        formats = ["parquet", "orc", "avro"]
        
        # Processar cada tabela
        for table_config in table_configs:
            table_name = table_config["name"]
            key_cols = table_config["key_cols"]
            is_dimension = table_config["is_dimension"]
            description = table_config["description"]
            
            print(f"\n📊 Processando {description} ({table_name})")
            
            try:
                # 1) Ler todos os CSVs da tabela (incluindo partições por batch)
                bronze_path = f"s3a://bronze/{table_name}/*/*"
                
                try:
                    df_bronze = (
                        spark.read
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .csv(bronze_path)
                    )
                    
                    total_records = df_bronze.count()
                    if total_records == 0:
                        print(f"   ⚠️  Nenhum dado encontrado para {table_name}, pulando...")
                        continue
                        
                    print(f"   📥 Lendo {total_records:,} registros do bronze")
                    
                except Exception as e:
                    print(f"   ❌ Erro ao ler dados bronze para {table_name}: {str(e)}")
                    continue
                
                # 2) Identificar novos batches se a tabela já existir
                new_batches_only = df_bronze
                
                # Verificar se existem dados silver para determinar novos batches
                silver_base_path = f"s3a://silver/silver_catalog.db/silver.{table_name}_parquet"
                try:
                    # Tentar ler tabela silver existente para identificar último batch processado
                    existing_df = spark.read.parquet(silver_base_path)
                    if "ingestion_timestamp" in existing_df.columns:
                        last_ingestion = existing_df.agg(spark_max("ingestion_timestamp")).collect()[0][0]
                        new_batches_only = df_bronze.filter(col("ingestion_timestamp") > lit(last_ingestion))
                        new_count = new_batches_only.count()
                        print(f"   🔄 Identificados {new_count:,} registros novos (incrementais)")
                        
                        if new_count == 0:
                            print(f"   ✅ Tabela {table_name} já está atualizada")
                            continue
                except:
                    print(f"   🆕 Primeira carga para {table_name}")
                
                # 3) Preparar dados para staging
                df_staging = new_batches_only.dropDuplicates(key_cols)  # Remove duplicatas por chave
                staging_count = df_staging.count()
                
                if staging_count == 0:
                    print(f"   ⚠️  Nenhum registro único para processar em {table_name}")
                    continue
                
                print(f"   🔄 Processando {staging_count:,} registros únicos")
                
                # Criar view temporária
                df_staging.createOrReplaceTempView("staging")
                
                # 4) Processar para cada formato Iceberg
                for fmt in formats:
                    table_target = f"silver_catalog.silver.{table_name}_{fmt}"
                    
                    print(f"      📝 Processando formato {fmt.upper()}...")
                    
                    try:
                        # Verificar se tabela existe
                        table_exists = spark.catalog.tableExists(table_target)
                        
                        if not table_exists:
                            print(f"         🆕 Criando nova tabela {table_target}")
                            
                            # Criar tabela com propriedades específicas
                            table_properties = {
                                "write.format.default": fmt,
                                "write.target-file-size-bytes": "134217728",  # 128MB
                                "write.parquet.compression-codec": "snappy" if fmt == "parquet" else None,
                            }
                            
                            # Filtrar propriedades None
                            props = {k: v for k, v in table_properties.items() if v is not None}
                            
                            writer = df_staging.writeTo(table_target).using("iceberg")
                            for key, value in props.items():
                                writer = writer.tableProperty(key, value)
                            
                            writer.create()
                            
                            records_inserted = staging_count
                            
                        else:
                            print(f"         🔄 Fazendo MERGE na tabela existente")
                            
                            # Realizar MERGE baseado no tipo de tabela
                            if is_dimension and table_name.startswith('d_') and table_name not in ['d_customers', 'd_customer_identifiers']:
                                # Dimensões estáticas - apenas INSERT de novos registros
                                join_cond = " AND ".join([f"t.{c} = s.{c}" for c in key_cols])
                                
                                merge_sql = f"""
                                    MERGE INTO {table_target} AS t
                                    USING staging AS s
                                    ON {join_cond}
                                    WHEN NOT MATCHED THEN
                                        INSERT *
                                """
                                
                            else:
                                # Tabelas de fatos e dimensões mutáveis - UPSERT completo
                                join_cond = " AND ".join([f"t.{c} = s.{c}" for c in key_cols])
                                
                                merge_sql = f"""
                                    MERGE INTO {table_target} AS t
                                    USING staging AS s
                                    ON {join_cond}
                                    WHEN MATCHED THEN
                                        UPDATE SET *
                                    WHEN NOT MATCHED THEN
                                        INSERT *
                                """
                            
                            # Executar MERGE
                            spark.sql(merge_sql)
                            
                            # Contar registros após merge
                            records_inserted = spark.sql(f"SELECT COUNT(*) FROM {table_target}").collect()[0][0]
                        
                        print(f"         ✅ {fmt.upper()}: {records_inserted:,} registros na tabela")
                        
                    except Exception as e:
                        print(f"         ❌ Erro no formato {fmt}: {str(e)}")
                        continue
                
                # Limpar view temporária
                spark.catalog.dropTempView("staging")
                print(f"   ✅ Concluído processamento de {table_name}")
                
            except Exception as e:
                print(f"   ❌ Erro geral ao processar {table_name}: {str(e)}")
                continue
        
        print("\n🎉 Processamento Bronze → Silver concluído!")
        
        # 5) Gerar relatório final
        print("\n📊 RELATÓRIO FINAL:")
        for fmt in formats:
            print(f"\n   📈 Formato {fmt.upper()}:")
            for table_config in table_configs:
                table_name = table_config["name"]
                table_target = f"silver_catalog.silver.{table_name}_{fmt}"
                
                try:
                    if spark.catalog.tableExists(table_target):
                        count = spark.sql(f"SELECT COUNT(*) FROM {table_target}").collect()[0][0]
                        print(f"      {table_name}: {count:,} registros")
                    else:
                        print(f"      {table_name}: tabela não existe")
                except:
                    print(f"      {table_name}: erro ao contar")

    upsert_iceberg_batch()

bronze_to_silver_batch()