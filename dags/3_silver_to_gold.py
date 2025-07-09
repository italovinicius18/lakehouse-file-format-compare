from airflow.decorators import dag, task
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from airflow.models import Variable

@dag(
    dag_id="silver_to_gold_analytics",
    start_date=datetime(2025, 6, 28),
    schedule="*/15 * * * *",  # A cada 15 minutos (3 min após silver)
    catchup=False,
    tags=["iceberg", "gold", "analytics", "aggregations"],
    max_active_runs=1,
)
def silver_to_gold_analytics():

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
            
            # Catálogos Silver e Gold
            "spark.sql.catalog.silver_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.silver_catalog.type": "hive",
            "spark.sql.catalog.silver_catalog.uri": "thrift://hive-metastore:9083",
            "spark.sql.catalog.silver_catalog.warehouse": "s3a://silver",
            
            "spark.sql.catalog.gold_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.gold_catalog.type": "hive", 
            "spark.sql.catalog.gold_catalog.uri": "thrift://hive-metastore:9083",
            "spark.sql.catalog.gold_catalog.warehouse": "s3a://gold",
            
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
            
            # Performance para agregações
            "spark.executor.memory": "4g",
            "spark.driver.memory": "3g",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
        }
    )
    def build_gold_analytics(spark: SparkSession, sc: SparkContext):
        """Cria tabelas analíticas na camada Gold"""
        
        print("🏆 Iniciando processamento Silver → Gold (Analytics)")
        
        # Criar database gold se não existir
        spark.sql("CREATE DATABASE IF NOT EXISTS gold_catalog.gold")
        
        formats = ["parquet", "orc", "avro"]  # Processar todos os formatos
        
        # Verificar quais formatos têm dados completos no Silver
        base_tables = [
            "f_transactions",
            "f_contracts", 
            "d_customers",
            "d_products",
            "d_transaction_types"
        ]
        
        available_formats = []
        for fmt in formats:
            format_complete = True
            missing_tables = []
            
            for table in base_tables:
                table_name = f"silver_catalog.silver.{table}_{fmt}"
                if not spark.catalog.tableExists(table_name):
                    format_complete = False
                    missing_tables.append(f"{table}_{fmt}")
            
            if format_complete:
                available_formats.append(fmt)
                print(f"✅ Formato {fmt.upper()} tem todas as tabelas necessárias")
            else:
                print(f"⚠️  Formato {fmt.upper()} incompleto - faltam: {missing_tables}")
        
        if not available_formats:
            print("❌ Nenhum formato tem dados completos no Silver. Aguardando...")
            return
        
        print(f"🎯 Processando formatos disponíveis: {available_formats}")
        
        # =================================================================
        # PROCESSAR CADA FORMATO SEPARADAMENTE
        # =================================================================
        
        for fmt in available_formats:
            print(f"\n🔄 PROCESSANDO FORMATO: {fmt.upper()}")
            print("=" * 60)
            
            # Variáveis para controle de fluxo
            timestamp_column = None
            timestamp_conversion = None
            financial_count = 0
            
            try:
                # =================================================================
                # 1. AGREGAÇÃO: SALDO MENSAL POR CONTRATO
                # =================================================================
                print(f"\n💰 [{fmt.upper()}] Criando tabela: Saldo Mensal por Contrato")
                
                # Criar view temporária para usar Spark SQL
                spark.sql(f"""
                    CREATE OR REPLACE TEMPORARY VIEW temp_transactions_{fmt} AS
                    SELECT * FROM silver_catalog.silver.f_transactions_{fmt}
                """)
                
                spark.sql(f"""
                    CREATE OR REPLACE TEMPORARY VIEW temp_transaction_types_{fmt} AS
                    SELECT * FROM silver_catalog.silver.d_transaction_types_{fmt}
                """)
                
                # Verificar colunas disponíveis
                df_transactions = spark.read.table(f"silver_catalog.silver.f_transactions_{fmt}")
                print(f"   🔍 Colunas em f_transactions_{fmt}: {df_transactions.columns}")
                
                # Determinar coluna de timestamp
                timestamp_column = None
                if "completed_at_time_id" in df_transactions.columns:
                    timestamp_column = "completed_at_time_id"
                    timestamp_conversion = "CAST(completed_at_time_id AS TIMESTAMP)"
                elif "transaction_timestamp" in df_transactions.columns:
                    timestamp_column = "transaction_timestamp"
                    timestamp_conversion = "transaction_timestamp"
                else:
                    print(f"   ⚠️  Nenhuma coluna de timestamp encontrada em {fmt}")
                    continue
                
                # Verificar se há dados financeiros válidos usando SQL
                financial_count = spark.sql(f"""
                    SELECT COUNT(*) as count
                    FROM temp_transactions_{fmt} t
                    JOIN temp_transaction_types_{fmt} tt ON t.transaction_type_id = tt.transaction_type_id
                    WHERE tt.is_financial = true 
                    AND {timestamp_conversion} IS NOT NULL
                """).collect()[0]['count']
                
                if financial_count == 0:
                    print(f"   ⚠️  Nenhuma transação financeira com timestamp válido encontrada em {fmt}")
                    continue
                
                # Criar agregação mensal usando Spark SQL
                df_monthly_balance = spark.sql(f"""
                    SELECT 
                        YEAR({timestamp_conversion}) as ano,
                        MONTH({timestamp_conversion}) as mes,
                        t.contract_id,
                        SUM(CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END) as entradas,
                        SUM(CASE WHEN t.amount < 0 THEN ABS(t.amount) ELSE 0 END) as saidas,
                        SUM(t.amount) as saldo_liquido,
                        COUNT(*) as total_transacoes,
                        AVG(t.amount) as valor_medio_transacao,
                        MAX(t.amount) as maior_entrada,
                        MIN(t.amount) as maior_saida,
                        CURRENT_TIMESTAMP() as created_at
                    FROM temp_transactions_{fmt} t
                    JOIN temp_transaction_types_{fmt} tt ON t.transaction_type_id = tt.transaction_type_id
                    WHERE tt.is_financial = true 
                    AND {timestamp_conversion} IS NOT NULL
                    GROUP BY 
                        YEAR({timestamp_conversion}),
                        MONTH({timestamp_conversion}),
                        t.contract_id
                """)
                
                gold_table = f"gold_catalog.gold.monthly_balance_{fmt}"
                
                # Recriar tabela (estratégia full refresh para agregações)
                spark.sql(f"DROP TABLE IF EXISTS {gold_table}")
                
                print(f"   📊 Criando {gold_table}")
                df_monthly_balance.writeTo(gold_table) \
                    .using("iceberg") \
                    .partitionedBy("ano", "mes") \
                    .tableProperty("write.format.default", fmt) \
                    .create()
                
                count = spark.sql(f"SELECT COUNT(*) FROM {gold_table}").collect()[0][0]
                print(f"   ✅ {count:,} registros inseridos")
                
                # Carregar tabelas para próximas seções - criar views temporárias
                spark.sql(f"""
                    CREATE OR REPLACE TEMPORARY VIEW temp_contracts_{fmt} AS
                    SELECT * FROM silver_catalog.silver.f_contracts_{fmt}
                """)
                
                spark.sql(f"""
                    CREATE OR REPLACE TEMPORARY VIEW temp_products_{fmt} AS
                    SELECT * FROM silver_catalog.silver.d_products_{fmt}
                """)
                
                spark.sql(f"""
                    CREATE OR REPLACE TEMPORARY VIEW temp_customers_{fmt} AS
                    SELECT * FROM silver_catalog.silver.d_customers_{fmt}
                """)
                
                # =================================================================
                # 2. AGREGAÇÃO: RESUMO POR CLIENTE
                # =================================================================
                print(f"\n👤 [{fmt.upper()}] Criando tabela: Resumo por Cliente")
                
                # Verificar se temos dados financeiros válidos
                if financial_count == 0:
                    print(f"   ⚠️  Não há dados financeiros válidos para processar resumo de clientes em {fmt}")
                    
                    # Criar resumo básico sem dados financeiros usando SQL
                    df_customer_analytics = spark.sql(f"""
                        SELECT 
                            c.customer_id,
                            c.first_name,
                            c.last_name,
                            COUNT(DISTINCT ct.contract_id) as total_contratos,
                            COUNT(DISTINCT p.product_category) as categorias_produtos,
                            0 as saldo_total,
                            0 as total_transacoes,
                            CAST(NULL AS TIMESTAMP) as ultima_atividade,
                            COLLECT_SET(p.product_category) as produtos_utilizados,
                            CURRENT_TIMESTAMP() as created_at
                        FROM temp_customers_{fmt} c
                        LEFT JOIN temp_contracts_{fmt} ct ON c.customer_id = ct.customer_id
                        LEFT JOIN temp_products_{fmt} p ON ct.product_id = p.product_id
                        GROUP BY c.customer_id, c.first_name, c.last_name
                    """)
                else:
                    # Criar view dos dados financeiros agregados por contrato
                    spark.sql(f"""
                        CREATE OR REPLACE TEMPORARY VIEW temp_financial_summary_{fmt} AS
                        SELECT 
                            t.contract_id,
                            SUM(t.amount) as saldo_total_cliente,
                            COUNT(*) as total_transacoes_cliente,
                            MAX({timestamp_conversion}) as ultima_transacao
                        FROM temp_transactions_{fmt} t
                        JOIN temp_transaction_types_{fmt} tt ON t.transaction_type_id = tt.transaction_type_id
                        WHERE tt.is_financial = true 
                        AND {timestamp_conversion} IS NOT NULL
                        GROUP BY t.contract_id
                    """)
                    
                    # Agregar por cliente usando SQL
                    df_customer_analytics = spark.sql(f"""
                        SELECT 
                            c.customer_id,
                            c.first_name,
                            c.last_name,
                            COUNT(DISTINCT ct.contract_id) as total_contratos,
                            COUNT(DISTINCT p.product_category) as categorias_produtos,
                            SUM(COALESCE(fs.saldo_total_cliente, 0)) as saldo_total,
                            SUM(COALESCE(fs.total_transacoes_cliente, 0)) as total_transacoes,
                            MAX(fs.ultima_transacao) as ultima_atividade,
                            COLLECT_SET(p.product_category) as produtos_utilizados,
                            CURRENT_TIMESTAMP() as created_at
                        FROM temp_customers_{fmt} c
                        LEFT JOIN temp_contracts_{fmt} ct ON c.customer_id = ct.customer_id
                        LEFT JOIN temp_products_{fmt} p ON ct.product_id = p.product_id
                        LEFT JOIN temp_financial_summary_{fmt} fs ON ct.contract_id = fs.contract_id
                        GROUP BY c.customer_id, c.first_name, c.last_name
                    """)
                
                gold_table = f"gold_catalog.gold.customer_summary_{fmt}"
                
                spark.sql(f"DROP TABLE IF EXISTS {gold_table}")
                
                print(f"   👥 Criando {gold_table}")
                df_customer_analytics.writeTo(gold_table) \
                    .using("iceberg") \
                    .tableProperty("write.format.default", fmt) \
                    .create()
                
                count = spark.sql(f"SELECT COUNT(*) FROM {gold_table}").collect()[0][0]
                print(f"   ✅ {count:,} clientes processados")
                
                # =================================================================
                # 3. AGREGAÇÃO: MÉTRICAS DIÁRIAS DE TRANSAÇÕES
                # =================================================================
                print(f"\n📈 [{fmt.upper()}] Criando tabela: Métricas Diárias de Transações")
                
                # Verificar se temos dados financeiros válidos
                if financial_count == 0:
                    print(f"   ⚠️  Não há dados financeiros válidos para métricas diárias em {fmt}")
                    continue
                
                # Agregar por dia e tipo de transação usando SQL
                df_daily_metrics = spark.sql(f"""
                    SELECT 
                        DATE({timestamp_conversion}) as data_transacao,
                        tt.transaction_type_name,
                        tt.is_financial,
                        COUNT(*) as quantidade_transacoes,
                        SUM(t.amount) as volume_total,
                        AVG(t.amount) as valor_medio,
                        STDDEV(t.amount) as desvio_padrao,
                        MIN(t.amount) as valor_minimo,
                        MAX(t.amount) as valor_maximo,
                        COUNT(DISTINCT t.contract_id) as contratos_unicos,
                        CURRENT_TIMESTAMP() as created_at
                    FROM temp_transactions_{fmt} t
                    JOIN temp_transaction_types_{fmt} tt ON t.transaction_type_id = tt.transaction_type_id
                    WHERE tt.is_financial = true 
                    AND {timestamp_conversion} IS NOT NULL
                    GROUP BY 
                        DATE({timestamp_conversion}),
                        tt.transaction_type_name,
                        tt.is_financial
                """)
                
                gold_table = f"gold_catalog.gold.daily_transaction_metrics_{fmt}"
                
                spark.sql(f"DROP TABLE IF EXISTS {gold_table}")
                
                print(f"   📊 Criando {gold_table}")
                df_daily_metrics.writeTo(gold_table) \
                    .using("iceberg") \
                    .partitionedBy("data_transacao") \
                    .tableProperty("write.format.default", fmt) \
                    .create()
                
                count = spark.sql(f"SELECT COUNT(*) FROM {gold_table}").collect()[0][0]
                print(f"   ✅ {count:,} métricas diárias criadas")
                
                # =================================================================
                # 4. AGREGAÇÃO: RANKING DE PRODUTOS
                # =================================================================
                print(f"\n🏆 [{fmt.upper()}] Criando tabela: Ranking de Produtos")
                
                # Verificar se temos dados financeiros válidos
                if financial_count == 0:
                    print(f"   ⚠️  Não há dados financeiros válidos para ranking de produtos em {fmt}")
                    
                    # Criar ranking básico sem dados financeiros usando SQL
                    df_product_ranking = spark.sql(f"""
                        SELECT 
                            p.product_name,
                            p.product_category,
                            0 as total_transacoes,
                            0 as volume_financeiro,
                            COUNT(DISTINCT ct.contract_id) as contratos_ativos,
                            COUNT(DISTINCT ct.customer_id) as clientes_unicos,
                            0 as ticket_medio,
                            1 as rank_volume,
                            1 as rank_transacoes,
                            CURRENT_TIMESTAMP() as created_at
                        FROM temp_contracts_{fmt} ct
                        JOIN temp_products_{fmt} p ON ct.product_id = p.product_id
                        GROUP BY p.product_name, p.product_category
                    """)
                else:
                    # Ranking de produtos por volume de transações usando SQL com Window Functions
                    df_product_ranking = spark.sql(f"""
                        WITH product_metrics AS (
                            SELECT 
                                p.product_name,
                                p.product_category,
                                COUNT(*) as total_transacoes,
                                SUM(t.amount) as volume_financeiro,
                                COUNT(DISTINCT t.contract_id) as contratos_ativos,
                                COUNT(DISTINCT ct.customer_id) as clientes_unicos,
                                AVG(t.amount) as ticket_medio,
                                CURRENT_TIMESTAMP() as created_at
                            FROM temp_transactions_{fmt} t
                            JOIN temp_transaction_types_{fmt} tt ON t.transaction_type_id = tt.transaction_type_id
                            JOIN temp_contracts_{fmt} ct ON t.contract_id = ct.contract_id
                            JOIN temp_products_{fmt} p ON ct.product_id = p.product_id
                            WHERE tt.is_financial = true 
                            AND {timestamp_conversion} IS NOT NULL
                            GROUP BY p.product_name, p.product_category
                        )
                        SELECT 
                            *,
                            ROW_NUMBER() OVER (ORDER BY volume_financeiro DESC) as rank_volume,
                            ROW_NUMBER() OVER (ORDER BY total_transacoes DESC) as rank_transacoes
                        FROM product_metrics
                    """)
                
                gold_table = f"gold_catalog.gold.product_ranking_{fmt}"
                
                spark.sql(f"DROP TABLE IF EXISTS {gold_table}")
                
                print(f"   🥇 Criando {gold_table}")
                df_product_ranking.writeTo(gold_table) \
                    .using("iceberg") \
                    .tableProperty("write.format.default", fmt) \
                    .create()
                
                count = spark.sql(f"SELECT COUNT(*) FROM {gold_table}").collect()[0][0]
                print(f"   ✅ {count:,} produtos ranqueados")
                
                print(f"\n✅ FORMATO {fmt.upper()} PROCESSADO COM SUCESSO!")
                
            except Exception as e:
                print(f"\n❌ ERRO NO FORMATO {fmt.upper()}: {str(e)}")
                import traceback
                traceback.print_exc()
                continue
        
        # Relatório final das tabelas Gold por formato
        print("\n📊 RELATÓRIO TABELAS GOLD POR FORMATO:")
        print("=" * 60)
        
        gold_table_types = [
            "monthly_balance",
            "customer_summary", 
            "daily_transaction_metrics",
            "product_ranking"
        ]
        
        for fmt in available_formats:
            print(f"\n📈 FORMATO {fmt.upper()}:")
            for table_type in gold_table_types:
                table_name = f"gold_catalog.gold.{table_type}_{fmt}"
                try:
                    if spark.catalog.tableExists(table_name):
                        count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
                        print(f"   ✅ {table_type}_{fmt}: {count:,} registros")
                    else:
                        print(f"   ⚠️  {table_type}_{fmt}: não encontrada")
                except Exception as e:
                    print(f"   ❌ {table_type}_{fmt}: erro - {str(e)}")
        
        print(f"\n🎉 Pipeline Silver → Gold concluído para {len(available_formats)} formato(s)!")

    build_gold_analytics()

silver_to_gold_analytics()