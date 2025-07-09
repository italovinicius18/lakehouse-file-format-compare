#!/usr/bin/env python3
"""
Script para comparar performance e armazenamento entre formatos Iceberg
Execute: python3 compare_formats_performance.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum as spark_sum, avg, max as spark_max
import time
import sys

def create_spark_session():
    """Cria sessão Spark para comparação"""
    
    spark = SparkSession.builder \
        .appName("IcebergFormatComparison") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.silver_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.silver_catalog.type", "hive") \
        .config("spark.sql.catalog.silver_catalog.uri", "thrift://localhost:9083") \
        .config("spark.sql.catalog.silver_catalog.warehouse", "s3a://silver") \
        .config("spark.sql.catalog.gold_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.gold_catalog.type", "hive") \
        .config("spark.sql.catalog.gold_catalog.uri", "thrift://localhost:9083") \
        .config("spark.sql.catalog.gold_catalog.warehouse", "s3a://gold") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def measure_query_performance(spark, table_name, query_description, query_func):
    """Mede performance de uma query específica"""
    
    try:
        print(f"      🔍 {query_description}")
        start_time = time.time()
        
        result = query_func(spark, table_name)
        result.show(5, truncate=False)
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"         ⏱️  Tempo: {duration:.2f}s")
        return duration, result.count() if hasattr(result, 'count') else 0
        
    except Exception as e:
        print(f"         ❌ Erro: {str(e)}")
        return None, 0

def compare_table_formats(spark, table_base_name, layer="silver"):
    """Compara um tipo de tabela entre diferentes formatos"""
    
    formats = ["parquet", "orc", "avro"]
    catalog = f"{layer}_catalog.{layer}"
    
    print(f"\n📊 COMPARANDO TABELA: {table_base_name}")
    print("-" * 50)
    
    results = {}
    
    for fmt in formats:
        table_name = f"{catalog}.{table_base_name}_{fmt}"
        
        print(f"\n   📁 FORMATO: {fmt.upper()}")
        
        try:
            if not spark.catalog.tableExists(table_name):
                print(f"      ⚠️  Tabela não existe: {table_name}")
                continue
            
            # 1. Informações básicas da tabela
            df = spark.read.table(table_name)
            record_count = df.count()
            column_count = len(df.columns)
            
            print(f"      📈 Registros: {record_count:,}")
            print(f"      📋 Colunas: {column_count}")
            
            # 2. Informações de arquivos e tamanho
            try:
                files_df = spark.sql(f"SELECT * FROM {table_name}.files")
                total_files = files_df.count()
                if total_files > 0:
                    total_size = files_df.agg(spark_sum("file_size_in_bytes")).collect()[0][0]
                    avg_file_size = total_size / total_files if total_files > 0 else 0
                    
                    print(f"      📁 Arquivos: {total_files}")
                    print(f"      💾 Tamanho total: {total_size / (1024*1024):.2f} MB")
                    print(f"      📦 Tamanho médio por arquivo: {avg_file_size / (1024*1024):.2f} MB")
                    
                    results[fmt] = {
                        'records': record_count,
                        'files': total_files,
                        'total_size_mb': total_size / (1024*1024),
                        'avg_file_size_mb': avg_file_size / (1024*1024)
                    }
                else:
                    print(f"      ⚠️  Nenhum arquivo encontrado")
                    
            except Exception as e:
                print(f"      ⚠️  Erro ao obter informações de arquivos: {e}")
            
            # 3. Testes de performance
            print(f"      🚀 Testes de Performance:")
            
            # Query 1: COUNT simples
            def simple_count(spark, table):
                return spark.sql(f"SELECT COUNT(*) as total FROM {table}")
            
            duration1, _ = measure_query_performance(
                spark, table_name, "COUNT(*)", simple_count
            )
            
            # Query 2: Agregação complexa (se aplicável)
            if 'transactions' in table_base_name or 'contracts' in table_base_name:
                def complex_aggregation(spark, table):
                    if 'transactions' in table: