-- Script de inicialização do Trino
-- Nota: Os schemas serão criados automaticamente pelo Spark quando necessário
-- Aqui apenas validamos a conectividade

-- Listar catálogos disponíveis
SHOW CATALOGS;

-- Se precisar criar schemas manualmente (backup):
-- CREATE SCHEMA IF NOT EXISTS iceberg.silver;
-- CREATE SCHEMA IF NOT EXISTS iceberg.gold;