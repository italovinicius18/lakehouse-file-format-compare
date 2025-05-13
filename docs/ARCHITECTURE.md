# SISVAN Lakehouse Architecture Documentation

## Architecture Overview

This document provides detailed information about the architecture and design decisions for the SISVAN Lakehouse Pipeline.

## Medallion Architecture

The pipeline follows a classic medallion architecture with four layers:

### 1. Landing Layer
- Raw, unprocessed data from BigQuery
- No transformations, just conversion to Delta format
- Partitioning by year, month, state 
- Full history preserved
- Direct extraction from `basedosdados.br_ms_sisvan.microdados`

### 2. Bronze Layer
- Schema enforcement applied
- Data types corrected
- Minimal cleansing (null filters, basic validations)
- Optimization for Trino catalog access
- Still contains the complete dataset

### 3. Silver Layer
- Business rules and semantic mappings applied
- Code value translations (race, life stage, education)
- Unit standardization
- Filtering of invalid records and outliers
- Enrichment with additional context

### 4. Gold Layer
- Aggregated views for specific analysis needs
- Materialized metrics and KPIs
- Optimized for BI tool consumption
- Contains multiple tables for different analytical perspectives

## Technology Stack Details

### Apache Airflow
- Orchestrates the entire pipeline
- Manages dependencies between processing stages
- Handles scheduling and monitoring
- Uses the Astro CLI for local development

### Apache Spark
- Distributed processing engine
- Executes data transformations
- Integrates with Delta Lake for ACID capabilities 
- Configured to run in a master-worker setup

### Delta Lake
- ACID transaction layer
- Time travel capabilities
- Schema enforcement and evolution
- Metadata management

### MinIO
- S3-compatible object storage
- Stores all data layers
- Configured with path-style access
- Multiple buckets for different data zones

### Hive Metastore
- Central metadata catalog
- Tracks table schemas and partitions
- Uses MariaDB as backend storage
- Bridge between storage and query layer

### Trino
- Distributed SQL query engine
- Federation across data sources
- High-performance analytics queries
- Access to all medallion layers

### Apache Superset
- BI and visualization platform
- Creates dashboards on gold layer tables
- SQL editor for ad-hoc analysis
- Connects to Trino for data access

## Performance Considerations

The pipeline was designed with the following performance considerations:

- Partitioning strategy by year, month, and state to optimize query performance
- Delta Lake Z-ordering on frequently queried columns
- Trino query optimization through predicate pushdown
- Materialized aggregates for common analytical patterns
- Spark worker configuration for parallel processing

## Security Design

- MinIO authentication via access and secret keys
- Data isolation through bucket separation
- Trino catalog configuration to control access
- Potential for row/column level security using Trino
- Service account isolation for BigQuery access

## Future Architecture Enhancements

- Stream processing for real-time data updates
- Data quality framework integration
- Metadata-driven pipeline generation
- Integration with data governance tools
- Enhanced multi-tenancy support
