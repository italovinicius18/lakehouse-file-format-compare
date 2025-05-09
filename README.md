# Sisvan Lakehouse Pipeline

This repository implements an end-to-end medallion data pipeline orchestrated by **Apache Airflow** and powered by **Apache Spark**. It extracts SISVAN microdata from BigQuery, loads it into **landing**, **bronze**, **silver**, and **gold** layers in **Delta Lake** on **MinIO**, provides analytical access via **Trino**, and supports dashboards in **Apache Superset**.

## Architecture

- **Medallion Architecture**: organizes data into incremental layers (landing → bronze → silver → gold) to progressively improve data quality.  
- **Airflow orchestration**: defines Python DAGs for scheduling and monitoring pipeline tasks.  
- **Apache Spark**: handles distributed data processing with DataFrame APIs and Delta Lake integration.  
- **Delta Lake**: ensures ACID transactions and data versioning at each layer.  
- **MinIO**: provides S3-compatible object storage for Delta tables.  
- **Trino**: offers a high-performance SQL engine for querying data lake tables.  
- **Apache Superset**: enables interactive dashboarding and BI.

<p align="center">
  <img src="https://github.com/user-attachments/assets/caa845b8-a337-4ced-b5a9-636354b0a733">
</p>


## Technologies

- Apache Airflow  
- Apache Spark  
- Delta Lake  
- MinIO  
- Trino  
- Apache Superset  
- Python 3.8+  
- Docker & Docker Compose

## Prerequisites

- Docker >= 20.10 and Docker Compose  
- Python 3.8+ (for Airflow dependencies)  
- Google Cloud service account JSON (place in `include/gcp/service-account.json` with BigQuery access)


## Adptations

You can adapt the connections and configurations in the `airflow_settings.yaml` file to suit your environment. The default settings are configured for local development with Docker.

## Repository Structure

```text
.
├── Dockerfile
├── README.md
├── airflow_settings.yaml
├── apps/
├── dags/
│   ├── 1_bigquery_to_landing.py   # Landing layer DAG
│   ├── 2_landing_to_bronze.py     # Bronze layer DAG
│   ├── 3_bronze_to_silver.py      # Silver layer DAG
│   ├── 4_silver_to_gold.py        # Gold layer DAG
│   └── exampledag.py              # Example DAG template
├── data/
├── docker-compose.override.yml
├── hive/
│   └── metastore-site.xml
├── include/
│   ├── example/
│   │   └── read.py
│   └── gcp/
│       └── service-account.json
├── init_trino/
│   └── create_schemas.sql
├── packages.txt
├── plugins/
├── requirements.txt
├── superset_home/
│   └── superset.db
├── tests/
└── trino/
    ├── catalog/
    │   └── delta.properties
    ├── config.properties
    ├── jvm.config
    └── node.properties
```

## Getting Started

1. **Install Astro CLI** for local Airflow development: follow the [official installation guide](https://www.astronomer.io/docs/cloud/stable/develop/cli-installation/) to install the Astro CLI.
2. Setup the folder and permission for superset folder:
   ```bash
   mkdir -p ./config_superset
   chown 1000:1000 ./config_superset
   chmod 750 ./config_superset
   ``` 
3. **Start the environment**:
   ```bash
   astro dev start --build
   ```

This command will spin up the default Astro containers (Airflow metadata database, scheduler, webserver, triggerer) along with the services defined in `docker-compose.override.yml`:

- **spark-master**: runs Bitnami Spark 3.5.5 in master mode, exposed on ports **8081** (Web UI) and **7077** (master endpoint), mounts shared volumes for DAG includes, applications, and data.
- **spark-worker**: runs Bitnami Spark 3.5.5 in worker mode with 3 GB RAM and 2 cores per worker; configured to launch **2 replicas** for parallel execution.
- **minio**: S3-compatible object storage (MinIO), exposed on port **9000**; console UI on **9001**.
- **minio-init**: initializes the MinIO buckets (`landing`, `bronze`, `silver`, `gold`) using the MinIO Client.
- **trino-coordinator** & **trino-worker**: Trino cluster components; coordinator listens on **8085** (mapped to container’s 8080), worker registers with the coordinator; both mount the local `trino/` configuration.
- **trino-init**: bootstraps Trino by loading initial schemas from `init_trino/create_schemas.sql` after the cluster is healthy.
- **mariadb**: MariaDB instance on port **3307** used by Hive Metastore.
- **hive-metastore**: Hive Metastore service (Thrift on port **9083**) configured to use the MariaDB database.
- **superset**: Apache Superset BI platform on **8088**, connected to Hive Metastore, Trino, and MinIO for dashboarding.

Named Docker volumes are used for data persistence:
- `spark-data`
- `trino-data`
- `hive-metastore-db-data`

3. **Access services**:
   - Airflow UI: `http://localhost:8080` (default credentials: `admin` / `admin`)  
   - Superset UI: `http://localhost:8088`  
   - Trino HTTP: `http://localhost:8085`  
   - MinIO Console: `http://localhost:9001`

## DAG Overview

- **1_bigquery_to_landing**: extracts raw SISVAN microdata from BigQuery and writes it to the **landing** Delta layer on MinIO, partitioned by year, month, and state.
- **2_landing_to_bronze**: reads landing data, filters nulls and removes outliers, then writes cleaned records to the **bronze** Delta table in the Hive metastore.
- **3_bronze_to_silver**: loads the bronze table, applies semantic mappings (e.g., race, life stage, education), and writes the **silver** Delta table with updated partitions.
- **4_silver_to_gold**: performs gold‑level aggregations—nutritional state by age/sex, monthly follow‑ups by state, origin system distribution, unique users per year—and writes each result as a separate Delta table under the **gold** schema.
- **exampledag.py**: a template DAG demonstrating task structure and dependency setup for creating custom pipelines.

## Running the Pipeline

- Trigger DAGs manually in the Airflow UI or enable scheduling by updating the `schedule` parameter at the top of each DAG.  
- Monitor task status and logs directly in the Airflow interface.

## Contributing

1. Fork the repository  
2. Create a feature branch:  
   ```bash
   git checkout -b feature/your-feature
   ```  
3. Commit your changes:  
   ```bash
   git commit -m "Add feature"
   ```  
4. Push your branch and open a Pull Request.
