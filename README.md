# Sisvan Lakehouse Pipel## Technologies

- *## Prerequisites

- Docker >= 20.10 and Docker Compose
- Python 3.8+ (for Airflow dependencies)
- [Astro CLI](https://www.astronomer.io/docs/cloud/stable/develop/cli-installation/) for local Airflow development
- Google Cloud service account JSON (place in `include/gcp/service-account.json` with BigQuery access to the SISVAN dataset)
- At least 8GB of RAM available for Docker containershe Airflow 2.7+**: Workflow orchestration platform
- **Apache Spark 3.5.5**: Distributed data processing engine
- **Delta Lake 3.1.0**: ACID transaction layer for data lakes
- **MinIO**: S3-compatible object storage
- **Trino**: Distributed SQL query engine
- **Apache Superset**: Business intelligence and data visualization platform
- **Python 3.10+**: Programming language
- **Docker & Docker Compose**: Container platform for local deployment
- **Hive Metastore**: Metadata catalog service
- **MariaDB**: Database for the Hive Metastorecense: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7+-red.svg)](https://airflow.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5+-orange.svg)](https://spark.apache.org/)

This repository implements an end-to-end medallion data pipeline orchestrated by **Apache Airflow** and powered by **Apache Spark**. It extracts SISVAN (Brazilian Food and Nutrition Surveillance System) microdata from BigQuery, loads it into **landing**, **bronze**, **silver**, and **gold** layers in **Delta Lake** on **MinIO**, provides analytical access via **Trino**, and supports dashboards in **Apache Superset**.

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


## Adaptations & Configuration

You can adapt the connections and configurations in the `airflow_settings.yaml` file to suit your environment. The default settings are configured for local development with Docker.

Key configuration files:
- `airflow_settings.yaml`: Airflow connections and variables
- `config_trino/`: Trino configuration files
- `config_hive/`: Hive Metastore configuration
- `config_init_trino/`: SQL scripts to initialize Trino schemas
- `docker-compose.override.yml`: Custom services configuration

## Repository Structure

```text
.
├── Dockerfile                      # Airflow image configuration
├── LICENSE                         # MIT license file
├── README.md                       # Main documentation
├── airflow_settings.yaml           # Airflow connections and variables
├── dags/                           # Airflow DAG definitions
│   ├── 1_bigquery_to_landing.py    # Landing layer DAG
│   ├── 2_landing_to_bronze.py      # Bronze layer DAG
│   ├── 3_bronze_to_silver.py       # Silver layer DAG
│   ├── 4_silver_to_gold.py         # Gold layer DAG
│   ├── sisvan_full_pipeline.py     # End-to-end orchestration DAG
│   └── exampledag.py               # Example DAG template
├── docs/                           # Additional documentation
│   └── ARCHITECTURE.md             # Detailed architecture documentation
├── docker-compose.override.yml     # Container configuration
├── config_hive/                    # Hive Metastore configuration
│   └── metastore-site.xml          # Metastore properties
├── include/                        # Shared resources for DAGs
│   ├── example/                    # Example code
│   │   └── read.py                 # Sample data reading functions
│   ├── gcp/                        # Google Cloud resources
│   │   └── service-account.json    # GCP credentials (not in repo)
│   └── queries/                    # SQL queries for transformations
│       ├── acompanhamentos_mensais.sql
│       ├── distribuicao_sistema_origem.sql
│       ├── estado_nutricional_por_faixa_etaria.sql
│       └── usuarios_unicos_por_ano.sql
├── config_init_trino/              # Trino initialization scripts
│   └── create_schemas.sql          # Schema creation statements
├── packages.txt                    # System packages for Airflow
├── plugins/                        # Airflow plugins directory
├── requirements.txt                # Python dependencies
├── config_superset/                # Superset configuration
│   ├── docker/                     # Superset Docker config
│   │   └── requirements-local.txt  # Superset Python dependencies
│   └── superset.db                 # Superset metadata database
├── tests/                          # Test directory
└── config_trino/                   # Trino configuration
    ├── catalog/                    # Catalog definitions
    │   └── delta.properties        # Delta Lake connector config
    ├── config.properties           # Trino server config
    ├── jvm.config                  # JVM settings for Trino
    └── node.properties             # Trino node properties
```

For more detailed information about the architecture, see [ARCHITECTURE.md](docs/ARCHITECTURE.md).
```

## Getting Started

1. **Install Astro CLI** for local Airflow development: follow the [official installation guide](https://www.astronomer.io/docs/cloud/stable/develop/cli-installation/) to install the Astro CLI.
2. **Configure the service account**:
   ```bash
   # Create the directory if it doesn't exist
   mkdir -p ./include/gcp
   
   # Place your Google Cloud service account JSON in this location
   # This file should have BigQuery access to the SISVAN dataset
   cp /path/to/your/service-account.json ./include/gcp/service-account.json
   ```

3. **Setup the folder and permission for Superset**:
   ```bash
   mkdir -p ./config_superset
   chown 1000:1000 ./config_superset
   chmod 750 ./config_superset
   ```

4. **Start the environment**:
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
   - Superset UI: `http://localhost:8088`  (default credentials: `admin` / `admin`)  
   - Trino HTTP: `http://localhost:8085`  
   - MinIO Console: `http://localhost:9001` (default credentials: `minioadmin` / `minioadmin`)  

## DAG Overview

### Medallion Architecture Flow

This project implements a classic Medallion Architecture with four layers:

![Medallion Architecture](https://raw.githubusercontent.com/user-attachments/assets/caa845b8-a337-4ced-b5a9-636354b0a733)

1. **Landing Layer**: Raw data from source systems, unchanged
2. **Bronze Layer**: Cleaned data with basic schema enforcement
3. **Silver Layer**: Semantically enriched data with business logic applied
4. **Gold Layer**: Aggregated data optimized for analytics and reporting

For detailed documentation on the architecture, see [ARCHITECTURE.md](docs/ARCHITECTURE.md).

### Pipeline DAGs

- **1_bigquery_to_landing**: Extracts raw SISVAN microdata from BigQuery and writes it to the **landing** Delta layer on MinIO, partitioned by year, month, and state.
- **2_landing_to_bronze**: Reads landing data, filters nulls and removes outliers, then writes cleaned records to the **bronze** Delta table in the Hive metastore.
- **3_bronze_to_silver**: Loads the bronze table, applies semantic mappings (e.g., race, life stage, education), and writes the **silver** Delta table with updated partitions.
- **4_silver_to_gold**: Performs gold-level aggregations—nutritional state by age/sex, monthly follow-ups by state, origin system distribution, unique users per year—and writes each result as a separate Delta table under the **gold** schema. Uses SQL queries from the `include/queries/` directory.
- **sisvan_full_pipeline**: Orchestrates the entire pipeline by triggering the three DAGs sequentially, ensuring that each layer is processed before moving to the next. The pipeline flow is: bronze → silver → gold.
- **exampledag.py**: A template DAG demonstrating task structure and dependency setup for creating custom pipelines.

## Running the Pipeline

1. **Start the environment** (if not already running):
   ```bash
   astro dev start --build
   ```

2. **Trigger the pipeline**:
   - For a full run: Trigger the `sisvan_full_pipeline` DAG in the Airflow UI
   - For individual layer processing: Trigger specific DAGs in order (landing → bronze → silver → gold)
   - To enable scheduling: Update the `schedule` parameter at the top of each DAG file

3. **Monitor execution**:
   - Monitor task status and logs directly in the Airflow interface
   - The full pipeline typically takes 1-2 hours to process the complete SISVAN dataset

4. **Query the data**:
   - Access Trino at `http://localhost:8085` to query data using SQL
   - Use Superset at `http://localhost:8088` to build visualizations on the gold layer datasets
   - Direct SQL access to each layer is available via the `delta` catalog in Trino:
     ```sql
     -- Example: Query the silver layer
     SELECT * FROM delta.silver.sisvan LIMIT 10;
     
     -- Example: Query a gold layer aggregation
     SELECT * FROM delta.gold.estado_nutricional_por_faixa_etaria;
     ```

## Performance Considerations

- The pipeline is configured to run with 2 Spark workers, each with 6 cores and 4GB RAM
- Processing the full SISVAN dataset (~82 GB) takes approximately:
  - BigQuery to Landing: ~38 minutes
  - Landing to Bronze: ~29 minutes
  - Bronze to Silver: ~19 minutes
  - Silver to Gold: ~13 minutes
- Storage utilization after processing:
  - Landing layer: 34.5 GiB across 36,332 objects
  - Bronze layer: 34.3 GiB across 17,282 objects
  - Silver layer: 34.3 GiB across 14,866 objects
  - Gold layer: 505.9 KiB across 28 objects
- Adjust worker settings in `docker-compose.override.yml` if you have more resources available

## Data Dictionary

The SISVAN dataset contains nutritional surveillance data with the following key fields:

| Column | Description |
|--------|-------------|
| id_pessoa | Unique anonymized person identifier |
| id_municipio | Municipality identifier (IBGE code) |
| sigla_uf | State abbreviation (2 letters) |
| ano | Reference year |
| mes | Reference month |
| fase_vida | Life stage (Criança, Adolescente, Adulto, Idoso) |
| estado_nutricional_* | Nutritional status classification |
| idade | Age |
| sexo | Gender |
| raca_cor | Race/ethnicity |
| escolaridade | Education level |
| sistema_origem | Origin system |
| peso | Weight (in kg) |
| altura | Height (in cm) |
| imc | Body Mass Index |

## Common Troubleshooting

- **Connection issues with MinIO**: Verify that MinIO is running and accessible on port 9000.
  ```bash
  curl -I http://localhost:9001
  ```

- **Spark workers not connecting**: Check Spark master UI at http://localhost:8081 to verify worker registration.

- **Missing tables in Trino**: Ensure Hive Metastore is running and schemas are created via the init scripts.
  ```bash
  docker logs sisvan-lakehouse-pipeline-trino-init-1
  ```

- **"No such file" errors for service-account.json**: Make sure to place your Google Cloud credentials at `include/gcp/service-account.json`.

- **Memory issues with Spark**: Adjust worker memory in `docker-compose.override.yml` or consider increasing Docker memory allocation.

## Project Roadmap

- [ ] Add data quality validation rules at the Bronze and Silver layers
- [ ] Implement incremental loads with Delta Lake change data capture
- [ ] Add ML pipelines for nutritional trend prediction
- [ ] Enhance visualization dashboards in Superset
- [ ] Add authentication and role-based access to the data layers

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

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
