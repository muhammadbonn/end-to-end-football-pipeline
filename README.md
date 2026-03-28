# End-to-End Football Data Engineering Pipeline
## Project Overview
This project is an end-to-end data engineering pipeline designed to extract, transform, and load (ETL) advanced football statistics. The goal is to build a robust Data Warehouse that can support advanced analytics and BI dashboards.

---
## Technology Stack
**Current Stack:**
* **Language:** Python
* **Data Processing:** Apache Spark (PySpark)
* **Data Lake:** AWS S3
* **Data Warehouse:** Snowflake

---
**Upcoming Integration:**
* **Orchestration:** Apache Airflow
* **Infrastructure as Code (IaC):** Terraform
* **Data Visualization:** Streamlit

---
## Architecture & Workflow (Phase 1 - Completed)
1. **Data Ingestion:** Raw CSV data is processed locally.
2. **Data Transformation:** PySpark is utilized to clean data, handle nulls (e.g., casting 'NA' strings safely), generate derived metrics (like shot distances and zones), and enforce schema structures.
3. **Cloud Storage (Data Lake):** Transformed data is written in `Parquet` format to an AWS S3 staging bucket.
4. **Data Warehouse Loading:** Snowflake executes `COPY INTO` commands to securely load the staged Parquet files into structured relational tables (Star/Snowflake Schema).

---
## Project Roadmap
This project is being developed iteratively. Below is the current progress:

- [x] **Phase 1:** PySpark Transformations & AWS S3 Integration.
- [x] **Phase 2:** Snowflake Data Warehouse Setup & Staging.
- [ ] **Phase 3:** Pipeline Orchestration using **Apache Airflow** *(In Progress - Coming next week)*.
- [ ] **Phase 4:** Infrastructure setup via **Terraform** *(Upcoming)*.
- [ ] **Phase 5:** Interactive BI **Dashboards** *(Upcoming)*.

---
## Project Structure
```
end-to-end-football-pipeline/
│
├── dags/                      # (Future) Apache Airflow DAGs for orchestration
│   └── football_etl_dag.py    # Main DAG to trigger and monitor the pipeline
│
├── scripts/                   # (Current) PySpark data transformation modules
│   ├── utils/                 # Helper functions and environment setup
│   ├── processors/            #
│   ├── ingestion_pipeline.py  # Main script for data ingestion
│   └── production_pipeline.py # Main execution script for data processing
│
├── sql/                       # (Current) Snowflake DDL and DML scripts
│   ├── 01_setup_and_stage.sql
│   ├── 02_create_tables.sql
│   └── 03_load_data.sql
│
├── terraform/                 # (Future) Infrastructure as Code (IaC) configuration
│   ├── main.tf                # Main configuration for AWS and Snowflake resources
│   ├── variables.tf
│   └── providers.tf
│
├── dashboards/                # (Future) BI dashboards and visualizations
│   └── screenshots/           # Dashboard screenshots for the README file
│
├── requirements.txt           # Python project dependencies
├── docker-compose.yml         # (Future) Docker configuration for local Airflow
├── .env                       # Environment variables and configuration settings
└── README.md                  # Main project documentation and overview
```
