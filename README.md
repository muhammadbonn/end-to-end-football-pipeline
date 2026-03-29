![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-F1502F?style=for-the-badge&logo=apachespark&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Snowflake](https://img.shields.io/badge/snowflake-%2329B5E8.svg?style=for-the-badge&logo=snowflake&logoColor=white)
![Terraform](https://img.shields.io/badge/terraform-%235835CC.svg?style=for-the-badge&logo=terraform&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)

# End-to-End Football Data Engineering Pipeline
## Project Philosophy & Problem Statement

While this project uses European Football Data as a case study, the core objective was to engineer a production-grade data platform.

The Challenge: Football data is notoriously messy—player names have special characters, match events are deeply nested, and statistics like "Expected Goals (xG)" require complex derived calculations. Processing this at scale manually is impossible.

The Solution:
I built this pipeline to explore how modern data tools can work in harmony:

    Infrastructure: Using Terraform to treat the cloud as code, avoiding manual errors.

    Processing: Using PySpark to handle "Big Data" cleaning patterns (handling nulls, schema enforcement, and feature engineering like shot distances).

    Storage & Analytics: Bridging the gap between a Data Lake (S3) and a Cloud Data Warehouse (Snowflake) for high-performance BI

Why Football Data? > The dataset (scraped from Understat/Kaggle) includes over 600,000 shots and thousands of games. It provides a perfect playground for:

    Data Cleaning: Handling inconsistent naming conventions across leagues.

    Type Casting: Managing complex timestamps and coordinate systems (X, Y positions).

    Aggregation: Building complex analytical views like "Striker Efficiency" which requires joining multiple large tables.
    
---
## Technology Stack
* **Language:** Python
* **Data Processing:** Apache Spark (PySpark)
* **Data Lake:** AWS S3
* **Data Warehouse:** Snowflake
* **Orchestration:** Apache Airflow
* **Infrastructure as Code (IaC):** Terraform
* **Data Visualization:** Streamlit

---
## Architecture & Workflow

The pipeline follows a modern Medallion Architecture (Bronze, Silver, Gold) orchestrated entirely by Apache Airflow:

    Ingestion (Bronze): Automated extraction of raw football datasets from Kaggle directly to AWS S3 using Python.

    Transformation (Silver): PySpark jobs running inside Docker containers to clean data, handle nulls, and engineer new features (e.g., shot distances and zones). Data is saved back to S3 in optimized Parquet format.

    Data Warehouse (Gold): Dynamic Snowflake integration where clean data is loaded via COPY INTO commands into structured relational tables for high-performance analytics.

    Infrastructure as Code: The entire cloud environment (S3 buckets, Snowflake databases, and roles) is provisioned and managed using Terraform.

---
## Installation & Setup

Follow these steps to replicate the pipeline on your local machine:

### 0. Prerequisites

Before you begin, ensure you have the following:

    AWS Account: To create an S3 bucket and IAM keys.

    Snowflake Account: A trial account works perfectly (Note your Account Identifier).

    Docker & Docker Compose: Installed on your machine.

    Terraform: Installed if you want to manage infrastructure.

### 1. Clone the Repository

To get a local copy up and running, follow these simple steps:

First, clone the project to your local machine using Git:
```
git clone https://github.com/muhammadbonn/end-to-end-football-pipeline.git
cd end-to-end-football-pipeline
```

### 2. Environment Setup
The project uses two configuration files for security. Since `.env` is ignored by Git, you must create it manually:

   * **A. Create `.env` file (for Airflow & Python)** In the root directory, create a file named `.env` and add your credentials:
     ```env
     AWS_ACCESS_KEY_ID=your_aws_key
     AWS_SECRET_ACCESS_KEY=your_aws_secret
     AWS_DEFAULT_REGION=your_aws_region
     S3_BUCKET_NAME=your_unique_bucket
     KAGGLE_PATH=technika148/football-database
     PYTHONPATH=/opt/airflow/scripts:/opt/airflow
     ```

   * **B. Configure Infrastructure (Terraform)** Navigate to the `terraform/` directory and edit the `terraform.tfvars` file based on the template:  
     [View Template File](./terraform/terraform.tfvars)

### 3. Deploy Infrastructure (Terraform)
Run the following commands to build your AWS and Snowflake environment:

```
cd terraform
terraform init
terraform apply -auto-approve
```

### 4. Run the Pipeline (Docker & Airflow)
Start the automated orchestration:
```
docker-compose up airflow-init
docker-compose up -d
```

### 5. Trigger the DAG (Airflow)
Open your browser at http://localhost:2626 (User: admin, Pass: admin) and trigger the football_etl_pipeline DAG

---
## Project Structure
```
end-to-end-football-pipeline/
│
├── dags/                                       # Apache Airflow DAGs for orchestration
│   └── football_etl_dag.py                     # Main DAG to trigger and monitor the pipeline
│
├── scripts/                                    # PySpark data transformation modules
│   ├── utils/                                  # Helper functions and environment setup
│   ├── processors/                             # Helper modules for data processing
│   ├── ingestion_pipeline.py                   # Main script for data ingestion
│   └── production_pipeline.py                  # Main execution script for data processing
│
├── sql/                                        # Snowflake DDL and DML scripts
│   ├── 01_create_tables.sql
│   ├── 02_load_data.sql
│   └── 03_gold_layer_tables.sql
│
├── terraform/                                  # (Infrastructure as Code (IaC) configuration
│   ├── main.tf                                 # Main configuration for AWS and Snowflake resources
│   └── terraform.tfvars
│
├── dashboards/                                 # BI dashboard visualizations│
├── requirements.txt                            # Python project dependencies
├── Dockerfile                                  # Docker instructions
├── docker-compose.yml                          # Docker configuration for local Airflow
├── .env                                        # Environment variables and configuration settings
└── README.md                                   # Main project documentation and overview
```
