--- Retail Sales ETL Pipeline (Airflow + Docker + Python) ---

-- Overview
This project implements a fully containerized ETL pipeline that automates the extraction, transformation, and loading of retail sales data. The workflow is orchestrated using Apache Airflow and packaged with Docker Compose for easy deployment.

-- Features
- Modular ETL pipeline with separate extract, transform, and load stages
- Apache Airflow DAG using PythonOperators
- Docker Compose deployment with Airflow webserver, scheduler, triggerer, and Postgres
- Pandaas-based data processing
- Persisted data outputs for downstream analytics
- Debugged import issues, log service 403 errors, and Airflow secret key configuration

-- Architecture
- Docker Compose -> Airflow -> ETL Scripts -> Processed Data

Services included:
- Airflow Webserver
- Airflow Scheduler
- Airflow Triggerer
- PostgreSQL Metadata DB
- Airflow Init Container

-- Project Structure
retail-sales-etl-pipeline/
├── dags/
│   └── retail_sales_dag.py
├── scripts/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
├── data/raw/
├── data/processed/
├── data/final/
├── docker-compose.yaml
└── requirements.txt

-- How to Run
- 1. Clone Repo
git clone https://github.com/<your-username>/retail-sales-etl-pipeline.git
cd retail-sales-etl-pipeline

- 2. Initialize Airflow
docker compose up airflow-init

- 3. Start Airflow
docker compose up

- 4. Access Airflow UI
http://localhost:8080
username: admin
password: admin

- 5. Trigger DAG
retail_sales_pipeline_github

-- Technologies Used
- Apache Airflow
- Docker / Docker Compose
- Python
- Pandas
- PostgreSQL

-- What I Learned
- How to deploy Airflow using Docker Compsoe
- How to structure Python ETL code for Airflow
- Adding Airflow logging configuration (host: container secret_key sync)
- Debugging import path issues inside containers
- Working with DAG dependencies and task scheduling