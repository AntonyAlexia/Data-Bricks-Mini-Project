# Data-Bricks-Mini-Project

Mini Project: ETL Pipeline Using Databricks and Delta Lake
This project demonstrates a modular and reusable ETL (Extract, Transform, Load) pipeline built on Databricks. The pipeline processes raw CSV data, transforms it through Bronze, Silver, and Gold layers, and generates insights for high-priority customers. The implementation adheres to best practices in data engineering and ensures scalability, performance, and maintainability.

Key Features
Data Sources:

Raw data in CSV format provided as input.
Files include: sales.csv, customers.csv, costs.csv, promotions.csv, supplementary_demographics.csv, and times.csv.
ETL Pipeline Phases:

Bronze Layer:
Raw data is loaded into Delta tables.
Metadata columns (source_file, loaded_ts) are added for traceability.
Partitioning by loaded_ts for efficient querying.
Silver Layer:
Data is deduplicated using row_number() to ensure only the latest records are retained.
Dimension and fact tables are created for structured analytics.
Gold Layer:
A high-priority customer view identifies customers with purchases > $1500 in a year.
Results are materialized if query performance exceeds 3 seconds.
Schema Design:

Star Schema with clear separation of fact (sales_edw, cost_edw) and dimension tables (customers_edw, times_edw, etc.).
Surrogate keys and SCD1 approach for simplicity.
Cloud Integration:

Compatible with Azure ADLS, AWS S3, and GCP GCS for data storage.
Optional scheduling and orchestration using ADF, GCP Composer, or AWS Managed Airflow.
Performance Optimization:

Partitioning and deduplication ensure efficient queries.
Materialized views for faster reporting with acceptable data latency.
Technologies Used:

Databricks for data processing and transformation.
Delta Lake for data storage and versioning.
PySpark for transformations and modular code.
Project Highlights
Reusable and parameterized code for scalability.
Logging and monitoring for tracking ETL operations.
Supports both incremental and full data loads.
Detailed data lineage from raw CSV to Gold layer reporting.
How to Run the Project
Clone the repository and configure your Databricks workspace.
Place the raw CSV files in a cloud storage location (e.g., ADLS, S3, GCS).
Update the configuration file (config.yml) with the file paths and table names.
Run the ETL pipeline in sequence:
Split raw CSV files into smaller chunks.
Load data into the Bronze layer.
Transform and deduplicate data in the Silver layer.
Generate insights in the Gold layer.
Validate outputs using Databricks notebooks or SQL queries.
Use Cases
Automating data pipelines for scalable analytics.
Creating EDW (Enterprise Data Warehouse) structures for reporting.
Generating insights for customer segmentation and business decisions.
