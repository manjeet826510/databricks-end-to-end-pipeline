# Databricks End-to-End Data Engineering Pipeline

An end-to-end data engineering project built on Databricks using
Medallion Architecture (Bronze, Silver, Gold) and Delta Lake.

## Architecture
- Bronze: Incremental ingestion using Auto Loader
- Silver: Data cleansing and business transformations
- Gold: Dimensional modeling with fact and dimension tables

## Tech Stack
- Databricks
- Apache Spark (PySpark)
- Delta Lake
- Delta Live Tables (DLT)
- Unity Catalog

## Key Features
- Idempotent MERGE logic for fact tables
- Surrogate key-based dimensional modeling
- Parameterized pipeline design
- GitHub-backed Databricks Repos

## Execution
The pipeline is designed to run as a Databricks Job / DLT pipeline.
Screenshots of successful runs are included for reference.

## Screenshots
See screenshots folder for pipeline execution and outputs.

