# FMCG Sales Data Platform

An end-to-end FMCG (Fast-Moving Consumer Goods) data engineering and analytics platform built using **AWS S3, Databricks, PySpark, Delta Lake, SQL, and Databricks Genie**.

The project demonstrates a production-style data pipeline that ingests raw sales data, processes it through Bronze, Silver, and Gold layers, creates analytical fact and dimension tables, and exposes the final data through dashboards and natural-language AI analytics.

---

## 🚀 Project Overview

This project simulates a real-world FMCG data platform where raw sales data is continuously processed and transformed into business-ready datasets.

The platform implements:

- Cloud-based data storage using **Amazon S3**
- **Medallion Architecture** using Bronze, Silver, and Gold layers
- Data ingestion and transformation using **Databricks & PySpark**
- Incremental data processing using **Delta Lake**
- Fact and dimension table processing
- Data validation and transformation
- Automated ETL orchestration using **Databricks Jobs**
- Scheduled daily pipeline execution
- Business intelligence dashboards
- Natural-language analytics using **Databricks Genie**

---

## 🏗️ Architecture

![FMCG Architecture](docs/FMCG%20architecture.png)

The platform follows a layered data architecture:

### 1. Raw Data — Amazon S3

Raw FMCG sales data is stored in **Amazon S3**, acting as the cloud-based landing/storage layer.

### 2. Bronze Layer

The Bronze layer contains the ingested raw data with minimal transformation.

Purpose:

- Preserve source data
- Maintain an ingestion layer
- Provide traceability for downstream processing

### 3. Silver Layer

The Silver layer performs data cleaning and transformation.

Operations include:

- Data type standardization
- Missing-value handling
- Duplicate handling
- Data validation
- Business transformations
- Schema consistency

### 4. Gold Layer

The Gold layer contains business-ready analytical tables.

The project processes:

- Customer dimensions
- Product dimensions
- Pricing dimensions
- Order/sales fact data

These tables are optimized for analytics and reporting.

### 5. Serving Layer

The final Gold data is consumed by:

- Databricks SQL dashboards
- Databricks Genie
- Business analytics and reporting

---

## 🔄 Data Pipeline

The ETL workflow follows this general process:

```text
Amazon S3
   │
   ▼
Raw Data
   │
   ▼
Bronze Layer
   │
   ▼
Silver Layer
   │
   ▼
Gold Layer
   │
   ├──────────────► Databricks SQL Dashboards
   │
   └──────────────► Databricks Genie
