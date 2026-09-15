# FMCG Sales Data Platform

An end-to-end data engineering and analytics platform built for the **Fast-Moving Consumer Goods (FMCG)** domain using **AWS S3, Databricks, PySpark, Delta Lake, SQL, and Genie AI**.

The platform ingests sales data from cloud storage, processes dimension and fact data through an ETL pipeline, maintains Delta tables using incremental processing, and provides analytics through Databricks and Genie AI.

---

## 🚀 Project Overview

This project demonstrates a production-style data engineering workflow for processing FMCG sales data.

The platform is designed to:

- Ingest raw data through **AWS S3**
- Process data using **Databricks and PySpark**
- Clean and transform dimension and fact datasets
- Build and maintain **Delta Lake tables**
- Perform incremental data processing using **Delta MERGE**
- Create analytical tables for downstream consumption
- Orchestrate the complete ETL workflow using **Databricks Jobs**
- Execute the pipeline automatically on a daily schedule
- Provide business insights using **Genie AI**
- Support analytics and dashboard-based reporting

---

## 🏗️ Architecture

```text
                    FMCG Source Data
                           │
                           ▼
                    ┌─────────────┐
                    │   AWS S3    │
                    │ Cloud Storage│
                    └──────┬──────┘
                           │
                           ▼
                ┌─────────────────────┐
                │     Databricks      │
                │      PySpark        │
                └──────────┬──────────┘
                           │
              ┌────────────┴────────────┐
              │                         │
              ▼                         ▼
      Dimension Processing       Fact Processing
              │                         │
              ▼                         ▼
       Customer / Product        Sales / Orders
       Pricing / Date             Fact Tables
              │                         │
              └────────────┬────────────┘
                           │
                           ▼
                    ┌─────────────┐
                    │ Delta Lake  │
                    │  Gold Tables│
                    └──────┬──────┘
                           │
                ┌──────────┴──────────┐
                │                     │
                ▼                     ▼
        Databricks Analytics      Genie AI
                                      │
                                      ▼
                              Business Insights
