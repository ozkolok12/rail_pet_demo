### Rail Data Pipeline Demo

This project is a Data Engineering PET project showcasing a full end-to-end pipeline:
from ingesting raw files into PostgreSQL, orchestrating transformations with Airflow, and visualizing insights with Metabase.

It’s designed as a lightweight but realistic replica of a production-grade BI/Analytics setup.

## Tech Stack

Docker Compose – infrastructure as code with support for multiple profiles (demo and live).
PostgreSQL 15 – data warehouse: staging, DWH, and datamart layers.
Airflow 2.9+ – workflow orchestration: 
    DAGs for file ingestion, cleaning, and transformations.
    Automatic migrations and service user initialization.
Python – custom ETL scripts for parsing and loading XLSB files.
Metabase – self-service BI tool:
    interactive dashboards,
    pre-saved questions,
    filters for time ranges, cargo categories, and geographies.

## Dashboards

Metabase comes preloaded with sample dashboards:
🚂 Rail Traffic Overview – shipment dynamics over time.
📦 Cargo Breakdown – volumes by wagon types and cargo classes.
🌍 Geography Insights – top sending and receiving countries.
All dashboards are interactive — click elements to drill down, apply filters, and explore.

##  Modes of Operation
# DEMO Mode
Loads a PostgreSQL demo dump (demo_warehouse.dump) with pre-populated data.
Metabase starts with dashboards fully connected to demo data.
Ideal for a quick showcase of the pipeline outcome.

# LIVE Mode
Loads an empty schema (no demo data).
Airflow initializes and is ready to run real DAGs.
Metabase loads dashboards only (no data initially).
Data appears gradually as Airflow pipelines run — perfect for demonstrating end-to-end ETL.

## How to Run
# DEMO mode (data + dashboards)
docker compose --env-file .env.demo up -d

# LIVE mode (empty DB + Airflow pipelines)
docker compose --env-file .env.live up -d

Access Airflow UI → http://localhost:8083
(user: admin, password: admin)

Access Metabase → http://localhost:3000
Login = Demo User
Email = demo@yourdomain.com
Password = ssb1ZkMbjXMEMU

## Why This Project

Demonstrates realistic data engineering workflows.
Combines ETL orchestration, data warehouse design, and BI visualization.
Designed to be portable and easy to demo in both instant mode (DEMO) and pipeline mode (LIVE).