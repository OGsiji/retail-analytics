# Retail Analytics Pipeline

> **End-to-end retail analytics platform built with Apache Airflow 3.1.2, dbt, FastAPI, and Metabase**

A production-ready data pipeline for retail analytics, designed for Bidco Africa to analyze sales performance, detect promotions, monitor pricing strategies, and assess data quality across retail locations.

---

## 📋 Table of Contents

- [Features](#-features)
- [Architecture](#-architecture)
- [Technology Stack](#-technology-stack)
- [Prerequisites](#-prerequisites)
- [Quick Start](#-quick-start)
- [Deployment Guide](#-deployment-guide)
- [Project Structure](#-project-structure)
- [Business Questions Answered](#-business-questions-answered)
- [API Documentation](#-api-documentation)
- [Metabase Dashboards](#-metabase-dashboards)
- [Troubleshooting](#-troubleshooting)

---

## ✨ Features

- **Automated ETL Pipeline**: Daily orchestration of data ingestion, transformation, and analytics
- **Data Quality Monitoring**: Automated validation and quality scoring of retail data
- **Promotion Detection**: Intelligent algorithm to identify promotional periods and measure uplift
- **Competitive Pricing Analysis**: Track Bidco's price positioning against competitors
- **REST API**: FastAPI service for programmatic access to analytics insights
- **Interactive Dashboards**: Metabase visualizations for business intelligence
- **Production-Grade Infrastructure**: Built on Apache Airflow 3.1.2 with modern best practices

---

## 🏗️ Architecture

\`\`\`
┌─────────────────┐
│   CSV Data      │
│  (Retail Sales) │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────┐
│         Apache Airflow 3.1.2                │
│  ┌──────────────────────────────────────┐   │
│  │  DAG: retail_analytics_pipeline      │   │
│  │                                      │   │
│  │  1. Load CSV → PostgreSQL            │   │
│  │  2. Validate Data                    │   │
│  │  3. dbt Transformations              │   │
│  │  4. Generate Insights Summary        │   │
│  └──────────────────────────────────────┘   │
└─────────────────┬───────────────────────┘
                  │
                  ▼
         ┌─────────────────┐
         │  PostgreSQL 15  │
         │                 │
         │  Schemas:       │
         │  - public       │
         │  - retail_staging│
         │  - retail_marts │
         │  - retail_analytics│
         └────┬────────┬───┘
              │        │
      ┌───────┘        └────────┐
      ▼                         ▼
┌─────────────┐         ┌──────────────┐
│  FastAPI    │         │  Metabase    │
│  Port: 8001 │         │  Port: 3000  │
└─────────────┘         └──────────────┘
\`\`\`

---

## 🛠️ Technology Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Orchestration** | Apache Airflow | 3.1.2 | Workflow management |
| **Runtime** | Astronomer Runtime | 3.1-4 | Airflow distribution |
| **Transformation** | dbt Core | 1.7.18 | SQL-based transformations |
| **Database** | PostgreSQL | 15 | Data warehouse |
| **API** | FastAPI | 0.104+ | REST API service |
| **Visualization** | Metabase | 0.47.4 | Business intelligence |
| **Language** | Python | 3.12 | Backend logic |
| **Container** | Docker | Latest | Containerization |

---

## 📦 Prerequisites

- **Docker Desktop**: Version 20.10+ with 8GB+ RAM allocated
- **Astronomer CLI**: Install via \`brew install astro\` (macOS) or from https://www.astronomer.io/docs/astro/cli/install-cli
- **System Resources**: Minimum 8GB RAM, 20GB disk space

---

## 🚀 Quick Start

### Step 1: Clone and Prepare Data

\`\`\`bash
git clone <repository-url>
cd retail-analytics-pipeline

# Place your CSV file
cp /path/to/retail_sales.csv include/datasets/retail_sales.csv
\`\`\`

### Step 2: Build Retail API Image

\`\`\`bash
docker build -t retail-api:latest ./include/api
\`\`\`

### Step 3: Start All Services

\`\`\`bash
astro dev start
\`\`\`

**Wait 2-3 minutes** for services to initialize.

### Step 4: Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow UI** | http://localhost:8080 | admin / admin |
| **FastAPI Docs** | http://localhost:8001/docs | None |
| **Metabase** | http://localhost:3000 | Setup on first visit |
| **PostgreSQL** | localhost:5435 | postgres / postgres |

### Step 5: Run the Pipeline

1. Open Airflow: http://localhost:8080
2. Find DAG: \`retail_analytics_pipeline\`
3. Click **Play** button to trigger
4. Monitor in **Grid** view

**Expected runtime**: 2-5 minutes

---

## 📂 Project Structure

\`\`\`
retail-analytics-pipeline/
├── dags/
│   └── retail_analytics_pipeline.py    # Main Airflow DAG
├── include/
│   ├── api/
│   │   ├── Dockerfile                   # FastAPI container
│   │   └── retail_api.py                # REST API
│   ├── datasets/
│   │   └── retail_sales.csv             # Input data
│   └── dbt/
│       ├── models/retail/
│       │   ├── staging/                 # Data cleaning
│       │   ├── marts/                   # Business logic
│       │   └── analytics/               # KPIs
│       ├── macros/                      # Custom functions
│       ├── dbt_project.yml
│       └── profiles.yml
├── Dockerfile                           # Airflow container
├── requirements.txt
├── docker-compose.override.yml
└── README.md
\`\`\`

---

## 💼 Business Questions Answered

### 1. Data Quality
**Metric**: Overall health score, completeness %  
**Table**: \`retail_analytics.data_quality_summary\`  
**API**: \`GET /api/data-quality\`

### 2. Promotion Effectiveness
**Metric**: Uplift %, discount depth, top performers  
**Table**: \`retail_analytics.promo_summary\`  
**API**: \`GET /api/promos\`

\`\`\`sql
SELECT product_description, promo_uplift_pct, promo_sales_value
FROM retail_marts.promo_detection
WHERE is_bidco = 1 AND is_on_promo = 1
ORDER BY promo_uplift_pct DESC LIMIT 10;
\`\`\`

### 3. Pricing Competitiveness
**Metric**: Price index, market positioning  
**Table**: \`retail_analytics.pricing_summary\`  
**API**: \`GET /api/pricing\`

### 4. Store Performance
**Metric**: Sales by store, promotion frequency  
**Table**: \`retail_marts.promo_detection\` (aggregated)

### 5. Category Insights
**Metric**: Revenue, units sold by category  
**Table**: \`retail_staging.stg_retail_sales\`

### 6. Supplier Analysis
**Metric**: Bidco vs competitor performance  
**Table**: \`retail_marts.price_comparison\`

### 7. Temporal Trends
**Metric**: Daily/weekly/monthly patterns  
**Table**: \`retail_staging.stg_retail_sales\`

### 8. SKU Performance
**Metric**: Top sellers, sales velocity  
**Table**: \`retail_marts.promo_detection\`

### 9. Data Anomalies
**Metric**: Missing values, outliers  
**Table**: \`retail_analytics.data_quality_issues\`

### 10. Baseline Performance
**Metric**: Non-promo sales levels  
**Table**: \`retail_marts.store_baselines\`

---

## 🔌 API Documentation

### Available Endpoints

\`\`\`bash
# Health check
curl http://localhost:8001/health

# Get all metrics
curl http://localhost:8001/api/metrics | jq

# Promotion data
curl http://localhost:8001/api/promos | jq

# Pricing analysis
curl http://localhost:8001/api/pricing | jq

# Data quality
curl http://localhost:8001/api/data-quality | jq
\`\`\`

**Interactive Docs**: http://localhost:8001/docs

---

## 📊 Metabase Dashboards

### Setup Instructions

1. Visit http://localhost:3000
2. Create admin account
3. Add PostgreSQL connection:
   - Host: \`postgres\` (Docker internal)
   - Port: \`5432\`
   - Database: \`postgres\`
   - User/Pass: \`postgres\` / \`postgres\`

### Recommended Dashboard: Executive Summary

**Cards**:
- Total Sales (last 30 days)
- Data Quality Score
- Active Promotions
- Bidco Market Share
- Price Position Indicator

**SQL Example**:
\`\`\`sql
SELECT
    SUM("Total_Sales") as total_sales,
    COUNT(DISTINCT "Store_Name") as stores
FROM public.retail_sales
WHERE "Date_Of_Sale" >= CURRENT_DATE - INTERVAL '30 days';
\`\`\`

---

## 🔧 Troubleshooting

### CSV File Not Found
\`\`\`bash
# Verify file exists
ls -la include/datasets/retail_sales.csv

# Copy if missing
cp /path/to/data.csv include/datasets/retail_sales.csv
astro dev restart
\`\`\`

### Metabase Connection Issues
Use Docker hostname \`postgres\` (not localhost) with port \`5432\` (not 5435)

### DAG Not Appearing
\`\`\`bash
# Check for syntax errors
astro dev parse

# View logs
astro dev logs -f -s
\`\`\`

### Out of Memory
Increase Docker Desktop memory to 8GB+:
- Settings → Resources → Memory → 8GB → Apply & Restart

---

## 🛑 Stopping Services

\`\`\`bash
# Stop (preserves data)
astro dev stop

# Clean slate
astro dev kill
\`\`\`

---

## 📈 Production Deployment

### Step 1: Environment Variables
Create \`.env\` file:
\`\`\`env
AIRFLOW__CORE__FERNET_KEY=<generate-new-key>
AIRFLOW__WEBSERVER__SECRET_KEY=<generate-new-key>
POSTGRES_PASSWORD=<strong-password>
\`\`\`

### Step 2: Secure Configuration
- Change default Airflow credentials
- Use secrets manager for DB passwords
- Enable SSL for PostgreSQL
- Configure firewall rules

### Step 3: Deploy
\`\`\`bash
# Push to Astronomer Cloud
astro deploy

# Or use Docker Compose in production
docker-compose -f docker-compose.yml -f docker-compose.override.yml up -d
\`\`\`

---

## 📞 Support

**Issues?**
1. Check [Troubleshooting](#-troubleshooting)
2. Review logs: \`astro dev logs\`
3. Contact data engineering team

---

**Built with ❤️ for Bidco Africa**

*Last Updated: 2025-01-13 | Apache Airflow 3.1.2*
