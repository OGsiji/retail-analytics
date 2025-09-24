# Customer Churn Prediction Data Pipeline

A production-ready ETL pipeline for customer churn prediction model development, implementing scalable data ingestion, transformation, and serving using Apache Airflow, dbt, and FastAPI, Metabase.

![metabase](images/metabase.png)

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Quick Start](#quick-start)
- [Data Sources](#data-sources)
- [Pipeline Components](#pipeline-components)
- [ML-Ready Features](#ml-ready-features)
- [API Documentation](#api-documentation)
- [Testing](#testing)
- [Deployment](#deployment)
- [Assumptions & Design Decisions](#assumptions--design-decisions)
- [Troubleshooting](#troubleshooting)
- [Future Enhancements](#future-enhancements)

## Overview

This data pipeline consolidates customer transaction data and user activity logs into an ML-ready dataset for churn prediction modeling. The system processes multiple data sources, applies comprehensive feature engineering, and serves the results via a REST API.

![airflow](images/airflow1.png)

### Key Deliverables
- **Automated ETL Pipeline**: Orchestrated with Apache Airflow
- **Data Transformation**: Using dbt for scalable SQL transformations
- **ML-Ready Dataset**: Comprehensive churn prediction features
- **REST API**: FastAPI service for data access and export
- **Data Quality**: Automated validation and monitoring

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │     Airflow     │    │   PostgreSQL    │
│                 │───▶│   Orchestrator  │───▶│  Data Warehouse │
│ • CSV Files     │    │                 │    │                 │
│ • SQL Dumps     │    └─────────────────┘    └─────────────────┘
└─────────────────┘              │                       │
                                 │                       │
                                 ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   dbt Models    │    │   Data Quality  │    │    FastAPI      │
│                 │    │     Checks      │    │   REST API      │
│ • Staging       │    │                 │    │                 │
│ • Marts         │    └─────────────────┘    └─────────────────┘
└─────────────────┘                                     │
                                                        ▼
                                              ┌─────────────────┐
                                              │   ML Models /   │
                                              │  Applications   │
                                              └─────────────────┘
```

### Technology Stack
- **Orchestration**: Apache Airflow with Astronomer Runtime
- **Data Warehouse**: PostgreSQL 15
- **Transformations**: dbt (data build tool)
- **API**: FastAPI with automatic documentation
- **Containerization**: Docker & Docker Compose
- **Data Processing**: Pandas, SQLAlchemy

## Features

### Data Ingestion
- **PostgreSQL Dump Processing**: Automated loading of users and transactions data
- **CSV File Ingestion**: User activity logs with validation
- **Incremental Updates**: Support for daily data refreshes
- **Error Handling**: Comprehensive logging and retry mechanisms

### Data Transformation
- **Standardized Timestamps**: Consistent datetime formatting across sources
- **Deduplication**: Removal of duplicate records with configurable rules
- **Feature Engineering**: 25+ ML-ready features including RFM analysis
- **Data Quality Checks**: Automated validation of row counts, nulls, and constraints

### API & Serving
- **REST Endpoints**: Full CRUD operations for churn features
- **Export Capabilities**: CSV and JSON export functionality
- **Real-time Queries**: Dynamic filtering and pagination
- **Health Monitoring**: Service health checks and metrics




## Quick Start

### Prerequisites
- Docker Desktop
- Docker Compose
- Astro CLI (`curl -sSL install.astronomer.io | sudo bash`)
- Python 3.8+ (for local development)

### Installation

1. **Clone and Initialize Project**
```bash
astro dev init churn-prediction-pipeline
cd churn-prediction-pipeline
```

2. **Add Your Data Files**
```bash
# Place your data files in include/datasets/
cp user_activities.csv include/datasets/
cp postgres_transactions_dump.sql include/datasets/
```

3. **Start the Pipeline**
```bash
# Remove Docker version warning
sed -i '' '1d' docker-compose.override.yml

# Start all services
astro dev start
```

4. **Access Services**
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **API Documentation**: http://localhost:8000/docs
- **PostgreSQL**: localhost:5432 (postgres/postgres)

### First Run

1. **Trigger the Pipeline**
   - Go to Airflow UI → DAGs → `churn_prediction_pipeline`
   - Toggle ON and click "Trigger DAG"

2. **Verify Results**
   - Check pipeline completion in Airflow
   - Access ML features via API: http://localhost:8000/churn-features
   - View dataset statistics: http://localhost:8000/churn-features/stats

## Data Sources

### 1. PostgreSQL Transaction Dump
**File**: `postgres_transactions_dump.sql`

**Tables Created**:
- `users`: User profiles with signup information
- `transactions`: Transaction history with amounts and status

**Schema**:
```sql
-- Users table
user_id (INT) - Unique identifier
email (VARCHAR) - User email address
region (VARCHAR) - Geographic region
signup_channel (VARCHAR) - Acquisition channel (android/ios/web)
created_at (TIMESTAMP) - Registration date

-- Transactions table
transaction_id (UUID) - Unique transaction identifier
user_id (INT) - Foreign key to users
amount (NUMERIC) - Transaction amount in NGN
currency (VARCHAR) - Currency code
status (VARCHAR) - success/failed/refunded
created_at (TIMESTAMP) - Transaction timestamp
```

### 2. User Activity Logs
**File**: `user_activities.csv`

**Schema**:
```
user_id - User identifier
session_id - Session identifier  
event_name - Activity type (page_view, purchase, add_to_cart, session_start, session_end)
event_timestamp - Activity timestamp
device - Device type (phone, tablet, desktop)
app_version - Application version
```

## Pipeline Components

### 1. Data Ingestion Tasks
- **`create_database_schema`**: Initialize database schemas and tables
- **`load_transaction_dump`**: Process and load PostgreSQL dump
- **`load_activities_csv`**: Ingest and validate user activity data
- **`validate_raw_data`**: Data quality checks and integrity validation

### 2. dbt Transformations

#### Staging Models
- **`stg_users`**: Standardized user data with region/channel normalization
- **`stg_transactions`**: Cleaned transactions with currency standardization
- **`stg_activities`**: Processed activities with event standardization

#### Marts Model
- **`churn_features`**: Final ML-ready feature table

### 3. Data Quality & Validation
- **`validate_final_features`**: Comprehensive dataset validation
- **Automated Tests**: dbt tests for data integrity
- **Health Monitoring**: Service status and data freshness checks

## ML-Ready Features

The pipeline generates a comprehensive feature set optimized for churn prediction:

### User Demographics
- `user_id`: Unique identifier
- `email`: User email
- `region`: Standardized geographic region
- `channel`: Standardized acquisition channel
- `signup_date`: Registration timestamp
- `user_tenure_days`: Days since signup

### Transaction Features
- `total_transactions`: Total transaction count
- `successful_transactions`: Successful transaction count
- `total_spend_ngn`: Total spending amount (NGN)
- `avg_transaction_amount`: Average transaction value
- `last_transaction_date`: Most recent transaction
- `days_since_last_transaction`: Recency metric
- `transaction_success_rate`: Success rate percentage

### Activity Features
- `total_activity_events`: Total activity count
- `unique_sessions`: Number of unique sessions
- `active_days`: Number of active days
- `page_views`: Page view count
- `cart_additions`: Add-to-cart events
- `purchase_events`: Purchase events
- `last_activity_date`: Most recent activity
- `days_since_last_activity`: Activity recency
- `avg_session_duration_minutes`: Average session length

### Engagement Metrics
- `mobile_activity_ratio`: Mobile usage percentage
- `desktop_activity_ratio`: Desktop usage percentage
- `cart_conversion_rate`: Cart-to-purchase conversion
- `purchase_conversion_rate`: View-to-purchase conversion

### RFM Analysis
- `recency_score`: Recency score (1-5)
- `frequency_score`: Frequency score (1-5)
- `monetary_score`: Monetary score (1-5)
- `rfm_total_score`: Combined RFM score (3-15)

### Churn Prediction
- `user_lifecycle_stage`: User segment (new/activated/engaged/loyal/active/inactive)
- `churn_flag`: Binary churn indicator (0=active, 1=churned)

### Churn Logic
Users are classified as churned (`churn_flag = 1`) if:
- No activity for 30+ days AND no transactions for 60+ days
- No activity for 60+ days
- Active for 90+ days but fewer than 5 total activities
- Active for 30+ days with zero spend and fewer than 3 sessions

## API Documentation

![fastapi](images/fastapimain.png)

### Base URL
- **Local Development**: http://localhost:8000
- **Production**: Configure in deployment

### Core Endpoints

#### GET `/churn-features`
Retrieve churn prediction features with filtering and pagination.

**Parameters**:
- `limit` (int): Number of records (1-10000, default: 100)
- `offset` (int): Records to skip (default: 0)
- `churn_flag` (int): Filter by churn status (0 or 1)
- `region` (str): Filter by region
- `channel` (str): Filter by acquisition channel
- `min_spend` (float): Minimum total spend filter
- `max_days_inactive` (int): Maximum days since last activity

**Example**:
```bash
curl "http://localhost:8000/churn-features?churn_flag=1&limit=50"
```

#### GET `/churn-features/stats`
Dataset statistics and distribution metrics.

**Response**:
```json
{
  "total_users": 800,
  "churned_users": 240,
  "churn_rate_percent": 30.0,
  "avg_total_spend": 25430.50,
  "top_regions": [...],
  "lifecycle_distribution": {...}
}
```

#### GET `/churn-features/export`
Export complete dataset in CSV or JSON format.

**Parameters**:
- `format` (str): Export format ("csv" or "json")
- `churn_flag` (int): Optional filter

#### GET `/churn-features/user/{user_id}`
Get features for a specific user.

#### GET `/churn-features/segments`
User segmentation analysis including RFM and lifecycle segments.

#### GET `/health`
Service health check and system status.

### API Client Examples

**Python**:
```python
import requests
import pandas as pd

# Get churn features
response = requests.get("http://localhost:8000/churn-features?limit=100")
features = response.json()

# Export to DataFrame
df = pd.DataFrame(features)

# Get statistics
stats = requests.get("http://localhost:8000/churn-features/stats").json()
print(f"Churn rate: {stats['churn_rate_percent']}%")
```

**JavaScript**:
```javascript
// Fetch churn features
const response = await fetch('http://localhost:8000/churn-features?limit=50');
const features = await response.json();

// Export data
const exportResponse = await fetch('http://localhost:8000/churn-features/export?format=csv');
const csvData = await exportResponse.text();
```

## Testing

### Pipeline Testing
```bash
# Test DAG import
astro dev bash -c "python -c 'from dags.churn_pipeline import churn_prediction_pipeline'"

# Test database connection
astro dev bash -c "airflow connections test postgres_default"

# Run dbt tests
astro dev bash -c "cd include/dbt && dbt test"
```

### API Testing
```bash
# Health check
curl http://localhost:8000/health

# Feature endpoint
curl "http://localhost:8000/churn-features?limit=5"

# Statistics
curl http://localhost:8000/churn-features/stats
```

### Data Quality Validation
```bash
# Check record counts
astro dev bash -c "psql -h postgres -U postgres -d postgres -c 'SELECT COUNT(*) FROM public_churn_marts.churn_features;'"

# Validate churn distribution
astro dev bash -c "psql -h postgres -U postgres -d postgres -c 'SELECT churn_flag, COUNT(*) FROM public_churn_marts.churn_features GROUP BY churn_flag;'"
```

## Deployment

### Local Development
- Use `astro dev start` for local development
- Airflow UI for pipeline monitoring
- PostgreSQL for data inspection
- FastAPI docs for API testing

### Production Deployment
- Deploy Airflow to Kubernetes or Astronomer Cloud
- Use managed PostgreSQL (AWS RDS, Google Cloud SQL)
- Deploy API to container orchestration platform
- Configure monitoring and alerting

### Environment Variables
```bash
# Database
POSTGRES_USER=postgres
POSTGRES_PASSWORD=<secure-password>
POSTGRES_DB=churn_prediction
POSTGRES_HOST=<host>
POSTGRES_PORT=5432

# API
DATABASE_URL=postgresql://user:pass@host:port/db
API_HOST=0.0.0.0
API_PORT=8000

# Airflow
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__WEBSERVER__EXPOSE_CONFIG=False
```

## Assumptions & Design Decisions

### Data Assumptions
- **User Activities**: CSV format with consistent schema
- **Transactions**: PostgreSQL dump with users and transactions tables
- **Data Quality**: Source data is reasonably clean with minimal corruption
- **Timestamps**: All timestamps are in UTC or consistent timezone
- **User Matching**: user_id consistently links across all data sources

### Business Logic Assumptions
- **Churn Definition**: 30+ days of inactivity indicates churn risk
- **Currency**: All transactions assumed to be in NGN for aggregation
- **Deduplication**: Based on natural keys (user_id, timestamp combinations)
- **Feature Engineering**: RFM methodology appropriate for business context

### Technical Decisions
- **PostgreSQL**: Chosen for ACID compliance and analytical capabilities
- **dbt**: Selected for version control, testing, and SQL-based transformations
- **Airflow**: Industry standard for workflow orchestration
- **FastAPI**: Modern Python API framework with automatic documentation
- **Docker**: Containerization for consistent deployment environments

### Scalability Considerations
- **Incremental Processing**: Pipeline designed for daily incremental updates
- **Resource Management**: Configurable parallelism in Airflow tasks
- **API Performance**: Pagination and filtering to handle large datasets
- **Database Optimization**: Indexes on frequently queried columns

### Data Quality Strategy
- **Input Validation**: Schema validation on data ingestion
- **Transformation Testing**: dbt tests for model validation
- **Output Verification**: Final feature validation before serving
- **Monitoring**: Health checks and data freshness alerts

## Troubleshooting

### Common Issues

**Pipeline Not Starting**
```bash
# Check Docker status
docker ps

# Restart services
astro dev restart

![Codebase](images/codebase.png)

# Check logs
astro dev logs
```

**Database Connection Errors**
```bash
# Verify connection
astro dev bash -c "airflow connections list | grep postgres"

# Test connection
astro dev bash -c "airflow connections test postgres_default"
```

**dbt Model Failures**
```bash
# Debug dbt connection
astro dev bash -c "cd include/dbt && dbt debug"

# Run specific model
astro dev bash -c "cd include/dbt && dbt run --select stg_users"
```

**API Not Accessible**
```bash
# Check container status
docker ps | grep churn-api

# Check API logs
docker logs <api-container-name>
```

### Performance Optimization

**Large Dataset Processing**
- Increase Airflow worker memory allocation
- Use database connection pooling
- Implement data partitioning strategies
- Configure appropriate batch sizes

**API Response Times**
- Add database indexes on filtered columns
- Implement API response caching
- Use connection pooling
- Consider read replicas for query workloads

## Future Enhancements

### Short Term
- **Real-time Processing**: Stream processing for live churn scoring
- **Advanced Features**: Time-series features and seasonality analysis
- **Model Integration**: Direct ML model serving endpoints
- **Enhanced Monitoring**: Comprehensive data quality dashboards

### Long Term
- **Multi-tenant Architecture**: Support for multiple business units
- **Advanced Analytics**: Cohort analysis and customer segmentation
- **Data Lineage**: Comprehensive data governance and lineage tracking
- **AutoML Integration**: Automated model training and deployment

### Infrastructure
- **Cloud Migration**: Deploy to AWS/GCP/Azure
- **Kubernetes**: Container orchestration for production
- **CI/CD Pipeline**: Automated testing and deployment
- **Security**: Authentication, authorization, and data encryption

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For questions and support:
- **Documentation**: Check this README and API docs
- **Issues**: Create GitHub issues for bugs and feature requests
- **Discussion**: Use GitHub Discussions for questions

---

**Built with**: Apache Airflow, dbt, FastAPI, PostgreSQL, Docker
**Designed for**: Scalable ML data pipeline workflows