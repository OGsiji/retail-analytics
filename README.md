# Retail Analytics Pipeline - Bidco Africa

A production-ready data pipeline for retail analytics, implementing ETL/ELT orchestration with Apache Airflow, dbt transformations, FastAPI service, and Metabase visualization.

**Purpose**: Analyze supermarket POS data to provide actionable insights for Bidco Africa's commercial and marketing teams.

![metabase](images/metabase.png)

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Challenge Questions Answered](#challenge-questions-answered)
- [Data Model](#data-model)
- [API Documentation](#api-documentation)
- [Metabase Dashboards](#metabase-dashboards)
- [Troubleshooting](#troubleshooting)

---

## Overview

This retail analytics platform provides comprehensive insights into:

1. **Data Quality Assessment** - Health scores, reliability metrics, and anomaly detection
2. **Promotional Effectiveness** - Uplift calculation, coverage analysis, and ROI metrics
3. **Competitive Pricing Intelligence** - Price positioning, benchmarking, and market analysis

### Key Features

✅ Automated data quality scoring (0-100 scale)
✅ Promotional period detection with uplift calculation
✅ Competitive price indexing by product category
✅ Store and supplier benchmarking
✅ REST API with 10+ endpoints
✅ Metabase-ready dashboards

![airflow](images/airflow1.png)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      DATA SOURCE                                 │
│           retail_sales.csv (Supermarket POS Data)                │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                   APACHE AIRFLOW                                 │
│        retail_analytics_pipeline DAG                             │
│        • CSV Load → PostgreSQL                                   │
│        • Data Validation                                         │
│        • dbt Transformation                                      │
│        • Insight Generation                                      │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                   POSTGRESQL (Data Warehouse)                    │
│  Schemas:                                                        │
│  • public (raw: retail_sales)                                   │
│  • retail_staging (cleaned data)                                │
│  • retail_marts (business logic: promos, pricing)               │
│  • retail_analytics (aggregated KPIs)                           │
└────────────────┬────────────────────────────────────────────────┘
                 │
         ┌───────┴───────┐
         ▼               ▼
┌──────────────┐  ┌──────────────┐
│  dbt Models  │  │   FastAPI    │
│              │  │              │
│  7 Models:   │  │  Port 8001   │
│  • Staging   │  │              │
│  • Marts     │  │  10 Endpoints│
│  • Analytics │  │              │
└──────┬───────┘  └──────┬───────┘
       │                 │
       └────────┬────────┘
                ▼
       ┌─────────────────┐
       │    METABASE     │
       │  Visualization  │
       │      :3000      │
       └─────────────────┘
```

---

## Quick Start

### Prerequisites
- Docker Desktop (running)
- Astro CLI: `brew install astro` (Mac) or [install instructions](https://docs.astronomer.io/astro/cli/install-cli)

### 1. Prepare Your Data

Place your retail sales CSV at:
```
include/datasets/retail_sales.csv
```

**Required columns**:
```
Store_Name, Item_Code, Item_Barcode, Description, Category, Department,
Sub_Department, Section, Quantity, Total_Sales, RRP, Supplier, Date_Of_Sale
```

**Sample format**:
```csv
Store_Name,Item_Code,Item_Barcode,Description,Category,Department,Sub_Department,Section,Quantity,Total_Sales,RRP,Supplier,Date_Of_Sale
RUAKA,440113,0338232137077,BID ELIANTO CORN OIL 1L,FOODS,COOKING OILS AND FATS,COOKING OIL,CORN OIL,4,2065.52,528.19,BIDCO AFRICA LIMITED,2025-09-26
```

### 2. Start Services

```bash
cd retail-analytics-pipeline
astro dev start
```

Services will be available at:
- **Airflow**: http://localhost:8080 (admin/admin)
- **API**: http://localhost:8001
- **Metabase**: http://localhost:3000
- **PostgreSQL**: localhost:5435

### 3. Run Pipeline

**Via Airflow UI:**
1. Open http://localhost:8080
2. Enable `retail_analytics_pipeline` DAG
3. Click ▶️ to trigger

**Via CLI:**
```bash
astro dev bash
airflow dags trigger retail_analytics_pipeline
exit
```

Pipeline takes ~5-10 minutes depending on data size.

### 4. Access Results

**API (Recommended for Quick Check):**
```bash
# Health check
curl http://localhost:8001/health

# Overall data quality
curl http://localhost:8001/api/data_quality | jq

# Bidco quality specifically
curl "http://localhost:8001/api/data_quality?dimension=Supplier" | \
  jq '.[] | select(.dimension_value | contains("BIDCO"))'

# Top 5 Bidco promo performers
curl "http://localhost:8001/api/promo_summary?bidco_only=true&top_n=5" | jq

# Bidco price positioning
curl "http://localhost:8001/api/price_index/summary?view_level=Overall" | jq

# Executive insights
curl http://localhost:8001/api/insights | jq
```

**Full API docs**: http://localhost:8001/docs

**Metabase Setup**:
1. Navigate to http://localhost:3000
2. Complete setup wizard
3. Add PostgreSQL database:
   - Host: `postgres`, Port: `5432`
   - Database: `postgres`, User: `postgres`, Password: `postgres`
4. Browse schemas: `retail_staging`, `retail_marts`, `retail_analytics`

---

## Challenge Questions Answered

### Context

Bidco Africa (major FMCG manufacturer in East Africa) needs retail intelligence across three key areas:

### 1. Data Health ✅

**Question**: Which stores/suppliers provide reliable data?

**Solution**:
- Data quality scoring (0-100) by store, supplier, category
- Health ratings: Excellent/Good/Fair/Poor/Critical
- Automated outlier detection (negative quantities, extreme prices)
- Duplicate record identification

**Endpoints**:
```http
GET /api/data_quality?dimension=Store|Supplier|Category
GET /api/data_quality/issues?bidco_only=true
```

**Example Output**:
```json
{
  "dimension": "Supplier",
  "dimension_value": "BIDCO AFRICA LIMITED",
  "health_rating": "Good",
  "avg_quality_score": 87.5,
  "reliability_status": "Reliable",
  "low_quality_pct": 3.2
}
```

### 2. Promotions & Performance ✅

**Question**: Which Bidco SKUs drive the most uplift? What discount depth works?

**Solution**:
- Automated promo detection (≥10% discount, ≥2 days)
- Uplift calculation vs baseline
- Coverage analysis (% stores running promo)
- Discount depth correlation

**Metrics Provided**:
- ✅ Promo Uplift % = `((Promo Units/Day - Baseline Units/Day) / Baseline) × 100`
- ✅ Promo Coverage % = `(Stores Running Promo / Total Stores) × 100`
- ✅ Discount Depth %
- ✅ Baseline vs Promo Pricing

**Endpoints**:
```http
GET /api/promo_summary?bidco_only=true&top_n=10
GET /api/promo_summary/aggregated
```

**Example Output**:
```json
{
  "item_code": "440113",
  "product_description": "BID ELIANTO CORN OIL 1L",
  "store_name": "ruaka",
  "promo_uplift_pct": 47.3,
  "promo_discount_depth_pct": 15.2,
  "promo_units_sold": 156,
  "baseline_daily_units": 8.2,
  "promo_daily_units": 12.1
}
```

**Key Insights Enabled**:
- Top performing SKUs by uplift
- Optimal discount depth (15-20% sweet spot)
- Store coverage gaps
- Bidco vs competitor promo effectiveness

### 3. Pricing Index ✅

**Question**: Where are Bidco prices too high/low vs competitors?

**Solution**:
- Price Index = `(Bidco Price / Competitor Avg Price in Section) × 100`
- Multi-level analysis (overall, store, section)
- Positioning classification (discount/at-market/premium)
- RRP vs realized price patterns

**Price Positioning**:
- **<80**: Significant Discount
- **80-90**: Moderate Discount
- **90-110**: At-Market (Competitive)
- **110-120**: Moderate Premium
- **>120**: Significant Premium

**Endpoints**:
```http
GET /api/price_index?bidco_only=true&store=ruaka
GET /api/price_index/summary?view_level=Overall|Store|Section
```

**Example Output**:
```json
{
  "view_level": "Overall",
  "bidco_avg_price_index": 112.5,
  "dominant_positioning": "Premium Positioning",
  "premium_pct": 62.3,
  "at_market_pct": 28.1,
  "discount_pct": 9.6
}
```

**Key Insights Enabled**:
- Price gaps vs competitors by section
- Positioning consistency across stores
- Sections with strongest competitive pricing
- RRP discount patterns (Bidco vs competitors)

---

## Data Model

### Source Data

**Table**: `public.retail_sales`

| Column | Type | Description |
|--------|------|-------------|
| Store_Name | VARCHAR | Retail outlet name |
| Item_Code | VARCHAR | Product identifier |
| Item_Barcode | VARCHAR | Standard barcode |
| Description | VARCHAR | Product description |
| Category | VARCHAR | Broad grouping (e.g., FOODS) |
| Department | VARCHAR | Mid-level grouping |
| Sub_Department | VARCHAR | Fine classification |
| Section | VARCHAR | Micro-segmentation |
| Quantity | INTEGER | Units sold |
| Total_Sales | DECIMAL | Sales value |
| RRP | DECIMAL | Recommended retail price |
| Supplier | VARCHAR | Supplier/distributor |
| Date_Of_Sale | DATE | Transaction date |

### Transformation Layers

#### 1. Staging (`retail_staging.stg_retail_sales`)

**Purpose**: Clean, standardize, and score data quality

**Key Transformations**:
- Calculate realized unit price: `Total_Sales / Quantity`
- Standardize store and supplier names
- Detect promotional pricing (≥10% discount from RRP)
- Calculate record quality scores (0-100)
- Flag data issues (negative quantities, missing fields, outliers)

**Output**: 30+ columns including quality flags and calculated metrics

#### 2. Marts (`retail_marts.*`)

**`promo_detection`** - Promotional analysis
- Identifies promo periods (≥2 days at discounted prices)
- Calculates baseline (non-promo) pricing
- Computes uplift % vs baseline
- Measures discount depth
- Tracks coverage by store

**`price_index`** - Competitive pricing
- Compares Bidco vs competitors within same Section
- Calculates price indices (100 = market average)
- Categorizes positioning (discount/at-market/premium)
- Shows realized vs RRP patterns

#### 3. Analytics (`retail_analytics.*`)

**`data_quality_summary`** - Health metrics
- Overall dataset health
- Quality scores by store, supplier, category
- Reliability classifications
- Missing data summaries

**`data_quality_issues`** - Issue tracking
- Duplicate records
- Quantity/price outliers
- Missing critical fields
- Prioritized by severity

**`promo_summary`** - Aggregated promo metrics
- Coverage by supplier
- Store-level performance
- Category-level analysis

**`pricing_summary`** - Multi-level positioning
- Overall Bidco positioning
- Store-level comparisons
- Section-level trends

### dbt Models

```
models/retail/
├── staging/
│   └── stg_retail_sales.sql          (1 model)
├── marts/
│   ├── promo_detection.sql           (2 models)
│   └── price_index.sql
└── analytics/
    ├── data_quality_summary.sql      (4 models)
    ├── data_quality_issues.sql
    ├── promo_summary.sql
    └── pricing_summary.sql
```

**Total: 7 dbt models**

### Configuration Variables

Edit `include/dbt/dbt_project.yml`:

```yaml
vars:
  promo_discount_threshold: 0.10  # 10% discount to flag promo
  promo_min_days: 2               # Minimum days for promo
  bidco_supplier_pattern: 'BIDCO%'  # Bidco identification
```

---

## API Documentation

### Base URL
`http://localhost:8001`

### Interactive Docs
http://localhost:8001/docs (Swagger UI with try-it-out functionality)

### Endpoints

#### Data Quality

**`GET /api/data_quality`**
- Query params: `dimension` (Store/Supplier/Category), `min_quality_score`
- Returns health scores and reliability status

**`GET /api/data_quality/issues`**
- Query params: `issue_type`, `supplier`, `bidco_only`, `limit`
- Returns detailed issue list with severity ratings

#### Promotions

**`GET /api/promo_summary`**
- Query params: `bidco_only`, `min_uplift`, `store`, `top_n`
- Returns SKU-level promo performance

**`GET /api/promo_summary/aggregated`**
- Returns rolled-up metrics (coverage, store performance, category analysis)

#### Pricing

**`GET /api/price_index`**
- Query params: `bidco_only`, `store`, `section`, `positioning`
- Returns competitive price comparison

**`GET /api/price_index/summary`**
- Query params: `view_level` (Overall/Store/Section), `store`
- Returns multi-level positioning analysis

#### Insights

**`GET /api/insights`**
- Returns top 3-5 actionable insights for stakeholders
- Combines data quality, promo, and pricing intelligence

**`GET /health`**
- Health check endpoint

### Example Queries

**Get problematic stores:**
```bash
curl "http://localhost:8001/api/data_quality?dimension=Store&min_quality_score=0" | \
  jq '[.[] | {store: .dimension_value, score: .avg_quality_score}] | sort_by(.score) | .[0:5]'
```

**Find best promo ROI:**
```bash
curl "http://localhost:8001/api/promo_summary?bidco_only=true" | \
  jq 'sort_by(-.promo_uplift_pct) | .[0:3] | .[] | {sku: .product_description, uplift: .promo_uplift_pct, discount: .promo_discount_depth_pct}'
```

**Check price competitiveness:**
```bash
curl "http://localhost:8001/api/price_index?bidco_only=true&positioning=premium" | \
  jq '[.[] | {sku: .product_description, store: .store_name, index: .price_index_vs_competitors}]'
```

---

## Metabase Dashboards

### Setup

1. Navigate to http://localhost:3000
2. Complete setup wizard (any email/password)
3. Add PostgreSQL database:
   - Database type: **PostgreSQL**
   - Name: **Retail Analytics**
   - Host: **postgres**, Port: **5432**
   - Database: **postgres**
   - Username: **postgres**, Password: **postgres**

### Available Schemas

- `retail_staging.stg_retail_sales` - Cleaned sales data
- `retail_marts.promo_detection` - Promo analysis
- `retail_marts.price_index` - Pricing data
- `retail_analytics.*` - Aggregated metrics

### Recommended Dashboard Queries

#### Dashboard 1: Data Quality Scorecard

**Overall Quality (Single Value)**:
```sql
SELECT
  health_rating,
  avg_quality_score,
  reliability_status
FROM retail_analytics.data_quality_summary
WHERE dimension = 'Overall Dataset';
```

**Store Quality (Bar Chart)**:
```sql
SELECT
  dimension_value as store,
  avg_quality_score as score,
  health_rating
FROM retail_analytics.data_quality_summary
WHERE dimension = 'Store'
ORDER BY avg_quality_score DESC;
```

**Bidco Quality Status (Scorecard)**:
```sql
SELECT
  avg_quality_score,
  low_quality_pct,
  reliability_status
FROM retail_analytics.data_quality_summary
WHERE dimension = 'Supplier'
  AND UPPER(dimension_value) LIKE '%BIDCO%';
```

**Issues by Type (Donut Chart)**:
```sql
SELECT
  issue_type,
  COUNT(*) as count
FROM retail_analytics.data_quality_issues
WHERE affects_bidco = 1
GROUP BY issue_type;
```

#### Dashboard 2: Promotion Performance

**Bidco Promo Metrics (Scorecards)**:
```sql
SELECT
  COUNT(DISTINCT item_code) as skus_on_promo,
  COUNT(DISTINCT store_name) as stores_with_promo,
  ROUND(AVG(promo_uplift_pct), 2) as avg_uplift,
  ROUND(AVG(promo_discount_depth_pct), 2) as avg_discount
FROM retail_marts.promo_detection
WHERE is_bidco = 1 AND is_on_promo = 1;
```

**Top 10 Performers (Table)**:
```sql
SELECT
  product_description,
  store_name,
  promo_uplift_pct,
  promo_discount_depth_pct,
  promo_units_sold
FROM retail_marts.promo_detection
WHERE is_bidco = 1 AND is_on_promo = 1
ORDER BY promo_uplift_pct DESC
LIMIT 10;
```

**Uplift vs Discount (Scatter Plot)**:
```sql
SELECT
  product_description,
  promo_discount_depth_pct as discount,
  promo_uplift_pct as uplift,
  CASE WHEN is_bidco = 1 THEN 'Bidco' ELSE 'Competitor' END as type
FROM retail_marts.promo_detection
WHERE is_on_promo = 1
  AND promo_uplift_pct IS NOT NULL;
```

#### Dashboard 3: Pricing Intelligence

**Overall Positioning (Gauge)**:
```sql
SELECT
  bidco_avg_price_index,
  dominant_positioning,
  premium_pct,
  at_market_pct,
  discount_pct
FROM retail_analytics.pricing_summary
WHERE view_level = 'Overall';
```

**Price Index by Store (Bar Chart)**:
```sql
SELECT
  store_name,
  bidco_avg_price_index,
  dominant_positioning
FROM retail_analytics.pricing_summary
WHERE view_level = 'Store'
ORDER BY bidco_avg_price_index DESC;
```

**Positioning Distribution (Stacked Bar)**:
```sql
SELECT
  store_name,
  premium_pct,
  at_market_pct,
  discount_pct
FROM retail_analytics.pricing_summary
WHERE view_level = 'Store'
ORDER BY premium_pct DESC;
```

**Section Comparison (Table)**:
```sql
SELECT
  section,
  ROUND(AVG(CASE WHEN is_bidco = 1 THEN avg_realized_price END), 2) as bidco_price,
  ROUND(AVG(CASE WHEN is_bidco = 0 THEN avg_realized_price END), 2) as competitor_price,
  ROUND(AVG(CASE WHEN is_bidco = 1 THEN price_index_vs_competitors END), 2) as price_index
FROM retail_marts.price_index
GROUP BY section
ORDER BY price_index DESC;
```

---

## Troubleshooting

### Issue: "CSV file not found"

```bash
# Check file location
ls -la include/datasets/retail_sales.csv

# If missing, place your file there
cp /path/to/your/Test_Data.csv include/datasets/retail_sales.csv
```

### Issue: dbt models fail

```bash
# Run dbt manually to see errors
astro dev bash
cd include/dbt
dbt debug
dbt run --select path:models/retail
exit
```

### Issue: API returns empty results

```bash
# Check tables exist
astro dev bash
psql postgresql://postgres:postgres@postgres:5432/postgres

\dt retail_staging.*
\dt retail_marts.*
\dt retail_analytics.*

SELECT COUNT(*) FROM retail_staging.stg_retail_sales;
SELECT COUNT(*) FROM retail_marts.promo_detection;

\q
exit
```

### Issue: Services won't start

```bash
# Check Docker
docker ps

# Clean restart
astro dev stop
docker-compose down -v
astro dev start
```

### View Logs

```bash
# Airflow UI: http://localhost:8080 → DAG → Task → Logs

# Container logs
docker-compose logs -f retail-api
docker-compose logs -f postgres

# dbt logs
cat include/dbt/logs/dbt.log
```

---

## Project Structure

```
retail-analytics-pipeline/
├── dags/
│   └── retail_analytics_pipeline.py        # Airflow DAG
│
├── include/
│   ├── dbt/
│   │   ├── models/
│   │   │   └── retail/
│   │   │       ├── staging/
│   │   │       │   └── stg_retail_sales.sql
│   │   │       ├── marts/
│   │   │       │   ├── promo_detection.sql
│   │   │       │   └── price_index.sql
│   │   │       ├── analytics/
│   │   │       │   ├── data_quality_summary.sql
│   │   │       │   ├── data_quality_issues.sql
│   │   │       │   ├── promo_summary.sql
│   │   │       │   └── pricing_summary.sql
│   │   │       └── schema.yml
│   │   ├── macros/
│   │   │   └── retail_utils.sql            # Reusable macros
│   │   ├── dbt_project.yml
│   │   └── profiles.yml
│   │
│   ├── api/
│   │   ├── retail_api.py                   # FastAPI (port 8001)
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   └── datasets/
│       └── retail_sales.csv                # ** YOUR DATA HERE **
│
├── docker-compose.override.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## Performance Optimization

### For Large Datasets (>1M rows)

**1. Add Database Indexes:**
```sql
CREATE INDEX idx_retail_sales_store ON public.retail_sales(Store_Name);
CREATE INDEX idx_retail_sales_supplier ON public.retail_sales(Supplier);
CREATE INDEX idx_retail_sales_date ON public.retail_sales(Date_Of_Sale);
CREATE INDEX idx_retail_sales_item ON public.retail_sales(Item_Code);
```

**2. Materialize Staging as Table:**

Edit `include/dbt/models/retail/staging/stg_retail_sales.sql`:
```sql
{{
  config(
    materialized='table',  -- Change from 'view'
    indexes=[
      {'columns': ['store_name'], 'type': 'btree'},
      {'columns': ['supplier'], 'type': 'btree'},
    ]
  )
}}
```

**3. Use Incremental Models:**

For daily loads:
```sql
{{
  config(
    materialized='incremental',
    unique_key='sales_record_id'
  )
}}

SELECT * FROM {{ source('retail_raw', 'retail_sales') }}
{% if is_incremental() %}
  WHERE Date_Of_Sale > (SELECT MAX(sale_date) FROM {{ this }})
{% endif %}
```

---

## Future Enhancements

- [ ] Time-series forecasting for demand
- [ ] Store clustering by performance
- [ ] SKU recommendation engine
- [ ] Real-time price monitoring
- [ ] Automated competitive gap alerts
- [ ] Seasonality and trend analysis
- [ ] Price elasticity modeling
- [ ] Market basket analysis
- [ ] CI/CD pipeline (GitHub Actions)
- [ ] Data quality alerting (Great Expectations)
- [ ] API authentication (OAuth2)
- [ ] Redis caching for API responses

---

## Support

- **Airflow Logs**: http://localhost:8080 → DAG → Task → Logs
- **dbt Logs**: `include/dbt/logs/dbt.log`
- **Container Logs**: `docker-compose logs -f <service>`
- **API Docs**: http://localhost:8001/docs

---

**Built with**: Apache Airflow • dbt • FastAPI • PostgreSQL • Metabase • Docker

**Architecture**: ELT (Extract, Load, Transform) with orchestration and API serving

**Client**: Bidco Africa - Retail Performance Analytics
