"""
FastAPI application for Retail Analytics - Bidco Africa Analysis

Provides endpoints for:
1. Data Quality metrics
2. Promotion analysis
3. Pricing index and competitor comparison
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional, List, Dict, Any
import pandas as pd
import numpy as np
import math
from sqlalchemy import create_engine, text
import os
import logging
from datetime import datetime
from pydantic import BaseModel, Field

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql://postgres:postgres@postgres:5432/postgres'
)

# Initialize FastAPI app
app = FastAPI(
    title="Retail Analytics API - Bidco Africa",
    description="API for retail sales analytics focusing on Bidco Africa products",
    version="1.0.0"
)


# ============================================================================
# Pydantic Models
# ============================================================================

class DataQualityMetric(BaseModel):
    """Data quality metric model"""
    dimension: str
    dimension_value: Optional[str] = None
    total_records: int
    high_quality_records: int
    low_quality_records: int
    low_quality_pct: float
    avg_quality_score: float
    health_rating: str
    reliability_status: str
    invalid_quantity_count: int
    invalid_sales_count: int
    missing_rrp_count: int
    missing_barcode_count: int


class PromoPerformance(BaseModel):
    """Promotion performance model"""
    item_code: str
    product_description: str
    store_name: str
    supplier: str
    is_bidco: int
    promo_uplift_pct: Optional[float]
    promo_discount_depth_pct: Optional[float]
    promo_units_sold: Optional[int]
    total_sales_value: float


class PriceIndex(BaseModel):
    """Price index model"""
    store_name: str
    item_code: str
    product_description: str
    supplier: str
    is_bidco: int
    sub_department: str
    section: str
    avg_realized_price: float
    competitor_avg_price_in_section: Optional[float]
    price_index_vs_competitors: Optional[float]
    price_positioning: str
    price_tier: str


class PricingSummary(BaseModel):
    """Pricing summary model"""
    view_level: str
    store_name: Optional[str]
    sub_department: Optional[str]
    section: Optional[str]
    bidco_avg_price_index: Optional[float]
    dominant_positioning: str
    discount_pct: Optional[float]
    at_market_pct: Optional[float]
    premium_pct: Optional[float]


# ============================================================================
# Database Functions
# ============================================================================

def get_db_engine():
    """Create database engine"""
    try:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        return engine
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Database connection failed"
        )


def execute_query(query: str, params: dict = None) -> pd.DataFrame:
    """Execute SQL query and return DataFrame"""
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params)
        return df
    except Exception as e:
        logger.error(f"Query execution failed: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Query failed: {str(e)}"
        )


def clean_dataframe_for_json(df: pd.DataFrame) -> List[Dict]:
    """
    Convert DataFrame to JSON-serializable list of dicts.
    Replaces NaN, Inf, and -Inf with None.
    """
    # Make a copy to avoid modifying original
    df = df.copy()

    # Replace inf/-inf with NaN first
    df = df.replace([np.inf, -np.inf], np.nan)

    # Convert to dict and manually replace NaN with None
    records = df.to_dict('records')

    # Replace any remaining NaN values with None
    cleaned_records = []
    for record in records:
        cleaned_record = {}
        for key, value in record.items():
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                cleaned_record[key] = None
            elif pd.isna(value):
                cleaned_record[key] = None
            else:
                cleaned_record[key] = value
        cleaned_records.append(cleaned_record)

    return cleaned_records


# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Retail Analytics API - Bidco Africa",
        "version": "1.0.0",
        "endpoints": {
            "data_quality": "/api/data_quality",
            "promo_summary": "/api/promo_summary",
            "price_index": "/api/price_index",
            "health": "/health"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


@app.get("/api/data_quality")
async def get_data_quality(
    dimension: Optional[str] = Query(
        None,
        description="Filter by dimension: Overall Dataset, Store, Supplier, Category"
    ),
    min_quality_score: Optional[float] = Query(
        None,
        description="Filter by minimum quality score (0-100)"
    )
):
    """
    Get data quality metrics by dimension.

    Returns data health scores, reliability status, and quality issues
    broken down by stores, suppliers, and categories.
    """
    query = """
    SELECT
        dimension,
        dimension_value,
        total_records,
        high_quality_records,
        low_quality_records,
        low_quality_pct,
        avg_quality_score,
        health_rating,
        reliability_status,
        invalid_quantity_count,
        invalid_sales_count,
        missing_rrp_count,
        missing_barcode_count
    FROM retail_analytics.data_quality_summary
    WHERE 1=1
    """

    params = {}

    if dimension:
        query += " AND dimension = :dimension"
        params['dimension'] = dimension

    if min_quality_score is not None:
        query += " AND avg_quality_score >= :min_score"
        params['min_score'] = min_quality_score

    query += " ORDER BY low_quality_pct DESC"

    df = execute_query(query, params)

    if df.empty:
        return []

    return clean_dataframe_for_json(df)


@app.get("/api/data_quality/issues")
async def get_data_quality_issues(
    issue_type: Optional[str] = Query(
        None,
        description="Filter by issue type: Quantity Outlier, Price Outlier, Missing Critical Data"
    ),
    supplier: Optional[str] = Query(None, description="Filter by supplier"),
    bidco_only: bool = Query(False, description="Show only Bidco product issues"),
    limit: int = Query(100, description="Limit results")
):
    """
    Get detailed data quality issues including duplicates, outliers, and missing data.
    """
    query = """
    SELECT
        sales_record_id,
        store_name,
        supplier,
        item_code,
        product_description,
        quantity,
        total_sales,
        sale_date,
        issue_type,
        issue_severity,
        priority_rank,
        deviation_from_avg,
        affects_bidco
    FROM retail_analytics.data_quality_issues
    WHERE 1=1
    """

    params = {}

    if issue_type:
        query += " AND issue_type = :issue_type"
        params['issue_type'] = issue_type

    if supplier:
        query += " AND UPPER(supplier) LIKE :supplier"
        params['supplier'] = f'%{supplier.upper()}%'

    if bidco_only:
        query += " AND affects_bidco = 1"

    query += f" ORDER BY priority_rank, affects_bidco DESC LIMIT :limit"
    params['limit'] = limit

    df = execute_query(query, params)

    return clean_dataframe_for_json(df)


@app.get("/api/promo_summary")
async def get_promo_summary(
    bidco_only: bool = Query(False, description="Show only Bidco products"),
    min_uplift: Optional[float] = Query(None, description="Minimum promo uplift %"),
    store: Optional[str] = Query(None, description="Filter by store name"),
    top_n: Optional[int] = Query(None, description="Return top N by uplift")
):
    """
    Get promotion performance summary including:
    - Promo uplift percentage
    - Promo coverage (stores running promo)
    - Discount depth
    - Top performing SKUs
    """
    query = """
    SELECT
        item_code,
        product_description,
        store_name,
        supplier,
        is_bidco,
        promo_uplift_pct,
        promo_discount_depth_pct,
        promo_units_sold,
        total_sales_value
    FROM retail_marts.promo_detection
    WHERE is_on_promo = 1
    """

    params = {}

    if bidco_only:
        query += " AND is_bidco = 1"

    if min_uplift is not None:
        query += " AND promo_uplift_pct >= :min_uplift"
        params['min_uplift'] = min_uplift

    if store:
        query += " AND LOWER(store_name) LIKE :store"
        params['store'] = f'%{store.lower()}%'

    query += " ORDER BY promo_uplift_pct DESC NULLS LAST"

    if top_n:
        query += f" LIMIT :top_n"
        params['top_n'] = top_n

    df = execute_query(query, params)

    return clean_dataframe_for_json(df)


@app.get("/api/promo_summary/aggregated")
async def get_promo_aggregated():
    """
    Get aggregated promotion metrics:
    - Promo coverage by supplier
    - Store-level performance
    - Category-level analysis
    """
    query = """
    SELECT
        metric_type,
        supplier,
        is_bidco,
        store_name,
        sub_department,
        section,
        metric_value_1,
        metric_value_2,
        metric_value_3,
        metric_value_4
    FROM retail_analytics.promo_summary
    ORDER BY metric_type, is_bidco DESC NULLS LAST
    """

    try:
        df = execute_query(query)
    except Exception as e:
        # Return empty structure if table doesn't exist or has no data
        return {
            'promo_coverage': [],
            'store_performance': [],
            'category_analysis': [],
            'message': 'No data available. Please run the DAG first.'
        }

    # Return empty structure if no data
    if df.empty:
        return {
            'promo_coverage': [],
            'store_performance': [],
            'category_analysis': [],
            'message': 'No promotion data available yet.'
        }

    # Transform into more readable structure
    result = {
        'promo_coverage': [],
        'store_performance': [],
        'category_analysis': []
    }

    for _, row in df.iterrows():
        metric_type = row['metric_type']
        # Clean NaN/None values
        def clean_value(val):
            if pd.isna(val):
                return None
            if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                return None
            return val

        if metric_type == 'promo_coverage':
            result['promo_coverage'].append({
                'supplier': clean_value(row['supplier']),
                'is_bidco': clean_value(row['is_bidco']),
                'stores_with_promo': clean_value(row['metric_value_1']),
                'skus_on_promo': clean_value(row['metric_value_2']),
                'avg_uplift_pct': clean_value(row['metric_value_3']),
                'avg_discount_depth_pct': clean_value(row['metric_value_4'])
            })
        elif metric_type == 'store_performance':
            result['store_performance'].append({
                'store_name': clean_value(row['store_name']),
                'bidco_skus_on_promo': clean_value(row['metric_value_1']),
                'competitor_skus_on_promo': clean_value(row['metric_value_2']),
                'bidco_avg_uplift': clean_value(row['metric_value_3']),
                'competitor_avg_uplift': clean_value(row['metric_value_4'])
            })
        elif metric_type == 'category_analysis':
            result['category_analysis'].append({
                'sub_department': clean_value(row['sub_department']),
                'section': clean_value(row['section']),
                'bidco_skus': clean_value(row['metric_value_1']),
                'competitor_skus': clean_value(row['metric_value_2']),
                'bidco_avg_uplift': clean_value(row['metric_value_3']),
                'competitor_avg_uplift': clean_value(row['metric_value_4'])
            })

    return result


@app.get("/api/price_index")
async def get_price_index(
    bidco_only: bool = Query(False, description="Show only Bidco products"),
    store: Optional[str] = Query(None, description="Filter by store"),
    section: Optional[str] = Query(None, description="Filter by section"),
    positioning: Optional[str] = Query(
        None,
        description="Filter by positioning: discount, at_market, premium"
    )
):
    """
    Get price index showing Bidco's pricing vs competitors
    by store, sub-department, and section.
    """
    query = """
    SELECT
        store_name,
        item_code,
        product_description,
        supplier,
        is_bidco,
        sub_department,
        section,
        avg_realized_price,
        competitor_avg_price_in_section,
        price_index_vs_competitors,
        price_positioning,
        price_tier
    FROM retail_marts.price_index
    WHERE 1=1
    """

    params = {}

    if bidco_only:
        query += " AND is_bidco = 1"

    if store:
        query += " AND LOWER(store_name) LIKE :store"
        params['store'] = f'%{store.lower()}%'

    if section:
        query += " AND UPPER(section) LIKE :section"
        params['section'] = f'%{section.upper()}%'

    if positioning:
        query += " AND price_positioning = :positioning"
        params['positioning'] = positioning

    query += " ORDER BY is_bidco DESC, store_name, section"

    df = execute_query(query, params)

    return clean_dataframe_for_json(df)


@app.get("/api/price_index/summary")
async def get_pricing_summary(
    view_level: Optional[str] = Query(
        None,
        description="Filter by view: Overall, Store, Section, Store + Section"
    ),
    store: Optional[str] = Query(None, description="Filter by store")
):
    """
    Get pricing summary showing Bidco's overall positioning:
    - Overall positioning (premium/discount/at-market)
    - Store-level pricing comparison
    - Section-level pricing trends
    """
    query = """
    SELECT
        view_level,
        store_name,
        sub_department,
        section,
        bidco_avg_price_index,
        dominant_positioning,
        discount_pct,
        at_market_pct,
        premium_pct
    FROM retail_analytics.pricing_summary
    WHERE 1=1
    """

    params = {}

    if view_level:
        query += " AND view_level = :view_level"
        params['view_level'] = view_level

    if store:
        query += " AND (store_name IS NULL OR LOWER(store_name) LIKE :store)"
        params['store'] = f'%{store.lower()}%'

    query += """
    ORDER BY
        CASE view_level
            WHEN 'Overall' THEN 1
            WHEN 'Store' THEN 2
            WHEN 'Section' THEN 3
            WHEN 'Store + Section' THEN 4
        END
    """

    df = execute_query(query, params)

    return clean_dataframe_for_json(df)


@app.get("/api/insights")
async def get_key_insights():
    """
    Get key commercial insights for Bidco stakeholders.

    Returns top 3-5 actionable insights across data quality,
    promotions, and pricing.
    """
    insights = {
        "generated_at": datetime.utcnow().isoformat(),
        "insights": []
    }

    # Data Quality Insight
    dq_query = """
    SELECT dimension_value, health_rating, low_quality_pct
    FROM retail_analytics.data_quality_summary
    WHERE dimension = 'Supplier' AND UPPER(dimension_value) LIKE '%BIDCO%'
    """
    dq_df = execute_query(dq_query)

    if not dq_df.empty:
        bidco_quality = dq_df.iloc[0]
        insights['insights'].append({
            "category": "Data Quality",
            "insight": f"Bidco data quality is rated '{bidco_quality['health_rating']}' with {bidco_quality['low_quality_pct']:.1f}% low-quality records.",
            "action": "Review data quality issues to ensure accurate reporting."
        })

    # Top Promo Performers
    promo_query = """
    SELECT product_description, store_name, promo_uplift_pct, promo_discount_depth_pct
    FROM retail_marts.promo_detection
    WHERE is_bidco = 1 AND is_on_promo = 1 AND promo_uplift_pct IS NOT NULL
    ORDER BY promo_uplift_pct DESC
    LIMIT 3
    """
    promo_df = execute_query(promo_query)

    if not promo_df.empty:
        top_promo = promo_df.iloc[0]
        insights['insights'].append({
            "category": "Promotions",
            "insight": f"Top performing promo: '{top_promo['product_description']}' at {top_promo['store_name']} with {top_promo['promo_uplift_pct']:.1f}% uplift.",
            "action": f"Replicate successful {top_promo['promo_discount_depth_pct']:.1f}% discount strategy across more stores."
        })

    # Pricing Position
    price_query = """
    SELECT dominant_positioning, bidco_avg_price_index, discount_pct, premium_pct
    FROM retail_analytics.pricing_summary
    WHERE view_level = 'Overall'
    """
    price_df = execute_query(price_query)

    if not price_df.empty:
        position = price_df.iloc[0]
        insights['insights'].append({
            "category": "Pricing",
            "insight": f"Bidco has '{position['dominant_positioning']}' with price index of {position['bidco_avg_price_index']:.1f}.",
            "action": f"{position['premium_pct']:.1f}% of SKUs are premium-priced - review if this aligns with brand strategy."
        })

    return insights


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
