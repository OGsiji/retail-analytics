# include/api/main.py
"""
FastAPI application for serving churn prediction features
"""

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.responses import FileResponse, StreamingResponse
from typing import Optional, List, Dict, Any
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import os
import logging
from datetime import datetime
import io
import json
from pydantic import BaseModel
from contextlib import asynccontextmanager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DATABASE_URL = "postgresql://postgres:postgres@postgres:5432/postgres"

# Global variables for caching
cached_data = None
cache_timestamp = None
CACHE_TTL_MINUTES = 30


class ChurnFeatureResponse(BaseModel):
    """Response model for churn features"""

    user_id: int
    email: str
    signup_date: str
    region: str
    channel: str
    user_tenure_days: int
    total_transactions: int
    successful_transactions: int
    total_spend_ngn: float
    avg_transaction_amount: float
    last_transaction_date: Optional[str]
    days_since_last_transaction: int
    unique_sessions: int
    active_days: int
    page_views: int
    last_activity_date: Optional[str]
    days_since_last_activity: int
    avg_session_duration_minutes: float
    cart_conversion_rate: float
    purchase_conversion_rate: float
    recency_score: int
    frequency_score: int
    monetary_score: int
    rfm_total_score: int
    user_lifecycle_stage: str
    churn_flag: int
    feature_created_at: str


class StatsResponse(BaseModel):
    """Response model for dataset statistics"""

    total_users: int
    churned_users: int
    active_users: int
    churn_rate_percent: float
    avg_total_spend: float
    avg_session_count: float
    avg_tenure_days: float
    top_regions: List[Dict[str, Any]]
    top_channels: List[Dict[str, Any]]
    lifecycle_distribution: Dict[str, int]


def get_db_connection():
    """Create database connection"""
    try:
        engine = create_engine(DATABASE_URL)
        return engine
    except Exception as e:
        logger.error(f"Failed to connect to database: {str(e)}")
        raise HTTPException(status_code=500, detail="Database connection failed")


def load_churn_data():
    """Load churn features from database"""
    global cached_data, cache_timestamp

    try:
        engine = get_db_connection()

        query = """
        SELECT 
            user_id,
            email,
            signup_date,
            region,
            channel,
            user_tenure_days,
            total_transactions,
            successful_transactions,
            total_spend_ngn,
            avg_transaction_amount,
            last_transaction_date,
            days_since_last_transaction,
            unique_sessions,
            active_days,
            page_views,
            last_activity_date,
            days_since_last_activity,
            avg_session_duration_minutes,
            cart_conversion_rate,
            purchase_conversion_rate,
            recency_score,
            frequency_score,
            monetary_score,
            rfm_total_score,
            user_lifecycle_stage,
            churn_flag,
            feature_created_at
        FROM "public_churn_marts".churn_features
        ORDER BY user_id
        """

        df = pd.read_sql(query, engine)

        # Convert dates to strings for JSON serialization
        date_columns = [
            "signup_date",
            "last_transaction_date",
            "last_activity_date",
            "feature_created_at",
        ]
        for col in date_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)

        # Fill NaN values
        df = df.fillna(
            {
                "last_transaction_date": pd.NaT,
                "last_activity_date": pd.NaT,
                "avg_transaction_amount": 0,
                "avg_session_duration_minutes": 0,
                "cart_conversion_rate": 0,
                "purchase_conversion_rate": 0,
            }
        )

        cached_data = df
        cache_timestamp = datetime.now()

        logger.info(f"Loaded {len(df)} records from churn_features table")
        return df

    except Exception as e:
        logger.error(f"Failed to load churn data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to load data: {str(e)}")


def get_cached_data():
    """Get cached data or refresh if stale"""
    global cached_data, cache_timestamp

    if cached_data is None or cache_timestamp is None:
        return load_churn_data()

    # Check if cache is stale
    minutes_since_cache = (datetime.now() - cache_timestamp).total_seconds() / 60
    if minutes_since_cache > CACHE_TTL_MINUTES:
        logger.info("Cache is stale, refreshing...")
        return load_churn_data()

    return cached_data


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    # Startup
    logger.info("Starting Churn Prediction API...")
    try:
        load_churn_data()
        logger.info("Initial data load completed")
    except Exception as e:
        logger.warning(f"Initial data load failed: {str(e)}")

    yield

    # Shutdown
    logger.info("Shutting down Churn Prediction API...")


# Initialize FastAPI app
app = FastAPI(
    title="Churn Prediction API",
    description="REST API for accessing customer churn prediction features and analytics",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/", tags=["Root"])
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Churn Prediction API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
    }


@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint"""
    try:
        engine = get_db_connection()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            db_status = "healthy" if result.fetchone() else "unhealthy"
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        db_status = "unhealthy"

    cache_status = "cached" if cached_data is not None else "no_cache"
    cache_age_minutes = 0
    if cache_timestamp:
        cache_age_minutes = (datetime.now() - cache_timestamp).total_seconds() / 60

    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "database": db_status,
        "cache": cache_status,
        "cache_age_minutes": round(cache_age_minutes, 1),
        "cached_records": len(cached_data) if cached_data is not None else 0,
    }


@app.get(
    "/churn-features", response_model=List[ChurnFeatureResponse], tags=["Features"]
)
async def get_churn_features(
    limit: int = Query(100, ge=1, le=10000, description="Number of records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    churn_flag: Optional[int] = Query(
        None, description="Filter by churn flag (0 or 1)"
    ),
    region: Optional[str] = Query(None, description="Filter by region"),
    channel: Optional[str] = Query(None, description="Filter by channel"),
    lifecycle_stage: Optional[str] = Query(
        None, description="Filter by user lifecycle stage"
    ),
    min_spend: Optional[float] = Query(None, ge=0, description="Minimum total spend"),
    max_days_inactive: Optional[int] = Query(
        None, ge=0, description="Maximum days since last activity"
    ),
):
    """Get churn prediction features with optional filtering"""

    try:
        df = get_cached_data()

        # Apply filters
        if churn_flag is not None:
            df = df[df["churn_flag"] == churn_flag]

        if region:
            df = df[df["region"].str.contains(region, case=False, na=False)]

        if channel:
            df = df[df["channel"].str.contains(channel, case=False, na=False)]

        if lifecycle_stage:
            df = df[df["user_lifecycle_stage"] == lifecycle_stage]

        if min_spend is not None:
            df = df[df["total_spend_ngn"] >= min_spend]

        if max_days_inactive is not None:
            df = df[df["days_since_last_activity"] <= max_days_inactive]

        # Apply pagination
        total_records = len(df)
        df_paginated = df.iloc[offset : offset + limit]

        # Convert to dict for response
        records = df_paginated.to_dict("records")

        logger.info(f"Returning {len(records)} of {total_records} records")
        return records

    except Exception as e:
        logger.error(f"Failed to get churn features: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get features: {str(e)}")


@app.get("/churn-features/stats", response_model=StatsResponse, tags=["Analytics"])
async def get_dataset_stats():
    """Get comprehensive statistics about the churn dataset"""

    try:
        df = get_cached_data()

        if df.empty:
            raise HTTPException(status_code=404, detail="No data available")

        total_users = len(df)
        churned_users = int(df["churn_flag"].sum())
        active_users = total_users - churned_users
        churn_rate = (churned_users / total_users * 100) if total_users > 0 else 0

        # Regional distribution
        region_stats = df["region"].value_counts().head(5)
        top_regions = [{"region": k, "count": int(v)} for k, v in region_stats.items()]

        # Channel distribution
        channel_stats = df["channel"].value_counts().head(5)
        top_channels = [
            {"channel": k, "count": int(v)} for k, v in channel_stats.items()
        ]

        # Lifecycle distribution
        lifecycle_stats = df["user_lifecycle_stage"].value_counts()
        lifecycle_distribution = {k: int(v) for k, v in lifecycle_stats.items()}

        return StatsResponse(
            total_users=total_users,
            churned_users=churned_users,
            active_users=active_users,
            churn_rate_percent=round(churn_rate, 2),
            avg_total_spend=float(df["total_spend_ngn"].mean()),
            avg_session_count=float(df["unique_sessions"].mean()),
            avg_tenure_days=float(df["user_tenure_days"].mean()),
            top_regions=top_regions,
            top_channels=top_channels,
            lifecycle_distribution=lifecycle_distribution,
        )

    except Exception as e:
        logger.error(f"Failed to get stats: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to get statistics: {str(e)}"
        )


@app.get("/churn-features/export", tags=["Export"])
async def export_churn_features(
    format: str = Query("csv", description="Export format: csv or json"),
    churn_flag: Optional[int] = Query(
        None, description="Filter by churn flag (0 or 1)"
    ),
):
    """Export churn features dataset in CSV or JSON format"""

    try:
        df = get_cached_data()

        # Apply churn filter if specified
        if churn_flag is not None:
            df = df[df["churn_flag"] == churn_flag]
            suffix = f"_churn_{churn_flag}"
        else:
            suffix = ""

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if format.lower() == "csv":
            # Create CSV
            output = io.StringIO()
            df.to_csv(output, index=False)
            output.seek(0)

            return StreamingResponse(
                io.StringIO(output.getvalue()),
                media_type="text/csv",
                headers={
                    "Content-Disposition": f"attachment; filename=churn_features{suffix}_{timestamp}.csv"
                },
            )

        elif format.lower() == "json":
            # Create JSON
            records = df.to_dict("records")
            json_str = json.dumps(records, indent=2, default=str)

            return StreamingResponse(
                io.StringIO(json_str),
                media_type="application/json",
                headers={
                    "Content-Disposition": f"attachment; filename=churn_features{suffix}_{timestamp}.json"
                },
            )

        else:
            raise HTTPException(
                status_code=400, detail="Format must be 'csv' or 'json'"
            )

    except Exception as e:
        logger.error(f"Failed to export data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


@app.post("/refresh-cache", tags=["Cache"])
async def refresh_cache(background_tasks: BackgroundTasks):
    """Manually refresh the data cache"""

    def refresh_background():
        try:
            load_churn_data()
            logger.info("Cache refresh completed in background")
        except Exception as e:
            logger.error(f"Background cache refresh failed: {str(e)}")

    background_tasks.add_task(refresh_background)

    return {
        "message": "Cache refresh initiated",
        "timestamp": datetime.now().isoformat(),
    }


@app.get(
    "/churn-features/user/{user_id}",
    response_model=ChurnFeatureResponse,
    tags=["Features"],
)
async def get_user_features(user_id: int):
    """Get churn features for a specific user"""

    try:
        df = get_cached_data()
        user_data = df[df["user_id"] == user_id]

        if user_data.empty:
            raise HTTPException(status_code=404, detail=f"User {user_id} not found")

        return user_data.iloc[0].to_dict()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get user features: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to get user features: {str(e)}"
        )


@app.get("/churn-features/segments", tags=["Analytics"])
async def get_user_segments():
    """Get user segmentation analysis"""

    try:
        df = get_cached_data()

        # RFM segments
        rfm_segments = (
            df.groupby(["recency_score", "frequency_score", "monetary_score"])
            .agg({"user_id": "count", "churn_flag": "mean", "total_spend_ngn": "mean"})
            .reset_index()
        )

        rfm_segments.columns = [
            "recency",
            "frequency",
            "monetary",
            "user_count",
            "churn_rate",
            "avg_spend",
        ]
        rfm_segments["churn_rate"] = rfm_segments["churn_rate"].round(3)
        rfm_segments["avg_spend"] = rfm_segments["avg_spend"].round(2)

        # Lifecycle segments
        lifecycle_segments = (
            df.groupby("user_lifecycle_stage")
            .agg(
                {
                    "user_id": "count",
                    "churn_flag": "mean",
                    "total_spend_ngn": "mean",
                    "unique_sessions": "mean",
                }
            )
            .reset_index()
        )

        lifecycle_segments.columns = [
            "lifecycle_stage",
            "user_count",
            "churn_rate",
            "avg_spend",
            "avg_sessions",
        ]
        lifecycle_segments["churn_rate"] = lifecycle_segments["churn_rate"].round(3)
        lifecycle_segments["avg_spend"] = lifecycle_segments["avg_spend"].round(2)
        lifecycle_segments["avg_sessions"] = lifecycle_segments["avg_sessions"].round(1)

        return {
            "rfm_segments": rfm_segments.to_dict("records"),
            "lifecycle_segments": lifecycle_segments.to_dict("records"),
            "total_users": len(df),
            "analysis_timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error(f"Failed to get segments: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get segments: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
