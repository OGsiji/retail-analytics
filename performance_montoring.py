"""
Performance monitoring utilities for the churn prediction pipeline
"""

import psycopg2
import pandas as pd
from datetime import datetime
import logging


class PipelineMonitor:
    """Monitor pipeline performance and data quality"""

    def __init__(self, db_url="postgresql://postgres:postgres@localhost:5432/postgres"):
        self.db_url = db_url
        self.logger = logging.getLogger(__name__)

    def get_connection(self):
        """Get database connection"""
        return psycopg2.connect(self.db_url)

    def check_data_freshness(self):
        """Check how fresh the data is"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Check last feature update
            cursor.execute(
                """
                SELECT MAX(feature_created_at) as last_update
                FROM churn_marts.churn_features
            """
            )
            last_update = cursor.fetchone()[0]

            if last_update:
                hours_old = (datetime.now() - last_update).total_seconds() / 3600
                return {
                    "last_update": last_update.isoformat(),
                    "hours_old": round(hours_old, 1),
                    "is_fresh": hours_old < 24,
                }
            else:
                return {"status": "no_data"}

    def get_data_quality_metrics(self):
        """Get data quality metrics"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Basic counts
            cursor.execute(
                """
                SELECT 
                    'users' as table_name,
                    COUNT(*) as total_rows,
                    COUNT(CASE WHEN user_id IS NULL THEN 1 END) as null_user_ids,
                    COUNT(CASE WHEN signup_date IS NULL THEN 1 END) as null_signup_dates
                FROM churn_analytics.raw_users
                
                UNION ALL
                
                SELECT 
                    'activities' as table_name,
                    COUNT(*) as total_rows,
                    COUNT(CASE WHEN user_id IS NULL THEN 1 END) as null_user_ids,
                    COUNT(CASE WHEN event_timestamp IS NULL THEN 1 END) as null_timestamps
                FROM churn_analytics.raw_user_activities
                
                UNION ALL
                
                SELECT 
                    'transactions' as table_name,
                    COUNT(*) as total_rows,
                    COUNT(CASE WHEN user_id IS NULL THEN 1 END) as null_user_ids,
                    COUNT(CASE WHEN amount IS NULL OR amount <= 0 THEN 1 END) as invalid_amounts
                FROM transactions
            """
            )

            results = cursor.fetchall()
            return [
                {
                    "table": row[0],
                    "total_rows": row[1],
                    "quality_issues": row[2] + row[3],
                }
                for row in results
            ]

    def get_churn_distribution(self):
        """Get current churn distribution"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT 
                    churn_flag,
                    COUNT(*) as user_count,
                    ROUND(AVG(total_spend_ngn), 2) as avg_spend,
                    ROUND(AVG(unique_sessions), 1) as avg_sessions
                FROM churn_marts.churn_features
                GROUP BY churn_flag
                ORDER BY churn_flag
            """
            )

            results = cursor.fetchall()
            return [
                {
                    "churn_status": "Churned" if row[0] == 1 else "Active",
                    "user_count": row[1],
                    "avg_spend": float(row[2]) if row[2] else 0,
                    "avg_sessions": float(row[3]) if row[3] else 0,
                }
                for row in results
            ]

    def generate_monitoring_report(self):
        """Generate a comprehensive monitoring report"""
        report = {
            "timestamp": datetime.now().isoformat(),
            "data_freshness": self.check_data_freshness(),
            "data_quality": self.get_data_quality_metrics(),
            "churn_distribution": self.get_churn_distribution(),
        }

        return report


if __name__ == "__main__":
    monitor = PipelineMonitor()
    report = monitor.generate_monitoring_report()

    print("📊 Pipeline Monitoring Report")
    print("=" * 40)
    print(f"Generated at: {report['timestamp']}")
    print()

    # Data freshness
    freshness = report["data_freshness"]
    if "hours_old" in freshness:
        status = "✅ Fresh" if freshness["is_fresh"] else "⚠️ Stale"
        print(f"Data Freshness: {status} ({freshness['hours_old']} hours old)")
    else:
        print("Data Freshness: ❌ No data")
    print()

    # Data quality
    print("Data Quality:")
    for quality in report["data_quality"]:
        issues = quality["quality_issues"]
        status = "✅" if issues == 0 else f"⚠️ {issues} issues"
        print(f"  {quality['table']}: {quality['total_rows']} rows, {status}")
    print()

    # Churn distribution
    print("Churn Distribution:")
    for dist in report["churn_distribution"]:
        print(
            f"  {dist['churn_status']}: {dist['user_count']} users (avg spend: ₦{dist['avg_spend']}, avg sessions: {dist['avg_sessions']})"
        )
