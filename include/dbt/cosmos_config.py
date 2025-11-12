# include/dbt/cosmos_config.py
from cosmos.config import ProjectConfig, ProfileConfig
from pathlib import Path

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path=Path("/usr/local/airflow/include/dbt"),
)

DBT_CONFIG = ProfileConfig(
    profile_name="retail_analytics",
    target_name="dev",
    profiles_yml_filepath=Path("/usr/local/airflow/include/dbt/profiles.yml"),
)
