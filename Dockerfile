FROM astrocrpublic.azurecr.io/runtime:3.1-4

USER root
RUN apt-get update && apt-get install -y postgresql-client && \
    rm -rf /var/lib/apt/lists/*

USER astro

# Retail example pattern - works perfectly with Cosmos
# Using dbt 1.7.18 compatible with Airflow 3.1.2
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-postgres==1.7.18 dbt-core==1.7.18 && deactivate