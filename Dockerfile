FROM quay.io/astronomer/astro-runtime:11.8.0

USER root
RUN apt-get update && apt-get install -y postgresql-client && \
    rm -rf /var/lib/apt/lists/*

USER astro

# Retail example pattern - works perfectly with Cosmos
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-postgres==1.6.8 dbt-core==1.6.8 && deactivate