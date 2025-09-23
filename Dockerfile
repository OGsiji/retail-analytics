FROM astrocrpublic.azurecr.io/runtime:3.0-11

# Install additional system packages
USER root
RUN apt-get update && apt-get install -y \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER astro

# Create virtual environment for dbt
RUN python -m venv dbt_venv && \
    . dbt_venv/bin/activate && \
    pip install --no-cache-dir \
        dbt-postgres==1.7.4 \
        dbt-core==1.7.4 && \
    deactivate

# Install API dependencies
COPY include/api/requirements.txt /tmp/api_requirements.txt
RUN pip install --no-cache-dir -r /tmp/api_requirements.txt

