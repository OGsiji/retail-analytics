# Production Deployment Guide

This guide covers deploying the Retail Analytics Pipeline to production environments.

---

## Table of Contents

1. [Pre-Deployment Checklist](#pre-deployment-checklist)
2. [Deployment Options](#deployment-options)
3. [Astronomer Cloud Deployment](#astronomer-cloud-deployment)
4. [Self-Hosted Deployment](#self-hosted-deployment)
5. [Post-Deployment Verification](#post-deployment-verification)
6. [Monitoring & Maintenance](#monitoring--maintenance)

---

## Pre-Deployment Checklist

### Security

- [ ] Change default Airflow admin password
- [ ] Generate new Fernet key for Airflow
- [ ] Set strong PostgreSQL passwords
- [ ] Configure SSL/TLS for database connections
- [ ] Review and restrict network access
- [ ] Enable firewall rules
- [ ] Set up secrets management (AWS Secrets Manager, Vault, etc.)

### Configuration

- [ ] Update `.env` with production values
- [ ] Configure email notifications for DAG failures
- [ ] Set appropriate retry policies
- [ ] Configure log retention policies
- [ ] Set up backup strategy for PostgreSQL
- [ ] Configure resource limits (CPU, memory)

### Data

- [ ] Validate CSV data format
- [ ] Set up automated data delivery mechanism
- [ ] Configure data retention policies
- [ ] Plan for data archival

---

## Deployment Options

### Option 1: Astronomer Cloud (Recommended)

**Pros:**
- Fully managed Airflow
- Auto-scaling
- Built-in monitoring
- Easy upgrades
- SOC 2 compliant

**Cons:**
- Higher cost
- Less control over infrastructure

### Option 2: Self-Hosted (Docker Compose)

**Pros:**
- Full control
- Lower cost for small scale
- Runs anywhere

**Cons:**
- Manual maintenance
- Scaling complexity
- You manage security updates

### Option 3: Kubernetes (Advanced)

**Pros:**
- Highly scalable
- Cloud-agnostic
- Production-grade

**Cons:**
- Complex setup
- Requires K8s expertise
- Higher operational overhead

---

## Astronomer Cloud Deployment

### Step 1: Install Astronomer CLI

\`\`\`bash
# macOS
brew install astro

# Linux
curl -sSL install.astronomer.io | sudo bash
\`\`\`

### Step 2: Authenticate

\`\`\`bash
astro login
\`\`\`

### Step 3: Create Deployment

\`\`\`bash
# Via CLI
astro deployment create --name="retail-analytics-prod" \\
  --executor=local \\
  --runtime-version=3.1.2

# Or via UI at app.astronomer.io
\`\`\`

### Step 4: Configure Environment Variables

In Astronomer UI:
1. Go to Deployment → Environment
2. Add variables:
   - \`POSTGRES_PASSWORD\`: <secure-password>
   - \`AIRFLOW__CORE__FERNET_KEY\`: <generated-key>
   - \`DATABASE_URL\`: <production-db-url>

### Step 5: Deploy Code

\`\`\`bash
# From project root
astro deploy

# Select your deployment when prompted
\`\`\`

### Step 6: Configure External Services

**PostgreSQL:**
- Use managed service (AWS RDS, Google Cloud SQL, Azure Database)
- Enable automated backups
- Set up read replicas if needed

**Metabase:**
- Deploy to separate container/server
- Use separate database for Metabase metadata
- Configure behind reverse proxy with SSL

**FastAPI:**
- Deploy to Cloud Run, ECS, or App Engine
- Configure auto-scaling
- Set up health checks

---

## Self-Hosted Deployment

### Step 1: Server Preparation

\`\`\`bash
# Update system
sudo apt-get update && sudo apt-get upgrade -y

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
\`\`\`

### Step 2: Clone Repository

\`\`\`bash
cd /opt
sudo git clone <repository-url> retail-analytics
cd retail-analytics
\`\`\`

### Step 3: Configure Environment

\`\`\`bash
# Copy example env
cp .env.example .env

# Edit with production values
sudo nano .env
\`\`\`

**Required changes in .env:**
\`\`\`env
POSTGRES_PASSWORD=<strong-random-password>
AIRFLOW__CORE__FERNET_KEY=<run: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">
AIRFLOW__WEBSERVER__SECRET_KEY=<random-string-50-chars>
\`\`\`

### Step 4: Build Images

\`\`\`bash
# Build Retail API
docker build -t retail-api:prod ./include/api

# Start services
docker-compose -f docker-compose.yml -f docker-compose.override.yml up -d
\`\`\`

### Step 5: Initialize Airflow

\`\`\`bash
# Create admin user
docker exec -it <webserver-container> airflow users create \\
    --username admin \\
    --password <secure-password> \\
    --firstname Admin \\
    --lastname User \\
    --role Admin \\
    --email admin@company.com
\`\`\`

### Step 6: Configure Reverse Proxy (Nginx)

\`\`\`nginx
# /etc/nginx/sites-available/retail-analytics

server {
    listen 80;
    server_name analytics.yourcompany.com;

    location / {
        return 301 https://$server_name$request_uri;
    }
}

server {
    listen 443 ssl http2;
    server_name analytics.yourcompany.com;

    ssl_certificate /etc/letsencrypt/live/analytics.yourcompany.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/analytics.yourcompany.com/privkey.pem;

    location / {
        proxy_pass http://localhost:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
\`\`\`

Enable site:
\`\`\`bash
sudo ln -s /etc/nginx/sites-available/retail-analytics /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
\`\`\`

### Step 7: Set Up SSL (Let's Encrypt)

\`\`\`bash
sudo apt-get install certbot python3-certbot-nginx
sudo certbot --nginx -d analytics.yourcompany.com
\`\`\`

---

## Post-Deployment Verification

### 1. Check Service Health

\`\`\`bash
# Airflow
curl -f https://analytics.yourcompany.com/health

# API
curl -f http://localhost:8001/health

# PostgreSQL
docker exec -it <postgres-container> pg_isready -U postgres

# Metabase
curl -f http://localhost:3000/api/health
\`\`\`

### 2. Verify DAG

1. Login to Airflow UI
2. Check DAG appears without errors
3. Trigger manual run
4. Monitor all tasks complete successfully

### 3. Test API Endpoints

\`\`\`bash
# Get metrics
curl https://analytics.yourcompany.com/api/metrics

# Get promos
curl https://analytics.yourcompany.com/api/promos
\`\`\`

### 4. Verify Database

\`\`\`bash
docker exec -it <postgres-container> psql -U postgres -d postgres -c "\\dt retail_analytics.*"
\`\`\`

Should show:
- promo_summary
- pricing_summary
- data_quality_summary
- data_quality_issues

---

## Monitoring & Maintenance

### Set Up Monitoring

**Airflow Metrics:**
- DAG run success/failure rates
- Task duration trends
- Scheduler heartbeat
- Database connection pool

**System Metrics:**
- CPU utilization
- Memory usage
- Disk space
- Network I/O

**Tools:**
- Prometheus + Grafana
- Datadog
- New Relic
- CloudWatch (AWS)

### Backup Strategy

**Daily Automated Backups:**
\`\`\`bash
# PostgreSQL backup script
#!/bin/bash
BACKUP_DIR=/backups/postgres
DATE=$(date +%Y%m%d_%H%M%S)

docker exec <postgres-container> pg_dump -U postgres postgres | gzip > $BACKUP_DIR/retail_analytics_$DATE.sql.gz

# Keep last 30 days
find $BACKUP_DIR -name "*.sql.gz" -mtime +30 -delete
\`\`\`

**Cron Job:**
\`\`\`cron
0 2 * * * /opt/retail-analytics/scripts/backup.sh
\`\`\`

### Log Management

**Configure Log Rotation:**
\`\`\`json
// /etc/docker/daemon.json
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "10m",
    "max-file": "3"
  }
}
\`\`\`

### Update Strategy

**Monthly Updates:**
1. Review Airflow/dbt release notes
2. Test in staging environment
3. Schedule maintenance window
4. Backup database
5. Update images
6. Restart services
7. Verify functionality

\`\`\`bash
# Update process
docker-compose pull
docker-compose down
docker-compose up -d
\`\`\`

---

## Security Best Practices

### 1. Network Security

- Use VPN for admin access
- Restrict port access with firewall
- Use private subnets for database
- Enable WAF (Web Application Firewall)

### 2. Access Control

- Implement RBAC in Airflow
- Use MFA for admin accounts
- Rotate credentials quarterly
- Audit access logs regularly

### 3. Data Protection

- Encrypt data at rest
- Use SSL/TLS for data in transit
- Mask sensitive data in logs
- Implement data retention policies

### 4. Compliance

- GDPR compliance for EU data
- SOC 2 audit preparation
- Regular security assessments
- Incident response plan

---

## Troubleshooting Production Issues

### High Memory Usage

\`\`\`bash
# Check container stats
docker stats

# Increase memory limits
# Edit docker-compose.yml
services:
  webserver:
    deploy:
      resources:
        limits:
          memory: 4G
\`\`\`

### DAG Failures

\`\`\`bash
# View scheduler logs
docker logs <scheduler-container> --tail 100

# Check task logs in UI
# Airflow → DAGs → <dag-name> → Task → Logs
\`\`\`

### Database Connection Issues

\`\`\`bash
# Check connection pool
docker exec -it <postgres-container> psql -U postgres -c "SELECT * FROM pg_stat_activity;"

# Restart database
docker restart <postgres-container>
\`\`\`

---

## Scaling Considerations

### Horizontal Scaling

**Use CeleryExecutor:**
1. Update Dockerfile to include Celery
2. Add Redis service
3. Configure multiple workers
4. Update executor in airflow.cfg

### Vertical Scaling

**Increase Resources:**
\`\`\`yaml
# docker-compose.yml
services:
  webserver:
    deploy:
      resources:
        limits:
          cpus: '2.0'
          memory: 4G
\`\`\`

---

## Support & Escalation

**Level 1:** Check logs and restart services  
**Level 2:** Review configuration and database  
**Level 3:** Contact vendor support (Astronomer, Metabase)  

**Emergency Contact:** data-engineering-oncall@company.com

---

**Deployment Checklist Complete?** ✓

Your retail analytics pipeline is now production-ready!
