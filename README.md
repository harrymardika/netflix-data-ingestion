# 🎬 Netflix Prize Data Warehouse - Resumable ETL Pipeline

> **Production-grade ETL pipeline with automatic checkpoint system and real-time progress tracking**

## 📋 Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Docker Support](#docker-support)
- [Usage](#usage)
- [Architecture](#architecture)
- [Database Schema](#database-schema)
- [Resumable Processing](#resumable-processing)
- [Safety Features](#safety-features)
- [Troubleshooting](#troubleshooting)
- [Performance Tuning](#performance-tuning)
- [Project Structure](#project-structure)
- [Security Best Practices](#security-best-practices)
- [Future Roadmap](#future-roadmap)

---

## 📦 Overview

This project implements a **production-grade, resumable ETL (Extract-Transform-Load) pipeline** for the Netflix Prize dataset using **Apache Spark** (PySpark) and **PostgreSQL**. It processes 100M+ movie ratings from 480K customers across 17K titles spanning 1998-2005, transforming raw data into a normalized Star Schema data warehouse optimized for analytical queries.

### Key Highlights

| Feature                  | Value                                        |
| ------------------------ | -------------------------------------------- |
| **Total Ratings**        | 100M+ records                                |
| **Unique Customers**     | ~480K                                        |
| **Unique Movies**        | ~17K titles                                  |
| **Date Range**           | Oct 1998 - Dec 2005                          |
| **Processing Framework** | Apache Spark 3.4+                            |
| **Database**             | PostgreSQL 12+ / Azure PostgreSQL            |
| **Resumable**            | ✅ Yes - with automatic checkpoint system    |
| **Duplicate Safe**       | ✅ Yes - prevents data duplication on resume |

### Business Problem

The digital streaming industry generates millions of interactions daily. This pipeline transforms raw rating data into a dimensional data warehouse that supports:

- **Data-Driven Decisions**: Empirical insights from 100M+ ratings
- **Hyper-Personalization**: Foundation for recommendation systems
- **Churn Prediction**: Identifying at-risk customers
- **Content Optimization**: Understanding viewer preferences

---

## 🚀 Quick Start

### First Run

```bash
# 1. Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/macOS
# or
.\venv\Scripts\Activate.ps1  # Windows

# 2. Install dependencies
pip install -r requirements.txt

# 3. Create .env file
cp .env.example .env
# Edit .env with your PostgreSQL credentials

# 4. Create database schema
psql -h your-server -U adminuser -d postgres -f schema.sql

# 5. Run ETL pipeline
python etl_pipeline_spark.py
```

### Resume After Interruption

```bash
# Simply run the same command - it will resume automatically!
python etl_pipeline_spark.py
```

### Start Fresh

```bash
# Delete checkpoint file to reset progress
rm etl_checkpoint.json

# Run pipeline
python etl_pipeline_spark.py
```

---

## ✨ Features

### Resumable Processing

- **Automatic Checkpointing**: Track progress in etl_checkpoint.json
- **Smart Resume**: Continues from exact interruption point
- **No Duplicates**: Already-loaded data is automatically skipped
- **File-Level Tracking**: Each source file marked independently
- **Dimension Tracking**: Separate progress for each dimension table
- **Safe Re-runs**: Run multiple times without data corruption

### Performance & Scalability

- **Distributed Processing**: PySpark for parallel computation across CPU cores
- **Optimized Partitioning**: 200+ shuffle partitions for efficient data distribution
- **Memory Management**: Automatic memory optimization
- **JDBC Batching**: 10,000-record batches for efficient writes
- **Lazy Evaluation**: Spark's optimization for minimal computation

### Data Quality

- **Comprehensive Validation**: Schema enforcement at multiple stages
- **Surrogate Keys**: Data warehouse standards compliance
- **Type Safety**: Strong type checking with PySpark DataFrames
- **Idempotent Design**: Safe to re-run without corruption
- **Pre-run Validation**: Prevents data loss or duplication

### Real-Time Monitoring

- **Progress Updates**: Every 10,000 records with visual progress bar
- **Processing Speed**: Records/second throughput display
- **ETA Calculation**: Accurate estimated time to completion
- **Dual Logging**: Console + file (etl_pipeline_spark.log)
- **Error Tracking**: Comprehensive error messages with recovery guidance
- **Execution Timing**: Stage-by-stage performance metrics

---

## 📋 Prerequisites

### Software Requirements

| Component        | Version | Purpose                   |
| ---------------- | ------- | ------------------------- |
| **Python**       | 3.9+    | Main programming language |
| **Java**         | 11+     | Required for Apache Spark |
| **PostgreSQL**   | 12+     | Data warehouse            |
| **Apache Spark** | 3.4.0+  | Distributed processing    |

### Python Dependencies

```
pyspark>=3.5.0
python-dotenv>=1.0.0
pandas>=2.0.0
sqlalchemy>=2.0.0
psycopg2-binary>=2.9.0
```

### Database Driver

- **PostgreSQL JDBC Driver**: postgresql-42.6.0.jar (downloaded automatically or manually placed)

---

## Installation

### 1. Clone Repository

```bash
git clone <repository-url>
cd netflix-data-ingestion
```

### 2. Create Virtual Environment

**Linux/macOS:**

```bash
python3 -m venv venv
source venv/bin/activate
```

**Windows PowerShell:**

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Set Up JDBC Driver

Download postgresql-42.6.0.jar from https://jdbc.postgresql.org/download/ and place in project root:

```bash
# File should be at: ./postgresql-42.6.0.jar
ls postgresql-42.6.0.jar  # Verify
```

### 5. Prepare Data Files

Ensure data directory contains:

```
data/
├── movie_titles.csv        # 17K movies
├── combined_data_1.txt     # 25M ratings
├── combined_data_2.txt     # 25M ratings
├── combined_data_3.txt     # 25M ratings
└── combined_data_4.txt     # 25M ratings
```

Download from Netflix Prize Dataset: https://www.kaggle.com/netflix-inc/netflix-prize-data

---

## Configuration

### Create .env File

```bash
cp .env.example .env
```

Edit `.env` with your PostgreSQL credentials:

```env
# PostgreSQL Connection
PGHOST=your-server.postgres.database.azure.com
PGPORT=5432
PGDATABASE=netflix_dw
PGUSER=adminuser@servername
PGPASSWORD=YourSecurePassword123!

# Optional: Spark Settings
SPARK_LOCAL_IP=127.0.0.1
```

### Environment Variables Reference

| Variable   | Required | Example                              | Description       |
| ---------- | -------- | ------------------------------------ | ----------------- |
| PGHOST     | Yes      | myserver.postgres.database.azure.com | Database hostname |
| PGPORT     | Yes      | 5432                                 | Database port     |
| PGDATABASE | Yes      | netflix_dw                           | Database name     |
| PGUSER     | Yes      | adminuser@servername                 | Database username |
| PGPASSWORD | Yes      | SecurePass123!                       | Database password |

**Security Note**: Never commit .env to version control. It's in .gitignore.

---

## Docker Support

### Quick Start with Docker

**Local development with Docker Compose:**

```bash
# Build and run
docker-compose up --build

# Stop services
docker-compose down
```

### Docker Configuration

- **Base Image**: python:3.11-slim with OpenJDK 17
- **JDBC Driver**: Automatically included
- **Volumes**: Persistent checkpoint directory
- **Environment**: Loaded from .env file

For production deployment, see docker-compose.yml.

---

## Usage

### Run ETL Pipeline

```bash
python etl_pipeline_spark.py
```

### Example Output

```
2025-12-15 10:30:45 - INFO - NETFLIX DATA WAREHOUSE ETL PIPELINE

[STEP 1/5] Loading Date Dimension...
LOADED: 2,865 date records (1998-10-01 to 2005-12-31)

[STEP 2/5] Loading Movie Dimension...
LOADED: 17,770 movie records

[STEP 3/5] Loading Customer Dimension...
PROCESSING: [████████████░░] 85% (408K/480K) | 12,345 rec/s | ETA: 6s
LOADED: 480,189 customer records

[STEP 4/5] Loading Fact Table...
PROCESSING combined_data_1.txt: [██████░░░░] 60% (15M/25M) | 8,500 rec/s | ETA: 29m

[STEP 5/5] Post-Processing...
UPDATED: customer aggregates
ANALYZED: indexes

ETL Pipeline Completed Successfully!
Total execution time: 19 minutes 45 seconds
```

### View Logs

```bash
# Last 50 lines
tail -50 etl_pipeline_spark.log

# Search for errors
grep ERROR etl_pipeline_spark.log

# Follow in real-time
tail -f etl_pipeline_spark.log
```

---

## Architecture

### Data Flow Diagram

```
Raw Data Files (text/CSV)
        |
        v
   PySpark ETL
        |
        v
    Transformation
    - Parse
    - Normalize
    - Deduplicate
        |
        v
    Load to PostgreSQL
    - Dimensions
    - Facts
    - Aggregates
        |
        v
   Data Warehouse
   (Star Schema)
        |
        v
    Analytics & Reporting
    - OLAP Queries
    - Dashboards
```

### Star Schema Architecture

```
         dim_customer
              |
              |
movie -- fact_ratings -- date
 dim      (100M+)        dim
```

### Pipeline Stages

| Stage                 | Duration    | Records   | Purpose                   |
| --------------------- | ----------- | --------- | ------------------------- |
| 1. Date Dimension     | 2-3m        | 2,865     | Create temporal dimension |
| 2. Movie Dimension    | 1-2m        | 17,770    | Create movie reference    |
| 3. Customer Dimension | 3-5m        | 480K      | Extract unique customers  |
| 4. Fact Table         | 8-12m       | 100M+     | Load core rating data     |
| 5. Post-Processing    | 1-2m        | -         | Aggregates & indexes      |
| **Total**             | **~19-20m** | **100M+** | Complete warehouse load   |

---

## Database Schema

### Star Schema Design

```
dim_date                    dim_movie
- date_key (PK)            - movie_key (PK)
- date_actual              - movie_id
- year                     - title
- month                    - release_year
- day                          |
- quarter                      |
- day_of_week                  |
- month_name                   |
- is_weekend                   |
- created_at                   |
    |                          |
    |-------fact_ratings------|
              |
              |
          dim_customer
          - customer_key (PK)
          - customer_id
          - first_rating_date
          - last_rating_date
          - total_ratings
          - created_at
```

### Table Specifications

#### fact_ratings (Core Fact Table)

- **Records**: 100,232,651
- **Size**: ~2-3 GB
- **Primary Key**: rating_key (BIGSERIAL)
- **Foreign Keys**: References to all dimension tables
- **Indexes**: Composite on (customer_key, movie_key, date_key)

#### dim_date

- **Records**: 2,865 (Oct 1, 1998 - Dec 31, 2005)
- **Primary Key**: date_key (YYYYMMDD format)
- **Indexes**: date_actual, year/month

#### dim_movie

- **Records**: 17,770
- **Primary Key**: movie_key (SERIAL)
- **Natural Key**: movie_id (UNIQUE)
- **Indexes**: movie_id, release_year

#### dim_customer

- **Records**: 480,189
- **Primary Key**: customer_key (SERIAL)
- **Natural Key**: customer_id (UNIQUE)
- **Indexes**: customer_id, rating date ranges

### Sample Analytical Queries

**Top 10 Most-Rated Movies:**

```sql
SELECT m.title, COUNT(*) as rating_count, AVG(fr.rating) as avg_rating
FROM fact_ratings fr
JOIN dim_movie m ON fr.movie_key = m.movie_key
GROUP BY m.movie_key, m.title
ORDER BY rating_count DESC LIMIT 10;
```

**Average Rating Trend by Year:**

```sql
SELECT d.year, AVG(fr.rating) as avg_rating, COUNT(*) as rating_count
FROM fact_ratings fr
JOIN dim_date d ON fr.date_key = d.date_key
GROUP BY d.year
ORDER BY d.year;
```

**Customer Rating Frequency:**

```sql
SELECT dc.customer_id, COUNT(*) as total_ratings
FROM fact_ratings fr
JOIN dim_customer dc ON fr.customer_key = dc.customer_key
GROUP BY dc.customer_id
ORDER BY total_ratings DESC LIMIT 20;
```

---

## Resumable Processing

### How It Works

The pipeline uses checkpoint file (etl_checkpoint.json) to track completion status:

```json
{
  "dim_date": {
    "completed": true,
    "count": 2865
  },
  "dim_movie": {
    "completed": true,
    "count": 17770
  },
  "dim_customer": {
    "completed": true,
    "count": 480189
  },
  "fact_ratings": {
    "completed": false,
    "total_count": 100232651,
    "files_completed": ["combined_data_1.txt", "combined_data_2.txt"],
    "current_file": "combined_data_3.txt",
    "current_file_offset": 12345678
  },
  "last_updated": "2025-12-15T14:30:45.123456"
}
```

### Usage Scenarios

#### Scenario 1: Normal Execution

```bash
python etl_pipeline_spark.py
```

- Creates new checkpoint file
- Processes all data sequentially
- Updates progress continuously
- Saves final checkpoint

#### Scenario 2: Resume After Interruption

```bash
python etl_pipeline_spark.py
```

Output:

```
Loaded checkpoint from etl_checkpoint.json

ETL PROGRESS SUMMARY
dim_date        : COMPLETED    (2,865 records)
dim_movie       : COMPLETED    (17,770 records)
dim_customer    : COMPLETED    (480,189 records)
fact_ratings    : IN PROGRESS  (25M/100M processed)

dim_date: Skipping - already completed
dim_movie: Skipping - already completed
dim_customer: Skipping - already completed

Loading Fact Table
combined_data_1.txt: already loaded, skipping
combined_data_2.txt: already loaded, skipping
Resuming combined_data_3.txt from offset 12,345,678...
```

#### Scenario 3: Start Fresh

```bash
rm etl_checkpoint.json
python etl_pipeline_spark.py
```

### Progress Indicator

Every 10,000 records:

```
Processing combined_data_1.txt:
[████████░░] 80.5% (80,500/100,000)
4,523 rec/s | 17s elapsed | ETA: 4s
```

---

## Safety Features

### Pre-Run Validation

Before processing, the pipeline validates existing data:

```
[PRE-FLIGHT CHECK] Validating existing data...
VALID: dim_date:      2,920 rows (checkpoint: 2,920)
VALID: dim_movie:     17,770 rows (checkpoint: 17,770)
VALID: dim_customer:  480,189 rows (checkpoint: 480,189)
VALID: fact_ratings:  100M rows, 4 files marked complete

DATA SAFETY CHECK PASSED
```

### Safety Issue Detection

If issues are detected, pipeline stops automatically:

```
SAFETY CHECK FAILED

WARNING: fact_ratings has 50M rows, but checkpoint shows no files completed!
This could cause duplicate data if we proceed.

RECOMMENDED ACTIONS:
1. If using different database: rm etl_checkpoint.json && restart
2. If data was manually added: Update checkpoint manually
3. If unsure: Backup database before proceeding

EXECUTION BLOCKED TO PREVENT DATA CORRUPTION
```

### Duplicate Prevention

- File-level tracking: Each source file marked as completed
- Dimension idempotency: Reprocessing dimensions produces same result
- Fact table offset tracking: Resumes from exact record position
- Database constraints: UNIQUE and PRIMARY KEY enforcement

---

## Troubleshooting

### Connection Issues

**Issue**: PGHOST environment variable not found

```bash
# Solution: Create .env file
cp .env.example .env
# Edit with your database credentials
```

**Issue**: Connection refused to database server

```bash
# Verify server is reachable
ping your-server.postgres.database.azure.com

# Test connection
psql -h your-server -U adminuser -d postgres -c "SELECT 1;"
```

### Driver Issues

**Issue**: PostgreSQL JDBC driver not found

```bash
# Download from https://jdbc.postgresql.org/download/
# Place as: ./postgresql-42.6.0.jar
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
```

### Spark Issues

**Issue**: java.lang.NoSuchMethodError or version conflicts

```bash
# Reinstall PySpark
pip uninstall pyspark -y
pip install pyspark==3.4.0
```

**Issue**: Out of Memory during execution

Edit etl_pipeline_spark.py:

```python
class Config:
    SHUFFLE_PARTITIONS = 100      # Reduced from 200
    DEFAULT_PARALLELISM = 100
```

### Data Issues

**Issue**: "DATA SAFETY ISSUES DETECTED"

```bash
# Solution 1: Fresh database
rm etl_checkpoint.json
python etl_pipeline_spark.py

# Solution 2: Manual checkpoint creation
# Copy checkpoint JSON structure from any successful run
```

**Issue**: Progress seems stuck

```bash
# Check logs
tail -50 etl_pipeline_spark.log

# Query database for current counts
psql -h your-server -U adminuser -d netflix_dw -c \
  "SELECT COUNT(*) FROM fact_ratings;"
```

### Performance Issues

**Issue**: Pipeline runs very slowly

Diagnoses & Solutions:

- Check CPU usage - if 100%: Reduce SHUFFLE_PARTITIONS in config
- Check if data is actually being written: tail -f etl_pipeline_spark.log
- Monitor database connection pool: Check if connections are saturated

---

## Performance Tuning

### Configuration

Edit etl_pipeline_spark.py config section:

```python
class Config:
    # Adjust based on your hardware
    SHUFFLE_PARTITIONS = 200           # 400+ for large, 50-100 for small
    DEFAULT_PARALLELISM = 200
    JDBC_BATCH_SIZE = 10000            # 50K for large, 1K for small
    JDBC_NUM_PARTITIONS = 20           # 100 for large, 5 for small
    PROGRESS_UPDATE_INTERVAL = 10000   # Log every N records
    CHECKPOINT_FILE = "etl_checkpoint.json"
```

### Typical Execution Times

| Hardware    | Date | Movie | Customer | Fact (100M) | Total |
| ----------- | ---- | ----- | -------- | ----------- | ----- |
| 4-core SSD  | 2m   | 1.5m  | 4m       | 10m         | 17.5m |
| 8-core SSD  | 2m   | 1.5m  | 2m       | 6m          | 11.5m |
| 16-core SSD | 2m   | 1.5m  | 1.5m     | 4m          | 9m    |

---

## Project Structure

```
netflix-data-ingestion/
├── README.md                    # This file
├── requirements.txt             # Python dependencies
├── pyproject.toml              # Project metadata
├── .env.example                # Environment template
├── .env                        # Configuration (git-ignored)
├── .gitignore                  # Git ignore rules
│
├── etl_pipeline_spark.py       # Main ETL pipeline (1,647 lines)
├── schema.sql                  # Database DDL script
├── postgresql-42.6.0.jar       # JDBC driver
│
├── Dockerfile                  # Docker container definition
├── docker-compose.yml          # Docker Compose configuration
│
├── data/                       # Data directory
│   ├── movie_titles.csv        # 17K movies
│   ├── combined_data_1.txt     # 25M ratings
│   ├── combined_data_2.txt     # 25M ratings
│   ├── combined_data_3.txt     # 25M ratings
│   └── combined_data_4.txt     # 25M ratings
│
├── checkpoints/                # Checkpoint storage (auto-created)
├── logs/                       # Spark logs (auto-created)
│
├── etl_checkpoint.json         # Progress tracking (auto-generated)
├── etl_pipeline_spark.log      # Execution log (auto-generated)
│
└── __pycache__/                # Python cache (git-ignored)
```

---

## Security Best Practices

1. **Never Commit Secrets**
   - .env is in .gitignore
   - Use environment variables for credentials
   - Review .gitignore before commits

2. **Database Security**
   - Use strong passwords (12+ chars, special chars)
   - Enable SSL/TLS connections
   - Restrict database access via firewall rules
   - Regularly rotate credentials

3. **Access Control**
   - Limit database user privileges
   - Use separate credentials for different environments
   - Enable Azure SQL audit logging

4. **Monitoring**
   - Review etl_pipeline_spark.log regularly
   - Check Azure Portal for suspicious activity
   - Monitor unusual data changes

---

## Technical Stack

| Component     | Version | Purpose           |
| ------------- | ------- | ----------------- |
| Python        | 3.9+    | Main language     |
| PySpark       | 3.4.0+  | Distributed ETL   |
| PostgreSQL    | 12+     | Data warehouse    |
| Pandas        | 2.0+    | Data validation   |
| SQLAlchemy    | 2.0+    | Schema reflection |
| python-dotenv | 1.0+    | Config management |
| JDBC Driver   | 42.6.0  | DB connectivity   |

---

## References

- Netflix Prize Dataset: https://www.kaggle.com/netflix-inc/netflix-prize-data
- PySpark Documentation: https://spark.apache.org/docs/latest/api/python/
- PostgreSQL Documentation: https://www.postgresql.org/docs/
- Azure Database for PostgreSQL: https://docs.microsoft.com/azure/postgresql/
- Star Schema Design: https://en.wikipedia.org/wiki/Dimensional_modeling

---

## Future Roadmap

### Planned Features

- Incremental load support (CDC - Change Data Capture)
- Add genre dimension from external data source
- Type 2 SCD (Slowly Changing Dimensions) for customer tracking
- Materialized views for common aggregations
- Partition fact table by date for better performance
- Data quality monitoring and validation framework
- Power BI/Tableau dashboard templates
- Real-time data stream ingestion (Kafka)
- Data masking for PII protection
- Automated backup and recovery procedures

### Known Limitations

- Fact table partitioning not yet implemented
- No CDC (incremental) mode - full reload only
- No multi-database replication
- Single-threaded resume (not distributed)

---

## Support

### Getting Help

1. **Check This README** - Most questions answered in Troubleshooting section
2. **Review Logs** - etl_pipeline_spark.log contains detailed information
3. **Check Checkpoint** - etl_checkpoint.json shows progress status
4. **Monitor Database** - Query fact_ratings row count to verify loading

### Common Questions

**Q: How do I reset and start over?**

```bash
rm etl_checkpoint.json
python etl_pipeline_spark.py
```

**Q: Can I interrupt the pipeline?**

Yes! Press Ctrl+C anytime. Progress is saved. Run again to resume.

**Q: How long does it take?**

Typically 17-20 minutes on modern 4-core hardware with SSD.

**Q: Can I reprocess a specific file?**

Yes! Edit etl_checkpoint.json and remove the file from files_completed.

---

## License

This project is provided as-is for educational and data warehouse purposes.

---

**Version**: 3.0 (Resumable ETL with Checkpoint System)
**Last Updated**: December 15, 2025
**Status**: Production Ready

---

## Changelog

### v3.0 - Current

- Resumable processing with checkpoints
- Real-time progress tracking
- Enhanced safety features
- Improved error handling
- Modern UI with progress indicators

### v2.0

- PySpark refactor for distributed processing
- Star schema implementation
- JDBC batching for performance

### v1.0

- Initial Pandas-based ETL
- PostgreSQL backend
- Single-threaded processing
