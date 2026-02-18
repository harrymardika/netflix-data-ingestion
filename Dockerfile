# Multi-stage build for Netflix ETL Pipeline
# Stage 1: Builder
FROM python:3.11-slim as builder

WORKDIR /app

# Install system dependencies for building
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Stage 2: Runtime
FROM python:3.11-slim

WORKDIR /app

# Install Java (required for Spark) and other dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Copy Python packages from builder
COPY --from=builder /root/.local /root/.local

# Set PATH to include Python packages
ENV PATH=/root/.local/bin:$PATH
ENV PYTHONPATH=/root/.local/lib/python3.11/site-packages

# Copy application files
COPY etl_pipeline_spark.py .
COPY schema.sql .
COPY .env.example .env
COPY data ./data

# Copy JDBC driver for PostgreSQL
COPY postgresql-42.6.0.jar .

# Create checkpoints directory
RUN mkdir -p /app/checkpoints

# Set Spark environment variables for better performance
ENV SPARK_LOCAL_MEMORY=2g
ENV SPARK_DRIVER_MEMORY=2g
ENV SPARK_EXECUTOR_MEMORY=1g
ENV SPARK_EXECUTOR_CORES=2
ENV PYSPARK_PYTHON=/usr/local/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3

# Set logging
ENV LOG_LEVEL=INFO

# Health check - verify Python and Spark can start
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python3 -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.appName('health').master('local').getOrCreate(); spark.stop()" || exit 1

# Run the ETL pipeline
CMD ["python3", "etl_pipeline_spark.py"]
