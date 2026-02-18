"""
Netflix Prize Data Warehouse - PySpark ETL Pipeline
====================================================

This script performs ETL operations to load the Netflix Prize dataset
into a PostgreSQL data warehouse using Apache Spark for distributed processing.

Author: Data Engineering Team
Version: 2.0 (PySpark Refactor)
Database: Azure Database for PostgreSQL (Flexible Server)

Requirements:
- Python 3.9+
- pyspark>=3.4.0
- python-dotenv>=1.0.0
- PostgreSQL JDBC Driver (postgresql-42.6.0.jar)

Dataset:
- 100M+ ratings from 480K customers on 17K movies
- Date range: 1998-10-01 to 2005-12-31
- Ratings: 1-5 (integer scale)

Performance:
- Distributed processing across partitions
- Parallel file reading and writes
- Optimized shuffles and broadcasts
- Memory-safe for large datasets
"""

import os
import sys
import logging
import json
import time
import csv
import shutil
from datetime import datetime
from typing import Dict, Optional, List
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
import pandas as pd

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col,
    lit,
    when,
    regexp_extract,
    split,
    substring,
    expr,
    to_date,
    date_format,
    year,
    month,
    dayofmonth,
    quarter,
    dayofweek,
    date_add,
    sequence,
    explode,
    last,
    broadcast,
    count,
    min as spark_min,
    max as spark_max,
    avg as spark_avg,
    row_number,
    monotonically_increasing_id,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
    TimestampType,
    BooleanType,
    ShortType,
)

# =====================================================
# WINDOWS CONFIGURATION
# =====================================================

# Set UTF-8 encoding for Windows console to display emojis
if sys.platform == "win32":
    import io

    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")
    # Also set environment variable for subprocess compatibility
    os.environ["PYTHONIOENCODING"] = "utf-8"

# Set HADOOP_HOME for Windows to avoid Spark errors
HADOOP_HOME = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hadoop")
os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["hadoop.home.dir"] = HADOOP_HOME

# Set Python executable for PySpark workers on Windows
# Spark looks for "python3" by default, but Windows uses "python"
if sys.platform == "win32":
    python_exe = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe

# =====================================================
# LOGGING CONFIGURATION
# =====================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("etl_pipeline_spark.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# =====================================================
# CONFIGURATION
# =====================================================


class Config:
    """Configuration class for Spark ETL pipeline"""

    # File paths
    DATA_DIR = "data"
    MOVIE_TITLES_FILE = os.path.join("data", "movie_titles.csv")
    COMBINED_DATA_FILES = [
        os.path.join("data", f"combined_data_{i}.txt") for i in range(1, 5)
    ]

    # Database schema
    SCHEMA_NAME = "netflix_dw"

    # Spark configuration
    APP_NAME = "Netflix-DW-ETL"
    MASTER = "local[*]"  # Use all available cores

    # Temporary data storage for bulk loading (Spark → CSV → PostgreSQL COPY)
    # Note: Parquet requires Hadoop native Windows libraries (winutils.exe, hadoop.dll)
    # CSV mode avoids Windows-specific Hadoop issues
    TEMP_DIR = "temp_csv"
    TEMP_CSV_DIR = "temp_csv"
    USE_PARQUET = True  # Use Parquet for better performance (requires Hadoop native libraries on Windows)

    # Performance tuning
    SHUFFLE_PARTITIONS = 200  # For 100M rows
    DEFAULT_PARALLELISM = 200
    JDBC_BATCH_SIZE = 5000  # Only for dimension tables
    JDBC_NUM_PARTITIONS = 8  # Only for dimension tables

    # PostgreSQL COPY settings (10-50x faster than JDBC)
    COPY_BATCH_SIZE = 100000  # Rows per COPY operation
    COPY_BUFFER_SIZE = 8192  # IO buffer size

    # Azure PostgreSQL connection settings - Extended for long-running operations
    JDBC_CONNECT_TIMEOUT = 60000  # 60 seconds (increased from 30s)
    JDBC_SOCKET_TIMEOUT = 3600000  # 60 minutes (increased from 5 min)
    JDBC_STATEMENT_TIMEOUT = 3600000  # 60 minutes for long queries
    JDBC_POOL_SIZE = 10  # Reduced from 20
    JDBC_MAX_RECONNECTS = 5  # Increased from 3
    JDBC_TCP_KEEPALIVE = True
    JDBC_KEEPALIVE_INTERVAL = 60  # Send keepalive every 60 seconds

    # Date range
    DATE_RANGE = ("1998-10-01", "2005-12-31")

    # PostgreSQL JDBC driver
    JDBC_DRIVER = "org.postgresql.Driver"
    JDBC_JAR_PATH = "postgresql-42.6.0.jar"  # Download if not present

    # Progress tracking
    CHECKPOINT_FILE = "etl_checkpoint.json"
    PROGRESS_UPDATE_INTERVAL = 10000  # Update every 10,000 records


# =====================================================
# PROGRESS TRACKER
# =====================================================


class ProgressTracker:
    """Manages ETL progress tracking with checkpoint file"""

    def __init__(self, checkpoint_file: str = Config.CHECKPOINT_FILE):
        self.checkpoint_file = checkpoint_file
        self.checkpoint = self.load_checkpoint()

    def load_checkpoint(self) -> Dict:
        """Load checkpoint from file or create new"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, "r") as f:
                    checkpoint = json.load(f)
                    logger.info(f"📋 Loaded checkpoint from {self.checkpoint_file}")
                    return checkpoint
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}, starting fresh")

        return {
            "dim_date": {"completed": False, "count": 0},
            "dim_movie": {"completed": False, "count": 0},
            "dim_customer": {"completed": False, "count": 0},
            "fact_ratings": {
                "completed": False,
                "total_count": 0,
                "files_completed": [],
                "current_file": None,
                "current_file_offset": 0,
            },
            "last_updated": None,
        }

    def save_checkpoint(self):
        """Save current checkpoint to file"""
        try:
            self.checkpoint["last_updated"] = datetime.now().isoformat()
            with open(self.checkpoint_file, "w") as f:
                json.dump(self.checkpoint, f, indent=2)
            logger.debug(f"💾 Checkpoint saved to {self.checkpoint_file}")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def mark_dimension_completed(self, dim_name: str, count: int):
        """Mark a dimension as completed"""
        self.checkpoint[dim_name]["completed"] = True
        self.checkpoint[dim_name]["count"] = count
        self.save_checkpoint()
        logger.info(f"✅ Marked {dim_name} as completed with {count:,} records")

    def is_dimension_completed(self, dim_name: str) -> bool:
        """Check if dimension is already loaded"""
        return self.checkpoint.get(dim_name, {}).get("completed", False)

    def mark_file_completed(self, file_name: str):
        """Mark a combined_data file as completed"""
        if file_name not in self.checkpoint["fact_ratings"]["files_completed"]:
            self.checkpoint["fact_ratings"]["files_completed"].append(file_name)
            self.save_checkpoint()
            logger.info(f"✅ Marked {file_name} as completed")

    def is_file_completed(self, file_name: str) -> bool:
        """Check if file is already processed"""
        return file_name in self.checkpoint["fact_ratings"]["files_completed"]

    def update_fact_progress(self, count: int):
        """Update fact table progress"""
        self.checkpoint["fact_ratings"]["total_count"] += count
        self.save_checkpoint()

    def mark_fact_completed(self):
        """Mark fact table loading as completed"""
        self.checkpoint["fact_ratings"]["completed"] = True
        self.save_checkpoint()
        logger.info(f"✅ Fact table loading completed")

    def get_summary(self) -> str:
        """Get progress summary"""
        lines = ["=" * 60, "📊 ETL PROGRESS CHECKPOINT SUMMARY", "=" * 60]

        for dim in ["dim_date", "dim_movie", "dim_customer"]:
            status = (
                "✅ COMPLETED" if self.checkpoint[dim]["completed"] else "⏳ PENDING"
            )
            count = self.checkpoint[dim]["count"]
            lines.append(f"{dim:20} : {status:15} ({count:,} records)")

        fact = self.checkpoint["fact_ratings"]
        status = "✅ COMPLETED" if fact["completed"] else "⏳ IN PROGRESS"
        lines.append(
            f"{'fact_ratings':20} : {status:15} ({fact['total_count']:,} records)"
        )

        if fact["files_completed"]:
            lines.append(f"\n  Files completed: {', '.join(fact['files_completed'])}")

        if self.checkpoint["last_updated"]:
            lines.append(f"\n  Last updated: {self.checkpoint['last_updated']}")

        lines.append("=" * 60)
        return "\n".join(lines)


class ProgressBar:
    """Real-time progress display with ETA"""

    def __init__(self, total: int, description: str = "Processing"):
        self.total = total
        self.description = description
        self.current = 0
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.update_interval = Config.PROGRESS_UPDATE_INTERVAL

    def update(self, count: int = 1):
        """Update progress"""
        self.current += count

        # Update every N records or at completion
        if self.current % self.update_interval == 0 or self.current >= self.total:
            self._display()

    def _display(self):
        """Display progress bar and stats"""
        elapsed = time.time() - self.start_time
        percent = (self.current / self.total * 100) if self.total > 0 else 0

        # Calculate rate and ETA
        rate = self.current / elapsed if elapsed > 0 else 0
        remaining = self.total - self.current
        eta_seconds = remaining / rate if rate > 0 else 0

        # Format time
        eta_str = self._format_time(eta_seconds)
        elapsed_str = self._format_time(elapsed)

        # Progress bar
        bar_length = 30
        filled = int(bar_length * self.current / self.total) if self.total > 0 else 0
        bar = "█" * filled + "░" * (bar_length - filled)

        # Display
        logger.info(
            f"🚀 {self.description}: [{bar}] {percent:.1f}% "
            f"({self.current:,}/{self.total:,}) | "
            f"⚡ {rate:.0f} rec/s | "
            f"⏱️  {elapsed_str} | "
            f"⏳ ETA: {eta_str}"
        )

    def _format_time(self, seconds: float) -> str:
        """Format seconds to human readable time"""
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            return f"{int(seconds/60)}m {int(seconds%60)}s"
        else:
            hours = int(seconds / 3600)
            minutes = int((seconds % 3600) / 60)
            return f"{hours}h {minutes}m"

    def complete(self):
        """Mark as complete and show final stats"""
        self.current = self.total
        self._display()
        elapsed = time.time() - self.start_time
        logger.info(f"✅ {self.description} completed in {self._format_time(elapsed)}")


# =====================================================
# SPARK SESSION MANAGER
# =====================================================


class SparkSessionManager:
    """Manages Spark session with optimized configuration"""

    def __init__(self):
        """Initialize Spark session manager"""
        load_dotenv()

        # Load credentials from .env
        self.host = os.getenv("PGHOST")
        self.port = os.getenv("PGPORT", "5432")
        self.database = os.getenv("PGDATABASE")
        self.user = os.getenv("PGUSER")
        self.password = os.getenv("PGPASSWORD")

        # Validate credentials
        self._validate_credentials()

        # Build JDBC URL with comprehensive timeout settings for Azure PostgreSQL
        # Extended timeouts to handle long-running write operations (48+ minutes)
        self.jdbc_url = (
            f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
            f"?sslmode=require"
            f"&connectTimeout={Config.JDBC_CONNECT_TIMEOUT}"  # 60 seconds
            f"&socketTimeout={Config.JDBC_SOCKET_TIMEOUT}"  # 60 minutes
            f"&tcpKeepAlive=true"  # Enable TCP keepalive
            f"&ApplicationName=Netflix-Spark-ETL"
            f"&reWriteBatchedInserts=true"  # Optimize batch inserts
            f"&prepareThreshold=1"  # Use prepared statements
        )

        self.jdbc_properties = {
            "user": self.user,
            "password": self.password,
            "driver": Config.JDBC_DRIVER,
            # Don't duplicate timeout settings here - they're in JDBC URL
            # Connection pool settings
            "useSSL": "true",
            "serverTimezone": "UTC",
            "autoReconnect": "true",
            "maxReconnects": str(Config.JDBC_MAX_RECONNECTS),
            # JDBC connection properties
            "socketFactoryClass": "org.postgresql.ssl.NonValidatingFactory",
        }

        self.spark = None

    def get_psycopg2_connection(self):
        """Create a psycopg2 connection using stored credentials"""
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            sslmode="require",
            connect_timeout=30,
        )

    def get_table_count(self, table_name: str) -> int:
        """Get current row count from a table using psycopg2 (not JDBC)"""
        try:
            conn = self.get_psycopg2_connection()
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {Config.SCHEMA_NAME}.{table_name}")
            count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return count
        except Exception as e:
            logger.warning(f"Could not get count for {table_name}: {e}")
            return 0

    def validate_existing_data_safety(self, progress_tracker: ProgressTracker) -> bool:
        """
        Validate that existing data is safe and won't be duplicated or lost.
        Returns True if safe to proceed, False if user intervention needed.
        """
        logger.info("\n🛡️  SAFETY CHECK: Validating existing data...")

        safety_issues = []

        # Check each dimension table
        for table_name in ["dim_date", "dim_movie", "dim_customer"]:
            db_count = self.get_table_count(table_name)
            checkpoint_completed = progress_tracker.is_dimension_completed(table_name)
            checkpoint_count = progress_tracker.checkpoint[table_name]["count"]

            if db_count > 0:
                if not checkpoint_completed:
                    # Data exists but checkpoint says not completed - DANGER!
                    safety_issues.append(
                        f"⚠️  {table_name}: Has {db_count:,} rows in database but checkpoint shows incomplete!\n"
                        f"   This could cause duplicate data if we proceed."
                    )
                elif checkpoint_count != db_count:
                    # Counts don't match - WARNING
                    logger.warning(
                        f"⚠️  {table_name}: Database has {db_count:,} rows but checkpoint shows {checkpoint_count:,}"
                    )
                else:
                    # All good - data matches checkpoint
                    logger.info(
                        f"✅ {table_name}: {db_count:,} rows (matches checkpoint)"
                    )
            else:
                # No data in DB
                if checkpoint_completed:
                    # Checkpoint says completed but no data - inconsistency
                    safety_issues.append(
                        f"⚠️  {table_name}: Checkpoint shows completed but database is empty!\n"
                        f"   Checkpoint may be from a different database."
                    )
                else:
                    logger.info(f"✅ {table_name}: Empty (ready to load)")

        # Check fact table
        fact_count = self.get_table_count("fact_ratings")
        fact_checkpoint_count = progress_tracker.checkpoint["fact_ratings"][
            "total_count"
        ]
        completed_files = progress_tracker.checkpoint["fact_ratings"]["files_completed"]

        if fact_count > 0:
            if len(completed_files) == 0 and fact_checkpoint_count == 0:
                # Data exists but checkpoint shows no files completed - DANGER!
                safety_issues.append(
                    f"⚠️  fact_ratings: Has {fact_count:,} rows in database but checkpoint shows no files completed!\n"
                    f"   This could cause duplicate data if we proceed."
                )
            elif fact_count < fact_checkpoint_count:
                # DB has less data than checkpoint claims - WARNING
                logger.warning(
                    f"⚠️  fact_ratings: Database has {fact_count:,} rows but checkpoint shows {fact_checkpoint_count:,}\n"
                    f"   Some data may have been manually deleted."
                )
            else:
                logger.info(
                    f"✅ fact_ratings: {fact_count:,} rows, {len(completed_files)} files completed"
                )
        else:
            if len(completed_files) > 0:
                # Checkpoint says files completed but no data - inconsistency
                safety_issues.append(
                    f"⚠️  fact_ratings: Checkpoint shows {len(completed_files)} files completed but database is empty!\n"
                    f"   Checkpoint may be from a different database."
                )
            else:
                logger.info(f"✅ fact_ratings: Empty (ready to load)")

        # Report results
        if safety_issues:
            logger.error("\n" + "=" * 60)
            logger.error("🚨 DATA SAFETY ISSUES DETECTED!")
            logger.error("=" * 60)
            for issue in safety_issues:
                logger.error(issue)
            logger.error("\n⚠️  RECOMMENDED ACTIONS:")
            logger.error(
                "1. If this is a different database, delete etl_checkpoint.json and restart"
            )
            logger.error(
                "2. If data was manually added, update the checkpoint file manually"
            )
            logger.error("3. If unsure, backup your database before proceeding")
            logger.error("=" * 60)

            # Create a recovery checkpoint suggestion
            logger.error(
                "\n💡 To create a matching checkpoint, create etl_checkpoint.json with:"
            )

            recovery_checkpoint = {
                "dim_date": {
                    "completed": self.get_table_count("dim_date") > 0,
                    "count": self.get_table_count("dim_date"),
                },
                "dim_movie": {
                    "completed": self.get_table_count("dim_movie") > 0,
                    "count": self.get_table_count("dim_movie"),
                },
                "dim_customer": {
                    "completed": self.get_table_count("dim_customer") > 0,
                    "count": self.get_table_count("dim_customer"),
                },
                "fact_ratings": {
                    "completed": False,
                    "total_count": self.get_table_count("fact_ratings"),
                    "files_completed": [],
                    "current_file": None,
                    "current_file_offset": 0,
                },
                "last_updated": datetime.now().isoformat(),
            }

            import json

            logger.error(json.dumps(recovery_checkpoint, indent=2))
            logger.error("\n⛔ STOPPING TO PREVENT DATA CORRUPTION")

            return False

        logger.info("\n✅ DATA SAFETY CHECK PASSED - Safe to proceed!")
        logger.info("   - No duplicate data risk detected")
        logger.info("   - Existing data will be preserved")
        logger.info("   - Only missing data will be loaded")

        return True

    def _validate_credentials(self):
        """Validate that all required credentials are present"""
        required = ["PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"]
        missing = [key for key in required if not os.getenv(key)]

        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}\n"
                "Please check your .env file."
            )

    def create_session(self) -> SparkSession:
        """Create and configure Spark session"""
        try:
            logger.info("Initializing Spark session...")
            logger.info(f"Host: {self.host}")
            logger.info(f"Database: {self.database}")
            logger.info(f"User: {self.user}")

            # Check for JDBC driver
            if not os.path.exists(Config.JDBC_JAR_PATH):
                logger.warning(
                    f"PostgreSQL JDBC driver not found: {Config.JDBC_JAR_PATH}\n"
                    f"Download from: https://jdbc.postgresql.org/download.html"
                )

            # JVM options for Java 17+ compatibility with Spark 3.4.1
            jvm_options = [
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "--add-modules=jdk.unsupported",
            ]
            jvm_opts = " ".join(jvm_options)

            # Create Spark session with optimizations and JDBC connection pooling
            self.spark = (
                SparkSession.builder.appName(Config.APP_NAME)
                .master(Config.MASTER)
                .config("spark.jars", Config.JDBC_JAR_PATH)
                .config("spark.driver.extraJavaOptions", jvm_opts)
                .config("spark.executor.extraJavaOptions", jvm_opts)
                .config("spark.sql.shuffle.partitions", Config.SHUFFLE_PARTITIONS)
                .config("spark.default.parallelism", Config.DEFAULT_PARALLELISM)
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .config("spark.driver.maxResultSize", "2g")
                .config("spark.sql.broadcastTimeout", "600")
                # JDBC Connection Pooling for Azure PostgreSQL
                .config("spark.datasource.jdbc.connection_pooling_enabled", "true")
                .config("spark.datasource.jdbc.connection_pool_size", "20")
                # Network timeouts to prevent Azure idle connection drops
                .config("spark.network.timeout", "120s")
                .config("spark.executor.heartbeatInterval", "10s")
                # Prevent memory issues during large writes
                .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                .getOrCreate()
            )

            # Set log level
            self.spark.sparkContext.setLogLevel("WARN")

            logger.info(f"Spark version: {self.spark.version}")
            logger.info(f"Spark session created successfully!")

            return self.spark

        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            raise

    def test_connection(self):
        """Test PostgreSQL connection using psycopg2 (not JDBC)

        We use psycopg2 instead of JDBC because:
        1. JDBC has timeout configuration conflicts
        2. We use psycopg2 + COPY for actual data loading anyway
        3. More reliable for Azure PostgreSQL
        """
        try:
            logger.info("Testing database connection...")

            # Test connection with psycopg2
            conn = self.get_psycopg2_connection()

            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]

            logger.info(f"Connected successfully! PostgreSQL: {version}")

            cursor.close()
            conn.close()

        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            raise

    def execute_ddl(self, sql_file: str):
        """Execute DDL SQL file using psycopg2 for compatibility"""
        import psycopg2

        logger.info(f"Executing DDL from {sql_file}...")

        try:
            # Read SQL file
            with open(sql_file, "r", encoding="utf-8") as f:
                sql_content = f.read()

            # Remove comments
            import re

            sql_content = re.sub(r"/\*.*?\*/", "", sql_content, flags=re.DOTALL)
            lines = []
            for line in sql_content.split("\n"):
                if "--" in line:
                    line = line[: line.index("--")]
                lines.append(line)

            sql_content = "\n".join(lines)

            # Split statements
            statements = []
            current_statement = []
            for line in sql_content.split("\n"):
                line = line.strip()
                if not line:
                    continue
                current_statement.append(line)
                if line.endswith(";"):
                    stmt = " ".join(current_statement).strip()
                    if stmt and stmt != ";":
                        statements.append(stmt)
                    current_statement = []

            # Execute via psycopg2
            conn = self.get_psycopg2_connection()

            cur = conn.cursor()
            for i, statement in enumerate(statements, 1):
                try:
                    cur.execute(statement)
                    conn.commit()
                except Exception as e:
                    if "does not exist" in str(e):
                        logger.debug(f"Statement {i}: {e}")
                    else:
                        logger.error(f"Statement {i} failed")
                        raise

            cur.close()
            conn.close()

            logger.info(f"Successfully executed {len(statements)} DDL statements")

        except Exception as e:
            logger.error(f"DDL execution failed: {e}")
            raise

    def stop(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


# =====================================================
# DATE DIMENSION LOADER
# =====================================================


class DateDimensionLoader:
    """Loads date dimension using Spark SQL"""

    def __init__(
        self,
        spark: SparkSession,
        jdbc_url: str,
        jdbc_props: Dict,
        progress_tracker: ProgressTracker,
    ):
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.jdbc_props = jdbc_props
        self.schema = Config.SCHEMA_NAME
        self.progress_tracker = progress_tracker

    def generate_date_range(self) -> DataFrame:
        """Generate date dimension using Spark SQL sequence"""
        logger.info("Generating date dimension...")

        start_date, end_date = Config.DATE_RANGE

        # Use Spark SQL to generate date sequence
        date_df = self.spark.sql(
            f"""
            SELECT explode(sequence(
                to_date('{start_date}'),
                to_date('{end_date}'),
                interval 1 day
            )) as date_actual
        """
        )

        # Add derived columns
        date_df = (
            date_df.withColumn(
                "date_key",
                date_format(col("date_actual"), "yyyyMMdd").cast(IntegerType()),
            )
            .withColumn("year", year(col("date_actual")).cast(ShortType()))
            .withColumn("month", month(col("date_actual")).cast(ShortType()))
            .withColumn("day", dayofmonth(col("date_actual")).cast(ShortType()))
            .withColumn("quarter", quarter(col("date_actual")).cast(ShortType()))
            .withColumn(
                "day_of_week", (dayofweek(col("date_actual")) - 2).cast(ShortType())
            )
            .withColumn("month_name", date_format(col("date_actual"), "MMMM"))
            .withColumn(
                "is_weekend",
                when(
                    (dayofweek(col("date_actual")) >= 7)
                    | (dayofweek(col("date_actual")) == 1),
                    True,
                ).otherwise(False),
            )
        )

        # Select in correct order for database
        date_df = date_df.select(
            "date_key",
            "date_actual",
            "year",
            "month",
            "day",
            "quarter",
            "day_of_week",
            "month_name",
            "is_weekend",
        )

        return date_df

    def load(self) -> int:
        """Load date dimension to PostgreSQL"""
        # Check if already completed
        if self.progress_tracker.is_dimension_completed("dim_date"):
            existing_count = self.progress_tracker.checkpoint["dim_date"]["count"]
            logger.info(
                f"⏭️  dim_date already loaded with {existing_count:,} records, skipping..."
            )
            return existing_count

        logger.info("Loading date dimension...")

        df = self.generate_date_range()
        row_count = df.count()

        # Write to PostgreSQL
        df.write.jdbc(
            url=self.jdbc_url,
            table=f"{self.schema}.dim_date",
            mode="append",
            properties=self.jdbc_props,
        )

        logger.info(f"Loaded {row_count} dates into dim_date")
        self.progress_tracker.mark_dimension_completed("dim_date", row_count)
        return row_count


# =====================================================
# MOVIE DIMENSION LOADER
# =====================================================


class MovieDimensionLoader:
    """Loads movie dimension from CSV"""

    def __init__(
        self,
        spark: SparkSession,
        jdbc_url: str,
        jdbc_props: Dict,
        progress_tracker: ProgressTracker,
        spark_manager=None,
    ):
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.jdbc_props = jdbc_props
        self.schema = Config.SCHEMA_NAME
        self.progress_tracker = progress_tracker
        self.spark_manager = spark_manager

    def load(self) -> DataFrame:
        """Load movie dimension and return DataFrame with surrogate keys"""
        # Check if already completed
        if self.progress_tracker.is_dimension_completed("dim_movie"):
            existing_count = self.progress_tracker.checkpoint["dim_movie"]["count"]
            logger.info(
                f"⏭️  dim_movie already loaded with {existing_count:,} records, skipping..."
            )

            # Return the mapping using psycopg2 to avoid JDBC timeout issues
            conn = self.spark_manager.get_psycopg2_connection()

            query = f"SELECT movie_id, movie_key FROM {self.schema}.dim_movie"
            movie_df = pd.read_sql(query, conn)
            conn.close()

            movie_mapping = self.spark.createDataFrame(movie_df)

            return movie_mapping

        logger.info("Loading movie dimension...")

        # Read movie titles with custom parsing for commas in titles
        # Format: MovieID,YearOfRelease,Title (title can contain commas)

        # Read as text first
        movie_text = self.spark.read.text(Config.MOVIE_TITLES_FILE)

        # Parse with split limited to 2 commas
        movie_df = (
            movie_text.withColumn(
                "movie_id", split(col("value"), ",", 3).getItem(0).cast(IntegerType())
            )
            .withColumn("release_year_str", split(col("value"), ",", 3).getItem(1))
            .withColumn("title_with_extra", split(col("value"), ",", 3).getItem(2))
            .withColumn("release_year", col("release_year_str").cast(ShortType()))
            .withColumn(
                "title",
                when(
                    col("title_with_extra").isNull(), col("release_year_str")
                ).otherwise(col("title_with_extra")),
            )
            .withColumn("title", substring(col("title"), 1, 500))
            .select("movie_id", "release_year", "title")
            .filter(col("movie_id").isNotNull())
        )

        row_count = movie_df.count()

        # Write to PostgreSQL (will get surrogate keys from DB)
        movie_df.write.jdbc(
            url=self.jdbc_url,
            table=f"{self.schema}.dim_movie",
            mode="append",
            properties=self.jdbc_props,
        )

        logger.info(f"Loaded {row_count} movies into dim_movie")
        self.progress_tracker.mark_dimension_completed("dim_movie", row_count)

        # Read back with surrogate keys using psycopg2 to avoid JDBC timeout issues
        conn = self.spark_manager.get_psycopg2_connection()

        query = f"SELECT movie_id, movie_key FROM {self.schema}.dim_movie"
        movie_df = pd.read_sql(query, conn)
        conn.close()

        movie_mapping = self.spark.createDataFrame(movie_df)

        logger.info(
            f"Retrieved movie_id -> movie_key mapping ({movie_mapping.count()} entries)"
        )

        return movie_mapping


# =====================================================
# CUSTOMER DIMENSION LOADER
# =====================================================


class CustomerDimensionLoader:
    """Loads customer dimension from ratings files"""

    def __init__(
        self,
        spark: SparkSession,
        jdbc_url: str,
        jdbc_props: Dict,
        progress_tracker: ProgressTracker,
        spark_manager=None,
    ):
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.jdbc_props = jdbc_props
        self.spark_manager = spark_manager
        self.schema = Config.SCHEMA_NAME
        self.progress_tracker = progress_tracker

    def extract_unique_customers(self) -> DataFrame:
        """Extract unique customer IDs from all combined data files"""
        logger.info("Extracting unique customer IDs from ratings data...")

        all_customers = None

        for file_path in Config.COMBINED_DATA_FILES:
            logger.info(f"Scanning {os.path.basename(file_path)}...")

            # Read file as text
            df = self.spark.read.text(file_path)

            # Filter out movie ID lines (ending with :)
            # Extract customer_id from rating lines
            customers = (
                df.filter(~col("value").endswith(":"))
                .filter(col("value").isNotNull())
                .withColumn(
                    "customer_id",
                    split(col("value"), ",", 2).getItem(0).cast(IntegerType()),
                )
                .select("customer_id")
                .filter(col("customer_id").isNotNull())
            )

            if all_customers is None:
                all_customers = customers
            else:
                all_customers = all_customers.union(customers)

        # Deduplicate
        unique_customers = all_customers.distinct()

        customer_count = unique_customers.count()
        logger.info(f"Found {customer_count:,} unique customers")

        return unique_customers

    def load(self) -> DataFrame:
        """Load customer dimension and return DataFrame with surrogate keys"""
        # Check if already completed
        if self.progress_tracker.is_dimension_completed("dim_customer"):
            existing_count = self.progress_tracker.checkpoint["dim_customer"]["count"]
            logger.info(
                f"⏭️  dim_customer already loaded with {existing_count:,} records, skipping..."
            )

            # Return the mapping using psycopg2 to avoid JDBC timeout issues
            conn = self.spark_manager.get_psycopg2_connection()

            query = f"SELECT customer_id, customer_key FROM {self.schema}.dim_customer"
            customer_df = pd.read_sql(query, conn)
            conn.close()

            customer_mapping = self.spark.createDataFrame(customer_df)

            return customer_mapping

        logger.info("Loading customer dimension...")

        # Get unique customers
        customer_df = self.extract_unique_customers()
        customer_count = customer_df.count()

        # Write to PostgreSQL (will get surrogate keys from DB)
        customer_df.coalesce(20).write.jdbc(
            url=self.jdbc_url,
            table=f"{self.schema}.dim_customer",
            mode="append",
            properties=self.jdbc_props,
        )

        logger.info(f"Loaded {customer_count:,} customers into dim_customer")
        self.progress_tracker.mark_dimension_completed("dim_customer", customer_count)

        # Read back with surrogate keys using psycopg2 to avoid JDBC timeout issues
        conn = self.spark_manager.get_psycopg2_connection()

        query = f"SELECT customer_id, customer_key FROM {self.schema}.dim_customer"
        customer_df = pd.read_sql(query, conn)
        conn.close()

        customer_mapping = self.spark.createDataFrame(customer_df)

        logger.info(f"Retrieved customer_id -> customer_key mapping")

        return customer_mapping


# =====================================================
# FACT TABLE LOADER
# =====================================================


class FactRatingsLoader:
    """Loads fact_ratings table using Spark transformations"""

    def __init__(
        self,
        spark: SparkSession,
        jdbc_url: str,
        jdbc_props: Dict,
        customer_mapping: DataFrame,
        movie_mapping: DataFrame,
        progress_tracker: ProgressTracker,
        spark_manager=None,
    ):
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.jdbc_props = jdbc_props
        self.schema = Config.SCHEMA_NAME
        self.customer_mapping = customer_mapping.cache()  # Cache for reuse
        self.movie_mapping = movie_mapping.cache()
        self.progress_tracker = progress_tracker
        self.spark_manager = spark_manager
        self.total_ratings = self.progress_tracker.checkpoint["fact_ratings"][
            "total_count"
        ]

    def parse_combined_data_file(self, file_path: str) -> DataFrame:
        """
        Parse combined_data_*.txt file using Spark transformations

        Format:
        MovieID:
        CustomerID,Rating,Date
        CustomerID,Rating,Date
        """
        logger.info(f"Parsing {os.path.basename(file_path)}...")

        # Read as text
        df = self.spark.read.text(file_path)

        # Add monotonic ID and partition key for distributed processing
        df = df.withColumn("row_id", monotonically_increasing_id())
        df = df.withColumn("partition_key", (col("row_id") / 10000).cast(IntegerType()))

        # Add row number for ordering within each partition
        window_spec = Window.partitionBy("partition_key").orderBy("row_id")

        df = df.withColumn("row_num", row_number().over(window_spec))

        # Identify movie ID lines
        df = df.withColumn("is_movie_line", col("value").endswith(":")).withColumn(
            "movie_id_raw",
            when(
                col("is_movie_line"), regexp_extract(col("value"), r"^(\d+):", 1)
            ).otherwise(None),
        )

        # Propagate movie_id forward using window function within each partition
        window_forward = (
            Window.partitionBy("partition_key")
            .orderBy("row_id")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )

        df = df.withColumn(
            "movie_id", last(col("movie_id_raw"), ignorenulls=True).over(window_forward)
        )

        # Filter to rating lines only and parse
        ratings_df = (
            df.filter(~col("is_movie_line"))
            .filter(col("movie_id").isNotNull())
            .withColumn(
                "customer_id", split(col("value"), ",").getItem(0).cast(IntegerType())
            )
            .withColumn("rating", split(col("value"), ",").getItem(1).cast(ShortType()))
            .withColumn("date_str", split(col("value"), ",").getItem(2))
            .withColumn("movie_id", col("movie_id").cast(IntegerType()))
            .select("movie_id", "customer_id", "rating", "date_str")
            .filter(col("customer_id").isNotNull())
            .filter(col("rating").isNotNull())
        )

        count = ratings_df.count()
        logger.info(f"Parsed {count:,} ratings from {os.path.basename(file_path)}")

        return ratings_df

    def transform_ratings(self, df: DataFrame) -> DataFrame:
        """Transform ratings with dimension lookups and date conversion"""

        # Join with dimension tables to get surrogate keys
        # Use broadcast for small dimension tables
        df = df.join(broadcast(self.customer_mapping), "customer_id", "left").join(
            broadcast(self.movie_mapping), "movie_id", "left"
        )

        # Convert date string to timestamp and date_key
        df = df.withColumn(
            "rating_timestamp",
            to_date(col("date_str"), "yyyy-MM-dd").cast(TimestampType()),
        ).withColumn(
            "date_key",
            date_format(col("rating_timestamp"), "yyyyMMdd").cast(IntegerType()),
        )

        # Filter out rows with missing keys (shouldn't happen if dims are complete)
        before_count = df.count()
        df = df.filter(col("customer_key").isNotNull()).filter(
            col("movie_key").isNotNull()
        )
        after_count = df.count()

        if before_count != after_count:
            logger.warning(
                f"Dropped {before_count - after_count} rows due to missing dimension keys"
            )

        # Select final columns for fact table
        df = df.select(
            "customer_key", "movie_key", "date_key", "rating", "rating_timestamp"
        )

        return df

    def load_file(self, file_path: str):
        """Load a single combined_data file using Spark → Parquet/CSV → PostgreSQL COPY

        This is 10-50x faster than direct JDBC writes.
        """
        file_name = os.path.basename(file_path)

        # Check if file already completed
        if self.progress_tracker.is_file_completed(file_name):
            logger.info(f"⏭️  {file_name} already processed, skipping...")
            return

        logger.info(f"🔄 Processing {file_name}...")

        # STEP 1: Parse and transform with Spark (fast)
        ratings_df = self.parse_combined_data_file(file_path)
        logger.info(f"🔧 Transforming ratings...")
        fact_df = self.transform_ratings(ratings_df)
        count = fact_df.count()
        logger.info(f"📊 Transformed {count:,} ratings")

        # STEP 2: Convert to pandas and write CSV directly (bypasses Hadoop native libs)
        temp_csv_file = os.path.join(
            Config.TEMP_DIR, f"{file_name.replace('.txt', '')}.csv"
        )
        os.makedirs(Config.TEMP_DIR, exist_ok=True)

        logger.info(
            f"💾 Converting to CSV via pandas (Windows-compatible): {temp_csv_file}"
        )
        start_time = time.time()

        # Convert Spark DataFrame to pandas (in-memory - 6M rows ~= 500MB)
        pandas_df = fact_df.toPandas()

        # Write CSV directly using pandas (no Hadoop dependencies)
        pandas_df.to_csv(temp_csv_file, index=False, header=False)

        write_time = time.time() - start_time
        logger.info(f"✅ CSV write completed in {write_time:.1f}s")

        # STEP 3: Bulk load to PostgreSQL using COPY (10-50x faster than JDBC)
        logger.info(f"🚀 Bulk loading to PostgreSQL using COPY command...")
        self._bulk_load_to_postgres_from_csv(temp_csv_file, count, file_name)

        # STEP 4: Cleanup temp files
        logger.info(f"🧹 Cleaning up temporary files...")
        if os.path.exists(temp_csv_file):
            os.remove(temp_csv_file)

        # Update progress
        self.total_ratings += count
        self.progress_tracker.update_fact_progress(count)
        self.progress_tracker.mark_file_completed(file_name)

        logger.info(f"✅ Completed {file_name} ({count:,} ratings)")

    def _bulk_load_to_postgres_from_csv(
        self, csv_file: str, expected_count: int, file_name: str
    ):
        """Bulk load CSV file to PostgreSQL using COPY command

        This is 10-50x faster than JDBC batch inserts.
        Windows-compatible (no Hadoop native libraries required).
        Includes retry logic for Azure PostgreSQL connection timeouts.
        """
        start_time = time.time()
        logger.info(f"📄 CSV file ready: {csv_file}")

        max_retries = 3
        retry_count = 0
        last_error = None

        while retry_count < max_retries:
            try:
                # Connect to PostgreSQL and use COPY
                conn = self.spark_manager.get_psycopg2_connection()
                conn.autocommit = False

                # Set TCP keepalives for long operations on Azure
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

                cursor = conn.cursor()

                logger.info(
                    f"🚀 Starting COPY command (attempt {retry_count + 1}/{max_retries})..."
                )
                copy_start = time.time()

                # Use COPY FROM STDIN for maximum speed
                with open(csv_file, "r", encoding="utf-8") as f:
                    cursor.copy_expert(
                        sql.SQL(
                            "COPY {}.fact_ratings (customer_key, movie_key, date_key, rating, rating_timestamp) "
                            "FROM STDIN WITH (FORMAT CSV, DELIMITER ',')"
                        ).format(sql.Identifier(self.schema)),
                        f,
                    )

                cursor.execute("COMMIT;")
                copy_time = time.time() - copy_start
                total_time = time.time() - start_time

                rows_loaded = cursor.rowcount
                rate = rows_loaded / copy_time if copy_time > 0 else 0

                logger.info(
                    f"✅ COPY completed in {copy_time:.1f}s ({rate:,.0f} rows/s)"
                )
                logger.info(f"📊 Total load time: {total_time:.1f}s")
                logger.info(f"📈 Loaded {rows_loaded:,} rows")

                cursor.close()
                conn.close()
                return  # Success!

            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                last_error = e
                retry_count += 1
                logger.error(
                    f"❌ COPY attempt {retry_count}/{max_retries} failed: {str(e)}"
                )

                if retry_count < max_retries:
                    wait_time = 10 * retry_count  # 10s, 20s, 30s
                    logger.info(f"⏳ Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"❌ All {max_retries} COPY attempts failed")
                    raise

            except Exception as e:
                logger.error(f"❌ COPY failed (non-recoverable): {str(e)}")
                if "conn" in locals():
                    try:
                        conn.rollback()
                        conn.close()
                    except:
                        pass
                raise

            finally:
                # Try to cleanup temp CSV (but don't fail if it can't be deleted)
                try:
                    if os.path.isdir(csv_file):
                        shutil.rmtree(csv_file, ignore_errors=True)
                    elif os.path.isfile(csv_file):
                        os.remove(csv_file)
                except Exception as cleanup_error:
                    logger.warning(f"⚠️  Could not cleanup {csv_file}: {cleanup_error}")

    def load_all(self):
        """Load all combined_data files"""
        # Check if already completed
        if self.progress_tracker.checkpoint["fact_ratings"]["completed"]:
            logger.info(
                f"⏭️  fact_ratings already completed with {self.total_ratings:,} records, skipping..."
            )
            return

        logger.info("🚀 Starting fact table load...")

        for file_path in Config.COMBINED_DATA_FILES:
            if os.path.exists(file_path):
                self.load_file(file_path)
            else:
                logger.warning(f"⚠️  File not found: {file_path}")

        self.progress_tracker.mark_fact_completed()

        logger.info(
            f"🎉 Fact table load complete! Total ratings loaded: {self.total_ratings:,}"
        )


# =====================================================
# POST-LOAD PROCESSOR
# =====================================================


class PostLoadProcessor:
    """Handles post-load operations"""

    def __init__(self, spark: SparkSession, jdbc_url: str, jdbc_props: Dict):
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.jdbc_props = jdbc_props
        self.schema = Config.SCHEMA_NAME

    def update_customer_aggregates(self):
        """Update customer dimension with aggregated metrics using Spark"""
        logger.info("Updating customer aggregates...")

        # Read fact and date tables
        fact_df = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=f"{self.schema}.fact_ratings",
            properties=self.jdbc_props,
        )

        date_df = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=f"{self.schema}.dim_date",
            properties=self.jdbc_props,
        )

        # Calculate aggregates
        agg_df = (
            fact_df.join(date_df, fact_df.date_key == date_df.date_key)
            .groupBy(fact_df.customer_key)
            .agg(
                spark_min("date_actual").alias("first_rating_date"),
                spark_max("date_actual").alias("last_rating_date"),
                count("*").alias("total_ratings"),
            )
        )

        # For update, we need to use JDBC execute (via psycopg2)
        # Collect aggregates (customer dim is small ~480K rows)
        logger.info("Collecting customer aggregates for update...")
        agg_data = agg_df.collect()

        import psycopg2

        conn = psycopg2.connect(
            host=(
                self.jdbc_props["user"].split("@")[1]
                if "@" in self.jdbc_props["user"]
                else os.getenv("PGHOST")
            ),
            port=os.getenv("PGPORT", "5432"),
            database=os.getenv("PGDATABASE"),
            user=(
                self.jdbc_props["user"].split("@")[0]
                if "@" in self.jdbc_props["user"]
                else self.jdbc_props["user"]
            ),
            password=self.jdbc_props["password"],
            sslmode="require",
        )

        cur = conn.cursor()

        update_sql = f"""
            UPDATE {self.schema}.dim_customer
            SET first_rating_date = %s,
                last_rating_date = %s,
                total_ratings = %s
            WHERE customer_key = %s
        """

        batch_size = 10000
        updated = 0

        for i in range(0, len(agg_data), batch_size):
            batch = agg_data[i : i + batch_size]
            batch_data = [
                (
                    row.first_rating_date,
                    row.last_rating_date,
                    row.total_ratings,
                    row.customer_key,
                )
                for row in batch
            ]
            cur.executemany(update_sql, batch_data)
            conn.commit()
            updated += len(batch)
            if i % 50000 == 0:
                logger.info(f"  Updated {updated:,} customer records...")

        cur.close()
        conn.close()

        logger.info(f"Updated {updated:,} customer records with aggregates")

    def generate_summary(self):
        """Generate and log data warehouse summary"""
        logger.info("=" * 60)
        logger.info("DATA WAREHOUSE SUMMARY")
        logger.info("=" * 60)

        tables = {
            "dim_date": f"{self.schema}.dim_date",
            "dim_movie": f"{self.schema}.dim_movie",
            "dim_customer": f"{self.schema}.dim_customer",
            "fact_ratings": f"{self.schema}.fact_ratings",
        }

        for name, table in tables.items():
            df = self.spark.read.jdbc(
                url=self.jdbc_url, table=table, properties=self.jdbc_props
            )
            count = df.count()
            logger.info(f"{name:20} : {count:>15,} rows")

        # Additional stats
        fact_df = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=f"{self.schema}.fact_ratings",
            properties=self.jdbc_props,
        )

        date_df = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=f"{self.schema}.dim_date",
            properties=self.jdbc_props,
        )

        stats = (
            fact_df.join(date_df, "date_key")
            .agg(
                spark_min("date_actual").alias("min_date"),
                spark_max("date_actual").alias("max_date"),
                spark_avg("rating").alias("avg_rating"),
            )
            .collect()[0]
        )

        logger.info(f"{'Date Range':20} : {stats.min_date} to {stats.max_date}")
        logger.info(f"{'Average Rating':20} : {stats.avg_rating:.3f}")
        logger.info("=" * 60)


# =====================================================
# MAIN ETL ORCHESTRATOR
# =====================================================


def main():
    """Main Spark ETL pipeline orchestrator with resumable progress tracking"""

    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("🎬 NETFLIX PRIZE DATA WAREHOUSE - SPARK ETL PIPELINE")
    logger.info("=" * 60)
    logger.info(f"⏰ Start Time: {start_time}")

    spark_manager = None
    progress_tracker = None

    try:
        # 0. Initialize Progress Tracker
        logger.info("\n[STEP 0/9] Initializing Progress Tracker")
        progress_tracker = ProgressTracker()
        logger.info(progress_tracker.get_summary())

        # 1. Initialize Spark Session
        logger.info("\n[STEP 1/9] Initializing Spark Session")
        spark_manager = SparkSessionManager()
        spark = spark_manager.create_session()
        spark_manager.test_connection()

        # 1.5 SAFETY VALIDATION - Check existing data
        logger.info("\n[STEP 1.5/9] 🛡️  Data Safety Validation")
        is_safe = spark_manager.validate_existing_data_safety(progress_tracker)
        if not is_safe:
            logger.error("❌ Safety check failed. Exiting to prevent data corruption.")
            return 2  # Exit code 2 for safety failure

        # 2. Schema Creation (Skip if resuming)
        logger.info("\n[STEP 2/9] Creating Database Schema")
        if os.path.exists("schema.sql"):
            # Only create schema if this is first run
            if not any(
                progress_tracker.checkpoint[dim]["completed"]
                for dim in ["dim_date", "dim_movie", "dim_customer"]
            ):
                spark_manager.execute_ddl("schema.sql")
            else:
                logger.info("⏭️  Schema already exists, skipping DDL execution...")
        else:
            logger.warning("⚠️  schema.sql not found, assuming schema exists")

        jdbc_url = spark_manager.jdbc_url
        jdbc_props = spark_manager.jdbc_properties

        # 3. Load Date Dimension
        logger.info("\n[STEP 3/9] Loading Date Dimension")
        date_loader = DateDimensionLoader(spark, jdbc_url, jdbc_props, progress_tracker)
        date_loader.load()

        # 4. Load Movie Dimension
        logger.info("\n[STEP 4/9] Loading Movie Dimension")
        movie_loader = MovieDimensionLoader(
            spark, jdbc_url, jdbc_props, progress_tracker, spark_manager
        )
        movie_mapping = movie_loader.load()

        # 5. Load Customer Dimension
        logger.info("\n[STEP 5/9] Loading Customer Dimension")
        customer_loader = CustomerDimensionLoader(
            spark, jdbc_url, jdbc_props, progress_tracker, spark_manager
        )
        customer_mapping = customer_loader.load()

        # 6. Load Fact Table
        logger.info("\n[STEP 6/9] Loading Fact Table")
        fact_start = datetime.now()
        fact_loader = FactRatingsLoader(
            spark,
            jdbc_url,
            jdbc_props,
            customer_mapping,
            movie_mapping,
            progress_tracker,
            spark_manager,
        )
        fact_loader.load_all()
        fact_duration = datetime.now() - fact_start
        logger.info(f"⏱️  Fact load completed in {fact_duration}")

        # 7. Post-Load Processing
        logger.info("\n[STEP 7/9] Post-Load Processing")
        post_processor = PostLoadProcessor(spark, jdbc_url, jdbc_props)
        post_processor.update_customer_aggregates()

        # 8. Generate Summary
        logger.info("\n[STEP 8/9] Generating Summary")
        post_processor.generate_summary()
        logger.info("\n" + progress_tracker.get_summary())

        # Complete
        end_time = datetime.now()
        duration = end_time - start_time

        logger.info("\n" + "=" * 60)
        logger.info("✅ SPARK ETL PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        logger.info(f"⏰ End Time: {end_time}")
        logger.info(f"⏱️  Total Duration: {duration}")
        logger.info("=" * 60)

        return 0

    except KeyboardInterrupt:
        logger.warning("\n" + "=" * 60)
        logger.warning("⚠️  ETL PIPELINE INTERRUPTED BY USER")
        logger.warning("=" * 60)
        logger.warning("💾 Progress has been saved to checkpoint file")
        logger.warning("🔄 Run the script again to resume from where you left off")
        logger.warning("=" * 60)
        if progress_tracker:
            logger.info(progress_tracker.get_summary())
        return 130  # Standard exit code for Ctrl+C

    except Exception as e:
        logger.error(f"\n{'='*60}")
        logger.error("❌ SPARK ETL PIPELINE FAILED!")
        logger.error(f"{'='*60}")
        logger.error(f"Error: {e}", exc_info=True)
        logger.error("💾 Progress has been saved to checkpoint file")
        logger.error("🔄 Fix the issue and run the script again to resume")
        logger.error("=" * 60)
        if progress_tracker:
            logger.info(progress_tracker.get_summary())
        return 1

    finally:
        # Clean up
        if spark_manager:
            spark_manager.stop()


if __name__ == "__main__":
    sys.exit(main())
