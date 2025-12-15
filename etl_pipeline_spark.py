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
from datetime import datetime
from typing import Dict
from dotenv import load_dotenv

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
# LOGGING CONFIGURATION
# =====================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("etl_pipeline_spark.log"),
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

    # Performance tuning
    SHUFFLE_PARTITIONS = 200  # For 100M rows
    DEFAULT_PARALLELISM = 200
    JDBC_BATCH_SIZE = 10000
    JDBC_NUM_PARTITIONS = 20

    # Date range
    DATE_RANGE = ("1998-10-01", "2005-12-31")

    # PostgreSQL JDBC driver
    JDBC_DRIVER = "org.postgresql.Driver"
    JDBC_JAR_PATH = "postgresql-42.6.0.jar"  # Download if not present


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

        # Build JDBC URL
        self.jdbc_url = (
            f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
            f"?sslmode=require"
        )

        self.jdbc_properties = {
            "user": self.user,
            "password": self.password,
            "driver": Config.JDBC_DRIVER,
        }

        self.spark = None

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

            # Create Spark session with optimizations
            self.spark = (
                SparkSession.builder.appName(Config.APP_NAME)
                .master(Config.MASTER)
                .config("spark.jars", Config.JDBC_JAR_PATH)
                .config("spark.sql.shuffle.partitions", Config.SHUFFLE_PARTITIONS)
                .config("spark.default.parallelism", Config.DEFAULT_PARALLELISM)
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .config("spark.driver.maxResultSize", "2g")
                .config("spark.sql.broadcastTimeout", "600")
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
        """Test PostgreSQL connection"""
        try:
            logger.info("Testing database connection...")

            # Simple query to test connection
            query = "(SELECT version() as version) as test"
            df = self.spark.read.jdbc(
                url=self.jdbc_url, table=query, properties=self.jdbc_properties
            )

            version = df.collect()[0]["version"]
            logger.info(f"Connected successfully! PostgreSQL: {version}")

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
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                sslmode="require",
            )

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

    def __init__(self, spark: SparkSession, jdbc_url: str, jdbc_props: Dict):
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.jdbc_props = jdbc_props
        self.schema = Config.SCHEMA_NAME

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
        return row_count


# =====================================================
# MOVIE DIMENSION LOADER
# =====================================================


class MovieDimensionLoader:
    """Loads movie dimension from CSV"""

    def __init__(self, spark: SparkSession, jdbc_url: str, jdbc_props: Dict):
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.jdbc_props = jdbc_props
        self.schema = Config.SCHEMA_NAME

    def load(self) -> DataFrame:
        """Load movie dimension and return DataFrame with surrogate keys"""
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

        # Read back with surrogate keys
        movie_mapping = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=f"{self.schema}.dim_movie",
            properties=self.jdbc_props,
        ).select("movie_id", "movie_key")

        logger.info(
            f"Retrieved movie_id -> movie_key mapping ({movie_mapping.count()} entries)"
        )

        return movie_mapping


# =====================================================
# CUSTOMER DIMENSION LOADER
# =====================================================


class CustomerDimensionLoader:
    """Loads customer dimension from ratings files"""

    def __init__(self, spark: SparkSession, jdbc_url: str, jdbc_props: Dict):
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.jdbc_props = jdbc_props
        self.schema = Config.SCHEMA_NAME

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
        logger.info("Loading customer dimension...")

        # Get unique customers
        customer_df = self.extract_unique_customers()

        # Write to PostgreSQL (will get surrogate keys from DB)
        customer_df.coalesce(20).write.jdbc(
            url=self.jdbc_url,
            table=f"{self.schema}.dim_customer",
            mode="append",
            properties=self.jdbc_props,
        )

        logger.info(f"Loaded {customer_df.count():,} customers into dim_customer")

        # Read back with surrogate keys
        customer_mapping = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=f"{self.schema}.dim_customer",
            properties=self.jdbc_props,
        ).select("customer_id", "customer_key")

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
    ):
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.jdbc_props = jdbc_props
        self.schema = Config.SCHEMA_NAME
        self.customer_mapping = customer_mapping.cache()  # Cache for reuse
        self.movie_mapping = movie_mapping.cache()
        self.total_ratings = 0

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

        # Add row number for ordering
        window_spec = Window.orderBy(monotonically_increasing_id())

        df = df.withColumn("row_num", row_number().over(window_spec))

        # Identify movie ID lines
        df = df.withColumn("is_movie_line", col("value").endswith(":")).withColumn(
            "movie_id_raw",
            when(
                col("is_movie_line"), regexp_extract(col("value"), r"^(\d+):", 1)
            ).otherwise(None),
        )

        # Propagate movie_id forward using window function
        window_forward = Window.orderBy("row_num").rowsBetween(
            Window.unboundedPreceding, Window.currentRow
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
        """Load a single combined_data file into fact table"""

        # Parse file
        ratings_df = self.parse_combined_data_file(file_path)

        # Transform
        logger.info(f"Transforming ratings...")
        fact_df = self.transform_ratings(ratings_df)

        count = fact_df.count()
        self.total_ratings += count

        # Write to PostgreSQL with partitioning
        logger.info(f"Writing {count:,} ratings to fact_ratings...")

        fact_df.repartition(Config.JDBC_NUM_PARTITIONS).write.jdbc(
            url=self.jdbc_url,
            table=f"{self.schema}.fact_ratings",
            mode="append",
            properties={**self.jdbc_props, "batchsize": str(Config.JDBC_BATCH_SIZE)},
        )

        logger.info(f"Completed loading {os.path.basename(file_path)}")

    def load_all(self):
        """Load all combined_data files"""
        logger.info("Starting fact table load...")

        for file_path in Config.COMBINED_DATA_FILES:
            if os.path.exists(file_path):
                self.load_file(file_path)
            else:
                logger.warning(f"File not found: {file_path}")

        logger.info(
            f"Fact table load complete! Total ratings loaded: {self.total_ratings:,}"
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
    """Main Spark ETL pipeline orchestrator"""

    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("NETFLIX PRIZE DATA WAREHOUSE - SPARK ETL PIPELINE")
    logger.info("=" * 60)
    logger.info(f"Start Time: {start_time}")

    spark_manager = None

    try:
        # 1. Initialize Spark Session
        logger.info("\n[STEP 1/7] Initializing Spark Session")
        spark_manager = SparkSessionManager()
        spark = spark_manager.create_session()
        spark_manager.test_connection()

        # 2. Schema Creation
        logger.info("\n[STEP 2/7] Creating Database Schema")
        if os.path.exists("schema.sql"):
            spark_manager.execute_ddl("schema.sql")
        else:
            logger.warning("schema.sql not found, assuming schema exists")

        jdbc_url = spark_manager.jdbc_url
        jdbc_props = spark_manager.jdbc_properties

        # 3. Load Date Dimension
        logger.info("\n[STEP 3/7] Loading Date Dimension")
        date_loader = DateDimensionLoader(spark, jdbc_url, jdbc_props)
        date_loader.load()

        # 4. Load Movie Dimension
        logger.info("\n[STEP 4/7] Loading Movie Dimension")
        movie_loader = MovieDimensionLoader(spark, jdbc_url, jdbc_props)
        movie_mapping = movie_loader.load()

        # 5. Load Customer Dimension
        logger.info("\n[STEP 5/7] Loading Customer Dimension")
        customer_loader = CustomerDimensionLoader(spark, jdbc_url, jdbc_props)
        customer_mapping = customer_loader.load()

        # 6. Load Fact Table
        logger.info("\n[STEP 6/7] Loading Fact Table")
        fact_start = datetime.now()
        fact_loader = FactRatingsLoader(
            spark, jdbc_url, jdbc_props, customer_mapping, movie_mapping
        )
        fact_loader.load_all()
        fact_duration = datetime.now() - fact_start
        logger.info(f"Fact load completed in {fact_duration}")

        # 7. Post-Load Processing
        logger.info("\n[STEP 7/7] Post-Load Processing")
        post_processor = PostLoadProcessor(spark, jdbc_url, jdbc_props)
        post_processor.update_customer_aggregates()
        post_processor.generate_summary()

        # Complete
        end_time = datetime.now()
        duration = end_time - start_time

        logger.info("\n" + "=" * 60)
        logger.info("SPARK ETL PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        logger.info(f"End Time: {end_time}")
        logger.info(f"Total Duration: {duration}")
        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.error(f"\n{'='*60}")
        logger.error("SPARK ETL PIPELINE FAILED!")
        logger.error(f"{'='*60}")
        logger.error(f"Error: {e}", exc_info=True)
        return 1

    finally:
        # Clean up
        if spark_manager:
            spark_manager.stop()


if __name__ == "__main__":
    sys.exit(main())
