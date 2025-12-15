"""
Netflix Prize Data Warehouse - ETL Pipeline
==============================================

This script performs ETL operations to load the Netflix Prize dataset
into a PostgreSQL data warehouse with a Star Schema design.

Author: Data Engineering Team
Version: 1.0
Database: Azure Database for PostgreSQL (Flexible Server)

Requirements:
- Python 3.9+
- pandas
- sqlalchemy
- psycopg2-binary
- python-dotenv

Dataset:
- 100M+ ratings from 480K customers on 17K movies
- Date range: 1998-10-01 to 2005-12-31
- Ratings: 1-5 (integer scale)
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Dict, Set, List, Tuple
from urllib.parse import quote_plus
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# =====================================================
# LOGGING CONFIGURATION
# =====================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("etl_pipeline.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# =====================================================
# CONFIGURATION
# =====================================================


class Config:
    """Configuration class for ETL pipeline"""

    # File paths
    DATA_DIR = "data"
    MOVIE_TITLES_FILE = os.path.join("data", "movie_titles.csv")
    COMBINED_DATA_FILES = [
        os.path.join("data", f"combined_data_{i}.txt") for i in range(1, 5)
    ]

    # Database schema
    SCHEMA_NAME = "netflix_dw"

    # Processing parameters
    CHUNK_SIZE = (
        100000  # Rows per batch for fact table insert (increased for performance)
    )
    DATE_RANGE = ("1998-10-01", "2005-12-31")  # Dataset date range

    # Progress reporting
    PROGRESS_INTERVAL = 100000  # Log progress every N rows


# =====================================================
# DATABASE CONNECTION
# =====================================================


class DatabaseConnection:
    """Manages PostgreSQL database connection"""

    def __init__(self):
        """Initialize database connection from environment variables"""
        load_dotenv()

        # Load credentials from .env
        self.host = os.getenv("PGHOST")
        self.port = os.getenv("PGPORT", "5432")
        self.database = os.getenv("PGDATABASE")
        self.user = os.getenv("PGUSER")
        self.password = os.getenv("PGPASSWORD")

        # Validate credentials
        self._validate_credentials()

        # URL-encode username and password to handle special characters
        encoded_user = quote_plus(self.user)
        encoded_password = quote_plus(self.password)

        # Create connection string
        self.connection_string = (
            f"postgresql://{encoded_user}:{encoded_password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

        self.engine = None

    def _validate_credentials(self):
        """Validate that all required credentials are present"""
        required = ["PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"]
        missing = [key for key in required if not os.getenv(key)]

        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}\n"
                "Please check your .env file."
            )

    def connect(self):
        """Establish database connection"""
        try:
            logger.info("Connecting to PostgreSQL database...")
            logger.info(f"Host: {self.host}")
            logger.info(f"Port: {self.port}")
            logger.info(f"Database: {self.database}")
            logger.info(f"User: {self.user}")

            # Optimized connection pool for bulk operations
            self.engine = create_engine(
                self.connection_string,
                pool_pre_ping=True,
                pool_size=20,  # Increased pool size
                max_overflow=40,  # Increased overflow
                pool_recycle=3600,  # Recycle connections after 1 hour
                echo=False,  # Disable SQL logging for performance
            )

            # Test connection
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT version();"))
                version = result.fetchone()[0]
                logger.info(f"Connected successfully! PostgreSQL version: {version}")

            return self.engine

        except SQLAlchemyError as e:
            logger.error(f"Database connection failed: {e}")
            logger.error(
                f"Connection details - Host: {self.host}, Port: {self.port}, Database: {self.database}, User: {self.user}"
            )
            logger.error("Please verify your .env file and ensure:")
            logger.error("  1. PGHOST is correct (without username/password)")
            logger.error("  2. Azure PostgreSQL firewall allows your IP")
            logger.error("  3. Username and password are correct")
            raise

    def execute_sql_file(self, sql_file: str):
        """Execute SQL DDL file to create schema"""
        try:
            with open(sql_file, "r", encoding="utf-8") as f:
                sql_content = f.read()

            # Remove block comments /* */
            import re

            sql_content = re.sub(r"/\*.*?\*/", "", sql_content, flags=re.DOTALL)

            # Remove single-line comments --
            lines = []
            for line in sql_content.split("\n"):
                # Remove inline comments
                if "--" in line:
                    line = line[: line.index("--")]
                lines.append(line)

            sql_content = "\n".join(lines)

            # Split into statements by semicolon
            statements = []
            current_statement = []

            for line in sql_content.split("\n"):
                line = line.strip()
                if not line:
                    continue

                current_statement.append(line)

                # Check if statement ends with semicolon
                if line.endswith(";"):
                    stmt = " ".join(current_statement).strip()
                    if stmt and stmt != ";":
                        statements.append(stmt)
                    current_statement = []

            # Execute each statement
            with self.engine.begin() as conn:
                for i, statement in enumerate(statements, 1):
                    try:
                        conn.execute(text(statement))
                    except Exception as e:
                        # Continue with other statements even if one fails (for DROP IF EXISTS)
                        if "does not exist" in str(e):
                            logger.debug(f"Statement {i}: {e}")
                        else:
                            logger.error(f"Statement {i} failed: {statement[:100]}...")
                            raise

            logger.info(
                f"Successfully executed SQL file: {sql_file} ({len(statements)} statements)"
            )

        except Exception as e:
            logger.error(f"Failed to execute SQL file: {e}")
            raise


# =====================================================
# DATE DIMENSION LOADER
# =====================================================


class DateDimensionLoader:
    """Loads date dimension table"""

    def __init__(self, engine):
        self.engine = engine
        self.schema = Config.SCHEMA_NAME

    def generate_date_range(self) -> pd.DataFrame:
        """Generate date dimension data for entire date range"""
        start_date = datetime.strptime(Config.DATE_RANGE[0], "%Y-%m-%d")
        end_date = datetime.strptime(Config.DATE_RANGE[1], "%Y-%m-%d")

        date_list = []
        current_date = start_date

        while current_date <= end_date:
            date_key = int(current_date.strftime("%Y%m%d"))
            date_list.append(
                {
                    "date_key": date_key,
                    "date_actual": current_date.date(),
                    "year": current_date.year,
                    "month": current_date.month,
                    "day": current_date.day,
                    "quarter": (current_date.month - 1) // 3 + 1,
                    "day_of_week": current_date.weekday(),
                    "month_name": current_date.strftime("%B"),
                    "is_weekend": current_date.weekday() >= 5,
                }
            )
            current_date += timedelta(days=1)

        return pd.DataFrame(date_list)

    def load(self):
        """Load date dimension"""
        logger.info("Loading date dimension...")

        df_dates = self.generate_date_range()

        df_dates.to_sql(
            "dim_date",
            self.engine,
            schema=self.schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )

        logger.info(f"Loaded {len(df_dates)} dates into dim_date")
        return len(df_dates)


# =====================================================
# MOVIE DIMENSION LOADER
# =====================================================


class MovieDimensionLoader:
    """Loads movie dimension table"""

    def __init__(self, engine):
        self.engine = engine
        self.schema = Config.SCHEMA_NAME

    def load(self) -> Dict[int, int]:
        """
        Load movie dimension and return mapping of movie_id -> movie_key

        Returns:
            Dictionary mapping natural key (movie_id) to surrogate key (movie_key)
        """
        logger.info("Loading movie dimension...")

        # Read movie titles - parse manually to handle commas in titles
        # Format: MovieID,YearOfRelease,Title (title can contain commas)
        movies_data = []
        with open(Config.MOVIE_TITLES_FILE, "r", encoding="latin-1") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                # Split only on first 2 commas (movie_id, year, rest is title)
                parts = line.split(",", 2)
                if len(parts) >= 3:
                    movie_id = int(parts[0])
                    release_year = parts[1]
                    title = parts[2]
                    movies_data.append(
                        {
                            "movie_id": movie_id,
                            "release_year": release_year,
                            "title": title,
                        }
                    )

        df_movies = pd.DataFrame(movies_data)

        # Clean data
        df_movies["release_year"] = pd.to_numeric(
            df_movies["release_year"], errors="coerce"
        )
        df_movies["title"] = df_movies["title"].str[:500]  # Truncate long titles

        # Insert into database (use larger chunksize for performance)
        df_movies.to_sql(
            "dim_movie",
            self.engine,
            schema=self.schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )

        logger.info(f"Loaded {len(df_movies)} movies into dim_movie")

        # Retrieve surrogate keys
        query = text(f"SELECT movie_id, movie_key FROM {self.schema}.dim_movie")
        with self.engine.connect() as conn:
            df_mapping = pd.read_sql(query, conn)

        movie_mapping = dict(zip(df_mapping["movie_id"], df_mapping["movie_key"]))
        logger.info(
            f"Created movie_id -> movie_key mapping ({len(movie_mapping)} entries)"
        )

        return movie_mapping


# =====================================================
# CUSTOMER DIMENSION LOADER
# =====================================================


class CustomerDimensionLoader:
    """Loads customer dimension table"""

    def __init__(self, engine):
        self.engine = engine
        self.schema = Config.SCHEMA_NAME

    def extract_unique_customers(self) -> Set[int]:
        """Extract unique customer IDs from combined data files (optimized)"""
        logger.info("Extracting unique customer IDs from ratings data...")

        unique_customers = set()
        processed_lines = 0

        for file_path in Config.COMBINED_DATA_FILES:
            logger.info(f"Scanning {os.path.basename(file_path)}...")

            with open(
                file_path, "r", buffering=1024 * 1024
            ) as f:  # 1MB buffer for faster I/O
                for line in f:
                    line = line.strip()
                    if line and not line.endswith(":"):
                        # Parse customer_id from rating line (faster split with maxsplit)
                        customer_id = int(line.split(",", 1)[0])
                        unique_customers.add(customer_id)

                    processed_lines += 1
                    # Reduced progress reporting
                    if processed_lines % 10000000 == 0:
                        logger.info(
                            f"  Processed {processed_lines:,} lines, found {len(unique_customers):,} unique customers"
                        )

        logger.info(
            f"Found {len(unique_customers):,} unique customers from {processed_lines:,} total ratings"
        )
        return unique_customers

    def load(self) -> Dict[int, int]:
        """
        Load customer dimension and return mapping

        Returns:
            Dictionary mapping natural key (customer_id) to surrogate key (customer_key)
        """
        logger.info("Loading customer dimension...")

        # Get unique customers
        unique_customers = self.extract_unique_customers()

        # Create DataFrame
        df_customers = pd.DataFrame({"customer_id": sorted(unique_customers)})

        # Insert in batches (larger chunksize for better performance)
        df_customers.to_sql(
            "dim_customer",
            self.engine,
            schema=self.schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=50000,
        )

        logger.info(f"Loaded {len(df_customers)} customers into dim_customer")

        # Retrieve surrogate keys
        query = text(
            f"SELECT customer_id, customer_key FROM {self.schema}.dim_customer"
        )
        with self.engine.connect() as conn:
            df_mapping = pd.read_sql(query, conn)

        customer_mapping = dict(
            zip(df_mapping["customer_id"], df_mapping["customer_key"])
        )
        logger.info(
            f"Created customer_id -> customer_key mapping ({len(customer_mapping)} entries)"
        )

        return customer_mapping


# =====================================================
# FACT TABLE LOADER
# =====================================================


class FactRatingsLoader:
    """Loads fact_ratings table"""

    def __init__(
        self, engine, customer_mapping: Dict[int, int], movie_mapping: Dict[int, int]
    ):
        self.engine = engine
        self.schema = Config.SCHEMA_NAME
        self.customer_mapping = customer_mapping
        self.movie_mapping = movie_mapping
        self.total_ratings = 0
        self.skipped_ratings = 0

    def parse_combined_data_file(self, file_path: str) -> pd.DataFrame:
        """
        Parse a combined_data_*.txt file

        File format:
        MovieID:
        CustomerID,Rating,Date
        CustomerID,Rating,Date
        ...
        """
        logger.info(f"Parsing {os.path.basename(file_path)}...")

        ratings_data = []
        current_movie_id = None

        with open(file_path, "r") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()

                if not line:
                    continue

                if line.endswith(":"):
                    # Movie ID line
                    current_movie_id = int(line[:-1])
                else:
                    # Rating line
                    try:
                        parts = line.split(",")
                        customer_id = int(parts[0])
                        rating = int(parts[1])
                        date_str = parts[2]

                        ratings_data.append(
                            {
                                "movie_id": current_movie_id,
                                "customer_id": customer_id,
                                "rating": rating,
                                "date_str": date_str,
                            }
                        )

                    except (ValueError, IndexError) as e:
                        logger.warning(f"Skipping malformed line {line_num}: {line}")
                        self.skipped_ratings += 1

                # Progress reporting (reduced frequency for performance)
                if line_num % 5000000 == 0:
                    logger.info(f"  Processed {line_num:,} lines...")

        df = pd.DataFrame(ratings_data)
        logger.info(f"Parsed {len(df):,} ratings from {os.path.basename(file_path)}")

        return df

    def transform_ratings(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform ratings data with dimension lookups"""

        # Map to surrogate keys
        df["customer_key"] = df["customer_id"].map(self.customer_mapping)
        df["movie_key"] = df["movie_id"].map(self.movie_mapping)

        # Convert date to date_key (YYYYMMDD format)
        df["rating_timestamp"] = pd.to_datetime(df["date_str"])
        df["date_key"] = df["rating_timestamp"].dt.strftime("%Y%m%d").astype(int)

        # Filter out any rows with missing mappings
        before_count = len(df)
        df = df.dropna(subset=["customer_key", "movie_key"])
        after_count = len(df)

        if before_count != after_count:
            logger.warning(
                f"Dropped {before_count - after_count} rows due to missing dimension keys"
            )

        # Select final columns for fact table
        df_fact = df[
            ["customer_key", "movie_key", "date_key", "rating", "rating_timestamp"]
        ].copy()

        return df_fact

    def load_file(self, file_path: str):
        """Load a single combined_data file into fact table"""

        # Parse file
        df_ratings = self.parse_combined_data_file(file_path)

        if df_ratings.empty:
            logger.warning(f"No ratings found in {file_path}")
            return

        # Transform
        logger.info(f"Transforming {len(df_ratings):,} ratings...")
        df_fact = self.transform_ratings(df_ratings)

        # Load in chunks
        logger.info(f"Loading {len(df_fact):,} ratings into fact_ratings...")

        total_chunks = (len(df_fact) + Config.CHUNK_SIZE - 1) // Config.CHUNK_SIZE

        for chunk_num, start_idx in enumerate(
            range(0, len(df_fact), Config.CHUNK_SIZE), 1
        ):
            end_idx = min(start_idx + Config.CHUNK_SIZE, len(df_fact))
            df_chunk = df_fact.iloc[start_idx:end_idx]

            # Use method='multi' with larger chunksize for better performance
            df_chunk.to_sql(
                "fact_ratings",
                self.engine,
                schema=self.schema,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=10000,
            )

            self.total_ratings += len(df_chunk)

            # Progress reporting every 5 chunks to reduce log overhead
            if chunk_num % 5 == 0 or chunk_num == total_chunks:
                logger.info(
                    f"  Progress: {chunk_num}/{total_chunks} chunks "
                    f"({self.total_ratings:,} ratings, {(chunk_num/total_chunks)*100:.1f}%)"
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
            f"Fact table load complete! "
            f"Total ratings loaded: {self.total_ratings:,}, "
            f"Skipped: {self.skipped_ratings}"
        )


# =====================================================
# POST-LOAD UPDATES
# =====================================================


class PostLoadProcessor:
    """Performs post-load aggregations and updates"""

    def __init__(self, engine):
        self.engine = engine
        self.schema = Config.SCHEMA_NAME

    def disable_fact_indexes(self):
        """Temporarily drop non-PK indexes on fact table for faster loading"""
        logger.info("Disabling fact table indexes for faster bulk insert...")

        drop_indexes = [
            f"DROP INDEX IF EXISTS {self.schema}.idx_fact_ratings_customer;",
            f"DROP INDEX IF EXISTS {self.schema}.idx_fact_ratings_movie;",
            f"DROP INDEX IF EXISTS {self.schema}.idx_fact_ratings_date;",
            f"DROP INDEX IF EXISTS {self.schema}.idx_fact_ratings_customer_date;",
            f"DROP INDEX IF EXISTS {self.schema}.idx_fact_ratings_movie_date;",
            f"DROP INDEX IF EXISTS {self.schema}.idx_fact_ratings_rating;",
        ]

        with self.engine.begin() as conn:
            for sql in drop_indexes:
                try:
                    conn.execute(text(sql))
                except Exception as e:
                    logger.debug(f"Index drop note: {e}")

        logger.info("Indexes disabled")

    def rebuild_fact_indexes(self):
        """Rebuild indexes on fact table after bulk insert"""
        logger.info("Rebuilding fact table indexes (this will take time)...")

        create_indexes = [
            f"CREATE INDEX idx_fact_ratings_customer ON {self.schema}.fact_ratings(customer_key);",
            f"CREATE INDEX idx_fact_ratings_movie ON {self.schema}.fact_ratings(movie_key);",
            f"CREATE INDEX idx_fact_ratings_date ON {self.schema}.fact_ratings(date_key);",
            f"CREATE INDEX idx_fact_ratings_customer_date ON {self.schema}.fact_ratings(customer_key, date_key);",
            f"CREATE INDEX idx_fact_ratings_movie_date ON {self.schema}.fact_ratings(movie_key, date_key);",
            f"CREATE INDEX idx_fact_ratings_rating ON {self.schema}.fact_ratings(rating);",
        ]

        with self.engine.begin() as conn:
            for i, sql in enumerate(create_indexes, 1):
                logger.info(f"  Creating index {i}/{len(create_indexes)}...")
                conn.execute(text(sql))

        logger.info("All indexes rebuilt successfully")

    def update_customer_aggregates(self):
        """Update customer dimension with aggregated metrics"""
        logger.info("Updating customer aggregates...")

        update_sql = f"""
        UPDATE {self.schema}.dim_customer c
        SET 
            first_rating_date = agg.first_date,
            last_rating_date = agg.last_date,
            total_ratings = agg.rating_count
        FROM (
            SELECT 
                f.customer_key,
                MIN(d.date_actual) as first_date,
                MAX(d.date_actual) as last_date,
                COUNT(*) as rating_count
            FROM {self.schema}.fact_ratings f
            JOIN {self.schema}.dim_date d ON f.date_key = d.date_key
            GROUP BY f.customer_key
        ) agg
        WHERE c.customer_key = agg.customer_key;
        """

        with self.engine.begin() as conn:
            result = conn.execute(text(update_sql))
            logger.info(f"Updated {result.rowcount} customer records with aggregates")

    def analyze_tables(self):
        """Run ANALYZE on all tables for query optimization"""
        logger.info("Running ANALYZE on tables...")

        tables = ["dim_date", "dim_movie", "dim_customer", "fact_ratings"]

        with self.engine.begin() as conn:
            for table in tables:
                conn.execute(text(f"ANALYZE {self.schema}.{table};"))
                logger.info(f"  Analyzed {table}")

    def vacuum_fact_table(self):
        """Run VACUUM on fact table to reclaim space and update statistics"""
        logger.info("Running VACUUM on fact_ratings (this may take a few minutes)...")

        # VACUUM cannot run inside a transaction block
        with self.engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            conn.execute(text(f"VACUUM ANALYZE {self.schema}.fact_ratings;"))
            logger.info("VACUUM completed")

    def generate_summary(self):
        """Generate and log data warehouse summary"""
        logger.info("=" * 60)
        logger.info("DATA WAREHOUSE SUMMARY")
        logger.info("=" * 60)

        queries = {
            "dim_date": f"SELECT COUNT(*) FROM {self.schema}.dim_date",
            "dim_movie": f"SELECT COUNT(*) FROM {self.schema}.dim_movie",
            "dim_customer": f"SELECT COUNT(*) FROM {self.schema}.dim_customer",
            "fact_ratings": f"SELECT COUNT(*) FROM {self.schema}.fact_ratings",
        }

        with self.engine.connect() as conn:
            for table, query in queries.items():
                result = conn.execute(text(query))
                count = result.scalar()
                logger.info(f"{table:20} : {count:>15,} rows")

        # Additional stats
        stats_query = f"""
        SELECT 
            MIN(date_actual) as min_date,
            MAX(date_actual) as max_date,
            AVG(rating) as avg_rating
        FROM {self.schema}.fact_ratings f
        JOIN {self.schema}.dim_date d ON f.date_key = d.date_key;
        """

        with self.engine.connect() as conn:
            result = conn.execute(text(stats_query))
            row = result.fetchone()
            logger.info(f"{'Date Range':20} : {row[0]} to {row[1]}")
            logger.info(f"{'Average Rating':20} : {row[2]:.3f}")

        logger.info("=" * 60)


# =====================================================
# MAIN ETL ORCHESTRATOR
# =====================================================


def main():
    """Main ETL pipeline orchestrator"""

    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("NETFLIX PRIZE DATA WAREHOUSE - ETL PIPELINE")
    logger.info("=" * 60)
    logger.info(f"Start Time: {start_time}")

    try:
        # 1. Database Connection
        logger.info("\n[STEP 1/7] Establishing Database Connection")
        db = DatabaseConnection()
        engine = db.connect()

        # 2. Schema Creation
        logger.info("\n[STEP 2/7] Creating Database Schema")
        if os.path.exists("schema.sql"):
            db.execute_sql_file("schema.sql")
        else:
            logger.warning("schema.sql not found, assuming schema exists")

        # 3. Load Date Dimension
        logger.info("\n[STEP 3/7] Loading Date Dimension")
        date_loader = DateDimensionLoader(engine)
        date_loader.load()

        # 4. Load Movie Dimension
        logger.info("\n[STEP 4/7] Loading Movie Dimension")
        movie_loader = MovieDimensionLoader(engine)
        movie_mapping = movie_loader.load()

        # 5. Load Customer Dimension
        logger.info("\n[STEP 5/7] Loading Customer Dimension")
        customer_loader = CustomerDimensionLoader(engine)
        customer_mapping = customer_loader.load()

        # 6. Load Fact Table (with optimizations)
        logger.info("\n[STEP 6/7] Loading Fact Table")
        post_processor = PostLoadProcessor(engine)

        # 6a. Disable indexes for faster bulk insert
        post_processor.disable_fact_indexes()

        # 6b. Load all fact data
        fact_start = datetime.now()
        fact_loader = FactRatingsLoader(engine, customer_mapping, movie_mapping)
        fact_loader.load_all()
        fact_duration = datetime.now() - fact_start
        logger.info(f"Fact load completed in {fact_duration}")

        # 6c. Rebuild indexes
        post_processor.rebuild_fact_indexes()

        # 7. Post-Load Processing
        logger.info("\n[STEP 7/7] Post-Load Processing")
        post_processor.update_customer_aggregates()
        post_processor.vacuum_fact_table()
        post_processor.analyze_tables()
        post_processor.generate_summary()

        # Complete
        end_time = datetime.now()
        duration = end_time - start_time

        logger.info("\n" + "=" * 60)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        logger.info(f"End Time: {end_time}")
        logger.info(f"Total Duration: {duration}")
        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.error(f"\n{'='*60}")
        logger.error("ETL PIPELINE FAILED!")
        logger.error(f"{'='*60}")
        logger.error(f"Error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
