-- =====================================================
-- Netflix Prize Data Warehouse - DDL Script
-- Star Schema Implementation
-- Database: PostgreSQL (Azure Flexible Server)
-- =====================================================

-- =====================================================
-- SCHEMA CREATION
-- =====================================================

CREATE SCHEMA IF NOT EXISTS netflix_dw;

SET search_path TO netflix_dw, public;

-- =====================================================
-- DROP EXISTING TABLES (for idempotent execution)
-- =====================================================

DROP TABLE IF EXISTS fact_ratings CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_movie CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

-- =====================================================
-- DIMENSION TABLE: dim_date
-- =====================================================

CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    date_actual DATE NOT NULL UNIQUE,
    year SMALLINT NOT NULL,
    month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
    day SMALLINT NOT NULL CHECK (day BETWEEN 1 AND 31),
    quarter SMALLINT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    day_of_week SMALLINT NOT NULL CHECK (day_of_week BETWEEN 0 AND 6),
    month_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for date lookups
CREATE INDEX idx_dim_date_actual ON dim_date(date_actual);
CREATE INDEX idx_dim_date_year_month ON dim_date(year, month);

COMMENT ON TABLE dim_date IS 'Date dimension covering 1998-2005 for temporal analysis';
COMMENT ON COLUMN dim_date.date_key IS 'Surrogate key in YYYYMMDD format (e.g., 20050101)';
COMMENT ON COLUMN dim_date.day_of_week IS '0=Monday, 6=Sunday';

-- =====================================================
-- DIMENSION TABLE: dim_movie
-- =====================================================

CREATE TABLE dim_movie (
    movie_key SERIAL PRIMARY KEY,
    movie_id INTEGER NOT NULL UNIQUE,
    title VARCHAR(500) NOT NULL,
    release_year SMALLINT CHECK (release_year BETWEEN 1890 AND 2010),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for natural key lookup
CREATE UNIQUE INDEX idx_dim_movie_id ON dim_movie(movie_id);
CREATE INDEX idx_dim_movie_year ON dim_movie(release_year);

COMMENT ON TABLE dim_movie IS 'Movie dimension with 17,770 Netflix movies';
COMMENT ON COLUMN dim_movie.movie_key IS 'Surrogate key (auto-increment)';
COMMENT ON COLUMN dim_movie.movie_id IS 'Natural key from Netflix dataset (1-17770)';

-- =====================================================
-- DIMENSION TABLE: dim_customer
-- =====================================================

CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL UNIQUE,
    first_rating_date DATE,
    last_rating_date DATE,
    total_ratings INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for natural key lookup
CREATE UNIQUE INDEX idx_dim_customer_id ON dim_customer(customer_id);
CREATE INDEX idx_dim_customer_rating_dates ON dim_customer(first_rating_date, last_rating_date);

COMMENT ON TABLE dim_customer IS 'Customer dimension with ~480K anonymous customers';
COMMENT ON COLUMN dim_customer.customer_key IS 'Surrogate key (auto-increment)';
COMMENT ON COLUMN dim_customer.customer_id IS 'Natural key from Netflix dataset (anonymized)';
COMMENT ON COLUMN dim_customer.total_ratings IS 'Aggregate count updated after fact load';

-- =====================================================
-- FACT TABLE: fact_ratings
-- =====================================================

CREATE TABLE fact_ratings (
    rating_key BIGSERIAL PRIMARY KEY,
    customer_key INTEGER NOT NULL,
    movie_key INTEGER NOT NULL,
    date_key INTEGER NOT NULL,
    rating SMALLINT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    rating_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign Key Constraints
    CONSTRAINT fk_customer FOREIGN KEY (customer_key) 
        REFERENCES dim_customer(customer_key) ON DELETE CASCADE,
    CONSTRAINT fk_movie FOREIGN KEY (movie_key) 
        REFERENCES dim_movie(movie_key) ON DELETE CASCADE,
    CONSTRAINT fk_date FOREIGN KEY (date_key) 
        REFERENCES dim_date(date_key) ON DELETE CASCADE
);

-- Performance Indexes
CREATE INDEX idx_fact_ratings_customer ON fact_ratings(customer_key);
CREATE INDEX idx_fact_ratings_movie ON fact_ratings(movie_key);
CREATE INDEX idx_fact_ratings_date ON fact_ratings(date_key);
CREATE INDEX idx_fact_ratings_customer_date ON fact_ratings(customer_key, date_key);
CREATE INDEX idx_fact_ratings_movie_date ON fact_ratings(movie_key, date_key);
CREATE INDEX idx_fact_ratings_rating ON fact_ratings(rating);

COMMENT ON TABLE fact_ratings IS 'Fact table storing 100M+ rating events';
COMMENT ON COLUMN fact_ratings.rating_key IS 'Surrogate primary key';
COMMENT ON COLUMN fact_ratings.rating IS 'Rating value: 1 (worst) to 5 (best)';
COMMENT ON COLUMN fact_ratings.rating_timestamp IS 'Original timestamp for audit trail';

-- =====================================================
-- HELPER VIEWS (Optional)
-- =====================================================

-- View: Daily Rating Summary
CREATE OR REPLACE VIEW v_daily_rating_summary AS
SELECT 
    d.date_actual,
    d.year,
    d.month,
    d.month_name,
    COUNT(*) as total_ratings,
    AVG(f.rating) as avg_rating,
    COUNT(DISTINCT f.customer_key) as unique_customers,
    COUNT(DISTINCT f.movie_key) as unique_movies
FROM fact_ratings f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.date_actual, d.year, d.month, d.month_name
ORDER BY d.date_actual;

COMMENT ON VIEW v_daily_rating_summary IS 'Daily aggregated rating statistics';

-- View: Movie Performance
CREATE OR REPLACE VIEW v_movie_performance AS
SELECT 
    m.movie_id,
    m.title,
    m.release_year,
    COUNT(*) as total_ratings,
    AVG(f.rating) as avg_rating,
    COUNT(DISTINCT f.customer_key) as unique_raters
FROM fact_ratings f
JOIN dim_movie m ON f.movie_key = m.movie_key
GROUP BY m.movie_id, m.title, m.release_year
ORDER BY total_ratings DESC;

COMMENT ON VIEW v_movie_performance IS 'Movie-level aggregated metrics';

-- =====================================================
-- STATISTICS & MAINTENANCE
-- =====================================================

-- Analyze tables for query optimization (run after data load)
-- ANALYZE dim_date;
-- ANALYZE dim_movie;
-- ANALYZE dim_customer;
-- ANALYZE fact_ratings;

-- =====================================================
-- VERIFICATION QUERIES
-- =====================================================

-- Check row counts
-- SELECT 'dim_date' as table_name, COUNT(*) as row_count FROM dim_date
-- UNION ALL
-- SELECT 'dim_movie', COUNT(*) FROM dim_movie
-- UNION ALL
-- SELECT 'dim_customer', COUNT(*) FROM dim_customer
-- UNION ALL
-- SELECT 'fact_ratings', COUNT(*) FROM fact_ratings;

-- Check referential integrity
-- SELECT COUNT(*) as orphaned_ratings
-- FROM fact_ratings f
-- LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
-- WHERE c.customer_key IS NULL;

-- =====================================================
-- SAMPLE ANALYTICAL QUERIES
-- =====================================================

-- Top 10 highest rated movies (min 100 ratings)
/*
SELECT 
    m.title,
    m.release_year,
    COUNT(*) as rating_count,
    AVG(f.rating) as avg_rating,
    ROUND(AVG(f.rating)::numeric, 2) as avg_rating_rounded
FROM fact_ratings f
JOIN dim_movie m ON f.movie_key = m.movie_key
GROUP BY m.movie_id, m.title, m.release_year
HAVING COUNT(*) >= 100
ORDER BY avg_rating DESC, rating_count DESC
LIMIT 10;
*/

-- Rating trends by year
/*
SELECT 
    d.year,
    COUNT(*) as total_ratings,
    AVG(f.rating) as avg_rating,
    COUNT(DISTINCT f.customer_key) as active_customers,
    COUNT(DISTINCT f.movie_key) as rated_movies
FROM fact_ratings f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year
ORDER BY d.year;
*/

-- Most active customers
/*
SELECT 
    c.customer_id,
    c.total_ratings,
    c.first_rating_date,
    c.last_rating_date,
    (c.last_rating_date - c.first_rating_date) as days_active
FROM dim_customer c
ORDER BY c.total_ratings DESC
LIMIT 20;
*/
