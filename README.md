# Netflix Prize Data Warehouse - Complete Solution

## 📦 Project Overview

A production-ready **Star Schema data warehouse** implementation for the Netflix Prize Dataset, designed for Azure Database for PostgreSQL.

**Dataset**: 100M+ ratings, 480K customers, 17K movies (1998-2005)

---

## 📁 Deliverables

### 1️⃣ **STAR_SCHEMA_DESIGN.md**

Comprehensive Star Schema documentation with:

- Visual schema diagram
- Design rationale and best practices
- Table specifications (fact + 3 dimensions)
- Indexing strategy
- Analytical capabilities

### 2️⃣ **schema.sql** (DDL Script)

Production-ready SQL including:

- Schema creation (`netflix_dw`)
- 4 tables: `fact_ratings`, `dim_movie`, `dim_customer`, `dim_date`
- Surrogate keys (SERIAL/BIGSERIAL)
- Foreign key constraints
- Strategic indexes
- Helper views for analytics
- Sample verification queries

### 3️⃣ **etl_pipeline.py** (Python ETL)

Robust, modular ETL pipeline with:

- **Environment-based config** (python-dotenv)
- **7-stage process**: Connection → Schema → 3 Dimensions → Fact → Post-processing
- **Efficient parsing**: Handles 100M+ rows with chunking
- **Dimension handling**: Deduplication + surrogate key mapping
- **Logging**: Console + file (`etl_pipeline.log`)
- **Idempotent design**: Safe to re-run
- **Progress tracking**: Real-time status updates

### 4️⃣ **SETUP_GUIDE.md** (Execution Manual)

Complete documentation including:

- Prerequisites and dependencies
- Step-by-step setup instructions
- Expected runtime benchmarks
- Common pitfalls & solutions
- Verification queries
- Sample analytical queries
- Troubleshooting commands

### 5️⃣ **Supporting Files**

- `.env.example` - Template for database credentials
- `requirements.txt` - Python dependencies
- `.gitignore` - Version control exclusions

---

## 🎯 Star Schema Summary

```
┌─────────────────┐
│   dim_date      │ (2,650 rows)
│ date_key (PK)   │
│ year, month...  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│ dim_customer    │      │  fact_ratings   │      │   dim_movie     │
│ customer_key(PK)│◄─────┤ rating_key (PK) ├─────►│ movie_key (PK)  │
│ customer_id     │      │ customer_key(FK)│      │ movie_id        │
│ ~480K rows      │      │ movie_key (FK)  │      │ title           │
└─────────────────┘      │ date_key (FK)   │      │ ~17K rows       │
                         │ rating (1-5)    │      └─────────────────┘
                         │ ~100M rows      │
                         └─────────────────┘
```

### Key Design Decisions

✅ **Surrogate Keys**: All dimensions use auto-increment surrogate keys  
✅ **Date Dimension**: Proper time dimension instead of raw dates  
✅ **Referential Integrity**: FK constraints enforced  
✅ **Denormalization**: Optimized for analytical queries  
✅ **Strategic Indexing**: Multi-column indexes for common patterns  
✅ **Type Optimization**: SMALLINT for ratings, BIGSERIAL for fact PK

---

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Database

```bash
cp .env.example .env
# Edit .env with your Azure PostgreSQL credentials
```

### 3. Run ETL Pipeline

```bash
python etl_pipeline.py
```

**Expected Duration**: 2.5-4.5 hours (100M rows)

---

## 📊 What Gets Created

| Object                   | Type  | Rows         | Description                |
| ------------------------ | ----- | ------------ | -------------------------- |
| `dim_date`               | Table | ~2,650       | Date dimension (1998-2005) |
| `dim_movie`              | Table | 17,770       | Movie dimension            |
| `dim_customer`           | Table | ~480,189     | Customer dimension         |
| `fact_ratings`           | Table | ~100,480,507 | Rating events (FACT)       |
| `v_daily_rating_summary` | View  | -            | Daily aggregates           |
| `v_movie_performance`    | View  | -            | Movie-level metrics        |

---

## 🎓 Star Schema Best Practices Applied

1. ✅ **Grain Definition**: One fact row = one rating event
2. ✅ **Conformed Dimensions**: Shared date dimension for temporal analysis
3. ✅ **Surrogate Keys**: Decoupled from natural keys
4. ✅ **Slowly Changing Dimensions**: Type 1 SCD (current state only)
5. ✅ **Fact Table Optimization**: Only FKs and measures
6. ✅ **Query Performance**: Denormalized dimensions
7. ✅ **Scalability**: Handles 100M+ rows efficiently
8. ✅ **BI Tool Ready**: Standard star schema pattern

---

## 🔍 Verification Checklist

After ETL completion, verify:

- [ ] **Row Counts Match**:

  - dim_date: 2,650 ✓
  - dim_movie: 17,770 ✓
  - dim_customer: ~480,189 ✓
  - fact_ratings: ~100,480,507 ✓

- [ ] **No Orphaned Records**: All FKs resolve

- [ ] **Data Quality**:

  - Ratings are 1-5 (integer)
  - Dates within 1998-2005
  - No NULL surrogate keys

- [ ] **Customer Aggregates Updated**:

  - first_rating_date populated
  - total_ratings > 0

- [ ] **Indexes Created**: Check with `\d+ fact_ratings` in psql

---

## 💡 Use Cases Enabled

### 1. **Collaborative Filtering (ML)**

```python
# User-item matrix for recommendation systems
query = """
SELECT customer_key, movie_key, rating
FROM netflix_dw.fact_ratings
"""
```

### 2. **Trend Analysis**

```sql
-- Rating volume over time
SELECT d.year, d.month, COUNT(*) as ratings
FROM netflix_dw.fact_ratings f
JOIN netflix_dw.dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
```

### 3. **Movie Recommendations**

```sql
-- Similar customers (who rated the same movies highly)
-- Top-rated movies by genre/year
-- Cold-start problem analysis
```

### 4. **Business Intelligence**

- Tableau/Power BI dashboards
- Customer segmentation
- Content performance analysis
- Temporal rating patterns

---

## ⚙️ Technical Specifications

### Database

- **Platform**: Azure Database for PostgreSQL (Flexible Server)
- **Schema**: `netflix_dw`
- **Total Size**: ~15-20 GB (including indexes)
- **Performance**: Optimized for OLAP queries

### Python Requirements

- **Version**: Python 3.9+
- **Key Libraries**: pandas, SQLAlchemy, psycopg2-binary, python-dotenv
- **Memory**: 4GB+ RAM recommended
- **Processing**: Single-threaded (can be parallelized)

### ETL Characteristics

- **Idempotent**: Safe to re-run
- **Chunked Processing**: 50K rows per batch
- **Error Handling**: Graceful skips for malformed data
- **Logging**: Detailed progress tracking
- **Resumability**: Can restart from schema creation

---

## 📈 Performance Metrics

### ETL Pipeline

| Stage              | Duration   | Throughput          |
| ------------------ | ---------- | ------------------- |
| Date Dimension     | ~2 sec     | 1,325 rows/sec      |
| Movie Dimension    | ~5 sec     | 3,554 rows/sec      |
| Customer Dimension | ~5 min     | 1,600 rows/sec      |
| **Fact Table**     | **~3 hrs** | **~9,300 rows/sec** |
| Post-Processing    | ~10 min    | -                   |

### Query Performance (Post-ANALYZE)

- Simple aggregations: <1 second
- Complex JOINs (3 tables): 1-5 seconds
- Full table scans: 10-30 seconds

**Note**: Performance depends on Azure tier (vCores, memory, IOPS)

---

## 🛡️ Data Governance

### Security

- ✅ Credentials via `.env` (not hardcoded)
- ✅ `.gitignore` prevents credential leaks
- ✅ Azure SSL/TLS encryption supported
- ✅ Role-based access control (configure in Azure)

### Data Quality

- ✅ CHECK constraints on ratings (1-5)
- ✅ Foreign key integrity enforced
- ✅ Unique constraints on natural keys
- ✅ NOT NULL on critical fields

### Audit Trail

- ✅ `created_at` timestamp on all tables
- ✅ `rating_timestamp` preserved in fact table
- ✅ ETL logs with timestamps

---

## 🔄 Maintenance

### Regular Tasks

1. **VACUUM ANALYZE** (weekly):

   ```sql
   VACUUM ANALYZE netflix_dw.fact_ratings;
   ```

2. **Index Maintenance** (monthly):

   ```sql
   REINDEX TABLE netflix_dw.fact_ratings;
   ```

3. **Backup** (daily):
   - Use Azure automated backups
   - Or: `pg_dump` for point-in-time snapshots

### Monitoring

- Track table sizes: `pg_total_relation_size()`
- Monitor query performance: `pg_stat_statements`
- Check index usage: `pg_stat_user_indexes`

---

## 📞 Next Steps

### Immediate Actions

1. ✅ Review `STAR_SCHEMA_DESIGN.md`
2. ✅ Configure `.env` file
3. ✅ Run `python etl_pipeline.py`
4. ✅ Verify data with queries in `SETUP_GUIDE.md`

### Future Enhancements

- [ ] Add movie genre dimension (from external data)
- [ ] Implement Type 2 SCD for customer evolution
- [ ] Create materialized views for popular aggregations
- [ ] Partition fact table by date for better performance
- [ ] Implement incremental loads (CDC)
- [ ] Add data quality monitoring
- [ ] Create Tableau/Power BI templates

---

## 🎉 Success Criteria

You'll know it worked when:

✅ All tables created with correct row counts  
✅ No referential integrity violations  
✅ Sample analytical queries return results in <5 seconds  
✅ Customer aggregates populated correctly  
✅ `etl_pipeline.log` shows "COMPLETED SUCCESSFULLY"  
✅ You can run ML models on the user-item rating matrix

---

## 📚 Additional Resources

- **Dataset**: [Netflix Prize on Academic Torrents](http://academictorrents.com/details/9b13183dc4d60676b773c9e2cd6de5e5542cee9a)
- **Star Schema**: [Kimball Group - Data Warehouse Toolkit](https://www.kimballgroup.com/)
- **PostgreSQL**: [Official Documentation](https://www.postgresql.org/docs/)
- **Azure PostgreSQL**: [Microsoft Docs](https://docs.microsoft.com/en-us/azure/postgresql/)

---

**Project Status**: ✅ Production Ready  
**Version**: 1.0  
**Last Updated**: December 14, 2025

**Questions?** Check `SETUP_GUIDE.md` for troubleshooting.
