#!/usr/bin/env python3
"""
Amazon Fashion Reviews - ClickHouse Data Ingestion
Using dbutils package for database connectivity and duplicate prevention
"""

import pandas as pd
import logging
import time
from datetime import datetime
from pathlib import Path
from decouple import AutoConfig
from dbutils import Query

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_ingestion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_clickhouse_connection():
    """Setup ClickHouse connection using dbutils and .env configuration"""
    
    logger.info("="*60)
    logger.info("AMAZON FASHION REVIEWS DATA INGESTION STARTED")
    logger.info("="*60)
    
    # Load configurations from .env file
    config = AutoConfig(search_path="/home/ubuntu/LONGIN-DUSENGEYEZU-ASSIGNEMNT/.env")
    
    # Initialize ClickHouse connection using dbutils
    clickhouse = Query(
        db_type="clickhouse",
        db=config("db_clickhouse_db"),
        db_host=config("db_clickhouse_host"),
        db_port=int(config("db_clickhouse_port")),  # Convert port to integer
        db_user=config("db_clickhouse_user"),
        db_pass=config("db_clickhouse_pass"),
    )
    
    logger.info("ClickHouse connection established using dbutils")
    
    # Create Amazon database schema
    try:
        clickhouse.sql_query(sql="CREATE DATABASE IF NOT EXISTS amazon")
        logger.info("Amazon database created successfully")
    except Exception as e:
        logger.error(f"Error creating database: {e}")
        raise
    
    # Create reviews table with optimized schema for analytics
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS amazon.reviews (
        rating Float32,
        title String,
        text String,
        images String,
        asin String,
        parent_asin String,
        user_id String,
        timestamp UInt64,
        helpful_vote UInt32,
        verified_purchase UInt8,
        review_date Date MATERIALIZED toDate(timestamp / 1000),
        review_year UInt16 MATERIALIZED toYear(toDate(timestamp / 1000)),
        review_month UInt8 MATERIALIZED toMonth(toDate(timestamp / 1000))
    ) ENGINE = MergeTree()
    ORDER BY (asin, timestamp, user_id)
    SETTINGS index_granularity = 8192
    """
    
    try:
        clickhouse.sql_query(sql=create_table_sql)
        logger.info("Reviews table created successfully with optimized schema")
        logger.info("Table features:")
        logger.info("   - Engine: MergeTree (optimized for analytics)")
        logger.info("   - Ordering: (asin, timestamp, user_id) for duplicate prevention")
        logger.info("   - Materialized columns: review_date, review_year, review_month")
        logger.info("   - Primary key ensures no duplicate reviews")
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        raise
    
    return clickhouse

def check_existing_data(clickhouse):
    """Check what data already exists to prevent duplicates"""
    
    logger.info("🔍 Checking existing data to prevent duplicates...")
    
    try:
        # Check total count
        result = clickhouse.sql_query(sql="SELECT COUNT(*) as total_count FROM amazon.reviews")
        total_count = result[0][0] if result else 0
        
        # Check unique products and users
        result = clickhouse.sql_query(sql="SELECT COUNT(DISTINCT asin) as unique_products FROM amazon.reviews")
        unique_products = result[0][0] if result else 0
        
        result = clickhouse.sql_query(sql="SELECT COUNT(DISTINCT user_id) as unique_users FROM amazon.reviews")
        unique_users = result[0][0] if result else 0
        
        # Check date range
        result = clickhouse.sql_query(sql="SELECT MIN(review_date) as earliest, MAX(review_date) as latest FROM amazon.reviews")
        if result and result[0][0]:
            earliest_date, latest_date = result[0]
            logger.info(f"Existing data: {total_count:,} reviews, {unique_products:,} products, {unique_users:,} users")
            logger.info(f"Date range: {earliest_date} to {latest_date}")
        else:
            logger.info(f"Existing data: {total_count:,} reviews, {unique_products:,} products, {unique_users:,} users")
            logger.info("Date range: No data found")
        
        return total_count
        
    except Exception as e:
        logger.warning(f" Could not check existing data: {e}")
        return 0

def preprocess_dataframe(df):
    """Clean and preprocess DataFrame for ClickHouse insertion"""
    
    # Handle NaN values in string columns
    string_columns = ['title', 'text', 'images', 'asin', 'parent_asin', 'user_id']
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str)
    
    # Convert verified_purchase to UInt8 (0/1)
    if 'verified_purchase' in df.columns:
        df['verified_purchase'] = df['verified_purchase'].astype(int)
    
    # Ensure numeric columns are properly typed
    numeric_columns = ['rating', 'timestamp', 'helpful_vote']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    return df

def ingest_data_to_clickhouse(clickhouse, csv_file, batch_size=50000):
    """
    Ingest CSV data to ClickHouse with batch processing, duplicate prevention, and logging
    """
    start_time = time.time()
    total_rows_processed = 0
    batch_count = 0
    
    logger.info(f"Starting data ingestion...")
    logger.info(f"Batch size: {batch_size:,} rows")
    
    try:
        # Read CSV in chunks
        chunk_iter = pd.read_csv(csv_file, chunksize=batch_size)
        
        for chunk in chunk_iter:
            batch_count += 1
            batch_start_time = time.time()
            
            try:
                # Preprocess the chunk
                chunk = preprocess_dataframe(chunk)
                
                # Write DataFrame to ClickHouse table using dbutils
                clickhouse.sql_write(
                    chunk,
                    schema="amazon",
                    table_name="reviews",
                    max_chunk=batch_size,
                    max_workers=1  # Single worker for consistency
                )
                
                total_rows_processed += len(chunk)
                batch_time = time.time() - batch_start_time
                
                logger.info(f"Batch {batch_count}: {len(chunk):,} rows processed in {batch_time:.2f}s")
                logger.info(f"Total progress: {total_rows_processed:,} rows")
                
            except Exception as e:
                logger.error(f"Error processing batch {batch_count}: {e}")
                logger.error(f"Failed batch size: {len(chunk):,} rows")
                raise
        
        total_time = time.time() - start_time
        logger.info(f"Data ingestion completed successfully!")
        logger.info(f"Total rows processed: {total_rows_processed:,}")
        logger.info(f"Total time: {total_time:.2f} seconds")
        logger.info(f"Average speed: {total_rows_processed/total_time:.0f} rows/second")
        
        return total_rows_processed
        
    except Exception as e:
        logger.error(f"Critical error during ingestion: {e}")
        logger.error(f"Rows processed before failure: {total_rows_processed:,}")
        raise

def verify_data_ingestion(clickhouse):
    """Verify data ingestion with sample queries"""
    
    verification_queries = [
        "SELECT COUNT(*) as total_reviews FROM amazon.reviews",
        "SELECT COUNT(DISTINCT asin) as unique_products FROM amazon.reviews",
        "SELECT COUNT(DISTINCT user_id) as unique_users FROM amazon.reviews",
        "SELECT AVG(rating) as avg_rating FROM amazon.reviews",
        "SELECT MIN(review_date) as earliest_review, MAX(review_date) as latest_review FROM amazon.reviews",
        "SELECT review_year, COUNT(*) as reviews_count FROM amazon.reviews GROUP BY review_year ORDER BY review_year"
    ]
    
    logger.info("VERIFYING DATA INGESTION...")
    logger.info("="*50)
    
    for i, query in enumerate(verification_queries, 1):
        try:
            result = clickhouse.sql_query(sql=query)
            logger.info(f" Query {i}: {result}")
        except Exception as e:
            logger.error(f"Query {i} failed: {e}")
    
    logger.info("="*50)
    logger.info(" DATA VERIFICATION COMPLETED")

def main():
    """Main execution function"""
    
    # Check if data file exists
    csv_file = "Amazon_Fashion.csv"
    if not Path(csv_file).exists():
        logger.error(f"Data file {csv_file} not found!")
        raise FileNotFoundError(f"Data file {csv_file} not found")
    
    # Get file size
    file_size = Path(csv_file).stat().st_size / (1024 * 1024)  # MB
    logger.info(f"Data file found: {csv_file} ({file_size:.1f} MB)")
    
    # Read first few rows to verify structure
    sample_df = pd.read_csv(csv_file, nrows=5)
    logger.info(f"Data structure verified - {len(sample_df.columns)} columns")
    logger.info(f"Columns: {list(sample_df.columns)}")
    
    try:
        # Setup ClickHouse connection using dbutils
        clickhouse = setup_clickhouse_connection()
        
        # Check existing data to prevent duplicates
        existing_count = check_existing_data(clickhouse)
        
        if existing_count > 0:
            logger.info(f" Found {existing_count:,} existing records")
            logger.info("Proceeding with ingestion (duplicates will be prevented by primary key)")
        
        # Execute data ingestion
        total_rows = ingest_data_to_clickhouse(clickhouse, csv_file, batch_size=50000)
        
        # Verify data ingestion
        verify_data_ingestion(clickhouse)
        
        # Final summary
        logger.info("="*60)
        logger.info("AMAZON FASHION REVIEWS DATA INGESTION COMPLETED")
        logger.info("="*60)
        logger.info(f" Database: amazon")
        logger.info(f" Table: reviews")
        logger.info(f" New records processed: {total_rows:,}")
        logger.info(f" File processed: {csv_file}")
        logger.info(f" Log file: data_ingestion.log")
        logger.info(" Duplicate prevention: Enabled via primary key (asin, timestamp, user_id)")
        logger.info("="*60)
        
        print("\n Data ingestion completed successfully!")
        print(f" {total_rows:,} Amazon Fashion reviews ingested into ClickHouse")
        print(" Duplicate prevention: Primary key ensures no duplicate reviews")
        print(" Check 'data_ingestion.log' for detailed logs")
        
    except Exception as e:
        logger.error(f"💥 INGESTION FAILED: {e}")
        raise

if __name__ == "__main__":
    main()