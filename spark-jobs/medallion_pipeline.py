"""
Morocco Census Data Lake - Medallion Architecture Pipeline
============================================================
Main orchestrator for the Bronze → Silver → Gold pipeline.

Medallion Architecture:
- Bronze: Raw data ingestion from Kafka (append-only)
- Silver: Cleansed, validated, and enriched data
- Gold: Business-level aggregations for analytics

Usage:
    spark-submit medallion_pipeline.py [--mode streaming|batch]
"""

import sys
import time
import argparse
from pyspark.sql import SparkSession

# Import layer modules
from bronze_ingestion import create_bronze_tables, process_bronze_stream, get_bronze_stats
from silver_processing import create_silver_tables, process_silver_batch, process_silver_stream, get_silver_stats
from gold_aggregations import create_gold_tables, process_gold_layer, get_gold_stats


def create_spark_session():
    """Create and configure Spark session for Iceberg + Nessie"""
    
    print("🔧 Initializing Spark session...")
    
    spark = SparkSession.builder \
        .appName("Morocco Census - Medallion Pipeline") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.nessie.uri", "http://catalog:19120/api/v1") \
        .config("spark.sql.catalog.nessie.ref", "main") \
        .config("spark.sql.catalog.nessie.authentication.type", "NONE") \
        .config("spark.sql.catalog.nessie.warehouse", "s3a://bronze/") \
        .config("spark.sql.catalog.nessie.cache-enabled", "true") \
        .config("spark.sql.defaultCatalog", "nessie") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://storage:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.streaming.checkpointLocation", "s3a://checkpoints/") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Spark session initialized")
    return spark


def initialize_tables(spark: SparkSession):
    """Create all medallion layer tables"""
    
    print("\n" + "=" * 70)
    print("📦 INITIALIZING MEDALLION ARCHITECTURE TABLES")
    print("=" * 70)
    
    create_bronze_tables(spark)
    create_silver_tables(spark)
    create_gold_tables(spark)
    
    print("\n✅ All medallion tables initialized")


def run_streaming_pipeline(spark: SparkSession):
    """
    Run the complete medallion pipeline in streaming mode.
    Bronze streams from Kafka, Silver reads from Bronze, Gold is computed periodically.
    """
    
    print("\n" + "=" * 70)
    print("🚀 STARTING STREAMING MEDALLION PIPELINE")
    print("=" * 70)
    print("Mode: Streaming")
    print("Bronze: Kafka → Iceberg (continuous)")
    print("Silver: Bronze → Iceberg (micro-batch)")
    print("Gold: Silver → Iceberg (periodic)")
    print("=" * 70)
    
    try:
        # Start Bronze streaming (Kafka → Bronze Iceberg)
        bronze_query = process_bronze_stream(spark)
        
        # Give Bronze time to start and ingest some data
        print("\n⏳ Waiting for initial Bronze data (30 seconds)...")
        time.sleep(30)
        
        # Run initial Silver processing
        print("\n🔄 Running initial Silver processing...")
        process_silver_batch(spark)
        
        # Run initial Gold processing
        print("\n🔄 Running initial Gold processing...")
        process_gold_layer(spark)
        
        # Main processing loop
        gold_interval = 60  # Seconds between Gold refreshes
        silver_interval = 30  # Seconds between Silver processing
        last_silver = time.time()
        last_gold = time.time()
        
        print("\n" + "=" * 70)
        print("🔄 MEDALLION PIPELINE RUNNING")
        print(f"Bronze: Continuous streaming")
        print(f"Silver: Every {silver_interval}s")
        print(f"Gold: Every {gold_interval}s")
        print("=" * 70)
        print("Press Ctrl+C to stop...")
        
        while bronze_query.isActive:
            current_time = time.time()
            
            # Process Silver periodically
            if current_time - last_silver >= silver_interval:
                print("\n🔄 Processing Silver layer...")
                process_silver_batch(spark)
                last_silver = current_time
            
            # Process Gold periodically
            if current_time - last_gold >= gold_interval:
                print("\n🔄 Processing Gold layer...")
                process_gold_layer(spark)
                last_gold = current_time
            
            # Brief sleep to prevent busy waiting
            time.sleep(5)
        
    except KeyboardInterrupt:
        print("\n🛑 Shutdown requested...")
    except Exception as e:
        print(f"❌ Pipeline error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n📊 Final Statistics:")
        get_bronze_stats(spark)
        get_silver_stats(spark)
        get_gold_stats(spark)
        
        if 'bronze_query' in locals() and bronze_query.isActive:
            bronze_query.stop()
        
        print("\n✅ Pipeline stopped")


def run_batch_pipeline(spark: SparkSession):
    """
    Run the medallion pipeline in batch mode.
    Processes all layers sequentially.
    """
    
    print("\n" + "=" * 70)
    print("🚀 STARTING BATCH MEDALLION PIPELINE")
    print("=" * 70)
    print("Mode: Batch")
    print("Processing: Bronze → Silver → Gold (sequential)")
    print("=" * 70)
    
    try:
        # Process Bronze (read from Kafka in batch mode)
        print("\n🥉 Processing Bronze layer...")
        bronze_query = process_bronze_stream(spark)
        
        # Wait for some data
        print("⏳ Ingesting data for 60 seconds...")
        time.sleep(60)
        
        # Stop Bronze streaming for batch mode
        if bronze_query and bronze_query.isActive:
            bronze_query.stop()
        
        get_bronze_stats(spark)
        
        # Process Silver
        print("\n🥈 Processing Silver layer...")
        process_silver_batch(spark)
        get_silver_stats(spark)
        
        # Process Gold
        print("\n🥇 Processing Gold layer...")
        process_gold_layer(spark)
        get_gold_stats(spark)
        
        print("\n" + "=" * 70)
        print("✅ BATCH PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 70)
        
    except Exception as e:
        print(f"❌ Pipeline error: {str(e)}")
        import traceback
        traceback.print_exc()


def display_layer_info():
    """Display medallion architecture information"""
    
    info = """
╔══════════════════════════════════════════════════════════════════════╗
║           MOROCCO CENSUS DATA LAKE - MEDALLION ARCHITECTURE           ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                        ║
║  🥉 BRONZE LAYER (Raw Data)                                           ║
║  ├── Source: Kafka (census_persons_topic)                             ║
║  ├── Target: nessie.bronze.census_persons                             ║
║  ├── Mode: Append-only, no transformations                            ║
║  └── Contains: Raw census records with Kafka metadata                 ║
║                                                                        ║
║  🥈 SILVER LAYER (Cleansed Data)                                      ║
║  ├── Source: Bronze layer                                             ║
║  ├── Target: nessie.silver.census_persons                             ║
║  ├── Transformations:                                                 ║
║  │   ├── Data cleansing & standardization                             ║
║  │   ├── Validation & quality checks                                  ║
║  │   ├── Derived columns (age_group, income_level, etc.)              ║
║  │   └── Invalid records → quarantine table                           ║
║  └── Contains: Validated, enriched census records                     ║
║                                                                        ║
║  🥇 GOLD LAYER (Business Aggregations)                                ║
║  ├── Source: Silver layer                                             ║
║  ├── Tables:                                                          ║
║  │   ├── nessie.gold.regional_demographics                            ║
║  │   ├── nessie.gold.age_distribution                                 ║
║  │   ├── nessie.gold.employment_stats                                 ║
║  │   ├── nessie.gold.education_stats                                  ║
║  │   ├── nessie.gold.city_stats                                       ║
║  │   └── nessie.gold.national_summary                                 ║
║  └── Contains: Analytics-ready aggregated data                        ║
║                                                                        ║
╚══════════════════════════════════════════════════════════════════════╝
    """
    print(info)


def main():
    """Main entry point for the medallion pipeline"""
    
    # Parse arguments
    parser = argparse.ArgumentParser(description="Morocco Census Medallion Pipeline")
    parser.add_argument("--mode", choices=["streaming", "batch"], default="streaming",
                        help="Pipeline mode: streaming (continuous) or batch (one-time)")
    args = parser.parse_args()
    
    print("\n" + "=" * 70)
    print("🇲🇦 MOROCCO CENSUS DATA LAKE")
    print("   Medallion Architecture Pipeline")
    print("=" * 70)
    
    display_layer_info()
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Initialize all tables
        initialize_tables(spark)
        
        # Run appropriate pipeline mode
        if args.mode == "streaming":
            run_streaming_pipeline(spark)
        else:
            run_batch_pipeline(spark)
        
    except Exception as e:
        print(f"❌ Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()
        print("\n🏁 Spark session closed")


if __name__ == "__main__":
    main()
