"""
Morocco Census Data Lake - Bronze Layer Ingestion
==================================================
Ingests raw census data from Kafka into Bronze Iceberg tables.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, current_timestamp
)
from schemas import CENSUS_PERSON_SCHEMA


def create_bronze_tables(spark: SparkSession):
    """Create Bronze layer tables in Iceberg"""
    
    print("\n🥉 Creating Bronze layer tables...")
    
    # Create bronze namespace
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")
    
    # Create census_persons table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze.census_persons (
            person_id STRING,
            first_name STRING,
            last_name STRING,
            full_name STRING,
            age INT,
            gender STRING,
            marital_status STRING,
            region_code STRING,
            region_name STRING,
            city STRING,
            education_level STRING,
            employment_status STRING,
            occupation STRING,
            monthly_income DOUBLE,
            housing_type STRING,
            household_size INT,
            has_health_insurance BOOLEAN,
            event_time STRING,
            kafka_partition INT,
            kafka_offset LONG,
            kafka_timestamp TIMESTAMP,
            ingestion_timestamp TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(ingestion_timestamp), region_code)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)
    
    print("✅ Bronze tables created")


def process_bronze_stream(spark: SparkSession):
    """Stream data from Kafka to Bronze Iceberg table"""
    
    print("\n🥉 Starting Bronze streaming ingestion...")
    print("   Source: kafka:9092 / census_persons_topic")
    print("   Target: nessie.bronze.census_persons")
    
    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "census_persons_topic") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON and flatten structure
    bronze_df = kafka_df.select(
        from_json(
            col("value").cast("string"), 
            CENSUS_PERSON_SCHEMA
        ).alias("data"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp")
    ).select(
        col("data.person_id"),
        col("data.first_name"),
        col("data.last_name"),
        col("data.full_name"),
        col("data.age"),
        col("data.gender"),
        col("data.marital_status"),
        col("data.region_code"),
        col("data.region_name"),
        col("data.city"),
        col("data.education_level"),
        col("data.employment_status"),
        col("data.occupation"),
        col("data.monthly_income"),
        col("data.housing_type"),
        col("data.household_size"),
        col("data.has_health_insurance"),
        col("data.event_time"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),
        current_timestamp().alias("ingestion_timestamp")
    )
    
    # Write to Iceberg with foreachBatch
    def write_batch(batch_df: DataFrame, batch_id: int):
        if batch_df.count() > 0:
            print(f"\n{'=' * 60}")
            print(f"🥉 BRONZE Batch {batch_id}")
            print(f"{'=' * 60}")
            print(f"📊 Records: {batch_df.count()}")
            
            # Show sample
            print("\n📋 Sample records:")
            batch_df.select(
                "person_id", "full_name", "age", "city", "region_name"
            ).show(5, truncate=False)
            
            # Write to Iceberg
            batch_df.writeTo("nessie.bronze.census_persons").append()
            print(f"✅ Batch {batch_id} written to Bronze")
    
    # Start streaming query
    query = bronze_df.writeStream \
        .foreachBatch(write_batch) \
        .option("checkpointLocation", "s3a://checkpoints/bronze/census_persons") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    print("✅ Bronze streaming started")
    return query


def get_bronze_stats(spark: SparkSession):
    """Get statistics from Bronze layer"""
    
    try:
        stats = spark.sql("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT region_code) as regions,
                COUNT(DISTINCT city) as cities,
                MIN(ingestion_timestamp) as first_ingestion,
                MAX(ingestion_timestamp) as last_ingestion
            FROM nessie.bronze.census_persons
        """).collect()[0]
        
        print("\n📊 Bronze Layer Statistics:")
        print(f"   Total Records: {stats['total_records']:,}")
        print(f"   Regions: {stats['regions']}")
        print(f"   Cities: {stats['cities']}")
        print(f"   First Ingestion: {stats['first_ingestion']}")
        print(f"   Last Ingestion: {stats['last_ingestion']}")
        
    except Exception as e:
        print(f"⚠️ Could not get Bronze stats: {e}")