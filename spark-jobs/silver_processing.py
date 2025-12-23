"""
Morocco Census Data Lake - Silver Layer Processing
===================================================
Cleanses, validates, and enriches data from Bronze layer.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, trim, lower, initcap, length, coalesce, lit,
    current_timestamp
)


def create_silver_tables(spark: SparkSession):
    """Create Silver layer tables in Iceberg"""
    
    print("\n🥈 Creating Silver layer tables...")
    
    # Create silver namespace
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
    
    # Drop existing tables to ensure clean schema
    spark.sql("DROP TABLE IF EXISTS nessie.silver.census_persons")
    spark.sql("DROP TABLE IF EXISTS nessie.silver.census_persons_quarantine")
    
    # Create validated census_persons table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver.census_persons (
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
            age_group STRING,
            is_employed BOOLEAN,
            income_level STRING,
            location_type STRING,
            education_years INT,
            is_valid BOOLEAN,
            validation_errors STRING,
            bronze_ingestion_timestamp TIMESTAMP,
            silver_processing_timestamp TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (region_code)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)
    
    # Create quarantine table for invalid records
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver.census_persons_quarantine (
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
            validation_errors STRING,
            quarantine_timestamp TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)
    
    print("✅ Silver tables created")


def cleanse_data(df: DataFrame) -> DataFrame:
    """Apply data cleansing transformations"""
    
    return df \
        .withColumn("first_name", initcap(trim(col("first_name")))) \
        .withColumn("last_name", initcap(trim(col("last_name")))) \
        .withColumn("full_name", initcap(trim(col("full_name")))) \
        .withColumn("gender", lower(trim(col("gender")))) \
        .withColumn("marital_status", lower(trim(col("marital_status")))) \
        .withColumn("city", initcap(trim(col("city")))) \
        .withColumn("region_name", trim(col("region_name"))) \
        .withColumn("education_level", lower(trim(col("education_level")))) \
        .withColumn("employment_status", lower(trim(col("employment_status")))) \
        .withColumn("housing_type", lower(trim(col("housing_type")))) \
        .withColumn("age", coalesce(col("age"), lit(0))) \
        .withColumn("household_size", coalesce(col("household_size"), lit(1))) \
        .withColumn("monthly_income", coalesce(col("monthly_income"), lit(0.0)))


def validate_data(df: DataFrame) -> DataFrame:
    """Apply validation rules and flag invalid records"""
    
    # Define validation rules
    validated_df = df.withColumn("is_valid",
        (col("person_id").isNotNull()) &
        (length(col("person_id")) > 0) &
        (col("age") >= 0) &
        (col("age") <= 120) &
        (col("gender").isin("male", "female")) &
        (length(col("full_name")) >= 2) &
        (col("region_code").isNotNull()) &
        (col("household_size") >= 1) &
        (col("household_size") <= 20) &
        (col("monthly_income") >= 0)
    )
    
    # Build validation error messages
    validated_df = validated_df.withColumn("validation_errors",
        when(col("person_id").isNull(), "missing_person_id;")
        .when(col("age") < 0, "negative_age;")
        .when(col("age") > 120, "age_too_high;")
        .when(~col("gender").isin("male", "female"), "invalid_gender;")
        .when(length(col("full_name")) < 2, "name_too_short;")
        .when(col("region_code").isNull(), "missing_region;")
        .otherwise("")
    )
    
    return validated_df


def enrich_data(df: DataFrame) -> DataFrame:
    """Add derived columns for analytics"""
    
    # Major cities in Morocco (for location_type)
    major_cities = [
        "Casablanca", "Rabat", "Fès", "Marrakech", "Tanger", "Agadir",
        "Meknès", "Oujda", "Kénitra", "Tétouan", "Salé", "Nador"
    ]
    
    enriched_df = df \
        .withColumn("age_group",
            when(col("age") < 15, "child")
            .when(col("age") < 25, "youth")
            .when(col("age") < 45, "adult")
            .when(col("age") < 65, "middle_age")
            .otherwise("senior")
        ) \
        .withColumn("is_employed",
            col("employment_status").isin("employed", "self_employed")
        ) \
        .withColumn("income_level",
            when(col("monthly_income") <= 0, "no_income")
            .when(col("monthly_income") < 3000, "low")
            .when(col("monthly_income") < 8000, "medium")
            .when(col("monthly_income") < 20000, "high")
            .otherwise("very_high")
        ) \
        .withColumn("location_type",
            when(col("city").isin(major_cities), "urban")
            .otherwise("suburban")
        ) \
        .withColumn("education_years",
            when(col("education_level") == "none", 0)
            .when(col("education_level") == "primary", 6)
            .when(col("education_level") == "secondary", 9)
            .when(col("education_level") == "baccalaureate", 12)
            .when(col("education_level") == "university", 16)
            .when(col("education_level") == "postgraduate", 18)
            .otherwise(0)
        )
    
    return enriched_df


def process_silver_batch(spark: SparkSession):
    """Process Bronze data into Silver layer (batch mode)"""
    
    print("\n🥈 Processing Silver layer (batch)...")
    
    try:
        # Read from Bronze
        bronze_df = spark.read.format("iceberg").load("nessie.bronze.census_persons")
        
        if bronze_df.count() == 0:
            print("⚠️ No data in Bronze layer yet")
            return
        
        # Apply transformations
        cleansed_df = cleanse_data(bronze_df)
        validated_df = validate_data(cleansed_df)
        enriched_df = enrich_data(validated_df)
        
        # Add metadata
        final_df = enriched_df \
            .withColumnRenamed("ingestion_timestamp", "bronze_ingestion_timestamp") \
            .withColumn("silver_processing_timestamp", current_timestamp()) \
            .drop("kafka_partition", "kafka_offset", "kafka_timestamp")
        
        # Split valid and invalid records
        valid_df = final_df.filter(col("is_valid") == True)
        invalid_df = final_df.filter(col("is_valid") == False)
        
        # Write valid records to Silver
        valid_count = valid_df.count()
        if valid_count > 0:
            valid_df.writeTo("nessie.silver.census_persons").overwritePartitions()
            print(f"✅ Written {valid_count:,} valid records to Silver")
        
        # Write invalid records to quarantine
        invalid_count = invalid_df.count()
        if invalid_count > 0:
            quarantine_df = invalid_df.select(
                "person_id", "first_name", "last_name", "full_name",
                "age", "gender", "marital_status", "region_code", "region_name",
                "city", "education_level", "employment_status", "occupation",
                "monthly_income", "housing_type", "household_size",
                "has_health_insurance", "event_time", "validation_errors"
            ).withColumn("quarantine_timestamp", current_timestamp())
            
            quarantine_df.writeTo("nessie.silver.census_persons_quarantine").append()
            print(f"⚠️ Quarantined {invalid_count:,} invalid records")
        
        print(f"\n📊 Silver Processing Summary:")
        print(f"   Valid: {valid_count:,} ({valid_count * 100 / (valid_count + invalid_count):.1f}%)")
        print(f"   Invalid: {invalid_count:,} ({invalid_count * 100 / (valid_count + invalid_count):.1f}%)")
        
    except Exception as e:
        print(f"❌ Silver processing error: {str(e)}")


def process_silver_stream(spark: SparkSession):
    """Process Bronze data into Silver layer (streaming mode)"""
    
    print("\n🥈 Starting Silver streaming processing...")
    
    # Read from Bronze as stream
    bronze_stream = spark.readStream \
        .format("iceberg") \
        .load("nessie.bronze.census_persons")
    
    def process_batch(batch_df: DataFrame, batch_id: int):
        if batch_df.count() == 0:
            return
        
        print(f"\n{'=' * 60}")
        print(f"🥈 SILVER Batch {batch_id}")
        print(f"{'=' * 60}")
        
        # Apply transformations
        cleansed_df = cleanse_data(batch_df)
        validated_df = validate_data(cleansed_df)
        enriched_df = enrich_data(validated_df)
        
        # Add metadata
        final_df = enriched_df \
            .withColumnRenamed("ingestion_timestamp", "bronze_ingestion_timestamp") \
            .withColumn("silver_processing_timestamp", current_timestamp()) \
            .drop("kafka_partition", "kafka_offset", "kafka_timestamp")
        
        # Split valid and invalid
        valid_df = final_df.filter(col("is_valid") == True)
        invalid_df = final_df.filter(col("is_valid") == False)
        
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        
        # Write valid records
        if valid_count > 0:
            valid_df.writeTo("nessie.silver.census_persons").append()
            print(f"✅ Valid records: {valid_count:,}")
        
        # Write invalid to quarantine
        if invalid_count > 0:
            quarantine_df = invalid_df.select(
                "person_id", "first_name", "last_name", "full_name",
                "age", "gender", "marital_status", "region_code", "region_name",
                "city", "education_level", "employment_status", "occupation",
                "monthly_income", "housing_type", "household_size",
                "has_health_insurance", "event_time", "validation_errors"
            ).withColumn("quarantine_timestamp", current_timestamp())
            
            quarantine_df.writeTo("nessie.silver.census_persons_quarantine").append()
            print(f"⚠️ Quarantined records: {invalid_count:,}")
    
    query = bronze_stream.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "s3a://checkpoints/silver/census_persons") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    print("✅ Silver streaming started")
    return query


def get_silver_stats(spark: SparkSession):
    """Get statistics from Silver layer"""
    
    try:
        stats = spark.sql("""
            SELECT 
                COUNT(*) as total_records,
                SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) as valid_records,
                COUNT(DISTINCT region_code) as regions,
                COUNT(DISTINCT age_group) as age_groups,
                ROUND(AVG(age), 1) as avg_age,
                ROUND(AVG(CASE WHEN is_employed THEN 1 ELSE 0 END) * 100, 1) as employment_rate
            FROM nessie.silver.census_persons
        """).collect()[0]
        
        print("\n📊 Silver Layer Statistics:")
        print(f"   Total Records: {stats['total_records']:,}")
        print(f"   Valid Records: {stats['valid_records']:,}")
        print(f"   Regions: {stats['regions']}")
        print(f"   Avg Age: {stats['avg_age']}")
        print(f"   Employment Rate: {stats['employment_rate']}%")
        
    except Exception as e:
        print(f"⚠️ Could not get Silver stats: {str(e)}")
