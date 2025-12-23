"""
Morocco Census Data Lake - Gold Layer Aggregations
===================================================
Creates business-level aggregations from Silver layer data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, round, when, first, lit,
    current_timestamp, countDistinct
)


def create_gold_tables(spark: SparkSession):
    """Create Gold layer tables in Iceberg"""
    
    print("\n🥇 Creating Gold layer tables...")
    
    # Create gold namespace
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    
    # Drop existing tables to ensure clean schema
    spark.sql("DROP TABLE IF EXISTS nessie.gold.regional_demographics")
    spark.sql("DROP TABLE IF EXISTS nessie.gold.age_distribution")
    spark.sql("DROP TABLE IF EXISTS nessie.gold.employment_stats")
    spark.sql("DROP TABLE IF EXISTS nessie.gold.education_stats")
    spark.sql("DROP TABLE IF EXISTS nessie.gold.city_stats")
    spark.sql("DROP TABLE IF EXISTS nessie.gold.national_summary")
    
    # Regional Demographics
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.gold.regional_demographics (
            region_code STRING,
            region_name STRING,
            total_population LONG,
            male_count LONG,
            female_count LONG,
            male_percentage DOUBLE,
            urban_population LONG,
            rural_population LONG,
            urbanization_rate DOUBLE,
            avg_age DOUBLE,
            avg_household_size DOUBLE,
            health_insurance_rate DOUBLE,
            employment_rate DOUBLE,
            last_updated TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)
    
    # Age Distribution
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.gold.age_distribution (
            region_code STRING,
            region_name STRING,
            age_group STRING,
            count LONG,
            percentage DOUBLE,
            avg_income DOUBLE,
            employment_rate DOUBLE,
            last_updated TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)
    
    # Employment Stats
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.gold.employment_stats (
            region_code STRING,
            region_name STRING,
            employment_status STRING,
            count LONG,
            percentage DOUBLE,
            avg_age DOUBLE,
            avg_income DOUBLE,
            last_updated TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)
    
    # Education Stats
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.gold.education_stats (
            region_code STRING,
            region_name STRING,
            education_level STRING,
            count LONG,
            percentage DOUBLE,
            avg_income DOUBLE,
            employment_rate DOUBLE,
            last_updated TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)
    
    # City Stats
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.gold.city_stats (
            city STRING,
            region_code STRING,
            region_name STRING,
            population LONG,
            avg_age DOUBLE,
            avg_income DOUBLE,
            employment_rate DOUBLE,
            health_insurance_rate DOUBLE,
            last_updated TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)
    
    # National Summary
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.gold.national_summary (
            country STRING,
            total_population LONG,
            total_regions LONG,
            total_cities LONG,
            male_percentage DOUBLE,
            female_percentage DOUBLE,
            avg_age DOUBLE,
            median_income DOUBLE,
            employment_rate DOUBLE,
            health_insurance_rate DOUBLE,
            urbanization_rate DOUBLE,
            last_updated TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)
    
    print("✅ Gold tables created")


def process_gold_layer(spark: SparkSession):
    """Process Silver data into Gold aggregations"""
    
    print("\n🥇 Processing Gold layer aggregations...")
    
    try:
        # Read from Silver (only valid records)
        silver_df = spark.read.format("iceberg") \
            .load("nessie.silver.census_persons") \
            .filter(col("is_valid") == True)
        
        record_count = silver_df.count()
        if record_count == 0:
            print("⚠️ No valid data in Silver layer yet")
            return
        
        print(f"📊 Processing {record_count:,} records from Silver...")
        
        # 1. Regional Demographics
        print("   → Regional demographics...")
        regional_demographics = silver_df.groupBy("region_code").agg(
            first("region_name").alias("region_name"),
            count("*").alias("total_population"),
            sum(when(col("gender") == "male", 1).otherwise(0)).alias("male_count"),
            sum(when(col("gender") == "female", 1).otherwise(0)).alias("female_count"),
            round(
                sum(when(col("gender") == "male", 1).otherwise(0)) * 100.0 / count("*"), 2
            ).alias("male_percentage"),
            sum(when(col("location_type") == "urban", 1).otherwise(0)).alias("urban_population"),
            sum(when(col("location_type") != "urban", 1).otherwise(0)).alias("rural_population"),
            round(
                sum(when(col("location_type") == "urban", 1).otherwise(0)) * 100.0 / count("*"), 2
            ).alias("urbanization_rate"),
            round(avg("age"), 2).alias("avg_age"),
            round(avg("household_size"), 2).alias("avg_household_size"),
            round(
                sum(when(col("has_health_insurance") == True, 1).otherwise(0)) * 100.0 / count("*"), 2
            ).alias("health_insurance_rate"),
            round(
                sum(when(col("is_employed") == True, 1).otherwise(0)) * 100.0 /
                sum(when((col("age") >= 15) & (col("age") < 65), 1).otherwise(0)), 2
            ).alias("employment_rate")
        ).withColumn("last_updated", current_timestamp()) \
         .orderBy(col("total_population").desc())
        
        regional_demographics.writeTo("nessie.gold.regional_demographics").overwritePartitions()
        
        # 2. Age Distribution
        print("   → Age distribution...")
        age_distribution = silver_df.groupBy("region_code", "region_name", "age_group").agg(
            count("*").alias("count"),
            round(avg("monthly_income"), 2).alias("avg_income"),
            round(
                sum(when(col("is_employed") == True, 1).otherwise(0)) * 100.0 /
                sum(when((col("age") >= 15) & (col("age") < 65), 1).otherwise(1)), 2
            ).alias("employment_rate")
        ).withColumn("last_updated", current_timestamp())
        
        # Calculate percentage within region
        from pyspark.sql.window import Window
        region_window = Window.partitionBy("region_code")
        age_distribution = age_distribution.withColumn(
            "percentage",
            round(col("count") * 100.0 / sum("count").over(region_window), 2)
        )
        
        age_distribution.writeTo("nessie.gold.age_distribution").overwritePartitions()
        
        # 3. Employment Stats
        print("   → Employment statistics...")
        employment_stats = silver_df.filter(
            (col("age") >= 15) & (col("age") < 65)
        ).groupBy("region_code", "region_name", "employment_status").agg(
            count("*").alias("count"),
            round(avg("age"), 2).alias("avg_age"),
            round(avg("monthly_income"), 2).alias("avg_income")
        ).withColumn("last_updated", current_timestamp())
        
        employment_stats = employment_stats.withColumn(
            "percentage",
            round(col("count") * 100.0 / sum("count").over(region_window), 2)
        )
        
        employment_stats.writeTo("nessie.gold.employment_stats").overwritePartitions()
        
        # 4. Education Stats
        print("   → Education statistics...")
        education_stats = silver_df.groupBy("region_code", "region_name", "education_level").agg(
            count("*").alias("count"),
            round(avg("monthly_income"), 2).alias("avg_income"),
            round(
                sum(when(col("is_employed") == True, 1).otherwise(0)) * 100.0 /
                sum(when((col("age") >= 15) & (col("age") < 65), 1).otherwise(1)), 2
            ).alias("employment_rate")
        ).withColumn("last_updated", current_timestamp())
        
        education_stats = education_stats.withColumn(
            "percentage",
            round(col("count") * 100.0 / sum("count").over(region_window), 2)
        )
        
        education_stats.writeTo("nessie.gold.education_stats").overwritePartitions()
        
        # 5. City Stats
        print("   → City statistics...")
        city_stats = silver_df.groupBy("city", "region_code", "region_name").agg(
            count("*").alias("population"),
            round(avg("age"), 2).alias("avg_age"),
            round(avg("monthly_income"), 2).alias("avg_income"),
            round(
                sum(when(col("is_employed") == True, 1).otherwise(0)) * 100.0 /
                sum(when((col("age") >= 15) & (col("age") < 65), 1).otherwise(1)), 2
            ).alias("employment_rate"),
            round(
                sum(when(col("has_health_insurance") == True, 1).otherwise(0)) * 100.0 / count("*"), 2
            ).alias("health_insurance_rate")
        ).withColumn("last_updated", current_timestamp()) \
         .orderBy(col("population").desc())
        
        city_stats.writeTo("nessie.gold.city_stats").overwritePartitions()
        
        # 6. National Summary
        print("   → National summary...")
        national_summary = silver_df.agg(
            lit("Morocco").alias("country"),
            count("*").alias("total_population"),
            countDistinct("region_code").alias("total_regions"),
            countDistinct("city").alias("total_cities"),
            round(
                sum(when(col("gender") == "male", 1).otherwise(0)) * 100.0 / count("*"), 2
            ).alias("male_percentage"),
            round(
                sum(when(col("gender") == "female", 1).otherwise(0)) * 100.0 / count("*"), 2
            ).alias("female_percentage"),
            round(avg("age"), 2).alias("avg_age"),
            round(avg("monthly_income"), 2).alias("median_income"),
            round(
                sum(when(col("is_employed") == True, 1).otherwise(0)) * 100.0 /
                sum(when((col("age") >= 15) & (col("age") < 65), 1).otherwise(0)), 2
            ).alias("employment_rate"),
            round(
                sum(when(col("has_health_insurance") == True, 1).otherwise(0)) * 100.0 / count("*"), 2
            ).alias("health_insurance_rate"),
            round(
                sum(when(col("location_type") == "urban", 1).otherwise(0)) * 100.0 / count("*"), 2
            ).alias("urbanization_rate")
        ).withColumn("last_updated", current_timestamp())
        
        national_summary.writeTo("nessie.gold.national_summary").overwritePartitions()
        
        print("\n✅ Gold layer processing complete!")
        print(f"   📊 Regional Demographics: {regional_demographics.count()} regions")
        print(f"   📊 Age Distribution: {age_distribution.count()} records")
        print(f"   📊 Employment Stats: {employment_stats.count()} records")
        print(f"   📊 Education Stats: {education_stats.count()} records")
        print(f"   📊 City Stats: {city_stats.count()} cities")
        print(f"   📊 National Summary: 1 record")
        
    except Exception as e:
        print(f"❌ Gold processing error: {str(e)}")


def get_gold_stats(spark: SparkSession):
    """Get statistics from Gold layer"""
    
    try:
        # National summary
        summary = spark.sql("SELECT * FROM nessie.gold.national_summary").collect()
        
        if len(summary) > 0:
            s = summary[0]
            print("\n📊 Gold Layer - National Summary:")
            print(f"   🇲🇦 Morocco Census Overview")
            print(f"   Total Population: {s['total_population']:,}")
            print(f"   Regions: {s['total_regions']}")
            print(f"   Cities: {s['total_cities']}")
            print(f"   Avg Age: {s['avg_age']}")
            print(f"   Employment Rate: {s['employment_rate']}%")
            print(f"   Health Insurance: {s['health_insurance_rate']}%")
            print(f"   Urbanization: {s['urbanization_rate']}%")
        
    except Exception as e:
        print(f"⚠️ Could not get Gold stats: {str(e)}")
