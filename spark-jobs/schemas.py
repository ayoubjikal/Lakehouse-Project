"""
Morocco Census Data Lake - Schema Definitions
==============================================
Defines schemas for all medallion layers.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, TimestampType, LongType
)

# =============================================================================
# BRONZE LAYER SCHEMA - Must match exactly what the Kafka producer sends
# =============================================================================

CENSUS_PERSON_SCHEMA = StructType([
    StructField("person_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("marital_status", StringType(), True),
    StructField("region_code", StringType(), True),
    StructField("region_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("education_level", StringType(), True),
    StructField("employment_status", StringType(), True),
    StructField("occupation", StringType(), True),
    StructField("monthly_income", DoubleType(), True),  # Changed from IntegerType
    StructField("housing_type", StringType(), True),
    StructField("household_size", IntegerType(), True),
    StructField("has_health_insurance", BooleanType(), True),
    StructField("event_time", StringType(), True),  # Keep as String, convert later
])


# =============================================================================
# SILVER LAYER SCHEMA - Cleansed and validated data
# =============================================================================
person_silver_schema = StructType([
    # Core identifiers
    StructField("person_id", StringType(), False),
    StructField("national_id", StringType(), True),
    
    # Personal information (cleaned)
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("full_name", StringType(), False),
    StructField("gender", StringType(), False),
    StructField("age", IntegerType(), False),
    StructField("birth_date", StringType(), True),
    StructField("age_group", StringType(), False),  # Derived: child, youth, adult, senior
    StructField("marital_status", StringType(), False),
    
    # Location (standardized)
    StructField("region_code", StringType(), False),
    StructField("region_name", StringType(), False),
    StructField("city", StringType(), False),
    StructField("is_urban", BooleanType(), False),
    StructField("location_type", StringType(), False),  # Derived: urban/rural
    
    # Socioeconomic (cleaned)
    StructField("education_level", StringType(), False),
    StructField("education_years", IntegerType(), True),  # Derived
    StructField("employment_status", StringType(), False),
    StructField("is_employed", BooleanType(), False),  # Derived
    StructField("income_bracket", StringType(), True),
    StructField("income_level", StringType(), True),  # Derived: low, medium, high
    
    # Housing
    StructField("housing_type", StringType(), False),
    StructField("household_size", IntegerType(), False),
    
    # Health & Contact
    StructField("has_health_insurance", BooleanType(), False),
    StructField("has_phone", BooleanType(), False),  # Derived
    StructField("has_email", BooleanType(), False),  # Derived
    
    # Metadata
    StructField("census_year", IntegerType(), False),
    StructField("data_quality", StringType(), False),
    StructField("is_valid", BooleanType(), False),  # Derived validation flag
    StructField("validation_errors", StringType(), True),  # List of validation issues
    
    # Timestamps
    StructField("event_time", TimestampType(), False),
    StructField("ingestion_time", TimestampType(), False),
    StructField("processing_time", TimestampType(), False)
])


# =============================================================================
# GOLD LAYER SCHEMAS - Business-level aggregations
# =============================================================================

# Regional Demographics Summary
gold_regional_summary_schema = StructType([
    StructField("region_code", StringType(), False),
    StructField("region_name", StringType(), False),
    StructField("total_population", LongType(), False),
    StructField("male_count", LongType(), False),
    StructField("female_count", LongType(), False),
    StructField("male_percentage", DoubleType(), False),
    StructField("urban_population", LongType(), False),
    StructField("rural_population", LongType(), False),
    StructField("urbanization_rate", DoubleType(), False),
    StructField("avg_age", DoubleType(), False),
    StructField("avg_household_size", DoubleType(), False),
    StructField("health_insurance_rate", DoubleType(), False),
    StructField("last_updated", TimestampType(), False)
])

# Age Distribution by Region
gold_age_distribution_schema = StructType([
    StructField("region_code", StringType(), False),
    StructField("region_name", StringType(), False),
    StructField("age_group", StringType(), False),
    StructField("count", LongType(), False),
    StructField("percentage", DoubleType(), False),
    StructField("male_count", LongType(), False),
    StructField("female_count", LongType(), False),
    StructField("last_updated", TimestampType(), False)
])

# Employment Statistics
gold_employment_stats_schema = StructType([
    StructField("region_code", StringType(), False),
    StructField("region_name", StringType(), False),
    StructField("employment_status", StringType(), False),
    StructField("count", LongType(), False),
    StructField("percentage", DoubleType(), False),
    StructField("avg_age", DoubleType(), False),
    StructField("education_distribution", StringType(), True),  # JSON string
    StructField("last_updated", TimestampType(), False)
])

# Education Statistics
gold_education_stats_schema = StructType([
    StructField("region_code", StringType(), False),
    StructField("region_name", StringType(), False),
    StructField("education_level", StringType(), False),
    StructField("count", LongType(), False),
    StructField("percentage", DoubleType(), False),
    StructField("avg_age", DoubleType(), False),
    StructField("employment_rate", DoubleType(), False),
    StructField("last_updated", TimestampType(), False)
])

# City-level Statistics
gold_city_stats_schema = StructType([
    StructField("region_code", StringType(), False),
    StructField("region_name", StringType(), False),
    StructField("city", StringType(), False),
    StructField("population", LongType(), False),
    StructField("avg_age", DoubleType(), False),
    StructField("employment_rate", DoubleType(), False),
    StructField("health_insurance_rate", DoubleType(), False),
    StructField("avg_household_size", DoubleType(), False),
    StructField("last_updated", TimestampType(), False)
])