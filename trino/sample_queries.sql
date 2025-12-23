-- ============================================================================
-- MOROCCO CENSUS DATA LAKE - MEDALLION ARCHITECTURE QUERIES
-- ============================================================================

-- ===========================================
-- SHOW ALL TABLES IN EACH LAYER
-- ===========================================

-- Bronze Layer (Raw Data)
SHOW TABLES IN iceberg_datalake.bronze;

-- Silver Layer (Cleansed Data)
SHOW TABLES IN iceberg_datalake.silver;

-- Gold Layer (Business Aggregations)
SHOW TABLES IN iceberg_datalake.gold;

-- ===========================================
-- BRONZE LAYER QUERIES (Raw Census Data)
-- ===========================================

-- Count records in Bronze
SELECT COUNT(*) as total_bronze_records 
FROM iceberg_datalake.bronze.census_persons;

-- Sample Bronze records
SELECT person_id, full_name, age, city, region_name, event_time
FROM iceberg_datalake.bronze.census_persons
LIMIT 10;

-- Bronze ingestion rate (records per hour)
SELECT 
    DATE_TRUNC('hour', ingestion_timestamp) as hour,
    COUNT(*) as records_ingested
FROM iceberg_datalake.bronze.census_persons
GROUP BY DATE_TRUNC('hour', ingestion_timestamp)
ORDER BY hour DESC
LIMIT 24;

-- ===========================================
-- SILVER LAYER QUERIES (Validated Data)
-- ===========================================

-- Count valid vs invalid records
SELECT 
    is_valid,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM iceberg_datalake.silver.census_persons
GROUP BY is_valid;

-- Sample Silver records with enriched data
SELECT 
    full_name, age, age_group, 
    city, region_name, location_type,
    education_level, employment_status, is_employed,
    income_level
FROM iceberg_datalake.silver.census_persons
WHERE is_valid = true
LIMIT 10;

-- Age group distribution
SELECT 
    age_group,
    COUNT(*) as count,
    ROUND(AVG(age), 1) as avg_age
FROM iceberg_datalake.silver.census_persons
WHERE is_valid = true
GROUP BY age_group
ORDER BY 
    CASE age_group 
        WHEN 'child' THEN 1 
        WHEN 'youth' THEN 2 
        WHEN 'adult' THEN 3 
        WHEN 'middle_age' THEN 4 
        ELSE 5 
    END;

-- Employment by region
SELECT 
    region_name,
    SUM(CASE WHEN is_employed THEN 1 ELSE 0 END) as employed,
    SUM(CASE WHEN NOT is_employed AND age >= 15 THEN 1 ELSE 0 END) as not_employed,
    ROUND(SUM(CASE WHEN is_employed THEN 1 ELSE 0 END) * 100.0 / 
          SUM(CASE WHEN age >= 15 AND age < 65 THEN 1 ELSE 0 END), 2) as employment_rate
FROM iceberg_datalake.silver.census_persons
WHERE is_valid = true
GROUP BY region_name
ORDER BY employment_rate DESC;

-- Quarantined records (validation failures)
SELECT * FROM iceberg_datalake.silver.census_persons_quarantine
LIMIT 20;

-- ===========================================
-- GOLD LAYER QUERIES (Business Analytics)
-- ===========================================

-- National Summary
SELECT * FROM iceberg_datalake.gold.national_summary;

-- Regional Demographics (Top regions by population)
SELECT 
    region_name,
    total_population,
    urbanization_rate,
    avg_age,
    employment_rate,
    health_insurance_rate
FROM iceberg_datalake.gold.regional_demographics
ORDER BY total_population DESC;

-- Age Distribution
SELECT 
    region_name,
    age_group,
    count,
    percentage
FROM iceberg_datalake.gold.age_distribution
WHERE region_code = '06'  -- Casablanca-Settat
ORDER BY 
    CASE age_group 
        WHEN 'child' THEN 1 
        WHEN 'youth' THEN 2 
        WHEN 'adult' THEN 3 
        WHEN 'middle_age' THEN 4 
        ELSE 5 
    END;

-- Employment Statistics
SELECT 
    region_name,
    employment_status,
    count,
    percentage,
    avg_age
FROM iceberg_datalake.gold.employment_stats
ORDER BY region_name, count DESC;

-- Education Statistics
SELECT 
    region_name,
    education_level,
    count,
    percentage,
    employment_rate
FROM iceberg_datalake.gold.education_stats
ORDER BY region_name, 
    CASE education_level 
        WHEN 'none' THEN 1 
        WHEN 'primary' THEN 2 
        WHEN 'secondary' THEN 3 
        WHEN 'baccalaureate' THEN 4 
        WHEN 'university' THEN 5 
        ELSE 6 
    END;

-- Top Cities by Population
SELECT 
    city,
    region_name,
    population,
    avg_age,
    employment_rate,
    health_insurance_rate
FROM iceberg_datalake.gold.city_stats
ORDER BY population DESC
LIMIT 20;

-- ===========================================
-- ICEBERG METADATA QUERIES
-- ===========================================

-- Bronze table file count and snapshots
SELECT COUNT(*) as file_count FROM iceberg_datalake.bronze."census_persons$files";
SELECT * FROM iceberg_datalake.bronze."census_persons$snapshots" ORDER BY committed_at DESC LIMIT 5;

-- Silver table snapshots
SELECT * FROM iceberg_datalake.silver."census_persons$snapshots" ORDER BY committed_at DESC LIMIT 5;

-- Gold table metadata
SELECT * FROM iceberg_datalake.gold."regional_demographics$snapshots" ORDER BY committed_at DESC LIMIT 5;

-- Table creation DDL
SHOW CREATE TABLE iceberg_datalake.bronze.census_persons;
SHOW CREATE TABLE iceberg_datalake.silver.census_persons;
SHOW CREATE TABLE iceberg_datalake.gold.regional_demographics;