USE AI_Jobs_DB;
DROP TABLE ai_jobs_dirty;

CREATE TABLE ai_jobs_dirty (
    job_id INT,  -- NO PRIMARY KEY
    job_role VARCHAR(100),
    industry VARCHAR(50),
    country VARCHAR(50),
    year VARCHAR(20),
    automation_risk_percent DECIMAL(10,2),
    ai_replacement_score DECIMAL(10,2),
    skill_gap_index DECIMAL(10,2),
    salary_before_usd DECIMAL(18,2),
    salary_after_usd DECIMAL(18,2),
    salary_change_percent DECIMAL(10,2),
    skill_demand_growth_percent DECIMAL(10,2),
    remote_feasibility_score DECIMAL(10,2),
    ai_adoption_level DECIMAL(10,2),
    education_requirement_level VARCHAR(20),
    automation_risk_category VARCHAR(20),
    skill_transition_pressure DECIMAL(10,2),
    wage_volatility_index DECIMAL(10,2),
    reskilling_urgency_score DECIMAL(10,2),
    ai_disruption_intensity VARCHAR(30)
);


----------------------------------------------------------------------------------------------

USE AI_Jobs_DB;
GO

BULK INSERT ai_jobs_dirty
FROM "C:\Users\prati_t686nlu\OneDrive\Desktop\ai_jobs_dirty.csv"   
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',     -- handles both Windows & Unix line endings
    CODEPAGE = '65001',          -- handles UTF-8 encoding
    TABLOCK
);


-----------------------------------------------------------------------------------------
USE AI_Jobs_DB;

-- Total rows (should be ~15,600)
SELECT COUNT(*) AS total_rows FROM ai_jobs_dirty;

-- Check duplicates survived (should show job_id 13 twice+)
SELECT job_id, COUNT(*) as duplicates 
FROM ai_jobs_dirty 
GROUP BY job_id 
HAVING COUNT(*) > 1;

-- Verify dirty data
SELECT 
    SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) AS null_countries,
    SUM(CASE WHEN year LIKE '%O%' THEN 1 ELSE 0 END) AS bad_years,
    MIN(salary_before_usd) AS min_salary_negative,
    MAX(salary_before_usd) AS max_salary_outlier
FROM ai_jobs_dirty;


-------------------------------------------------------------------
--Remove Duplicates (Keep First Occurrence)
USE AI_Jobs_DB;
GO

WITH DuplicateCTE AS (
    SELECT *, 
           ROW_NUMBER() OVER (
               PARTITION BY job_id ORDER BY job_id
           ) AS row_num
    FROM ai_jobs_dirty
)
DELETE FROM DuplicateCTE WHERE row_num > 1;


--Standardize Casing (Title Case)
-- Fix inconsistent casing for country and industry
UPDATE ai_jobs_dirty
SET 
    country = UPPER(LEFT(TRIM(country), 1)) + LOWER(SUBSTRING(TRIM(country), 2, LEN(country))),
    industry = UPPER(LEFT(TRIM(industry), 1)) + LOWER(SUBSTRING(TRIM(industry), 2, LEN(industry)))
WHERE country IS NOT NULL AND industry IS NOT NULL;

-- Fix Invalid automation_risk_category
UPDATE ai_jobs_dirty 
SET automation_risk_category = CASE 
    WHEN automation_risk_category IN ('MED', 'medIum', 'Medium ') THEN 'Medium'
    WHEN automation_risk_category LIKE 'HIGH%' OR automation_risk_category = 'hIGH' THEN 'High'
    WHEN automation_risk_category LIKE 'low%' OR automation_risk_category = 'Lowww' THEN 'Low'
    ELSE automation_risk_category
END
WHERE automation_risk_category IS NOT NULL;


-- Fix Year Column ("2O21" → 2021)
UPDATE ai_jobs_dirty 
SET year = CASE 
    WHEN year = '2O20' THEN '2020'
    WHEN year = '2O21' THEN '2021'
    WHEN year = '2O22' THEN '2022'
    WHEN year = '2O23' THEN '2023'
    WHEN year = '2O24' THEN '2024'
    WHEN year = '2O25' THEN '2025'
    WHEN year = '2O26' THEN '2026'
    ELSE year
END
WHERE year LIKE '%O%';


--Handle NULLs (Mode/Default)
-- Fill country NULLs with mode (most frequent)
UPDATE ai_jobs_dirty 
SET country = (
    SELECT TOP 1 country 
    FROM ai_jobs_dirty 
    WHERE country IS NOT NULL 
    GROUP BY country 
    ORDER BY COUNT(*) DESC
)
WHERE country IS NULL;

-- Fill education_requirement_level NULLs with 'Bachelor''s' (common mode)
UPDATE ai_jobs_dirty 
SET education_requirement_level = 'Bachelor''s'
WHERE education_requirement_level IS NULL;


--Add salary_trend Column
ALTER TABLE ai_jobs_dirty 
ADD salary_trend VARCHAR(20);
GO                                -- this forces SQL Server to execute ALTER first

UPDATE ai_jobs_dirty 
SET salary_trend = CASE 
    WHEN salary_change_percent > 2 THEN 'Positive'
    WHEN salary_change_percent < -2 THEN 'Negative'
    ELSE 'Stable'
END;

--Add education_label Column
ALTER TABLE ai_jobs_dirty 
ADD education_label VARCHAR(20);
GO
UPDATE ai_jobs_dirty 
SET education_label = CASE education_requirement_level
    WHEN '1' THEN 'High School'
    WHEN '2' THEN 'Diploma'
    WHEN '3' THEN 'Bachelor''s'
    WHEN '4' THEN 'Master''s'
    WHEN '5' THEN 'PhD'
    ELSE 'Unknown'
END;


-- Final Verification
-- Check cleaning results
SELECT 
    COUNT(*) AS total_clean_rows,
    SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) AS remaining_null_countries,
    SUM(CASE WHEN year LIKE '%O%' THEN 1 ELSE 0 END) AS remaining_bad_years,
    COUNT(DISTINCT job_id) AS unique_jobs,
    COUNT(DISTINCT automation_risk_category) AS valid_risk_categories
FROM ai_jobs_dirty;

-- Sample cleaned data
SELECT TOP 10 job_id, country, industry, year, automation_risk_category, 
              salary_trend, education_label 
FROM ai_jobs_dirty;

