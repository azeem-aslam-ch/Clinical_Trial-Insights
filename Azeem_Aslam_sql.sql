-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS db

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS db.clinicaltrial_2023 AS SELECT * FROM default.clinicaltrial_2023

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS db.pharma_4 AS SELECT * FROM default.pharma_4

-- COMMAND ----------

SHOW TABLES IN db

-- COMMAND ----------

SELECT * FROM pharma_4

-- COMMAND ----------

-- Define the SQL query to concatenate all columns into a single column and split it into 14 columns
CREATE OR REPLACE TEMPORARY VIEW merged_and_split_columns_view AS
SELECT 
  SPLIT(CONCAT_WS(' ', *) , '\t')[0] AS Id,
  SPLIT(CONCAT_WS(' ', *) , '\t')[1] AS Study_Title,
  SPLIT(CONCAT_WS(' ', *) , '\t')[2] AS Acronym,
  SPLIT(CONCAT_WS(' ', *) , '\t')[3] AS Status,
  SPLIT(CONCAT_WS(' ', *) , '\t')[4] AS Conditions,
  SPLIT(CONCAT_WS(' ', *) , '\t')[5] AS Interventions,
  SPLIT(CONCAT_WS(' ', *) , '\t')[6] AS Sponsor,
  SPLIT(CONCAT_WS(' ', *) , '\t')[7] AS Collaborators,
  SPLIT(CONCAT_WS(' ', *) , '\t')[8] AS Enrollment,
  SPLIT(CONCAT_WS(' ', *) , '\t')[9] AS Funder_Type,
  SPLIT(CONCAT_WS(' ', *) , '\t')[10] AS Type,
  SPLIT(CONCAT_WS(' ', *) , '\t')[11] AS Study_Design,
  SPLIT(CONCAT_WS(' ', *) , '\t')[12] AS Start,
  SPLIT(CONCAT_WS(' ', *) , '\t')[13] AS Completion
FROM default.clinicaltrial_2023



-- COMMAND ----------

CREATE OR REPLACE TABLE db.clinicaltrial_2023 AS SELECT * FROM merged_and_split_columns_view

-- COMMAND ----------

SELECT * FROM db.clinicaltrial_2023

-- COMMAND ----------

SELECT COUNT(*) AS total_rows,
       SUM(CASE WHEN Type IS NULL THEN 1 ELSE 0 END) AS null_count,
       SUM(CASE WHEN Type = '' THEN 1 ELSE 0 END) AS empty_count,
       COUNT(Type) AS non_null_count
FROM db.clinicaltrial_2023;

-- COMMAND ----------

UPDATE db.clinicaltrial_2023
SET Type = COALESCE(NULLIF(Type, ''), 'default_value')
WHERE Type IS NULL OR Type = '';

-- COMMAND ----------

SELECT COUNT(*) AS total_rows,
       SUM(CASE WHEN Conditions IS NULL THEN 1 ELSE 0 END) AS null_count,
       SUM(CASE WHEN Conditions = '' THEN 1 ELSE 0 END) AS empty_count,
       COUNT(Conditions) AS non_null_count
FROM db.clinicaltrial_2023;

-- COMMAND ----------

UPDATE db.clinicaltrial_2023
SET Conditions = COALESCE(NULLIF(Conditions, ''), 'default_value')
WHERE Conditions IS NULL OR Conditions = '';

-- COMMAND ----------


SELECT COUNT(DISTINCT Study_Title) AS Number_Of_Studies_2023 
FROM db.clinicaltrial_2023

-- COMMAND ----------

--question 2
SELECT DISTINCT(Type), 
COUNT(Type) AS Frequency
FROM db.clinicaltrial_2023
WHERE 
  LENGTH(Type) > 0
  AND
  Type IS NOT NULL
GROUP BY Type
ORDER BY Frequency DESC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC
-- MAGIC # Create a SparkSession
-- MAGIC spark = SparkSession.builder.appName("BarGraph").getOrCreate()
-- MAGIC
-- MAGIC # Execute the SQL query
-- MAGIC query = """
-- MAGIC SELECT DISTINCT(Type), COUNT(Type) AS Frequency
-- MAGIC FROM db.clinicaltrial_2023
-- MAGIC WHERE LENGTH(Type) > 0 AND Type IS NOT NULL
-- MAGIC GROUP BY Type
-- MAGIC ORDER BY Frequency DESC
-- MAGIC """
-- MAGIC df = spark.sql(query)
-- MAGIC
-- MAGIC # Convert the DataFrame to a Pandas DataFrame
-- MAGIC pdf = df.toPandas()
-- MAGIC
-- MAGIC # Plot the bar graph
-- MAGIC plt.figure(figsize=(10, 6))
-- MAGIC plt.bar(pdf['Type'], pdf['Frequency'], color='skyblue')
-- MAGIC plt.xlabel('Type', fontsize=12)
-- MAGIC plt.ylabel('Frequency', fontsize=12)
-- MAGIC plt.title('Distribution of Clinical Trial Types', fontsize=14)
-- MAGIC plt.xticks(rotation=90)
-- MAGIC plt.tight_layout()
-- MAGIC plt.show()
-- MAGIC

-- COMMAND ----------


WITH trial_conditions AS (
    SELECT EXPLODE(SPLIT(conditions, '\\|')) AS Condition
    FROM db.clinicaltrial_2023
    WHERE conditions IS NOT NULL)
SELECT Condition AS TopFiveConditions, 
COUNT(*) AS Frequency
FROM trial_conditions
GROUP BY Condition
ORDER BY Frequency DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC
-- MAGIC # Create a SparkSession
-- MAGIC spark = SparkSession.builder.appName("BarGraph").getOrCreate()
-- MAGIC
-- MAGIC # Execute the SQL query
-- MAGIC query = """
-- MAGIC WITH trial_conditions AS (
-- MAGIC     SELECT EXPLODE(SPLIT(conditions, '\\|')) AS Condition
-- MAGIC     FROM db.clinicaltrial_2023
-- MAGIC     WHERE conditions IS NOT NULL
-- MAGIC ),
-- MAGIC top_conditions AS (
-- MAGIC     SELECT Condition AS TopFiveConditions, 
-- MAGIC            COUNT(*) AS Frequency
-- MAGIC     FROM trial_conditions
-- MAGIC     GROUP BY Condition
-- MAGIC     ORDER BY Frequency DESC
-- MAGIC     LIMIT 5
-- MAGIC )
-- MAGIC SELECT * FROM top_conditions
-- MAGIC """
-- MAGIC df = spark.sql(query)
-- MAGIC
-- MAGIC # Convert the DataFrame to a Pandas DataFrame
-- MAGIC pdf = df.toPandas()
-- MAGIC
-- MAGIC # Plot the bar graph
-- MAGIC plt.figure(figsize=(10, 6))
-- MAGIC plt.bar(pdf['TopFiveConditions'], pdf['Frequency'], color='skyblue')
-- MAGIC plt.xlabel('Condition', fontsize=12)
-- MAGIC plt.ylabel('Frequency', fontsize=12)
-- MAGIC plt.title('Top 5 Most Frequent Conditions', fontsize=14)
-- MAGIC plt.xticks(rotation=90)
-- MAGIC plt.tight_layout()
-- MAGIC plt.show()
-- MAGIC

-- COMMAND ----------


SELECT Sponsor AS MostCommonSponsor,
COUNT(*) AS NumberOfTrials
FROM db.clinicaltrial_2023
WHERE Sponsor NOT IN
    (SELECT Parent_Company FROM db.pharma_4)
GROUP BY Sponsor
ORDER BY NumberOfTrials DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC
-- MAGIC # Create a SparkSession
-- MAGIC spark = SparkSession.builder.appName("BarGraph").getOrCreate()
-- MAGIC
-- MAGIC # Execute the SQL query
-- MAGIC query = """
-- MAGIC WITH top_sponsors AS (
-- MAGIC     SELECT Sponsor AS MostCommonSponsor, 
-- MAGIC            COUNT(*) AS NumberOfTrials
-- MAGIC     FROM db.clinicaltrial_2023
-- MAGIC     WHERE Sponsor NOT IN (SELECT Parent_Company FROM db.pharma_4)
-- MAGIC     GROUP BY Sponsor
-- MAGIC     ORDER BY NumberOfTrials DESC
-- MAGIC     LIMIT 10
-- MAGIC )
-- MAGIC SELECT * FROM top_sponsors
-- MAGIC """
-- MAGIC df = spark.sql(query)
-- MAGIC
-- MAGIC # Convert the DataFrame to a Pandas DataFrame
-- MAGIC pdf = df.toPandas()
-- MAGIC
-- MAGIC # Plot the bar graph
-- MAGIC plt.figure(figsize=(12, 6))
-- MAGIC plt.bar(pdf['MostCommonSponsor'], pdf['NumberOfTrials'], color='skyblue')
-- MAGIC plt.xlabel('Sponsor', fontsize=12)
-- MAGIC plt.ylabel('Number of Trials', fontsize=12)
-- MAGIC plt.title('Top 10 Most Common Non-Parent Company Sponsors', fontsize=14)
-- MAGIC plt.xticks(rotation=45)
-- MAGIC plt.tight_layout()
-- MAGIC plt.show()
-- MAGIC

-- COMMAND ----------

--5
WITH completed_studies AS (
    SELECT
        TO_DATE(
            CASE
                WHEN LENGTH(Completion) = 7 THEN CONCAT(Completion, '-01')
                ELSE Completion
            END, 'yyyy-MM-dd'
        ) AS CompletionDate
    FROM db.clinicaltrial_2023
    WHERE Status = 'COMPLETED' AND Completion LIKE '2023%'
)
SELECT
    CASE MONTH(CompletionDate)
        WHEN 1 THEN 'Jan'
        WHEN 2 THEN 'Feb'
        WHEN 3 THEN 'Mar'
        WHEN 4 THEN 'Apr'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'Jun'
        WHEN 7 THEN 'Jul'
        WHEN 8 THEN 'Aug'
        WHEN 9 THEN 'Sep'
        WHEN 10 THEN 'Oct'
        WHEN 11 THEN 'Nov'
        WHEN 12 THEN 'Dec'
    END AS Month,
    COUNT(*) AS CompletedStudies
FROM completed_studies
GROUP BY MONTH(CompletionDate)
ORDER BY MONTH(CompletionDate);


-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC
-- MAGIC # Create a SparkSession
-- MAGIC spark = SparkSession.builder.appName("BarGraph").getOrCreate()
-- MAGIC
-- MAGIC # Execute the SQL query
-- MAGIC query = """
-- MAGIC WITH completed_studies AS (
-- MAGIC     SELECT
-- MAGIC         TO_DATE(
-- MAGIC             CASE
-- MAGIC                 WHEN LENGTH(Completion) = 7 THEN CONCAT(Completion, '-01')
-- MAGIC                 ELSE Completion
-- MAGIC             END, 'yyyy-MM-dd'
-- MAGIC         ) AS CompletionDate
-- MAGIC     FROM db.clinicaltrial_2023
-- MAGIC     WHERE Status = 'COMPLETED' AND Completion LIKE '2023%'
-- MAGIC ),
-- MAGIC monthly_completed_studies AS (
-- MAGIC     SELECT
-- MAGIC         CASE MONTH(CompletionDate)
-- MAGIC             WHEN 1 THEN 'Jan'
-- MAGIC             WHEN 2 THEN 'Feb'
-- MAGIC             WHEN 3 THEN 'Mar'
-- MAGIC             WHEN 4 THEN 'Apr'
-- MAGIC             WHEN 5 THEN 'May'
-- MAGIC             WHEN 6 THEN 'Jun'
-- MAGIC             WHEN 7 THEN 'Jul'
-- MAGIC             WHEN 8 THEN 'Aug'
-- MAGIC             WHEN 9 THEN 'Sep'
-- MAGIC             WHEN 10 THEN 'Oct'
-- MAGIC             WHEN 11 THEN 'Nov'
-- MAGIC             WHEN 12 THEN 'Dec'
-- MAGIC         END AS Month,
-- MAGIC         COUNT(*) AS CompletedStudies
-- MAGIC     FROM completed_studies
-- MAGIC     GROUP BY MONTH(CompletionDate)
-- MAGIC     ORDER BY MONTH(CompletionDate)
-- MAGIC )
-- MAGIC SELECT * FROM monthly_completed_studies
-- MAGIC """
-- MAGIC df = spark.sql(query)
-- MAGIC
-- MAGIC # Convert the DataFrame to a Pandas DataFrame
-- MAGIC pdf = df.toPandas()
-- MAGIC
-- MAGIC # Plot the bar graph
-- MAGIC plt.figure(figsize=(10, 6))
-- MAGIC plt.bar(pdf['Month'], pdf['CompletedStudies'], color='skyblue')
-- MAGIC plt.xlabel('Month', fontsize=12)
-- MAGIC plt.ylabel('Number of Completed Studies', fontsize=12)
-- MAGIC plt.title('Completed Clinical Studies in 2023 by Month', fontsize=14)
-- MAGIC plt.xticks(rotation=90)
-- MAGIC plt.tight_layout()
-- MAGIC plt.show()
-- MAGIC
