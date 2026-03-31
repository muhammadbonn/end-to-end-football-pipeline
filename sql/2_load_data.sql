-- ====================================================================
-- SCRIPT: 2_load_data.sql
-- PURPOSE: Load transformed data from AWS S3 into Snowflake Staging tables.
-- DEPENDENCY: Assumes 'football_s3_stage' is securely provisioned beforehand.
-- ====================================================================

-- 1. Set the correct context
USE DATABASE ete_football_db;
USE SCHEMA Staging;

-- 2. Define the File Format for incoming data
-- (This creates the rule for how to read the files, no credentials needed here)
CREATE OR REPLACE FILE FORMAT football_parquet_format
  TYPE = PARQUET;

-- ==========================================
-- 3. Load Leagues Data
-- ==========================================
COPY INTO LEAGUES 
FROM @football_s3_stage/leagues/ 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet';

-- ==========================================
-- 4. Load Teams Data
-- ==========================================
COPY INTO TEAMS 
FROM @football_s3_stage/teams/ 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet';

-- ==========================================
-- 5. Load Players Data
-- ==========================================
COPY INTO PLAYERS 
FROM @football_s3_stage/players/ 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet';

-- ==========================================
-- 6. Load Games Data
-- ==========================================
COPY INTO GAMES 
FROM @football_s3_stage/games/ 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet';

-- ==========================================
-- 7. Load Team Stats Data
-- ==========================================
COPY INTO TEAMSTATS 
FROM @football_s3_stage/teamstats/ 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet';

-- ==========================================
-- 8. Load Shots Data (With Error Skipping)
-- ==========================================
COPY INTO SHOTS 
FROM @football_s3_stage/shots/ 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet'
ON_ERROR = CONTINUE;

-- ==========================================
-- 9. Load Appearances Data (Forced Load)
-- ==========================================
COPY INTO APPEARANCES 
FROM @football_s3_stage/appearances/ 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet'
FORCE = TRUE; -- Overrides internal cache to force reload