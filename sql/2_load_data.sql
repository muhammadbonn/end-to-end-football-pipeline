-- 1. Set the correct context
USE DATABASE ete_football_db;
USE SCHEMA Staging;

-- 2. Define the File Format
CREATE OR REPLACE FILE FORMAT football_parquet_format
  TYPE = PARQUET;

-- ==========================================
-- 3. Load Leagues Data
-- ==========================================
COPY INTO LEAGUES 
FROM @football_s3_stage/staging/leagues/ 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet';

-- ==========================================
-- 4. Load Teams Data
-- ==========================================
COPY INTO TEAMS 
FROM @football_s3_stage/staging/teams/ 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet';

-- ==========================================
-- 5. Load Players Data
-- ==========================================
COPY INTO PLAYERS 
FROM @football_s3_stage/staging/players/ 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet';

-- ==========================================
-- 6. Load Games Data
-- ==========================================
COPY INTO GAMES 
FROM @football_s3_stage/staging/games/ 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet';

-- ==========================================
-- 7. Load Team Stats Data
-- ==========================================
COPY INTO TEAMSTATS 
FROM @football_s3_stage/staging/teamstats/ 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet';

-- ==========================================
-- 8. Load Shots Data
-- ==========================================
COPY INTO SHOTS 
FROM @football_s3_stage/staging/shots/ 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet'
ON_ERROR = CONTINUE;

-- ==========================================
-- 9. Load Appearances Data
-- ==========================================
COPY INTO APPEARANCES 
FROM @football_s3_stage/staging/appearances/ 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet'
FORCE = TRUE;
