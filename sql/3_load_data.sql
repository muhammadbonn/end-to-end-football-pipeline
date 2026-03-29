USE DATABASE ete_football_db;
USE SCHEMA Staging;

-- ==========================================
-- 1. Load Leagues Data
-- ==========================================
COPY INTO LEAGUES 
FROM @football_s3_stage/leagues/ 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet';

-- ==========================================
-- 2. Load Teams Data
-- ==========================================
COPY INTO TEAMS 
FROM @football_s3_stage/teams/ 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet';

-- ==========================================
-- 3. Load Players Data
-- ==========================================
COPY INTO PLAYERS 
FROM @football_s3_stage/players/ 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet';

-- ==========================================
-- 4. Load Games Data
-- ==========================================
COPY INTO GAMES 
FROM @football_s3_stage/games/ 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet';

-- ==========================================
-- 5. Load Team Stats Data
-- ==========================================
COPY INTO TEAMSTATS 
FROM @football_s3_stage/teamstats/ 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet';

-- ==========================================
-- 6. Load Shots Data (With Error Skipping)
-- ==========================================
COPY INTO SHOTS 
FROM @football_s3_stage/shots/ 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet'
ON_ERROR = CONTINUE;

-- ==========================================
-- 7. Load Appearances Data (Forced Load)
-- ==========================================
COPY INTO APPEARANCES 
FROM @football_s3_stage/appearances/ 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet'
FORCE = TRUE; -- This tells Snowflake to ignore its memory and load the files anyway
