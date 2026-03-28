-- ==========================================
-- Create Databse and Schema
-- ==========================================
CREATE DATABASE IF NOT EXISTS FOOTBALL_DB;
CREATE SCHEMA IF NOT EXISTS FOOTBALL_DB.STAGING;

USE DATABASE FOOTBALL_DB;
USE SCHEMA STAGING;

-- ==========================================
-- Define File Format
-- ==========================================
CREATE OR REPLACE FILE FORMAT parquet_format
  TYPE = PARQUET
  COMPRESSION = AUTO;

-- ==========================================
-- Create External Stage
-- ==========================================
CREATE OR REPLACE STAGE football_s3_stage
    URL = 's3://european-football-leagues/staging/'
    CREDENTIALS = (
        AWS_KEY_ID = '<YOUR_AWS_ACCESS_KEY_ID>' 
        AWS_SECRET_KEY = '<YOUR_AWS_SECRET_ACCESS_KEY>'
    )
    FILE_FORMAT = parquet_format;