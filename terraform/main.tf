# ==============================================================================
# 1. TERRAFORM CONFIGURATION & PROVIDERS
# ==============================================================================
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.85"
    }
  }
}

# Define variables (Values will be injected from terraform.tfvars)
variable "aws_access_key" {}
variable "aws_secret_key" {}
variable "snowflake_account" {}
variable "snowflake_user" {}
variable "snowflake_password" {}

variable "bucket_name" {}
variable "database_name" {}
variable "staging_schema_name" {}
variable "gold_schema_name" {}
variable "aws_region" {}

# Configure AWS Provider
provider "aws" {
  region     = var.aws_region
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
}

# Configure Snowflake Provider
provider "snowflake" {
  account  = var.snowflake_account
  user     = var.snowflake_user
  password = var.snowflake_password
  role     = "ACCOUNTADMIN"
}

# ==============================================================================
# 2. AWS INFRASTRUCTURE
# ==============================================================================
# Create S3 Bucket for raw data
resource "aws_s3_bucket" "football_data_bucket" {
  bucket = var.bucket_name 

  tags = {
    Environment = "Dev"
    Project     = "Football Data Pipeline"
  }
}

# ==============================================================================
# 3. SNOWFLAKE INFRASTRUCTURE
# ==============================================================================
# Create the main Data Warehouse Database
resource "snowflake_database" "football_db" {
  name = var.database_name
  comment = "Database for European Football Analytics Pipeline"
}

# Create the Staging Schema (Silver Layer)
resource "snowflake_schema" "staging_schema" {
  database = snowflake_database.football_db.name
  name = var.staging_schema_name
}

# Create the Analytics Schema (Gold Layer)
resource "snowflake_schema" "gold_schema" {
  database = snowflake_database.football_db.name
  name = var.gold_schema_name
}

# Create the External Stage linked to AWS S3
resource "snowflake_stage" "football_s3_stage" {
  name        = "FOOTBALL_S3_STAGE"
  database    = snowflake_database.football_db.name
  schema      = snowflake_schema.staging_schema.name
  url         = "s3://${var.bucket_name}/staging/"
  
  # Inject AWS credentials securely
  credentials = "AWS_KEY_ID='${var.aws_access_key}' AWS_SECRET_KEY='${var.aws_secret_key}'"
  file_format = "TYPE = PARQUET"
}