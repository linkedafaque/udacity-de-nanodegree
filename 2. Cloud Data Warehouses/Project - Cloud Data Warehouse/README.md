# Data Warehouse ETL Pipeline For Sparkify

## Introduction

The project builds an ETL pipeline that extracts their data from S3, 
stages them in Redshift, and transforms data into a set of dimensional tables for analytics.

## Context

Sparkify, has grown their user base and song database and want to move their processes and data 
onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, 
as well as a directory with JSON metadata on the songs in their app. Moving the data on cloud will
enable faster retrieval and help store humungous amounts of data without incurring huge costs. In 
order to make the querying easy, an optimized STAR schema has been designed for the fact and dim
tables. 

* Fact Tables - songplays
* Dimension Tables - users, songs, artists, time

## Datasets

* Song Data `s3://udacity-dend/song_data`
* Log Data `s3://udacity-dend/log_data`
* Log Format `s3://udacity-dend/log_json_path.json`



## Project Structure
1. `create_tables.py` - Script drops tables (if existing) and creates new tables
2. `etl.py` - Script achieves the following
    * Load data from S3 into staging tables in Redshift
    * Insert data from staging tables to fact and dim tables on Redshift
3. `sql_queries.py` - Contains all queries for the following
    * drops tables
    * creates tables
    * loads data from S3 to staging tables in redshift
    * inserts data from staging tables to fact and dim tables in redshift
4. `dwh.cfg` - Config file for IAM, Redshift, S3

## Steps to run ETL Pipeline
1. Create Redshift cluster
2. Enter credentials on `dwh.cfg`
3. Run `create_tables.py` to drop (if existing) and create all tables on Redshift
4. Run `etl.py`, runs the ETL pipeline to insert data into fact and dim tables on Redshift
    

