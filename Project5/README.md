# Udacity Data Engineering Project 5: Data Pipeline
## Project Introduction:
* Sparkify, a music company want to add more automatation and monitoring to their data warehouse ETL pipeline by applying Apache Airflow
* As their data engineer, you were asked to use Apache Airflow to create a high grade data pipeline that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. Moreover, you must need to add the data quality checking step in order to assure that all the data streaming throught the pipeline can keep its consistency

## Project Dataset:
* The source data resides in S3 consists of:
	* Log data: '''s3://udacity-dend/log_data'''
    * Song data: '''s3://udacity-dend/song-data'''
    
## DAG
* The ETL pipeline for this project consists 3 main phases:
1. Staging phase: Load event, songs data from S3 into events and songs staging table.
2. Insert fact table phase: Insert data from staging tables (events and songs) into songplays table.
3. Insert dimension tables phase:From songplays fact table, distribute data into each corresponding dimension table (songs, artists, users, and time)
4. Data quality evaluation phase: Using SQL to get all the record of each table, then evaluate the quality of the data.

## How to run this project:
1. Need to run the ```create_tables.py``` to drop all the existing tables in the Redshift cluster and create new one.
2. Use ```/opt/airflow/start.sh``` to make a connection and create an Airflow server
3. At the Airflow server,create AWS credential variable and Redshift cluster variable.
5. Monitoring the pipeline and check error.

    
