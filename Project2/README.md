# Project : Data Warehouse

## **Creator:** QUANG HUY CHU
## **Created date:** 2022/02/15

## Introduction and Objective definition:

### About Sparkify:
* Sparkify is a music streaming application starup. As their user grow up, also their database storage. Hence, to save the cost of physical database storage on the local server, they have decided to move their data to the cloud, in this case, Amazon Web Service. 
* Since they already got data in S3, which is **JSON logs** on user activity on the application, and **JSON metadata** on the songs.

### Objective definition:
* As a Data Engineer work for Sparkify, we need to build an ETL pipeline to move the **JSON data** to a schema of **fact and dimension tables** that can be used by the analytics team to further analysis.

## ETL Pipeline, step by step

### The process overview
An overview flowchart of the ETL pipeline as following:
**S3** (Extracting data from JSON files) &rarr; **Staging** (Copy data and load into staging table - Redshift) &rarr; **Fact & Dimension tables** (Transform data and insert into specifical tables)

### Schema:
* For the convenient for the entire ETL process, we should make a staging table to store our data from S3 JSON, then use that staging table as data source to insert into fact&dimension table.
* In this project, we will use the star schema design to describe our database schema, with a fact table in the center, surrounded by dimension tables. Fact and dimension tables will be named as:
    * **Fact table:** songplay: Store all the application event create by users. This table uses KEY Distribution to distribute to Redshift cluster by sorted songplay_id.
    * **Dimension tables:**
        * **user table:** application users (customers) information table. This table uses ALL Distribution to distribute to Redshift cluster by sorted user_id
        * **song table:** song information table. This table uses ALL Distribution to distribute to Redshift cluster by sorted song_id
        * **artist table:** song artist information table. This table uses ALL Distribution to distribute to Redshift cluster by sorted artist_id
        * **time table:** event time description table. This table uses ALL Distribution to distribute to Redshift cluster by sorted start_time
        
### ETL Pipeline step:
* The ETL pipeline in this project is processed as the following steps:
    1. Create staging, fact and dimension tables
    2. Copy JSON data from S3 to staging tables
    3. Insert data from staging tables to fact and dimension tables.
    
## Example query:
* To test the databse, let's use this sample query:
* Assume we want to know who listen to music the most, in the case of different songs, mean that listen to one song repeatedly is not count.
* Here is our query:
    ```
    select
        s.user_id,
        concat(u.first_name, u.last_name) as user_name,
        count(distinct s.songplay_id) as songplay_number
    from
        songplays as s
    join
        users as u on (s.user_id = u.user_id)
    group by s.user_id, user_name
    order by songplay_number desc
    limit 10
    ```
    
* After run this query, the result is as follow:

| user_id | user_name | songplay_number
| ------- | --------- | ---------------
| 49 | ChloeCuevas | 41
| 80 | TeganLevine | 31
| 97 | KateHarrell | 28
| 44 | AleenaKirby | 20
| 73 | JacobKlein | 18
| 88 | MohammadRodriguez | 17
| 15 | LilyKoch | 15
| 24 | LaylaGriffin | 13
| 29 | JacquelineLynch | 13
| 36 | MatthewJones| 11
