# **Project**: Data Modeling with Postgres
## **Creator:** QUANG HUY CHU
## **Created date:** 2022/02/05

## About Sparkify database:
* Sparkify is a music streaming application starup.
* With users data, the analytics team want to understand what song users are listening to.
* With the current JSON file, it is hard for the team to querry and analyze data, so that they want to move the data from JSON file into Postgres database.

## How to run the Python scripts:
* Since the script is automately looking fore the data source path, user just run the scripts in the following step:
    0. Prepare the SQL querries in **sql_queries.py**: 
    * This script contains all the queries to operate all the database schema's construction and ETL process. 
    1. Run the **create_tables.py** script:
    * Run this script to create and open the connection with the database, also create:
        * **Fact table:** 
            1. **songplays** table.
        * **Dimension Tables:**
            1. **users** table
            2. **songs** table
            3. **artists** table
            4. **time** table
        * These tables are connected in the *star schema* diagram.
    2. Run the **etl.py** script:
        * Run this script will convert (or move) the data from JSON file of all dataset (song dataset and log dataset) into corresponding tables.

## Explanation about each file in the repository:
1. **data** folder: Folder contains both song dataset and log dataset.
2. **sql_queries.py** script: Contain all the queries used in this project (**CREATE** table, **INSERT** values into tables, and **SELECT** specific data from specific table)
3. **create_tables.py** script: Create and open the connection with the database, also create fact and dimension tables in the database.
3. **etl.py** script: conduct ETL process, moving data from JSON file to each corresponding tables in the database.
4. **etl.ipynb** notebook: A notebook describe step by step how to conduct an ETL process for this project.
5. **test.ipynb** notebook: A notebook used for testing if the data had converted into the right tables.
6. **README.md** markdown file (this file): Markdown file giving detailed information about this project.

## State and justify the databse schema design and ETL process:
* In this project, the database is designed as the **star schema**. Specifically, the **songplays** table will be acted as a fact table, or the table that stand in the center of the schema, the rest tables (**users**, **songs**, **artists**, **time**) will be acted as four star points which connect to the fact table with their primary key id.
* In the ETL process: we **extract** the data from JSON file, **transform** the data into specific list, then **load** each list of corresponding data in to tables.

## Exmaple: Get top 5 interact listener user_id from songplays data:
Query:
%sql select user_id, count(user_id) as listen_time from songplays group by user_id order by listen_time desc limit 5;

this will return the follwing result:
| user_id | listen_time
| ------- | -----------
| 49 | 689
| 80 | 665
| 97 | 557
| 15 | 463
| 44 | 397

We see the user with ID 49 play songs 689 times, then user id 80 with 665 times, the last user id in the top 5 is user id 44 with 397 times.
