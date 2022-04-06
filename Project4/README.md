# Project 4: Data Lakes
## 1. Project Description
* Since Sparkify, an music streaming starup is getting more popular, its user also growth dramatically. Thus, their user and song database are growing bigger overtime.
* To handle such large data, the team want to move their data to a data lake. From this, the Sparkify team ask their data engineer to build an ETL pipeline that extracts their data from S3 data lake, processes the data using Spark, and loads back the data back into S3 as a set of dimensional tables.
* Their data included: user activity on the app as well as every song description such as song name, artist, or listen time.

## 2. Database Schema
* Since their (Sparkify) data (user activity data and song description data) reside in a datalake S3 and stored in .JSON type, we need to arrange the data into fact & dimensons table in order to allow the analytics team to continue finding the data insights.
* The project start schema is designed as follow:
()[]

## 3. ETL Step
* The ETL proposed for this project is operated as the following step:
    1. Load all the .JSON data from the S3 data lake.
    2. Distribute each element in the .JSON into each corresponding fact & dimension tables.
    3. Save each tables as .parquet with partition into S3 bucket.

## 4. Example query
* To test the analytical performace for the processed, we query an example on our new database:

```
         select distinct
            U.first_name,
            U.last_name,
            SP.user_id,
            SP.play_times
        from
            Users as U
        inner join
        (
            select
                count(user_id) as play_times,
                user_id
            from
                SongPlays
            group by
                user_id
            order by
                play_times desc
        ) AS SP
        on
            SP.user_id == U.user_id
```

* The above query will return the ranking of top 10 music listened users with their listen time.
* The query result with the local data:

|----------|---------|-------|----------|
|first_name|last_name|user_id|play_times|
|----------|---------|-------|----------|
|     Chloe|   Cuevas|     49|       689|
|     Tegan|   Levine|     80|       665|
|      Kate|  Harrell|     97|       557|
|      Lily|     Koch|     15|       463|
|    Aleena|    Kirby|     44|       397|
|Jacqueline|    Lynch|     29|       346|
|     Layla|  Griffin|     24|       321|
|     Jacob|    Klein|     73|       289|
|  Mohammad|Rodriguez|     88|       270|
|   Matthew|    Jones|     36|       248|
+----------+---------+-------+----------+