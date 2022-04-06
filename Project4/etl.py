# Import module for this project
import configparser
from datetime import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id, dayofweek
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, TimestampType

# Call the configuration file
config = configparser.ConfigParser()
config.read('dl.cfg')

# Parse the AWS credential in the configuration file into environment variables
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # song table schema definition
    song_table_Schema= R([
        Fld('num_songs', Int()),
        Fld('artist_id', Str()),
        Fld('artist_latitude', Dbl()),
        Fld('artist_longitude', Dbl()),
        Fld('artist_location', Str()),
        Fld('artist_name', Str()),
        Fld('song_id', Str()),
        Fld('title', Str()),
        Fld('duration', Dbl()),
        Fld('year', Int())
    ])                     
    
    # read song data file
    df = spark.read.json(song_data, schema= song_table_Schema)
    
    df.createOrReplaceTempView('SongsDFTemp')
    
    
    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(path=os.path.join(output_data, 'song_table.parquet'))

    # extract columns to create artists table
    artists_table = df.select(
        'artist_id', 
        col('artist_name').alias('name'), 
        col('artist_location').alias('location'), 
        col('artist_latitude').alias('latitude'), 
        col('artist_longitude').alias('longitude')
    ).distinct()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(path=os.path.join(output_data, 'artist_table.parquet'))
    
    return songs_table, artists_table


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log/*.json')

    # log table schema definition
    log_table_Schema = R([
        Fld('artist', Str()),
        Fld('auth', Str()),
        Fld('firstName', Str()),
        Fld('gender', Str()),
        Fld('itemInSession', Int()),
        Fld('lastName', Str()),
        Fld('length', Dbl()),
        Fld('level', Str()),
        Fld('location', Str()),
        Fld('method', Str()),
        Fld('page', Str()),
        Fld('registration', Dbl()),
        Fld('sessionId', Str()),
        Fld('song', Str()),
        Fld('status', Int()),
        Fld('ts', Dbl()),
        Fld('userAgent', Str()),
        Fld('userId', Str()),
    ])  
                             
    # read log data file
    df = spark.read.json(log_data, schema= log_table_Schema)
    
    # filter by actions for song plays
    df = df.filter(col('page') == 'NextSong')

    # extract columns for users table    
    users_table = df.select(
        col('userId').alias('user_id'), 
        col('firstName').alias('first_name'),
        col('lastName').alias('last_name'),
        'gender',
        'level',
        'page'
    ).distinct()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(path=os.path.join(output_data, 'users_table.parquet'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(
        lambda x: datetime.fromtimestamp(x/1000).replace(microsecond=0),
        TimestampType()
    )
    df = df.withColumn('timestamp', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(
        lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S')
    )
    df = (
        df
        .withColumn('start_time', get_datetime('ts'))
        .withColumn('hour', hour('timestamp'))
        .withColumn('day', dayofmonth('timestamp'))
        .withColumn('week', weekofyear('timestamp'))
        .withColumn('month', month('timestamp'))
        .withColumn('year', year('timestamp'))
        .withColumn('weekday', dayofweek('timestamp'))
    )
    # extract columns to create time table
    time_table = df.select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday').distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(path=os.path.join(output_data, 'time_table.parquet'))

    # read in song data to use for songplays table
    song_df = spark.sql(
        """
        select
            *
        from
            SongsDFTemp
        """
    )
    
    song_df
                             
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table_join= df.join(
        song_df,
        (df.song == song_df.title)&(df.artist == song_df.artist_name)&(df.length == song_df.duration),'left')
    
    songplays_table= songplays_table_join.select(
        'start_time',
        col('userId').alias('user_id'),
        'level',
        'song_id',
        'artist_id',
        col('sessionId').alias('session_id'),
        'location',
        col('UserAgent').alias('user_agent'),
        df['year'].alias('year'),
        df['month'].alias('month')
    ).withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(path= os.path.join(output_data, 'songplays_table.parquet'))
    
    return users_table, time_table, songplays_table

def example_query(spark, songs_table, users_table, time_table, songplays_table, artists_table):
    songplays_table.createOrReplaceTempView('SongPlays')
    #songs_table.createOrReplaceTempView('Songs')
    #time_table.createOrReplaceTempView('Times')
    users_table.createOrReplaceTempView('Users')
    #artists_table.createOrReplaceTempView('Artists')
    
    
    spark.sql(
        """
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
        """
    ).show(10)

def main():
    spark = create_spark_session()
    
    # Read in/out path from configuration file
    # Local data path
    input_data = config['LOCAL']['LC_INPUT_DATA']
    output_data = config['LOCAL']['LC_OUTPUT_DATA']
    
    # S3 data path
    #input_data = config['AWS_S3']['S3_INPUT_DATA']
    #output_data = config['AWS_S3']['S3_OUTPUT_DATA']
    
    # ETL step
    songs_table, artists_table= process_song_data(spark, input_data, output_data)    
    users_table, time_table, songplays_table= process_log_data(spark, input_data, output_data)

    example_query(spark, songs_table, users_table, time_table, songplays_table, artists_table)

if __name__ == "__main__":
    main()
