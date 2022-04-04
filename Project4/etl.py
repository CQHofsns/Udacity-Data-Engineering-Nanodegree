import configparser
from datetime import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

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
    
    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(
        os.path.join(output_data, 'song_table.parquet'),
        mode='overwrite',
        partitionBy=['year', 'artist_id']
    )

    # extract columns to create artists table
    artists_table = df.select(
        'artist_id', 
        col('artist_name').alias('name'), 
        col('artist_location').alias('location'), 
        col('artist_latitude').alias('latitude'), 
        col('artist_longitude').alias('longitude')
    )
    
    # write artists table to parquet files
    artists_table.write.parquet(
        os.path.join(output_data, 'artist_table.parquet'),
        mode='overwrite'
    )


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_event/*.json')

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
        'level'
    )
    
    # write users table to parquet files
    users_table.write.parquet(
        os.path.join(output_data, 'users_table.parquet'),
        mode='overwrite'
    )

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
    df = df.withColumn('start_time', get_datetime('ts'))
    
    # extract columns to create time table
    time_table = (
        df
        .withColumn('hour', hour('timestamp'))
        .withColumn('day', hour('timestamp'))
        .withColumn('week', hour('timestamp'))
        .withColumn('month', hour('timestamp'))
        .withColumn('year', hour('timestamp'))
        .withColumn('weekday', hour('timestamp'))
        .select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday').distinct()
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(
        os.path.join(output_data, 'time_table.parquet'),
        mode= 'overwrite',
        partitionBy= ['year', 'month']
    )

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'song_table.parquet'))

    # read in artist data t use for songplays table
    artist_df = spark.read.parquet(os.path.join(output_data, 'artist_table.parquet'))
                             
    # extract columns from joined song and log datasets to create songplays table 
    
    # full join song table with artist table
    song_artist_join= (
        song_df
        .join(artist_df, 'artist_id', 'full')
        .select('song_id', 'title', 'artist_id', 'name', 'duration')
    )
                            
    # left join log data with song_artist_join table
    songplays_table_join= df.join(
        song_artist_join,
        [
            df.song == song_artist_join.title,
            df.artist == song_artist_join.name,
            df.length == song_artist_join.duration
        ],
        'left'
    )
                             
    # final left join to time table and extract required data from dataframe and import to songplays table
    songplays_table = (
        songplays_table_join
        .join(time_table, 'start_time', 'left')
        .select(
            'start_time',
            col('userId').alias('user_id'),
            'level',
            'song_id',
            'artist_id',
            col('sessionId').alias('session_id'),
            'location',
            col('UserAgent').alias('user_agent'),
            'year',
            'month'
        )
        .withColumn('songplay_id', monotonically_increasing_id())
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(
        os.path.join(output_data, 'songplays_table.parquet'),
        mode= 'overwrite',
        partitionBy=['year', 'month']
    )


def main():
    spark = create_spark_session()
    
    # For testing                         
    '''
    input_data = "s3a://udacity-dend/"
    output_data = ""
    '''
    
    # Read in/out path from configuration file
    # Local test
    input_data = config['LOCAL']['LC_INPUT_DATA']
    output_data = config['LOCAL']['LC_OUTPUT_DATA']
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
