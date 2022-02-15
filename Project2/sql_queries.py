import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_event"
staging_songs_table_drop = "drop table if exists staging_song"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""
    create table if not exists staging_event (
            artist varchar(255),
            auth varchar(15),
            firstName varchar(50),
            gender varchar(1),
            itemInSession integer,
            lastName varchar(50),
            length double precision,
            level varchar(10),
            location varchar(255),
            method varchar(10),
            page varchar(20),
            registration varchar(255),
            sessionId bigint,
            song varchar(255),
            status integer,
            ts timestamp,
            userAgent text,
            userId varchar(100)
    )
""")

staging_songs_table_create = ("""
    create table if not exists staging_song (
            num_songs integer,
            artist_id varchar(50),
            artist_latitude double precision,
            artist_longitude double precision,
            artist_location varchar(255),
            artist_name varchar(255),
            song_id varchar(50),
            title varchar(255),
            duration double precision,
            year integer
    )
""")

songplay_table_create = ("""
    create table if not exists songplays (
            songplay_id integer identity(0,1) not null sortkey distkey,
            start_time timestamp,
            user_id varchar(100),
            level varchar(10),
            song_id varchar(50),
            artist_id varchar(50),
            session_id bigint,
            location varchar(255),
            user_agent text
    )
""")

user_table_create = ("""
    create table if not exists users (
            user_id varchar(100) not null sortkey,
            first_name varchar(50),
            last_name varchar(50),
            gender varchar(1),
            level varchar(10)
    )
""")

song_table_create = ("""
    create table if not exists songs (
            song_id varchar(50) not null sortkey,
            title varchar(255),
            artist_id varchar(50),
            year integer,
            duration double precision
    )
""")

artist_table_create = ("""
    create table if not exists artists (
            artist_id varchar(50) not null sortkey,
            name varchar(255),
            location varchar(255),
            latitude double precision,
            longitude double precision
    )
""")

time_table_create = ("""
    create table if not exists time (
            start_time timestamp not null sortkey,
            hour integer,
            day integer,
            week integer,
            month integer,
            year integer,
            weekday integer
    )
""")

# STAGING TABLES
## COPY params explain: https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html
staging_events_copy = ("""
    copy staging_event from {} 
    credentials 'aws_iam_role={}' 
    format as json {} 
    region 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    copy stagind_song from {} 
    credentials 'aws_iam_role={}' 
    format as json 'auto' 
    region 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select
    se.ts,
    se.user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.session_id,
    ss.location,
    se.user_agent
from 
    staging_event as se,
    staging_song as ss
where
    se.page = 'NextSong' and
    se.artist_name = ss.artist_name and
    se.song = ss.title and
    se.length = ss.duration
""")

user_table_insert = ("""
insert into table users (user_id, first_name, last_name, gender, level)
select
    user_id,
    first_name,
    last_name,
    gender,
    level
from
    staging_event
where
    page = 'NextSong'
""")

song_table_insert = ("""
insert into songs (song_id, title, artist_id, year, duration)
select
    song_id,
    title,
    artist_id,
    year,
    duration
from
    staging_song
where
    song_id is not null
""")

artist_table_insert = ("""
insert into artists (artist_id, name, location, latitude, longitude)
select
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
from
    staging_song
where
    artist_id is not null
""")

time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
select
    start_time,
    extract(hour from start_time),
    extract(day from start_time),
    extract(week from start_time),
    extract(month from start_time),
    extract(year from start_time),
    extract(dayofweek from start_time),
from
    songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
