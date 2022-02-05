# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

"""
1. CREATE SONGPLAYS TABLE
Query to create table "songplay", data imported from 30 log files.
Primary key is songplay_id set as serial,
Foreign keys group is consisted of:
1. start_time set as bigint dataype, foreign key reference to table "time"
2. user_id set as integer dataype, foreign key reference to table "users"
3. song_id set as text dataype, foreign key reference to table "song"
4. artist_id set as textt dataype, foreign key reference to table "artists"
"""

songplay_table_create = ("""
    create table if not exists songplays (
        songplay_id serial, 
        start_time timestamp without time zone references time(start_time), 
        user_id integer references users(user_id), 
        level text not null, 
        song_id text references songs(song_id), 
        artist_id text references artists(artist_id), 
        session_id integer not null, 
        location text not null, 
        user_agent text not null
        );
""")


"""
2. CREATE USERS TABLE
Query to create table "users", data imported from 30 log files.
Primary key is user_id set as integer, also is foreign key of "songplays" table
"""

user_table_create = ("""
    create table if not exists users (
        user_id integer primary key, 
        first_name text not null, 
        last_name text not null, 
        gender text not null, 
        level text not null
        );
""")

"""
3. CREATE SONGS TABLE
Query to create table "songs", data imported from 77 song files.
Primary key is song_id set as text, also is foreign key of "songplays" table
Foreign key is artist_id set as text dataype, reference to table "artists"
"""

song_table_create = ("""
    create table if not exists songs (
        song_id text primary key, 
        title text not null, 
        artist_id text not null, 
        year integer not null, 
        duration numeric not null
        );
""")

"""
4. CREATE ARTISTS TABLE
Query to create table "artists", data imported from 77 song files.
Primary key is artist_id set as text, also is foreign key of "songplays" table
"""

artist_table_create = ("""
    create table if not exists artists (
        artist_id text primary key, 
        name text not null, 
        location text not null, 
        latitude real not null, 
        longitude real not null
        );
""")

"""
5. CREATE TIME TABLE
Query to create table "time", data imported from 30 log files.
Primary key is start_time set as bigint, also is foreign key of "songplays" table
"""

time_table_create = ("""
    create table if not exists time (
        start_time timestamp without time zone primary key, 
        hour integer not null, 
        day integer not null, 
        week integer not null, 
        month integer not null, 
        year integer not null, 
        weekday text not null
        );
""")

# INSERT RECORDS

"""
1. INSERT DATA INTO SONGPLAYS TABLE:
Query to insert corresponding data into table songplays.
"""

songplay_table_insert = ("""
    insert into songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    values (default, %s, %s, %s, %s, %s, %s, %s, %s)
    on conflict do nothing;
""")

"""
2. INSERT DATA INTO USERS TABLE:
Query to insert corresponding data into table users. Also do not update data when insert same user_id
"""

user_table_insert = ("""
    insert into users (user_id, first_name, last_name, gender, level)
    values (%s, %s, %s, %s, %s)
    on conflict (user_id) do nothing;
""")

"""
3. INSERT DATA INTO SONGS TABLE:
Query to insert corresponding data into table songs. Also do not update data when insert same song_id
"""

song_table_insert = ("""
    insert into songs (song_id, title, artist_id, year, duration) 
    values (%s, %s, %s, %s, %s)
    on conflict (song_id) do nothing;
""")

"""
4. INSERT DATA INTO ARTISTS TABLE:
Query to insert corresponding data into table artists. Also do not update data when insert same artist_id
"""

artist_table_insert = ("""
    insert into artists (artist_id, name, location, latitude, longitude) 
    values (%s, %s, %s, %s, %s)
    on conflict (artist_id) do nothing;
""")

"""
5. INSERT DATA INTO TIME TABLE:
Query to insert corresponding data into table time. Also do not update data when insert same start_time
"""

time_table_insert = ("""
    insert into time (start_time, hour, day, week, month, year, weekday) 
    values (%s, %s, %s, %s, %s, %s, %s)
    on conflict (start_time) do nothing;
""")

# FIND SONGS

"""
FIND SONG QUERY:
Query to find song information (song_id, artist_id) base on condition song name, artist name and song duration.
"""

song_select = ("""
    select 
        songs.song_id, artists.artist_id
    from
        songs
    inner join
        artists on artists.artist_id = songs.artist_id
    where
        songs.title = %s and
        artists.name = %s and
        songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [artist_table_create, song_table_create, time_table_create, user_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]