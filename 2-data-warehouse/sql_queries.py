import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ROLE_ARN = config.get('IAM_ROLE','ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE staging_events(
        artist        text,
        auth          text,
        firstName     text,
        gender        text,
        itemInSession integer,
        lastName      text,
        length        float,
        level         text,
        location      text,
        method        text,
        page          text,
        registration  float,
        sessionId     integer,
        song          text,
        status        integer,
        ts            timestamp,
        userAgent     text,
        userId        integer
    )    
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        num_songs         integer,
        artist_id         text,
        artist_latitude   float,
        artist_longitude  float,
        artist_location   text,
        artist_name       text,
        song_id           text,
        title             text,
        duration          float,
        year              integer
    )   
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int IDENTITY(0,1) PRIMARY KEY, 
        start_time timestamp NOT NULL sortkey distkey REFERENCES time(start_time),
        user_id int NOT NULL REFERENCES users(user_id), 
        level text NOT NULL, 
        song_id text, 
        artist_id text, 
        session_id int, 
        location text, 
        user_agent text)
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id int NOT NULL sortkey PRIMARY KEY, 
        first_name text, 
        last_name text, 
        gender text, 
        level text)    
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id text NOT NULL sortkey PRIMARY KEY , 
        title text, 
        artist_id text REFERENCES artists(artist_id), 
        year int, 
        duration numeric)
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id text NOT NULL sortkey PRIMARY KEY, 
        name text, 
        location text, 
        latitude numeric, 
        longitude numeric)
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp NOT NULL sortkey distkey PRIMARY KEY, 
        hour int, 
        day int, 
        week int, 
        month int,
        year int, 
        weekday text)
""")

# STAGING TABLES
staging_events_copy = ("""
      COPY staging_events
      FROM 's3://udacity-dend/log_data' 
      iam_role {}
      JSON 's3://udacity-dend/log_json_path.json'
      timeformat 'epochmillisecs';
    """).format(IAM_ROLE_ARN)

staging_songs_copy = ("""
     COPY staging_songs
     FROM 's3://udacity-dend/song_data' 
     iam_role {}
     JSON 'auto';
  """).format(IAM_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays ( 
            start_time,
            user_id,
            level,
            song_id,
            artist_id,
            session_id,
            location, 
            user_agent)
        SELECT 
            se.ts,
            se.userId,
            se.level,
            ss.song_id,
            ss.artist_id,
            se.sessionId,
            se.location,
            se.userAgent            
        FROM staging_events se
        JOIN staging_songs ss ON (se.artist = ss.artist_name AND se.song = ss.title)
        WHERE (se.userId IS NOT NULL AND se.page = 'NextSong')    
""")

user_table_insert = ("""
    INSERT INTO users ( 
            user_id,
            first_name,
            last_name,
            gender,
            level)
        SELECT 
            DISTINCT se.userId,
            se.firstName,
            se.lastName,
            se.gender,
            se.userAgent           
        FROM staging_events se  
        WHERE (se.userId IS NOT NULL AND se.page = 'NextSong')   
""")

song_table_insert = ("""
    INSERT INTO songs ( 
            song_id,
            title,
            artist_id,
            year,
            duration)
        SELECT 
            ss.song_id,
            ss.title,
            ss.artist_id,
            ss.year,
            ss.duration          
        FROM staging_songs ss 
""")

artist_table_insert = ("""
    INSERT INTO artists ( 
            artist_id,
            name,
            location,
            latitude,
            longitude)
        SELECT 
            DISTINCT ss.artist_id,
            ss.artist_name,
            ss.artist_location,
            ss.artist_latitude,
            ss.artist_longitude           
        FROM staging_songs ss  
""")

time_table_insert = ("""
    INSERT INTO time(
            start_time,
            hour,
            day,
            week,
            month,
            year,
            weekday
        )
        SELECT 
            DISTINCT sp.start_time,
            date_part(h, sp.start_time),
            date_part(d, sp.start_time),
            date_part(w, sp.start_time),
            date_part(mon, sp.start_time),
            date_part(y, sp.start_time),
            date_part(dow, sp.start_time)
        FROM songplays sp;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, time_table_create, user_table_create, artist_table_create,song_table_create, songplay_table_create  ]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
