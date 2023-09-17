import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
ARN = config.get('IAM_ROLE', 'ARN')

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
    CREATE TABLE IF NOT EXISTS staging_events (
        artist              TEXT,
        auth                TEXT,
        first_name          TEXT,
        gender              CHAR,
        item_in_session     INT,
        last_name           TEXT,
        length              FLOAT,
        level               TEXT,
        location            TEXT,
        method              VARCHAR(3),
        page                TEXT,
        registration        BIGINT,
        session_id          INT,
        song                TEXT,
        status              INT,
        ts                  BIGINT,
        user_agent          TEXT,
        user_id             INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs           INT,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     TEXT,
        artist_name         TEXT,
        song_id             VARCHAR,
        title               TEXT,
        duration            FLOAT,
        year                INT
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id             INT                     PRIMARY KEY, 
        first_name          TEXT NOT NULL, 
        last_name           VARCHAR(255), 
        gender              CHAR, 
        level               TEXT
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id             VARCHAR                 PRIMARY KEY,
        title               TEXT NOT NULL, 
        artist_id           VARCHAR NOT NULL, 
        year                INT, 
        duration            FLOAT
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id           VARCHAR                 PRIMARY KEY, 
        name                TEXT NOT NULL, 
        location            TEXT, 
        latitude            FLOAT, 
        longitude           FLOAT);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time          TIMESTAMP                  PRIMARY KEY, 
        hour                INT, 
        day                 INT, 
        week                INT, 
        month               INT, 
        year                INT, 
        weekday             INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT IDENTITY(0,1)               PRIMARY KEY, 
        start_time          TIMESTAMP               REFERENCES time(start_time), 
        user_id             INT                     REFERENCES users(user_id), 
        level               TEXT, 
        song_id             VARCHAR                 REFERENCES songs(song_id), 
        artist_id           VARCHAR                 REFERENCES artists(artist_id), 
        session_id          INT, 
        location            TEXT, 
        user_agent          TEXT
    );
""")

# STAGING TABLES
staging_events_copy = ("""
    COPY staging_events
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON {}
    COMPUPDATE OFF 
    REGION 'us-west-2'
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON 'auto'
    COMPUPDATE OFF 
    REGION 'us-west-2'
""").format(SONG_DATA, ARN)

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
    (SELECT DISTINCT
        (SELECT TIMESTAMP 'epoch' + e.ts * INTERVAL '1 second') AS start_time, 
        e.user_id AS user_id, 
        e.level AS level, 
        s.song_id AS song_id, 
        s.artist_id AS artist_id, 
        e.session_id AS session_id, 
        e.location AS location, 
        e.user_agent AS user_agent
    FROM staging_events e
    JOIN staging_songs s
    ON s.title = e.song
    AND s.artist_name = e.artist
    WHERE e.page = 'NextSong');
""")

user_table_insert = ("""
    INSERT INTO users (
    SELECT DISTINCT user_id, first_name, last_name, gender, level
    FROM staging_events
    WHERE page = 'NextSong');
""")

song_table_insert = ("""
    INSERT INTO songs (
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs);
""")
artist_table_insert = ("""
    INSERT INTO artists (
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs);
""")

time_table_insert = ("""
    INSERT INTO time (
    SELECT DISTINCT 
        start_time, 
        EXTRACT(HOUR FROM start_time) AS hour, 
        EXTRACT(DAY FROM start_time) AS day, 
        EXTRACT(WEEK FROM start_time) AS week, 
        EXTRACT(MONTH FROM start_time) AS month, 
        EXTRACT(YEAR FROM start_time) AS year, 
        EXTRACT(WEEKDAY FROM start_time) AS weekday
    FROM songplays);
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]