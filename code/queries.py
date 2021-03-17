import configparser
cfg = configparser.ConfigParser()
cfg.read_file(open('dwh.cfg'))

#S3 File locations
LOG_DATA               = cfg.get("S3", "LOG_DATA")
LOG_JSONPATH           = cfg.get("S3", "LOG_JSONPATH")
SONG_DATA              = cfg.get("S3", "SONG_DATA")

#IAM Role associated with Rdshift Cluster & S3
IAM_ROLE_ARN           = cfg.get("IAM_ROLE","ARN")



#DROP TABLES
drop_songs_staging = "DROP TABLE IF EXISTS songs_staging;"
drop_logs_staging = "DROP TABLE IF EXISTS logs_staging;"
drop_users = "DROP TABLE IF EXISTS users;"
drop_songs = "DROP TABLE IF EXISTS songs;"
drop_artists = "DROP TABLE IF EXISTS artists;"
drop_time = "DROP TABLE IF EXISTS time;"
drop_songplays = "DROP TABLE IF EXISTS songplays;"

#CREATE TABLES
create_table_staging_songs = """
CREATE TABLE songs_staging (
    num_songs           INTEGER,
    artist_id           VARCHAR,
    artist_latitude     FLOAT,
    artist_longitude    FLOAT,
    artist_location     VARCHAR,
    artist_name         VARCHAR,
    song_id             VARCHAR,
    title               VARCHAR,
    duration            FLOAT,
    year                INTEGER
);
"""

create_table_staging_logs = """
CREATE TABLE logs_staging(
    artist          VARCHAR,
    auth            VARCHAR ,
    firstName       VARCHAR ,
    gender          CHAR ,
    itemInSession   INTEGER,
    lastname        VARCHAR,
    length          FLOAT,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    FLOAT,
    sessionId       INTEGER,
    song            VARCHAR,
    status          INTEGER,
    ts              TIMESTAMP,
    userAgent       VARCHAR,
    userId          INTEGER
);
"""


#CREATING TABLES FOR THE STAR SCHEMA
create_table_users = """
CREATE TABLE users(
user_id     INTEGER NOT NULL SORTKEY PRIMARY KEY,
first_name  VARCHAR NOT NULL,
last_name   VARCHAR NOT NULL,
gender      CHAR NOT NULL,
level       VARCHAR NOT NULL
);
"""

create_table_songs = """
CREATE TABLE songs(
song_id       VARCHAR NOT NULL SORTKEY PRIMARY KEY,
title         VARCHAR NOT NULL,
artist_id     INTEGER NOT NULL,
year          INTEGER NOT NULL,
duration      FLOAT NOT NULL
);
"""


create_table_arists = """
CREATE TABLE artists (
    artist_id       VARCHAR NOT NULL SORTKEY PRIMARY KEY,
    name            VARCHAR NOT NULL,
    location        VARCHAR,
    latitude        FLOAT,
    longitude       FLOAT
)
"""


create_table_time = """
CREATE TABLE time (
    start_time  TIMESTAMP DISTKEY SORTKEY PRIMARY KEY,
    hour        INTEGER,
    day         VARCHAR,
    week        VARCHAR,
    month       VARCHAR,
    year        VARCHAR,    
    weekday     VARCHAR
);
"""

create_table_songplays = """
CREATE TABLE songplays(
    songplay_id     INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time      TIMESTAMP NOT NULL SORTKEY DISTKEY,
    user_id         INTEGER NOT NULL
    level           VARCHAR NOT NULL,
    song_id         VARCHAR NOT NULL,
    artist_id       VARCHAR NOT NULL,
    session_id      INTEGER NOT NULL,
    location        VARCHAR ,
    user_agent      VARCHAR 
);
"""


#COPY STAGING TABLES 
insert_into_logs_staging = f"""
COPY logs_staging FROM {LOG_DATA}
CREDENTIALS 'aws_iam_role={IAM_ROLE_ARN}'
REGION 'us-west-2'
FORMAT AS JSON {LOG_JSONPATH}
TIMEFORMAT AS 'epochmillisecs';
"""


insert_into_songs_staging = f"""
COPY songs_staging FROM {SONG_DATA}
CREDENTIALS 'aws_iam_role={IAM_ROLE_ARN}'
REGION 'us-west-2'
FORMAT AS JSON 'auto';
"""


#INSERT TO DIMENSION TABLES
insert_into_users = """
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT 
    DISTINCT(userId) as user_id,
    firstName as first_name,
    lastName as last_name,
    gender,
    level
FROM 
    logs_staging
WHERE
    userId is NOT NULL;
    AND page = "NextSong";
"""

insert_into_songs = """

INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT
        DISTINCT(song_id) as song_id,
        title,
        artist_id,
        year,
        duration
FROM    
        songs_staging
WHERE   
        song_id IS NOT NULL;
"""

insert_into_artists = """
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT 
       DISTINCT(artist_id),
       artist_name as name,
       artist_location as location,
       artist_latitude as latitude,
       artist_longitude as longitude
FROM 
       songs_staging
WHERE
       artist_id IS NOT NULL;
"""

insert_into_time = """

INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT 
        DISTINCT(ts) as start_time,
        EXTRACT(hour FROM ts) as hour,
        EXTRACT(day FROM ts) as day,
        EXTRACT(week FROM ts) as week,
        EXTRACT(month FROM ts) as month,
        EXTRACT(year FROM ts) as year,
        EXTRACT(dayofweek FROM ts) as weekday,
FROM 
        logs_staging
WHERE 
        ts is NOT NULL;
"""

#INSERT INTO FACT TABLE
insert_into_songplays = """

INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
        l.ts as start_time,
        l.userId as user_id,
        l.level
        s.song_id,
        s.artist_id,
        l.session_id,
        l.location,
        l.userAgent as user_agent
FROM 
        logs_staging l
        JOIN songs_staging s ON (l.song = s.title AND l.artist = s.artist_name) AND page = "NextSong";
"""


# ANALYTIC QUERIES
get_number_logs_staging = """
    SELECT COUNT(1) FROM logs_staging;
"""

get_number_songs_staging = """
    SELECT COUNT(1) FROM songs_staging;
"""

get_number_songplays = """
    SELECT COUNT(1) FROM songplays;
"""

get_number_users = """
    SELECT COUNT(1) FROM users;
"""

get_number_songs = """
    SELECT COUNT(1) FROM songs;
"""

get_number_artists = """
    SELECT COUNT(1) FROM artists;
"""

get_number_time = """
    SELECT COUNT(1) FROM time;
"""


#Lists of queries to export from file

#Queries to drop all the tables
drop_table_queries = [drop_artists,drop_logs_staging,drop_songplays,drop_songs,drop_songs_staging,drop_time,drop_users]

#Queries to create Staging Tables
create_tables_staging = [create_table_staging_songs, create_table_staging_logs]

#Queries to create Facts and Dimension Tables
create_tables_main = [create_table_songs, create_table_arists, create_table_time, create_table_users]

#Queries to EXTRACT content into Staging Tables
copy_commands = [insert_into_logs_staging, insert_into_songs_staging]

#Queries to TRANSFORM AND LOAD the required data into Fact and Dimension Tables
insert_commands = [insert_into_artists,insert_into_songs, insert_into_time, insert_into_users, insert_into_songplays]


#Queries to test if the data was successfully loaded in the Star Schema format
analytic_queries = [get_number_users, get_number_time, get_number_artists, get_number_songs, get_number_songplays, get_number_songs_staging, get_number_logs_staging,]
