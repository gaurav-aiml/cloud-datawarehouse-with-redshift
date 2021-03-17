'''
Fact Table
    songplays - records in log data associated with song plays i.e. records with page NextSong
    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables
    users - users in the app
    user_id, first_name, last_name, gender, level
    songs - songs in music database
    song_id, title, artist_id, year, duration
    artists - artists in music database
    artist_id, name, location, latitude, longitude
    time - timestamps of records in songplays broken down into specific units
    start_time, hour, day, week, month, year, weekday
'''

create_table_songplays = """ CREATE TABLE IF NOT EXISTS songplays (
                             sonplay_id SERIAL PRIMARY_KEY NOT NULL,
                             start_time TIMESTAMP,
                             user_id INT NOT NULL,
                             level TEXT NOT NULL,
                             song_id TEXT NOT NULL,
                             artist_id TEXT NOT NULL,
                             session_id INT,
                             location TEXT,
                             user_agent TEXT); """

create_table_users = """ CREATE TABLE IF NOT EXISTS users (
                             user_id PRIMARY_KEY NOT NULL,
                             first_name TEXT,
                             last_name TEXT,
                             gender TEXT,
                             level TEXT NOT NULL); """


create_table_songs= """ CREATE TABLE IF NOT EXISTS songs (
                             song_id VARCHAR PRIMARY_KEY NOT NULL,
                             title TEXT,
                             artist_id TEXT NOT NULL,
                             year INT,
                             DURATION INT); """

create_table_artists= """ CREATE TABLE IF NOT EXISTS artists (
                             artist_id VARCHAR PRIMARY_KEY NOT NULL,
                             name TEXT,
                             lcoation TEXT NOT NULL,
                             latitude NUMERIC,
                             longitude NUMERIC); """

create_table_time= """ CREATE TABLE IF NOT EXISTS time (
                             start_time TIMESTAMP PRIMARY_KEY NOT NULL,
                             hour INT,
                             dat INT ,
                             week INT,
                             month INT,
                             year INT,
                             weekday INT); """


create_table_statements = [create_table_songplays, create_table_users, create_table_songs, create_table_artists, create_table_time]

