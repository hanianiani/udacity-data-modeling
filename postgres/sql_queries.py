# DROP TABLES

artist_table_drop = "DROP TABLE IF EXISTS artists"
song_table_drop = "DROP TABLE IF EXISTS songs"
user_table_drop = "DROP TABLE IF EXISTS users"
time_table_drop = "DROP TABLE IF EXISTS time"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"


# CREATE TABLES

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(song_id TEXT PRIMARY KEY
, title TEXT
, artist_id TEXT
, year INT
, duration FLOAT)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
(artist_id TEXT PRIMARY KEY
, name TEXT
, location TEXT
, latitude FLOAT
, longitude FLOAT)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
(start_time TIMESTAMP PRIMARY KEY
, hour INT
, day INT
, week INT
, month INT
, year INT
, weekday INT)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
(user_id INT PRIMARY KEY
, first_name TEXT
, last_name TEXT
, gender char(1)
, level TEXT)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(start_time TIMESTAMP REFERENCES time(start_time) NOT NULL
, user_id INT REFERENCES users(user_id) NOT NULL
, level TEXT
, song_id TEXT
, artist_id TEXT
, session_id INT
, location TEXT
, user_agent TEXT
, PRIMARY KEY (start_time, user_id))
""")

# INSERT RECORDS

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id)
DO NOTHING
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id)
DO NOTHING
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time)
DO NOTHING
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT (user_id)
DO UPDATE
SET level=excluded.level
""")

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time, user_id)
DO NOTHING
""")

# FIND SONGS

song_select = ("""
SELECT a.artist_id, s.song_id
FROM songs s 
JOIN artists a USING (artist_id)
WHERE a.name = %s
AND s.title = %s
AND s.duration = %s
""")

# QUERY LISTS

create_table_queries = [song_table_create, artist_table_create, time_table_create, user_table_create, songplay_table_create]
drop_table_queries = [song_table_drop, artist_table_drop, time_table_drop, user_table_drop, songplay_table_drop]