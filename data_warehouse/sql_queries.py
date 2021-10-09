from create_database import roleArn

LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

# DROP TABLES

## STAGING TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS stg_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs"

## MAIN TABLES
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

## STAGING TABLES
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS stg_events (
artist VARCHAR
, auth VARCHAR
, firstName VARCHAR
, gender CHAR(1)
, itemInSession SMALLINT
, lastName VARCHAR
, length DECIMAL
, level CHAR(4)
, location VARCHAR
, method VARCHAR
, page VARCHAR
, registration DOUBLE PRECISION
, sessionId SMALLINT
, song VARCHAR
, status SMALLINT
, ts TIMESTAMP
, userAgent VARCHAR
, userId SMALLINT
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS stg_songs (
artist_id CHAR(18)
, artist_latitude DOUBLE PRECISION
, artist_location VARCHAR
, artist_longitude DOUBLE PRECISION
, artist_name VARCHAR
, duration DOUBLE PRECISION
, num_songs SMALLINT
, song_id CHAR(18)
, title VARCHAR
, year SMALLINT
)
""")

## MAIN TABLES

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time TIMESTAMP PRIMARY KEY
, hour SMALLINT
, day SMALLINT
, week SMALLINT
, month SMALLINT
, year SMALLINT
, weekday SMALLINT
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id SMALLINT PRIMARY KEY
, first_name VARCHAR
, last_name VARCHAR
, gender CHAR(1)
, level CHAR(4)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id CHAR(18) PRIMARY KEY
, title VARCHAR
, artist_id CHAR(18)
, year SMALLINT
, duration DOUBLE PRECISION
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id CHAR(18) PRIMARY KEY
, name VARCHAR
, location VARCHAR
, latitude DOUBLE PRECISION
, longitude DOUBLE PRECISION
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
start_time TIMESTAMP REFERENCES time(start_time) NOT NULL
, user_id SMALLINT REFERENCES users(user_id) NOT NULL
, level CHAR(4)
, song_id CHAR(18)
, artist_id CHAR(18)
, session_id SMALLINT
, location VARCHAR
, user_agent VARCHAR
, PRIMARY KEY (start_time, user_id)
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY stg_events FROM '{}'
IAM_ROLE '{}'
format as json '{}'
TIMEFORMAT as 'epochmillisecs'
""").format(LOG_DATA,roleArn,LOG_JSONPATH)

staging_songs_copy = ("""
COPY stg_songs FROM '{}'
IAM_ROLE '{}'
format as json 'auto' 
""").format(SONG_DATA,roleArn)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
ts as start_time
, e.userid as user_id
, e.level
, s.song_id
, s.artist_id
, e.sessionid as session_id
, e.location
, e.useragent as user_agent
FROM stg_events e
JOIN stg_songs s ON e.song = s.title AND e.artist = s.artist_name
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)

-- Subqueries to get the latest level
WITH levels AS (
SELECT userid, level, firstname, lastname, gender, max(ts) AS latest_at_level
FROM stg_events
GROUP BY 1,2,3,4,5)

, levels_ranked AS (
SELECT levels.*, rank() OVER (PARTITION BY userid ORDER BY latest_at_level DESC) AS rank
FROM levels
ORDER BY userid)

SELECT userid as user_id
, firstname as first_name
, lastname as last_name
, gender
, level 
FROM levels_ranked lr
WHERE lr.rank = 1
AND userid IS NOT NULL

""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id
, title
, artist_id
, year
, duration
FROM stg_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id
, artist_name AS name
, artist_location AS location
, artist_latitude AS latitude
, artist_longitude AS longitude 
FROM stg_songs
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts
, DATE_PART('hour',ts)::int
, DATE_PART('day',ts)::int
, DATE_PART('week',ts)::int
, DATE_PART('month',ts)::int
, DATE_PART('year',ts)::int
, DATE_PART('weekday',ts)::int
FROM stg_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, time_table_create, user_table_create, song_table_create, artist_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
