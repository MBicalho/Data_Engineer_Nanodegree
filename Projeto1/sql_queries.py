# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay 
    (
            songplay_id SERIAL PRIMARY KEY,
            start_time TIMESTAMP NOT NULL,
            user_id int NOT NULL,
            artist_id varchar,
            session_id int,
            song_id varchar,
            location varchar,
            user_agent varchar,
            level varchar
    );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
    (
            user_id int PRIMARY KEY,
            first_name varchar NOT NULL,
            last_name varchar NOT NULL,
            gender char NOT NULL,
            level varchar
    );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song
    (
            song_id varchar PRIMARY KEY,
            title varchar NOT NULL,
            artist_id varchar,
            year int NOT NULL,
            duration numeric NOT NULL
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist
    (
            artist_id varchar PRIMARY KEY,
            name varchar NOT NULL,
            location varchar,
            latitude float,
            longitude float
    );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
    (
            start_time TIMESTAMP PRIMARY KEY,
            hour INT NOT NULL,
            day INT NOT NULL,
            week INT NOT NULL,
            month INT NOT NULL,
            year INT NOT NULL,
            weekday INT NOT NULL
    );
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplay 
                    (start_time,
                     user_id,
                     artist_id,
                     session_Id,
                     song_id,
                     location,
                     user_agent,
                     level)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (songplay_id)
DO NOTHING
""")

artist_table_insert = ("""

INSERT INTO artist 
                    (artist_id,
                     name,
                     location,
                     latitude,
                     longitude)
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT (artist_id)
DO NOTHING

""")

user_table_insert = ("""
INSERT INTO users 
                    (user_id,
                     first_name,
                     last_name,
                     gender,
                     level)
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT (user_id)
DO NOTHING
""")
    
song_table_insert = ("""
INSERT INTO song 
                    (song_id,
                     title,
                     artist_id,
                     year,
                     duration)
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT (song_id)
DO NOTHING
""")

time_table_insert = ("""
INSERT INTO time 
                    (start_time,
                     hour,
                     day,
                     week,
                     month,
                     year,
                     weekday)
VALUES(%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time)
DO NOTHING
""")

# FIND SONGS

song_select = ("""

SELECT a.artist_id, a.song_id FROM
            song A
            JOIN artist B ON A.artist_id = B.artist_id
            WHERE A.title = %s
            AND B.name = %s
            AND A.duration = %s
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]