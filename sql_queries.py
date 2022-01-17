# DROP TABLES
songplay_table_drop = "DROP TABLE IF NOT EXISTS songplay;"
users_table_drop = "DROP TABLE IF NOT EXISTS users;"
song_table_drop = "DROP TABLE IF NOT EXISTS song;"
artist_table_drop = "DROP TABLE IF NOT EXISTS artist;"
time_table_drop = "DROP TABLE IF NOT EXISTS time;"

# CREATE TABLES
# DIMENSION TABLES
users_table_create = ("""CREATE TABLE IF NOT EXISTS users
                        (user_id int NOT NULL PRIMARY KEY,
                         first_name varchar,
                         last_name varchar,
                         gender varchar, 
                         level varchar);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist
                         (artist_id varchar NOT NULL PRIMARY KEY,
                          artist_name varchar,
                          artist_location varchar,
                          artist_latitude float(8), 
                          artist_longitude float(8));
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song
                       (song_id varchar NOT NULL PRIMARY KEY,
                        title varchar,
                        artist_id varchar,
                        year int, 
                        duration float);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
                       (start_time timestamp NOT NULL PRIMARY KEY,
                        hour int,
                        day int,
                        week int, 
                        month int,
                        year int, 
                        weekday int);
""")

# FACT TABLE
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay
                            (songplay_id SERIAL NOT NULL PRIMARY KEY,
                             start_time timestamp NOT NULL,
                             user_id int NOT NULL,
                             level varchar,
                             song_id varchar, 
                             artist_id varchar,
                             session_id varchar,
                             location varchar, 
                             user_agent varchar);
""")

# INSERT RECORDS
songplay_table_insert = ("""INSERT INTO songplay
                            (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                             ON CONFLICT (songplay_id) 
                             DO NOTHING;
""")

users_table_insert = ("""INSERT INTO users
                         (user_id, first_name, last_name, gender, level)
                          VALUES (%s, %s, %s, %s, %s)
                          ON CONFLICT (user_id) 
                          DO UPDATE 
                              SET level = EXCLUDED.level;
""")

song_table_insert = ("""INSERT INTO song
                        (song_id, title, artist_id, year, duration)
                         VALUES (%s, %s, %s, %s, %s)
                         ON CONFLICT (song_id) 
                         DO NOTHING;
""")

artist_table_insert = ("""INSERT INTO artist
                          (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
                           VALUES (%s, %s, %s, %s, %s)
                           ON CONFLICT (artist_id) 
                           DO NOTHING;
""")


time_table_insert = ("""INSERT INTO time
                        (start_time, hour, day, week, month, year, weekday)
                         VALUES (%s, %s, %s, %s, %s, %s, %s)
                         ON CONFLICT (start_time) 
                         DO NOTHING;
""")

# FIND SONGS
song_select = ("""SELECT song.song_id, artist.artist_id 
                  FROM song INNER JOIN artist 
                  ON song.artist_id = artist.artist_id
                  WHERE song.title = %s AND artist.artist_name = %s AND song.duration = %s;
""")


# QUERY LISTS 
create_table_queries = [users_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, users_table_drop, song_table_drop, artist_table_drop, time_table_drop]
