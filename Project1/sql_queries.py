# DROP TABLES

songplay_table_drop = ""
user_table_drop = ""
song_table_drop = "DROP song_table"
artist_table_drop = "DROP artist_table"
time_table_drop = ""

# CREATE TABLES

#songplay_table_create = (""" """)

#user_table_create = (""" """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS song_table (
    song_id varchar, 
    title varchar NOT NULL, 
    artist_id varchar NOT NULL, 
    year int, 
    duration float,
    PRIMARY KEY(song_id))""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist_table (
    artist_id varchar, 
    artist_name varchar NOT NULL, 
    artist_location varchar, 
    artist_latitude float, 
    artist_longitude float,
    PRIMARY KEY(artist_id)) """)

#time_table_create = (""" """)

# INSERT RECORDS

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""INSERT INTO song_table (
    song_id, 
    title, 
    artist_id, 
    year, 
    duration) VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id) DO NOTHING""") 

artist_table_insert = ("""INSERT INTO artist_table (
    artist_id,
    artist_name, 
    artist_location, 
    artist_latitude, 
    artist_longitude) VALUES (%s, %s, %s, %s, %s) 
    ON CONFLICT (artist_id) DO NOTHING""")


time_table_insert = ("""
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [song_table_create, artist_table_create]#[songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]