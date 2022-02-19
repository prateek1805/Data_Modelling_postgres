# DROP TABLES

songplay_table_drop = """DROP TABLE IF EXISTS Songplays"""

user_table_drop = """DROP TABLE IF EXISTS Users""" 

song_table_drop = """DROP TABLE IF EXISTS Songs""" 

artist_table_drop = """DROP TABLE IF EXISTS Artists"""

time_table_drop = """DROP TABLE IF EXISTS Time"""
 

# CREATE TABLES


songplay_table_create =("""CREATE TABLE Songplays (
                                 songplay_id varchar  PRIMARY KEY,
                
                                 start_time TIME CONSTRAINT songplays_time                    
                                 REFERENCES Time(start_time),
                        
                                 user_id varchar CONSTRAINT songplays_users
                                 REFERENCES Users(user_id), 
                        
                                 level Varchar,
                                 song_id varchar CONSTRAINT songplays_songs
                                 REFERENCES Songs(song_id),
                        
                                 artist_id varchar CONSTRAINT songplays_Artists
                                 REFERENCES Artists(artist_id),
                        
                                 session_id int, 
                                 location varchar, 
                                 user_agent varchar
                        );""")


user_table_create = ("""CREATE TABLE Users(
                                user_id Varchar  PRIMARY KEY,
                                first_name Varchar   NOT NULL,
                                last_name varchar    NOT NULL, 
                                gender varchar, 
                                level varchar
                        );""")


song_table_create = ("""CREATE TABLE Songs(
                                song_id Varchar  PRIMARY KEY,
                                title Varchar,
                                artist_id Varchar NOT NULL,
                                year int,
                                duration float
                        );""")


artist_table_create = ("""CREATE TABLE Artists(artist_id Varchar  PRIMARY KEY,
                                name Varchar,
                                location varchar, 
                                latitude float, 
                                longitude float);""")


time_table_create =("""CREATE TABLE Time(
                                start_time timestamp  PRIMARY KEY,
                                 hour int, 
                                 day int, 
                                 week int,
                                 month int, 
                                 year int, 
                                 weekday int);""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO Songplays(songplay_id,start_time,user_id,level,song_id,artist_id,session_id,                                     location, user_agent)
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(songplay_id) DO NOTHING;
""")

user_table_insert = ("""INSERT INTO Users(user_id,first_name,last_name,gender,level) 
                         VALUES(%s,%s,%s,%s,%s) ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;
""")

song_table_insert = ("""INSERT INTO Songs(song_id,title,artist_id,year,duration) 
                        VALUES(%s,%s,%s,%s,%s) ON CONFLICT(song_id) DO NOTHING;
""")

artist_table_insert = ("""INSERT INTO Artists(artist_id,name,location, latitude,longitude) 
                          VALUES(%s,%s,%s,%s,%s)  ON CONFLICT (artist_id) DO UPDATE SET
                          location = EXCLUDED.location,
                          latitude = EXCLUDED.latitude,
                          longitude = EXCLUDED.longitude;
""")


time_table_insert = ("""INSERT INTO Time(start_time,hour,day,week,month,year,weekday) 
                        VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(start_time) DO NOTHING;
""")

# FIND SONGS

song_select = ("""
            SELECT ss.song_id, ss.artist_id FROM songs ss 
            JOIN artists ar on ss.artist_id = ar.artist_id
            WHERE ss.title = %s
            AND ar.name = %s
            AND ss.duration = %s
""")

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create,song_table_create, time_table_create,songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]