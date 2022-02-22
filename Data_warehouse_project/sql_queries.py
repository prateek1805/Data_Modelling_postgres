import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')                                          #Reading the dwh.cfg file to get the values under [cluster]

# DROP TABLES

staging_events_table_drop = """DROP TABLE iF EXISTS Staging_events"""

staging_songs_table_drop = """DROP TABLE iF EXISTS Staging_songs"""

songplay_table_drop = """DROP TABLE iF EXISTS Songplays"""

user_table_drop = """DROP TABLE iF EXISTS Users"""

song_table_drop = """DROP TABLE iF EXISTS Songs"""

artist_table_drop = """DROP TABLE iF EXISTS Artists"""

time_table_drop = """DROP TABLE iF EXISTS Time"""

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE Staging_events(
     artist VARCHAR,
     auth VARCHAR,
    first_Name VARCHAR(50),
    gender CHAR,
    itemInSession INTEGER,
    last_Name VARCHAR(50),
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER
                );

""")

staging_songs_table_create = ("""CREATE TABLE Staging_songs(
    num_songs INTEGER,
    artist_id VARCHAR,
    a_latitude FLOAT,
    a_longitude FLOAT,
    a_location VARCHAR,
    a_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year FLOAT
)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS Songplays(
                                songplay_id int NOT NULL PRIMARY KEY sortkey,
                
                                start_time TIME CONSTRAINT songplays_time
                                 REFERENCES Time(start_time),
                        
                                  user_id int CONSTRAINT songplays_users
                                  REFERENCES Users(user_id), 
                        
                                  level Varchar,
                                  
                                  song_id int CONSTRAINT songplays_songs
                                   REFERENCES Songs(song_id),
                        
                                  artist_id int CONSTRAINT songplays_Artists
                                    REFERENCES Artists(artist_id) distkey,
                        
                                  session_id int, 
                                  location varchar,
                                  user_agent varchar
                            );""")


user_table_create = ("""CREATE TABLE Users(

                          user_id Varchar PRIMARY KEY,
                          first_name Varchar, 
                          last_name varchar, 
                          gender varchar, 
                          level varchar
                          
                    );""")

song_table_create = ("""CREATE TABLE Songs(

                            song_id Varchar NOT NULL PRIMARY KEY,
                             title Varchar,
                             artist_id Varchar 
                              year int,
                             duration float
                             
                    );""")

artist_table_create = ("""CREATE TABLE Artists(

                                artist_id Varchar PRIMARY KEY,
                                name Varchar, 
                                location varchar, 
                                latitude float, 
                                longitude float
                            
                        );""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS Time(

                                start_time time NOT NULL PRIMARY KEY distkey,
                                hour int, 
                                day int, 
                                week int,
                                month int,
                                year int, 
                                weekday varchar
                                
                        );""")

# STAGING TABLES

LOG_DATA              = config.get("S3","LOG_DATA")
LOG_JSONPATH          = config.get("S3","LOG_JSONPATH")
SONG_DATA             = config.get("S3","SONG_DATA")
ARN                   = config.get("IAM_ROLE","ARN")

# Queries to copy the data from s3 to staging tables

staging_events_copy = ("""Copy Staging_events from {}
                          credentials 'aws_iam_role={}' 
                          gzip delimiter ';' compupdate off region 'us-west-2'
                          FORMAT AS JSON {};""").format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""Copy Staging_songs from {}
                          credentials 'aws_iam_role={}' 
                          gzip delimiter ';' compupdate off region 'us-west-2'
                          FORMAT AS JSON {};""").format(SONG_DATA,ARN,LOG_JSONPATH)

# FINAL TABLES
# These queries are used to insert the data from staging table to analytics table 

songplay_table_insert = ("""INSERT INTO Songplays(songplay_id,start_time,user_id,level,song_id,artist_id,session_id,location,                                   user_agent)
                
                SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
                se.userId as user_id,
                se.level as level,
                ss.song_id as song_id,
                ss.artist_id as artist_id,
                se.sessionId as session_id,
                se.location as location,
                se.userAgent as user_agent
                
                FROM staging_events se
                JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name;

""")

user_table_insert = (""" INSERT INTO Users(user_id,first_name,last_name,gender,level)

                            SELECT DISTINCT user_Id as user_id, 
                            first_name as first_name,
                            last_name as last_name,
                            gender as gender,
                            level as level
                            
                            FROM Staging_events group by user_Id 
                            order by user_Id
                            WHERE user_Id IS NOT NULL
""")

song_table_insert = ("""INSERT INTO Songs(song_id,title,artist_id,year,duration)

                        SELECT DISTINCT song_id as song_id, 
                            title as title,
                            artist_id as artist_id,
                            year as year,
                            duration as duration
                            
                            FROM Staging_songs group by song_Id 
                            order by song_Id
                            WHERE song_Id IS NOT NULL
""")

artist_table_insert = ("""INSERT INTO Artists(artist_id,name,location, latitude,longitude)

                          SELECT DISTINCT artist_id as artist_id, a_name as name,
                             a_location as location, a_latitude as latitude,
                             a_longitude as longitude
                             
                            FROM Staging_songs group by artist_id 
                            order by artist_id
                            WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""INSERT INTO Time(start_time,hour,day,week,month,year,weekday)

                        SELECT DISTINCT ts,
                        EXTRACT(hour from ts)
                        EXTRACT(day from ts)
                        EXTRACT(week from ts)
                        EXTRACT(month from ts)
                        EXTRACT(year from ts)
                        EXTRACT(weekday from ts)
                        
                        FROM Staging_events
                        WHERE ts IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert,]
