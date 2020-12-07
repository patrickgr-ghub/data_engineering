from connection import *

# DROP TABLES

events_staging_table_drop = ("DROP TABLE IF EXISTS events_staging")
songs_staging_table_drop = ("DROP TABLE IF EXISTS songs_staging")
songplays_table_drop = ("DROP TABLE IF EXISTS songplays")
users_table_drop = ("DROP TABLE IF EXISTS users")
songs_table_drop = ("DROP TABLE IF EXISTS songs")
artists_table_drop = ("DROP TABLE IF EXISTS artists")
time_table_drop = ("DROP TABLE IF EXISTS time")

# CREATE TABLES

events_staging_table_create= ("""
    CREATE TABLE "events_staging" (
        "artist" varchar,
        "auth" varchar,
        "firstName" varchar,
        "gender" varchar,
        "itemInSession" integer,
        "lastName" varchar,
        "length" float,
        "level" varchar,
        "location" varchar,
        "method" varchar,
        "page" varchar,
        "registration" float,
        "sessionId" integer,
        "song" varchar,
        "status" integer,
        "ts" numeric,
        "userAgent" varchar,
        "userId" integer
        );
""")

songs_staging_table_create = ("""
    CREATE TABLE "songs_staging" (
        "num_songs" integer,
        "artist_id" varchar,
        "artist_latitude" float,
        "artist_longitude" float,
        "artist_location" varchar,
        "artist_name" varchar,
        "song_id" varchar,
        "title" varchar,
        "duration" float,
        "year" integer
        );
""")

users_table_create = ("""
    CREATE TABLE "users" (
        "u_user_id"        integer      sortkey, 
        "u_first_name"     varchar, 
        "u_last_name"      varchar, 
        "u_gender"         varchar, 
        "u_level"          varchar)
    diststyle all;
""")

songs_table_create = ("""
    CREATE TABLE "songs" (
        "sgs_song_id"      varchar      sortkey  distkey,
        "sgs_title"        varchar,
        "sgs_artist_id"    varchar,
        "sgs_year"         integer,
        "sgs_duration"     float
        );

""")

artists_table_create = ("""
    CREATE TABLE "artists" (
        "a_artist_id"           varchar      not null   sortkey,
        "a_artist_name"         varchar,
        "a_artist_location"     varchar,
        "a_artist_latitude"     float,
        "a_artist_longitude"    float)
    diststyle all;
""")

time_table_create = ("""
    CREATE TABLE "time" (
        "t_ts"          numeric       not null sortkey,
        "t_hour"        integer,
        "t_day"         integer,
        "t_week"        integer,
        "t_month"       integer,
        "t_year"        integer,
        "t_dayofweek"   integer)
    diststyle all;

""")

songplays_table_create = ("""
    CREATE TABLE "songplays" (
        "sps_songplay_id"       integer identity(1,1) not null, 
        "sps_ts"                numeric               not null, 
        "sps_user_id"           integer               not null, 
        "sps_level"             varchar, 
        "sps_song_id"           varchar           distkey, 
        "sps_artist_id"         varchar           sortkey, 
        "sps_session_id"        integer, 
        "sps_artist_location"   varchar, 
        "sps_user_agent"        varchar
        );
""")


# STAGING TABLES

events_staging_copy = ("""

    COPY songs_staging
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    JSON 'auto' 

""").format(SONG_DATA, DWH_ROLE_ARN)

songs_staging_copy = ("""
    
    COPY events_staging
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    COMPUPDATE OFF
    FORMAT AS JSON {}
    TIMEFORMAT 'epochmillisecs'

""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)


# FINAL TABLES

time_table_insert = ("""
    INSERT INTO time (
            t_ts,
            t_hour,
            t_day,
            t_week,
            t_month,
            t_year,
            t_dayofweek            
            )
        
        SELECT distinct ts,
            EXTRACT(HOUR FROM t_start_time) AS t_hour,
            EXTRACT(DAY FROM t_start_time) AS t_day,
            EXTRACT(WEEK FROM t_start_time) AS t_week,
            EXTRACT(MONTH FROM t_start_time) AS t_month,
            EXTRACT(YEAR FROM t_start_time) AS t_year,
            EXTRACT(DOW FROM t_start_time) AS t_weekday

            FROM (SELECT distinct ts,'1970-01-01'::date + ts/1000 * interval '1 second' as t_start_time
                    FROM events_staging);
""")

songs_table_insert = ("""    
    INSERT INTO songs (
            sgs_song_id,
            sgs_title,
            sgs_artist_id,
            sgs_year,
            sgs_duration
            )
        
            SELECT 
                song_id,
                title,
                artist_id,
                year,
                duration
                
                FROM songs_staging;
""")

artists_table_insert = ("""
    INSERT INTO artists (
            a_artist_id,
            a_artist_name,
            a_artist_location,
            a_artist_latitude,
            a_artist_longitude
            )
        
            SELECT 
                DISTINCT artist_id,
                artist_name,
                artist_location,
                artist_latitude,
                artist_longitude
                
                FROM songs_staging
                WHERE artist_id IS NOT null;
""")

users_table_insert = ("""
    INSERT INTO users (
            u_user_id,
            u_first_name,
            u_last_name,
            u_gender,
            u_level           
            )

        SELECT 
            DISTINCT userId,
            firstName,
            lastName,
            gender,
            level
            
            FROM events_staging es1
                WHERE userId IS NOT null
                AND ts = (SELECT max(ts) FROM events_staging es2 WHERE es1.userId = es2.userId)
                ORDER BY userId DESC;
""")

songplays_table_insert = ("""
    INSERT INTO songplays (
            sps_ts,
            sps_user_id,
            sps_level,
            sps_song_id,
            sps_artist_id,
            sps_session_id,
            sps_artist_location,
            sps_user_agent
            )

                    SELECT
                        es.ts,
                        es.userId, 
                        es.level,
                        ss.song_id,
                        ss.artist_id, 
                        es.sessionId, 
                        es.location, 
                        es.userAgent
                    FROM events_staging es
                    JOIN songs_staging ss 
                        ON (es.artist = ss.artist_name)
                        AND (es.song = ss.title)
                        AND (es.length = ss.duration)
                        WHERE es.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [events_staging_table_create, songs_staging_table_create, songplays_table_create, users_table_create, songs_table_create, artists_table_create, time_table_create]

drop_table_queries = [events_staging_table_drop, songs_staging_table_drop, songplays_table_drop, users_table_drop, songs_table_drop, artists_table_drop, time_table_drop]

copy_table_queries = [events_staging_copy, songs_staging_copy]

insert_table_queries = [songplays_table_insert, users_table_insert, songs_table_insert, artists_table_insert, time_table_insert]
