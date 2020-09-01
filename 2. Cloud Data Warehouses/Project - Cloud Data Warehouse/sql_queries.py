import configparser

config = configparser.ConfigParser()
config.read("dwh.cfg")

DWH_IAM_ROLE = config['IAM_ROLE']['ARN']


staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


staging_events_table_create = """
    CREATE TABLE staging_events (
    artist varchar(200),
    auth varchar(100),
    first_name varchar(200),
    gender varchar(10),
    item_in_session integer,
    last_name varchar(200),
    length double precision,
    level varchar(100),
    location varchar(300),
    method varchar(20),
    page varchar(50),
    registration bigint,
    session_id integer,
    song varchar(300),
    status smallint,
    ts timestamp,
    user_agent varchar(500),
    user_id integer
);
"""

staging_songs_table_create = """
    CREATE TABLE staging_songs (
    num_songs integer,
    artist_id varchar(300),
    artist_latitude double precision,
    artist_longitude double precision,
    artist_location varchar(300),
    artist_name varchar(300),
    song_id varchar(100),
    title varchar(300) ,
    duration double precision,
    year integer
);
"""

songplay_table_create = """
    CREATE TABLE songplay (
    songplay_id bigint IDENTITY(0, 1) not null,
    start_time bigint,
    user_id integer not null,
    level varchar(100),
    song_id varchar(100) not null,
    artist_id varchar(300) not null, 
    session_id integer not null,
    location varchar(300),
    user_agent varchar(500)
);
"""

user_table_create = """
    CREATE TABLE users (
    user_id integer not null,
    first_name varchar(200),
    last_name varchar(200),
    gender varchar(10),
    level varchar(100)
);
"""

song_table_create = """
    CREATE TABLE songs (
    song_id varchar(100) not null,
    title varchar(300),
    artist_id varchar(300) not null,
    year integer,
    duration double precision
);
"""

artist_table_create = """
    CREATE TABLE artists (
    artist_id varchar(300) not null,
    name varchar(300),
    location varchar(300),
    latitude double precision,
    longitude double precision
);
"""

time_table_create = """
    CREATE TABLE time (
    startime timestamp not null,
    hour integer,
    day integer,
    week integer,
    month integer,
    year integer,
    weekday integer
);
"""


staging_events_copy = """
    COPY staging_events FROM 's3://udacity-dend/log_data/'
    CREDENTIALS 'aws_iam_role={}'
    JSON 'auto';
""".format(DWH_IAM_ROLE)

staging_songs_copy = """
    COPY staging_songs FROM 's3://udacity-dend/song_data/'
    CREDENTIALS 'aws_iam_role={}'
    JSON 'auto';
""".format(DWH_IAM_ROLE)


songplay_table_insert = """
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    (SELECt se.ts, 
            se.user_id, 
            se.level, 
            ss.song_id, 
            ss.artist_id, 
            se.session_id, 
            se.location, 
            se.user_agent
     FROM (SELECT ts, user_id, level, session_id, location, user_agent, song, artist
           FROM staging_events 
           WHERE page = 'NextSong' AND 
                 user_id IS NOT NULL AND
                 session_id IS NOT NULL) se
     JOIN (SELECT * FROM staging_songs 
           WHERE song_id IS NOT NULL AND 
                 artist_id IS NOT NULL) ss
     ON (se.song = ss.title AND se.artist = ss.artist_name))
"""

user_table_insert = """
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    (SELECT distinct user_id, 
            first_name, 
            last_name, 
            gender, 
            level 
     FROM staging_events WHERE user_id IS NOT NULL)
"""

song_table_insert = """
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    (SELECT song_id,
            title, 
            artist_id,
            year,
            duration
     FROM staging_songs WHERE song_id IS NOT NULL)
"""

artist_table_insert = """
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    (SELECT artist_id, 
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
     FROM staging_songs WHERE artist_id IS NOT NULL)
"""

time_table_insert = """
    INSERT INTO time (startime, hour, day, week, month, year, weekday)
    (SELECT se.starttime as starttime,
            EXTRACT(hour FROM se.starttime) as hour,
            EXTRACT(day FROM se.starttime) as day,
            EXTRACT(week FROM se.starttime) as week,
            EXTRACT(month FROM se.starttime) as month,
            EXTRACT(year FROM se.starttime) as year,
            EXTRACT(weekday FROM se.starttime) as weekday
     FROM
     (SELECT timestamp 'epoch' + ts * INTERVAL '1 second' as starttime
      FROM staging_events WHERE starttime IS NOT NULL) se)
"""


create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
