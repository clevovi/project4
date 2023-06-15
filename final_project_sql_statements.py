class SqlQueries:
    songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) SELECT 
    TIMESTAMP 'epoch' + (se.ts/1000 * INTERVAL '1 second'),
    se.user_id,
    se.level,
    so.song_id,
    so.artist_id,
    se.session_id,
    se.location,
    se.user_agent
 FROM staging_events se
 LEFT JOIN staging_songs so ON
    se.song = so.title AND
    se.artist = so.artist_name AND
    ABS(se.length - so.duration) <2
 WHERE
    se.page = 'NextSong'   
""")

    user_table_insert = ("""
INSERT INTO users SELECT DISTINCT (user_id)
    user_id,
    first_name,
    last_name,
    gender,
    level
    FROM staging_events
""")

    song_table_insert = ("""
INSERT INTO songs SELECT DISTINCT (song_id)
    song_id,
    title,
    artist_id,
    year,
    duration
    FROM staging_songs
""")

    artist_table_insert = ("""
INSERT INTO artists SELECT DISTINCT (artist_id)
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
    FROM staging_songs
""")

    time_table_insert = ("""
INSERT INTO time
    WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts FROM staging_events)
    SELECT DISTINCT
    ts,
    extract(hour from ts),
    extract(day from ts),
    extract(week from ts),
    extract(year from ts),
    extract(weekday from ts)
    FROM temp_time
""")