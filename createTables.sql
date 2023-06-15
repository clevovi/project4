
CREATE TABLE IF NOT EXISTS staging_events (
artist TEXT,
auth TEXT,
first_name TEXT,
gender TEXT,
item_in_session TEXT,
last_name TEXT,
length FLOAT,
level TEXT,
location TEXT,
method TEXT,
page TEXT,
registration FLOAT,
session_id TEXT,
song TEXT,
status INTEGER,
ts BIGINT,
user_agent TEXT,
user_id TEXT
);

CREATE TABLE IF NOT EXISTS staging_songs (
song_id TEXT,
title TEXT,
duration FLOAT,
year INTEGER,
artist_id TEXT,
artist_name TEXT,
artist_latitude TEXT,
artist_longitude TEXT,
artist_location TEXT,
num_songs INTEGER
);

CREATE TABLE IF NOT EXISTS songplays (
songplay_id INTEGER IDENTITY(1,1)PRIMARY KEY,
start_time TIMESTAMP NOT NULL SORTKEY,
user_id TEXT DISTKEY,
level TEXT,
song_id TEXT,
artist_id TEXT,
session_id TEXT,
location TEXT,
user_agent TEXT
) diststyle key;


CREATE TABLE IF NOT EXISTS users (
user_id TEXT PRIMARY KEY SORTKEY,
first_name TEXT,
last_name TEXT,
gender TEXT,
level TEXT
) diststyle all;


CREATE TABLE IF NOT EXISTS songs (
song_id TEXT PRIMARY KEY SORTKEY,
title TEXT,
artist_id TEXT,
year INTEGER,
duration FLOAT
)diststyle all;

CREATE TABLE IF NOT EXISTS artists (
artist_id TEXT PRIMARY KEY SORTKEY,
name TEXT,
location TEXT,
latitude TEXT,
longitude TEXT
) diststyle all;

CREATE TABLE IF NOT EXISTS time (
start_time TIMESTAMP PRIMARY KEY SORTKEY,
hour INTEGER,
day TEXT,
week TEXT,
month TEXT,
year INTEGER,
weekday TEXT
)diststyle all;
