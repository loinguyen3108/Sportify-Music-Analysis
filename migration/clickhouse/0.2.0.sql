CREATE DATABASE spotify;

-- artist
CREATE TABLE spotify.artist_metrics_overtimes
(
    artist_key 			String,
    tracked_at 			DateTime,
    followers_count		UInt64,
    monthly_listeners	UInt64,
    kafka_time 			Nullable(DateTime),
    kafka_offset 		UInt64
)
ENGINE = ReplacingMergeTree
ORDER BY (artist_key, tracked_at)
PARTITION BY toYYYYMMDD(tracked_at)
SETTINGS index_granularity = 8192;


CREATE TABLE spotify.kafka__artists
(
    artist_id 			String,
    updated_at 			UInt64,
    followers_count		UInt64,
    monthly_listeners	UInt64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
kafka_topic_list = 'spotify.public.artist',
kafka_group_name = 'clickhouse-artist',
kafka_format = 'AvroConfluent',
format_avro_schema_registry_url='http://schema-registry:8081';


CREATE MATERIALIZED VIEW spotify.consumer__artists TO spotify.artist_metrics_overtimes
(
    artist_key 			String,
    tracked_at 			DateTime,
    followers_count		UInt64,
    monthly_listeners	UInt64,
    kafka_time 			Nullable(DateTime),
    kafka_offset 		UInt64
) AS
SELECT
    artist_id AS artist_key,
    toDateTime(updated_at / 1000000) AS tracked_at,
    followers_count,
    monthly_listeners,
    _timestamp AS kafka_time,
    _offset AS kafka_offset
FROM spotify.kafka__artists;


-- playlist
CREATE TABLE spotify.playlist_metrics_overtimes
(
    playlist_key 		String,
    tracked_at 			DateTime,
    followers_count		UInt64,
    kafka_time 			Nullable(DateTime),
    kafka_offset 		UInt64
)
ENGINE = ReplacingMergeTree
ORDER BY (playlist_key, tracked_at)
PARTITION BY toYYYYMMDD(tracked_at)
SETTINGS index_granularity = 8192;


CREATE TABLE spotify.kafka__playlists
(
    playlist_id 		String,
    updated_at 			UInt64,
    followers_count		UInt64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
kafka_topic_list = 'spotify.public.playlist',
kafka_group_name = 'clickhouse-playlist',
kafka_format = 'AvroConfluent',
format_avro_schema_registry_url='http://schema-registry:8081';


CREATE MATERIALIZED VIEW spotify.consumer__playlists TO spotify.playlist_metrics_overtimes
(
    playlist_key 		String,
    tracked_at 			DateTime,
    followers_count		UInt64,
    kafka_time 			Nullable(DateTime),
    kafka_offset 		UInt64
) AS
SELECT
    playlist_id AS playlist_key,
    toDateTime(updated_at / 1000000) AS tracked_at,
    followers_count,
    _timestamp AS kafka_time,
    _offset AS kafka_offset
FROM spotify.kafka__playlists;


-- track
CREATE TABLE spotify.track_metrics_overtimes
(
    track_key 			String,
    tracked_at 			DateTime,
    plays_count		    UInt64,
    kafka_time 			Nullable(DateTime),
    kafka_offset 		UInt64
)
ENGINE = ReplacingMergeTree
ORDER BY (track_key, tracked_at)
PARTITION BY toYYYYMMDD(tracked_at)
SETTINGS index_granularity = 8192;


CREATE TABLE spotify.kafka__tracks
(
    track_id 		String,
    updated_at 		UInt64,
    plays_count	    UInt64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
kafka_topic_list = 'spotify.public.track',
kafka_group_name = 'clickhouse-track',
kafka_format = 'AvroConfluent',
format_avro_schema_registry_url='http://schema-registry:8081';


CREATE MATERIALIZED VIEW spotify.consumer__tracks TO spotify.track_metrics_overtimes
(
    track_key 		String,
    tracked_at 		DateTime,
    plays_count	    UInt64,
    kafka_time 		Nullable(DateTime),
    kafka_offset    UInt64
) AS
SELECT
    track_id AS track_key,
    toDateTime(updated_at / 1000000) AS tracked_at,
    plays_count,
    _timestamp AS kafka_time,
    _offset AS kafka_offset
FROM spotify.kafka__tracks;