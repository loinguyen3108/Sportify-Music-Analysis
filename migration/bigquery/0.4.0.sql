CREATE OR REPLACE TABLE `spotify_dwh.album_track_metric` (
  release_month   SMALLINT  NOT NULL,
  release_year    SMALLINT  NOT NULL,
  album_key       STRING    NOT NULL,
  artist_key      STRING    NOT NULL,
  tracks_count    INT64     NOT NULL
);


CREATE MATERIALIZED VIEW `spotify_dwh.user_genre_preference`
OPTIONS (
    enable_refresh = true, 
    refresh_interval_minutes = 1440,
    max_staleness = INTERVAL "3" DAY, 
    allow_non_incremental_definition = true
)
AS
SELECT 
  du.name,
  dg.name AS genre,
  SUM(CASE WHEN f.track_row = 1 THEN 1 ELSE 0 END) AS tracks_count,
  SUM(CASE WHEN f.track_row = 1 THEN dt.plays_count ELSE 0 END) AS tracks_plays_count
FROM (
  SELECT 
    ft.user_key,
    ft.genre_key, 
    ft.track_key,
    ROW_NUMBER() OVER (PARTITION BY ft.user_key, ft.genre_key, ft.track_key) AS track_row,
  FROM `spotify_dwh.fact_playlist_track` ft
  WHERE ft.genre_key != -1
) f
JOIN `spotify_dwh.dim_user` du
  ON f.user_key = du.user_key
JOIN `spotify_dwh.dim_genre` dg
  ON f.genre_key = dg.genre_key
JOIN `spotify_dwh.dim_track` dt
  ON f.track_key = dt.track_key
WHERE du.name != ''
GROUP BY 
  du.name,
  dg.name;


CREATE MATERIALIZED VIEW `spotify_dwh.favorite_track`
OPTIONS (
    enable_refresh = true, 
    refresh_interval_minutes = 1440,
    max_staleness = INTERVAL "3" DAY, 
    allow_non_incremental_definition = true
)
AS
WITH favorite_track AS (
  SELECT 
    dd.month_of_year AS added_month,
    dd.year AS added_year,
    f.track_key,
    dt.name AS track_name,
    dt.valid_from,
    dt.valid_to,
    CASE
      WHEN dt.duration_ms/60000 < 3 THEN 'short'
      WHEN dt.duration_ms/60000 <= 4 THEN 'normal'
      ELSE 'long'
    END as duration_category,
    dt.plays_count,
    ROW_NUMBER() OVER (
      PARTITION BY
        f.added_date_key,
        f.track_key,
        f.playlist_key
    ) AS playlist_row,
    ROW_NUMBER() OVER (
      PARTITION BY
        f.added_date_key,
        f.track_key,
        f.user_key 
    ) AS user_row
  FROM `spotify_dwh.fact_playlist_track` f
  JOIN `spotify_dwh.dim_date` dd
    ON f.added_date_key = dd.date_key
  LEFT JOIN `spotify_dwh.dim_track` dt
    ON f.track_key = dt.track_key
  WHERE dt.flag = TRUE
)
SELECT 
  added_month,
  added_year,
  track_key,
  track_name,
  duration_category,
  plays_count,
  SUM(CASE WHEN playlist_row = 1 THEN 1 ELSE 0 END) AS total_playlists,
  SUM(CASE WHEN user_row = 1 THEN 1 ELSE 0 END) AS total_users
FROM favorite_track
GROUP BY
  added_month,
  added_year,
  track_key,
  track_name,
  duration_category,
  plays_count;
