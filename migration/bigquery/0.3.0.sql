CREATE OR REPLACE TABLE `spotify_dwh.dim_date` (
  date_key                  INT         NOT NULL,
  date_actual               DATE,
  day_of_week               SMALLINT    NOT NULL,
  day_of_month              SMALLINT    NOT NULL,
  day_of_year               SMALLINT    NOT NULL,
  month_of_year             SMALLINT    NOT NULL,
  month_name                STRING      NOT NULL,
  quarter                   SMALLINT    NOT NULL,
  year                      SMALLINT    NOT NULL,
  is_weekend                BOOLEAN,
  PRIMARY KEY (date_key) NOT ENFORCED
);

CREATE OR REPLACE TABLE `spotify_dwh.dim_playlist` (
    playlist_key            String      NOT NULL,
    name                    String      NOT NULL,
    url                     String      NOT NULL,
    collaborative           Bool        NOT NULL,
    is_public               Bool        NOT NULL,
    snapshot_id             String      NOT NULL,
    followers_count         INT64       NOT NULL,
    user_key                String      NOT NULL,
    valid_from              Date        NOT NULL,
    valid_to                Date,
    flag                    Bool        NOT NULL,
    PRIMARY KEY (playlist_key) NOT ENFORCED
);

CREATE OR REPLACE TABLE `spotify_dwh.dim_user` (
    user_key                String      NOT NULL,
    name                    String      NOT NULL,
    url                     String      NOT NULL,
    created_at              Timestamp   NOT NULL,
    updated_at              Timestamp   NOT NULL,
    PRIMARY KEY (user_key) NOT ENFORCED
);

CREATE OR REPLACE TABLE `spotify_dwh.dim_genre` (
    genre_key               INT64       NOT NULL,
    name                    String      NOT NULL,
    created_at              Timestamp   NOT NULL,
    updated_at              Timestamp   NOT NULL,
    PRIMARY KEY (genre_key) NOT ENFORCED
);

CREATE OR REPLACE TABLE `spotify_dwh.dim_artist` (
    artist_key              String      NOT NULL,
    name                    String      NOT NULL,
    genres                  ARRAY<String>,
    url                     String      NOT NULL,
    followers_count         INT64       NOT NULL,
    monthly_listeners       INT64       NOT NULL,
    valid_from              Date        NOT NULL,
    valid_to                Date,
    flag                    Bool        NOT NULL,
    PRIMARY KEY (artist_key) NOT ENFORCED
);

CREATE OR REPLACE TABLE `spotify_dwh.dim_album` (
    album_key               String      NOT NULL,
    name                    String      NOT NULL,
    url                     String      NOT NULL,
    cover_image             String      NOT NULL,
    album_type              SMALLINT    NOT NULL,
    release_date_key        INT64       NOT NULL,
    release_date_precision  SMALLINT    NOT NULL,
    label                   String      NOT NULL,
    created_at              Timestamp   NOT NULL,
    updated_at              Timestamp   NOT NULL,
    PRIMARY KEY (album_key) NOT ENFORCED
);

CREATE OR REPLACE TABLE `spotify_dwh.dim_track` (
    track_key               String      NOT NULL,
    name                    String      NOT NULL,
    url                     STRING      NOT NULL,
    album_key               String      NOT NULL,
    disc_number             INT64       NOT NULL,
    duration_ms             INT64       NOT NULL,
    is_explicit             Bool        NOT NULL,
    plays_count             INT64       NOT NULL,
    valid_from              Date        NOT NULL,
    valid_to                Date,
    flag                    Bool        NOT NULL,
    PRIMARY KEY (track_key) NOT ENFORCED
);

CREATE OR REPLACE TABLE `spotify_dwh.fact_playlist_track` (
    added_date_key          INT64       NOT NULL,
    playlist_key            String      NOT NULL,
    user_key                String      NOT NULL,
    genre_key               INT64       NOT NULL,
    track_key               String      NOT NULL,
    date_actual             DATE        NOT NULL,
    PRIMARY KEY (added_date_key, playlist_key, user_key, genre_key, track_key) NOT ENFORCED,
    FOREIGN KEY (added_date_key) REFERENCES `spotify_dwh.dim_date` (date_key) NOT ENFORCED,
    FOREIGN KEY (playlist_key) REFERENCES `spotify_dwh.dim_playlist` (playlist_key) NOT ENFORCED,
    FOREIGN KEY (user_key) REFERENCES `spotify_dwh.dim_user` (user_key) NOT ENFORCED,
    FOREIGN KEY (genre_key) REFERENCES `spotify_dwh.dim_genre` (genre_key) NOT ENFORCED,
    FOREIGN KEY (track_key) REFERENCES `spotify_dwh.dim_track` (track_key) NOT ENFORCED
)
PARTITION BY
    DATE_TRUNC(date_actual, MONTH)
OPTIONS (
    require_partition_filter = TRUE
);

CREATE OR REPLACE TABLE `spotify_dwh.fact_album_track` (
    release_date_key        INT64       NOT NULL,
    album_key               String      NOT NULL,
    artist_key              String      NOT NULL,
    genre_key               INT64       NOT NULL,
    track_key               String      NOT NULL,
    PRIMARY KEY (release_date_key, album_key, artist_key, genre_key, track_key) NOT ENFORCED,
    FOREIGN KEY (release_date_key) REFERENCES `spotify_dwh.dim_date` (date_key) NOT ENFORCED,
    FOREIGN KEY (album_key) REFERENCES `spotify_dwh.dim_album` (album_key) NOT ENFORCED,
    FOREIGN KEY (artist_key) REFERENCES `spotify_dwh.dim_artist` (artist_key) NOT ENFORCED,
    FOREIGN KEY (genre_key) REFERENCES `spotify_dwh.dim_genre` (genre_key) NOT ENFORCED,
    FOREIGN KEY (track_key) REFERENCES `spotify_dwh.dim_track` (track_key) NOT ENFORCED
)
PARTITION BY
    RANGE_BUCKET(release_date_key, GENERATE_ARRAY(0, 20991231, 50000))
OPTIONS (
    require_partition_filter = TRUE
);

-- Insert explicit dates
INSERT INTO `spotify_dwh.dim_date`
SELECT
    CAST(FORMAT_DATE('%Y%m%d', datum) AS INT64) AS date_key,
    datum AS date_actual,
    EXTRACT(DAYOFWEEK FROM datum) AS day_of_week,  --Note:  BigQuery's DAYOFWEEK starts on Sunday=1
    EXTRACT(DAY FROM datum) AS day_of_month,
    EXTRACT(DAYOFYEAR FROM datum) AS day_of_year,
    EXTRACT(MONTH FROM datum) AS month_of_year,
    FORMAT_DATE('%B', datum) AS month_name,  --Use FORMAT_DATE for month name
    EXTRACT(QUARTER FROM datum) AS quarter,
    EXTRACT(YEAR FROM datum) AS year,
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM datum) IN (1, 7) THEN TRUE  --BigQuery's DAYOFWEEK starts on Sunday=1
        ELSE FALSE
    END AS is_weekend
FROM
    UNNEST(GENERATE_DATE_ARRAY('1700-01-01', '2099-12-31')) AS datum;

-- Insert row for unknown date
INSERT INTO `spotify_dwh.dim_date` (date_key, date_actual, day_of_week, day_of_month, day_of_year, month_of_year, month_name, quarter, year, is_weekend)
VALUES (0, NULL, -1, -1, -1, -1, 'unknown', -1, -1, NULL);

-- Insert rows for YEAR precision (YYYY0000)
INSERT INTO `spotify_dwh.dim_date` (date_key, date_actual, day_of_week, day_of_month, day_of_year, month_of_year, month_name, quarter, year, is_weekend)
SELECT
    CAST(FORMAT_DATE('%Y0000', datum) AS INT64),
    NULL,
    -1,
    -1,
    -1,
    -1,
    'unknown',
    -1,
    EXTRACT(YEAR FROM datum),
    NULL
FROM
    UNNEST(GENERATE_DATE_ARRAY('1700-01-01', '2099-12-31', INTERVAL 1 YEAR)) AS datum;

-- Insert rows for MONTH precision (YYYYMM00)
INSERT INTO `spotify_dwh.dim_date` (date_key, date_actual, day_of_week, day_of_month, day_of_year, month_of_year, month_name, quarter, year, is_weekend)
SELECT
    CAST(FORMAT_DATE('%Y%m00', datum) AS INT64),
    NULL,
    -1,
    -1,
    -1,
    EXTRACT(MONTH FROM datum),
    FORMAT_DATE('%B', datum),
    -1,
    EXTRACT(YEAR FROM datum),
    NULL
FROM
    UNNEST(GENERATE_DATE_ARRAY('1700-01-01', '2099-12-31', INTERVAL 1 MONTH)) AS datum;

INSERT INTO `spotify_dwh.dim_genre` (genre_key, name, created_at, updated_at)
VALUES (-1, 'unknown', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
