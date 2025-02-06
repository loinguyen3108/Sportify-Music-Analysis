
        BEGIN
            DECLARE table_exists BOOL;
            DECLARE min_date INT64;
            DECLARE max_date INT64;
            SET table_exists = (SELECT EXISTS(SELECT 1 FROM `spotify_dwh_staging.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'fact_album_track'));

            IF table_exists THEN
                
        SET (min_date, max_date) = (
            SELECT AS STRUCT MIN(release_date_key), MAX(release_date_key)
            FROM `spotify_dwh_staging.fact_album_track`
        );

        INSERT INTO `spotify_dwh.fact_album_track` (release_date_key, album_key, artist_key, genre_key, track_key)
        SELECT s.release_date_key, s.album_key, s.artist_key, s.genre_key, s.track_key
        FROM `spotify_dwh_staging.fact_album_track` s
        LEFT JOIN (
            SELECT *
            FROM `spotify_dwh.fact_album_track` t
            WHERE t.release_date_key BETWEEN min_date AND max_date
        ) p ON (p.release_date_key, p.album_key, p.artist_key, p.genre_key, p.track_key) = (s.release_date_key, s.album_key, s.artist_key, s.genre_key, s.track_key)
        WHERE p.release_date_key IS NULL;
    
            END IF;
        END;
    