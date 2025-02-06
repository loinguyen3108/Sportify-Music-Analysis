
        BEGIN
            DECLARE table_exists BOOL;
            DECLARE min_date INT64;
            DECLARE max_date INT64;
            SET table_exists = (SELECT EXISTS(SELECT 1 FROM `spotify_dwh_staging.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'fact_playlist_track'));

            IF table_exists THEN
                
        SET (min_date, max_date) = (
            SELECT AS STRUCT MIN(added_date_key), MAX(added_date_key)
            FROM `spotify_dwh_staging.fact_playlist_track`
        );

        INSERT INTO `spotify_dwh.fact_playlist_track` (added_date_key, playlist_key, user_key, genre_key, track_key, date_actual)
        SELECT s.added_date_key, s.playlist_key, s.user_key, s.genre_key, s.track_key, s.date_actual
        FROM `spotify_dwh_staging.fact_playlist_track` s
        LEFT JOIN (
            SELECT *
            FROM `spotify_dwh.fact_playlist_track` t
            WHERE t.added_date_key BETWEEN min_date AND max_date
        ) p ON (p.added_date_key, p.playlist_key, p.user_key, p.genre_key, p.track_key) = (s.added_date_key, s.playlist_key, s.user_key, s.genre_key, s.track_key)
        WHERE p.added_date_key IS NULL;
    
            END IF;
        END;
    