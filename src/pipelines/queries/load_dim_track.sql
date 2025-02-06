
        BEGIN
            DECLARE table_exists BOOL;
            DECLARE min_date INT64;
            DECLARE max_date INT64;
            SET table_exists = (SELECT EXISTS(SELECT 1 FROM `spotify_dwh_staging.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'dim_track'));

            IF table_exists THEN
                
            MERGE INTO `spotify_dwh.dim_track` p
            USING (
            SELECT track_key AS join_key, *
            FROM `spotify_dwh_staging.dim_track`
            UNION ALL
            SELECT NULL, s.*
            FROM `spotify_dwh_staging.dim_track` s
            JOIN `spotify_dwh.dim_track` t
                ON s.track_key = t.track_key
            WHERE SHA256(CONCAT(t.name, '|', t.url, '|', t.album_key, '|', t.disc_number, '|', t.duration_ms, '|', t.is_explicit, '|', t.plays_count)) <> SHA256(CONCAT(s.name, '|', s.url, '|', s.album_key, '|', s.disc_number, '|', s.duration_ms, '|', s.is_explicit, '|', s.plays_count))
                AND t.valid_to IS NULL
        ) sub
            ON sub.join_key = p.track_key
            WHEN matched
                AND SHA256(CONCAT(p.name, '|', p.url, '|', p.album_key, '|', p.disc_number, '|', p.duration_ms, '|', p.is_explicit, '|', p.plays_count)) <> SHA256(CONCAT(sub.name, '|', sub.url, '|', sub.album_key, '|', sub.disc_number, '|', sub.duration_ms, '|', sub.is_explicit, '|', sub.plays_count))
                AND p.valid_to IS NULL THEN
                UPDATE set valid_to = CURRENT_DATE(), flag = FALSE
            WHEN NOT matched THEN
            INSERT (track_key, name, url, album_key, disc_number, duration_ms, is_explicit, plays_count, valid_from, valid_to, flag)
            VALUES (sub.track_key, sub.name, sub.url, sub.album_key, sub.disc_number, sub.duration_ms, sub.is_explicit, sub.plays_count, CURRENT_DATE(), NULL, TRUE);
        
            END IF;
        END;
    