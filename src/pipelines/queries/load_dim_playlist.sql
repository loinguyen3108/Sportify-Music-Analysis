
        BEGIN
            DECLARE table_exists BOOL;
            DECLARE min_date INT64;
            DECLARE max_date INT64;
            SET table_exists = (SELECT EXISTS(SELECT 1 FROM `spotify_dwh_staging.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'dim_playlist'));

            IF table_exists THEN
                
            MERGE INTO `spotify_dwh.dim_playlist` p
            USING (
            SELECT playlist_key AS join_key, *
            FROM `spotify_dwh_staging.dim_playlist`
            UNION ALL
            SELECT NULL, s.*
            FROM `spotify_dwh_staging.dim_playlist` s
            JOIN `spotify_dwh.dim_playlist` t
                ON s.playlist_key = t.playlist_key
            WHERE SHA256(CONCAT(t.name, '|', t.url, '|', t.collaborative, '|', t.is_public, '|', t.snapshot_id, '|', t.followers_count, '|', t.user_key)) <> SHA256(CONCAT(s.name, '|', s.url, '|', s.collaborative, '|', s.is_public, '|', s.snapshot_id, '|', s.followers_count, '|', s.user_key))
                AND t.valid_to IS NULL
        ) sub
            ON sub.join_key = p.playlist_key
            WHEN matched
                AND SHA256(CONCAT(p.name, '|', p.url, '|', p.collaborative, '|', p.is_public, '|', p.snapshot_id, '|', p.followers_count, '|', p.user_key)) <> SHA256(CONCAT(sub.name, '|', sub.url, '|', sub.collaborative, '|', sub.is_public, '|', sub.snapshot_id, '|', sub.followers_count, '|', sub.user_key))
                AND p.valid_to IS NULL THEN
                UPDATE set valid_to = CURRENT_DATE(), flag = FALSE
            WHEN NOT matched THEN
            INSERT (playlist_key, name, url, collaborative, is_public, snapshot_id, followers_count, user_key, valid_from, valid_to, flag)
            VALUES (sub.playlist_key, sub.name, sub.url, sub.collaborative, sub.is_public, sub.snapshot_id, sub.followers_count, sub.user_key, CURRENT_DATE(), NULL, TRUE);
        
            END IF;
        END;
    