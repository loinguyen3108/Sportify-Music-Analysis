BEGIN
    DECLARE table_exists BOOL;
    SET table_exists = (SELECT EXISTS(SELECT 1 FROM `spotify_dwh_staging.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'dim_artist'));

    IF table_exists THEN
        
    MERGE INTO `spotify_dwh.dim_artist` p
    USING (
    SELECT artist_key AS join_key, *
    FROM `spotify_dwh_staging.dim_artist`
    UNION ALL
    SELECT NULL, s.*
    FROM `spotify_dwh_staging.dim_artist` s
    JOIN `spotify_dwh.dim_artist` t
        ON s.artist_key = t.artist_key
    WHERE SHA256(CONCAT(t.name, '|', t.url, '|', ARRAY_TO_STRING(t.genres, ','), '|', t.followers_count, '|', t.monthly_listeners)) <> SHA256(CONCAT(s.name, '|', s.url, '|', ARRAY_TO_STRING(s.genres, ','), '|', s.followers_count, '|', s.monthly_listeners))
        AND t.valid_to IS NULL
) sub
    ON sub.join_key = p.artist_key
    WHEN matched
        AND SHA256(CONCAT(p.name, '|', p.url, '|', ARRAY_TO_STRING(p.genres, ','), '|', p.followers_count, '|', p.monthly_listeners)) <> SHA256(CONCAT(sub.name, '|', sub.url, '|', ARRAY_TO_STRING(sub.genres, ','), '|', sub.followers_count, '|', sub.monthly_listeners))
        AND p.valid_to IS NULL THEN
        UPDATE set valid_to = CURRENT_DATE(), flag = FALSE
    WHEN NOT matched THEN
    INSERT (artist_key, name, genres, url, followers_count, monthly_listeners, valid_from, valid_to, flag)
    VALUES (sub.artist_key, sub.name, sub.genres, sub.url, sub.followers_count, sub.monthly_listeners, CURRENT_DATE(), NULL, TRUE);

    END IF;
END;
