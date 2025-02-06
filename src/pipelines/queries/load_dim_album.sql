
        BEGIN
            DECLARE table_exists BOOL;
            DECLARE min_date INT64;
            DECLARE max_date INT64;
            SET table_exists = (SELECT EXISTS(SELECT 1 FROM `spotify_dwh_staging.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'dim_album'));

            IF table_exists THEN
                
            MERGE INTO `spotify_dwh.dim_album` p
            USING `spotify_dwh_staging.dim_album` s
            ON s.album_key = p.album_key
            WHEN matched
                AND SHA256(CONCAT(p.name, '|', p.url, '|', p.cover_image, '|', p.album_type, '|', p.release_date_key, '|', p.release_date_precision, '|', p.label)) <> SHA256(CONCAT(s.name, '|', s.url, '|', s.cover_image, '|', s.album_type, '|', s.release_date_key, '|', s.release_date_precision, '|', s.label))
                THEN
                UPDATE set name = s.name, url = s.url, cover_image = s.cover_image, album_type = s.album_type, release_date_key = s.release_date_key, release_date_precision = s.release_date_precision, label = s.label, updated_at = CURRENT_TIMESTAMP()
            WHEN NOT matched THEN
            INSERT (album_key, name, url, cover_image, album_type, release_date_key, release_date_precision, label, created_at, updated_at)
            VALUES (s.album_key, s.name, s.url, s.cover_image, s.album_type, s.release_date_key, s.release_date_precision, s.label, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
        
            END IF;
        END;
    