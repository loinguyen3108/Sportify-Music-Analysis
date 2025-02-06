
        BEGIN
            DECLARE table_exists BOOL;
            DECLARE min_date INT64;
            DECLARE max_date INT64;
            SET table_exists = (SELECT EXISTS(SELECT 1 FROM `spotify_dwh_staging.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'dim_genre'));

            IF table_exists THEN
                
            MERGE INTO `spotify_dwh.dim_genre` p
            USING `spotify_dwh_staging.dim_genre` s
            ON s.genre_key = p.genre_key
            WHEN matched
                AND SHA256(CONCAT(p.name)) <> SHA256(CONCAT(s.name))
                THEN
                UPDATE set name = s.name, updated_at = CURRENT_TIMESTAMP()
            WHEN NOT matched THEN
            INSERT (genre_key, name, created_at, updated_at)
            VALUES (s.genre_key, s.name, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
        
            END IF;
        END;
    