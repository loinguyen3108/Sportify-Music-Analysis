
        BEGIN
            DECLARE table_exists BOOL;
            DECLARE min_date INT64;
            DECLARE max_date INT64;
            SET table_exists = (SELECT EXISTS(SELECT 1 FROM `spotify_dwh_staging.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'dim_user'));

            IF table_exists THEN
                
            MERGE INTO `spotify_dwh.dim_user` p
            USING `spotify_dwh_staging.dim_user` s
            ON s.user_key = p.user_key
            WHEN matched
                AND SHA256(CONCAT(p.name, '|', p.url)) <> SHA256(CONCAT(s.name, '|', s.url))
                THEN
                UPDATE set name = s.name, url = s.url, updated_at = CURRENT_TIMESTAMP()
            WHEN NOT matched THEN
            INSERT (user_key, name, url, created_at, updated_at)
            VALUES (s.user_key, s.name, s.url, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
        
            END IF;
        END;
    