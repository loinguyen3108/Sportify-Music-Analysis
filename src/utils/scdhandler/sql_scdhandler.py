from typing import List


class SQLSCDHandler:

    @classmethod
    def make_sql_scd1(
        cls, dataset_name: str, dataset_staging_name: str, table_name: str,
        join_key: str, table_main_cols: List[str], hashed_cols: List[str]
    ):
        query = """
            MERGE INTO `{dataset_name}.{table_name}` p
            USING `{dataset_staging_name}.{table_name}` s
            ON s.{join_key} = p.{join_key}
            WHEN matched
                AND {hashed_str_origin} <> {hashed_str_staging}
                THEN
                UPDATE set {update_statement}
            WHEN NOT matched THEN
            INSERT ({main_cols}, created_at, updated_at)
            VALUES ({staging_cols}, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
        """
        updated_cols = [col for col in table_main_cols if col != join_key]
        update_statement = ', '.join([f'{col} = s.{col}' for col in updated_cols]) + \
            ', updated_at = CURRENT_TIMESTAMP()'
        return query.format(
            dataset_name=dataset_name,
            table_name=table_name,
            dataset_staging_name=dataset_staging_name,
            join_key=join_key,
            hashed_str_origin=cls.make_hashed_query_str(hashed_cols, 'p'),
            hashed_str_staging=cls.make_hashed_query_str(hashed_cols, 's'),
            update_statement=update_statement,
            main_cols=', '.join(table_main_cols),
            staging_cols=', '.join([f's.{col}' for col in table_main_cols])
        )

    @classmethod
    def make_sql_scd2(
        cls, dataset_name: str, dataset_staging_name: str, table_name: str,
        join_key: str, table_main_cols: List[str], hashed_cols: List[str]
    ):
        query = """
            MERGE INTO `{dataset_name}.{table_name}` p
            USING ({sql_changed_rows}) sub
            ON sub.join_key = p.{join_key}
            WHEN matched
                AND {hashed_str_origin} <> {hashed_str_staging}
                AND p.valid_to IS NULL THEN
                UPDATE set valid_to = CURRENT_DATE(), flag = FALSE
            WHEN NOT matched THEN
            INSERT ({main_cols}, valid_from, valid_to, flag)
            VALUES ({sub_main_cols}, CURRENT_DATE(), NULL, TRUE);
        """
        sql_changed_rows = cls.make_sql_changed_rows(
            dataset_name=dataset_name, dataset_staging_name=dataset_staging_name,
            table_name=table_name,  join_key=join_key, hashed_cols=hashed_cols
        )
        return query.format(
            dataset_name=dataset_name,
            table_name=table_name,
            sql_changed_rows=sql_changed_rows,
            join_key=join_key,
            hashed_str_origin=cls.make_hashed_query_str(hashed_cols, 'p'),
            hashed_str_staging=cls.make_hashed_query_str(hashed_cols, 'sub'),
            main_cols=', '.join(table_main_cols),
            sub_main_cols=', '.join([f'sub.{col}' for col in table_main_cols])
        )

    @classmethod
    def make_sql_changed_rows(
        cls, dataset_name: str, dataset_staging_name: str, table_name: str,
        join_key: str, hashed_cols: List[str]
    ) -> str:
        query = """
            SELECT {join_key} AS join_key, *
            FROM `{dataset_staging_name}.{table_name}`
            UNION ALL
            SELECT NULL, s.*
            FROM `{dataset_staging_name}.{table_name}` s
            JOIN `{dataset_name}.{table_name}` t
                ON s.{join_key} = t.{join_key}
            WHERE {hashed_str_origin} <> {hashed_str_staging}
                AND t.valid_to IS NULL
        """
        return query.format(
            dataset_name=dataset_name,
            dataset_staging_name=dataset_staging_name,
            table_name=table_name,
            join_key=join_key,
            hashed_str_origin=cls.make_hashed_query_str(hashed_cols, 't'),
            hashed_str_staging=cls.make_hashed_query_str(hashed_cols, 's')
        )

    @staticmethod
    def make_hashed_query_str(hashed_cols: List[str], alias_name: str) -> str:
        concated_str = ', \'|\', '.join(
            [f'{alias_name}.{col}' for col in hashed_cols])
        return f'SHA256(CONCAT({concated_str}))'
