import yaml
import os
from functools import cached_property

from src.configs.logger import get_logger
from src.configs.spark import BQ_DATASET_ID, BQ_DATASET_STAGING_ID
from src.utils.scdhandler.sql_scdhandler import SQLSCDHandler
from src.pipelines.constants import CONFIG_DIR, QUERY_DIR


class SpotifyLoader:
    SCD_TYPE_1 = 1
    SCD_TYPE_2 = 2

    SCD_HANDLER_MAPPING = {
        SCD_TYPE_1: SQLSCDHandler.make_sql_scd1,
        SCD_TYPE_2: SQLSCDHandler.make_sql_scd2
    }

    BASE_QUERY = """
        BEGIN
            DECLARE table_exists BOOL;
            DECLARE min_date INT64;
            DECLARE max_date INT64;
            SET table_exists = (SELECT EXISTS(SELECT 1 FROM `{dataset_staging_name}.INFORMATION_SCHEMA.TABLES` WHERE table_name = '{table_name}'));

            IF table_exists THEN
                {merge_query}
            END IF;
        END;
    """

    F_MERGE_QUERY = """
        SET (min_date, max_date) = (
            SELECT AS STRUCT MIN(release_date_key), MAX(release_date_key)
            FROM `{dataset_staging_name}.{table_name}`
        );

        INSERT INTO `{dataset_name}.{table_name}` ({insert_cols})
        SELECT {staging_cols}
        FROM `{dataset_staging_name}.{table_name}` s
        LEFT JOIN (
            SELECT *
            FROM `{dataset_name}.{table_name}` t
            WHERE t.{partition_col} BETWEEN min_date AND max_date
        ) p ON ({p_join_keys}) = ({s_join_keys})
        WHERE p.{partition_col} IS NULL;
    """

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)

    @cached_property
    def dim_loader_config(self):
        with open(os.path.join(CONFIG_DIR, 'dim_loader_config.yaml'), 'r') as f:
            return yaml.safe_load(f)

    @cached_property
    def fact_loader_config(self):
        with open(os.path.join(CONFIG_DIR, 'fact_loader_config.yaml'), 'r') as f:
            return yaml.safe_load(f)

    def make_dim_load_query(self):
        for dim_conf in self.dim_loader_config['tables']:
            self.logger.info(
                f"Making dim load query for {dim_conf['name']}...")
            scd_handler = self.SCD_HANDLER_MAPPING[dim_conf['scd_type']]
            merge_query = scd_handler(
                dataset_name=BQ_DATASET_ID, dataset_staging_name=BQ_DATASET_STAGING_ID,
                table_name=dim_conf['name'], join_key=dim_conf['join_key'],
                table_main_cols=dim_conf['main_cols'], hashed_cols=dim_conf['hashed_cols']
            )
            query = self.BASE_QUERY.format(
                dataset_staging_name=BQ_DATASET_STAGING_ID, table_name=dim_conf['name'],
                merge_query=merge_query
            )
            with open(os.path.join(QUERY_DIR, f'load_{dim_conf["name"]}.sql'), 'w') as f:
                f.write(query)

    def make_fact_load_query(self):
        for fact_conf in self.fact_loader_config['tables']:
            self.logger.info(
                f"Making fact load query for {fact_conf['name']}...")
            merge_query = self.F_MERGE_QUERY.format(
                dataset_name=BQ_DATASET_ID,
                dataset_staging_name=BQ_DATASET_STAGING_ID,
                table_name=fact_conf['name'],
                partition_col=fact_conf['partition_col'],
                p_join_keys=', '.join([
                    f'p.{key}' for key in fact_conf['join_keys']]),
                s_join_keys=', '.join([
                    f's.{key}' for key in fact_conf['join_keys']]),
                insert_cols=', '.join(fact_conf['insert_cols']),
                staging_cols=', '.join([
                    f's.{key}' for key in fact_conf['insert_cols']])
            )
            query = self.BASE_QUERY.format(
                dataset_staging_name=BQ_DATASET_STAGING_ID, table_name=fact_conf['name'],
                merge_query=merge_query
            )
            with open(os.path.join(QUERY_DIR, f'load_{fact_conf["name"]}.sql'), 'w') as f:
                f.write(query)
