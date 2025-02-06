from typing import List, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import concat_ws, md5, col, current_date, lit, current_timestamp


class SCDHandler:
    FLAG_DEACTIVE = False
    FLAG_ACTIVE = True

    def apply_hash_and_alias(
        self, source_df: DataFrame, target_df: DataFrame, exclused_cols_to_hash: List[str]
    ) -> Tuple[DataFrame, DataFrame]:
        """Apply hash calculation and alias to source and target DataFrames.

        Args:
            source_df (pyspark.sql.DataFrame): Source DataFrame.
            target_df (pyspark.sql.DataFrame): Target DataFrame.
            exclused_cols_to_hash (list): List of columns to exclude from hash calculation.

        Returns:
            tuple: Tuple containing aliased source DataFrame and aliased target DataFrame.
        """
        # Extract columns from target DataFrame excluding columns
        target_cols = [x for x in target_df.columns
                       if x not in exclused_cols_to_hash]
        if cols_missing := {
            col for col in target_cols if col not in source_df.columns
        }:
            raise Exception(
                f'Cols missing in source DataFrame: {cols_missing}')

        # Calculate hash expression
        hash_expr = md5(concat_ws("|", *[
            col(target_col) for target_col in target_cols]))

        # Apply hash calculation and alias to source and target DataFrames
        source_df = source_df.withColumn(
            "hash_value", hash_expr).alias("source_df")
        target_df = target_df.withColumn(
            "hash_value", hash_expr).alias("target_df")

        return source_df, target_df

    def apply_scd_1(
        self, source_df: DataFrame, target_df: DataFrame, join_keys: List[str],
        exclused_cols_to_hash: List[str] = ['created_at', 'updated_at']
    ) -> DataFrame:
        """Apply SCD type 1 to source and target DataFrames

        Args:
            source_df (DataFrame): The source DataFrame
            target_df (DataFrame): The target DataFrame
            join_keys (List[str]): The join keys
            exclused_cols_to_hash (List[str], optional): The columns to exclude from hash calculation. 
                Defaults to ['created_at', 'updated_at'].

        Returns:
            DataFrame: The result DataFrame
        """
        target_cols = [target_col for target_col in target_df.columns]
        source_df, target_df = self.apply_hash_and_alias(
            source_df=source_df, target_df=target_df,
            exclused_cols_to_hash=exclused_cols_to_hash
        )

        # Perform full outer join between source and target DataFrames
        join_cond = [source_df[join_key] == target_df[join_key]
                     for join_key in join_keys]
        base_df = target_df.join(source_df, join_cond, 'full')

        # Filter unchanged records or same records
        unchanged_filter_expr = ' AND '.join([
            f'source_df.{key} IS NULL' for key in join_keys])
        unchanged_df = base_df.filter(
            f'({unchanged_filter_expr}) OR (source_df.hash_value = target_df.hash_value)') \
            .select('target_df.*')

        # Filter updated records
        delta_filter_expr = ' AND '.join([
            f'source_df.{key} IS NOT NULL' for key in join_keys])
        updated_df = base_df.filter(
            f'{delta_filter_expr} AND source_df.hash_value != target_df.hash_value') \
            .withColumn('updated_at', current_timestamp()) \
            .select('source_df.*', 'target_df.created_at', 'updated_at')

        # Filter new records
        new_df = base_df.filter(f'{delta_filter_expr} AND target_df.hash_value IS NULL') \
            .withColumns({
                'created_at': current_timestamp(),
                'updated_at': current_timestamp(),
            }) \
            .select('source_df.*', 'created_at', 'updated_at')

        # Combine all dfs into result DataFrame
        result_df = new_df.select(target_cols). \
            unionByName(updated_df.select(target_cols)). \
            unionByName(unchanged_df.select(target_cols))

        return result_df

    def apply_scd_2(
        self, source_df: DataFrame, target_df: DataFrame, join_keys: List[str],
        exclused_cols_to_hash: List[str] = ['valid_from', 'valid_to', 'flag']
    ) -> DataFrame:
        """Apply SCD type 2 to source and target DataFrames

        Args:
            source_df (DataFrame): The source DataFrame
            target_df (DataFrame): The target DataFrame
            join_keys (List[str]): The join keys
            exclused_cols_to_hash (List[str], optional): The columns to exclude from hash calculation. 
                Defaults to ['created_at', 'updated_at'].

        Returns:
            DataFrame: The result DataFrame
        """
        target_cols = [x for x in target_df.columns]
        # Apply hash calculation and alias
        source_df, target_df = self.apply_hash_and_alias(
            source_df=source_df, target_df=target_df,
            exclused_cols_to_hash=exclused_cols_to_hash
        )

        # Identify new records
        join_cond = [source_df[join_key] == target_df[join_key]
                     for join_key in join_keys]
        new_df = source_df.join(target_df, join_cond, 'left_anti')
        base_df = target_df.join(source_df, join_cond, 'left')

        # Filter unchanged records or same records
        unchanged_filter_expr = ' AND '.join([
            f'source_df.{key} IS NULL' for key in join_keys])
        unchanged_df = base_df.filter(
            f'({unchanged_filter_expr}) OR (source_df.hash_value = target_df.hash_value)') \
            .select('target_df.*')

        # identify updated records
        delta_filter_expr = ' AND '.join([
            f'source_df.{key} IS NOT NULL' for key in join_keys])
        updated_df = base_df.filter(
            f'{delta_filter_expr} AND source_df.hash_value != target_df.hash_value')

        # pick updated records from source_df for new entry
        updated_new_df = updated_df.select('source_df.*')

        # pick updated records from target_df for obsolete entry
        obsolete_df = updated_df.select('target_df.*') \
            .withColumn('valid_to', current_date()) \
            .withColumn('flag', lit(self.FLAG_DEACTIVE))

        # union : new & updated records and add scd2 meta-deta
        delta_df = new_df.union(updated_new_df) \
            .withColumn('valid_from', current_date()) \
            .withColumn("valid_to", lit(None)) \
            .withColumn('flag', lit(self.FLAG_ACTIVE))

        # union all datasets : delta_df + obsolete_df + unchanged_df
        result_df = unchanged_df.select(target_cols). \
            unionByName(delta_df.select(target_cols)). \
            unionByName(obsolete_df.select(target_cols))

        return result_df
