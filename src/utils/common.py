from pyspark.sql import functions as func


def coalesce_blank_n_null(column_name: str, replaced_value=None) -> func.Column:
    return func.when(
        (func.col(column_name).isNull()) |
        (func.col(column_name) == ''),
        func.lit(replaced_value)
    ).otherwise(func.col(column_name))
