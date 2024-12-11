import framework.utils.LoggerUtils as logger
import datacompy
import pandas as pd


def compare_dataframes(df1, df2):
    if not df1.index.equals(df2.index):
        logger.warning("The DataFrames do not have the same index.")
    
    comparison = datacompy.Compare(
        df1,
        df2,
        join_columns='key',  
        df1_name='DataFrame 1',
        df2_name='DataFrame 2'
    )

    logger.info(comparison.report())

    comparison_result = {
        "match": comparison.matches(),
        "mismatch_count": comparison.intersect_rows_mismatch,
        "df1_unmatched_rows": comparison.df1_unq_rows,
        "df2_unmatched_rows": comparison.df2_unq_rows,
        "df1_columns_missing_in_df2": comparison.df1.columns.difference(comparison.df2.columns).tolist(),
        "df2_columns_missing_in_df1": comparison.df2.columns.difference(comparison.df1.columns).tolist()
    }

    return comparison_result