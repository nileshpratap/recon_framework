import framework.utils.LoggerUtils as logger
import datacompy


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

    return comparison