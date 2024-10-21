import datacompy
import pandas as pd
from framework.engines.athena import athena
from framework.engines.jdbc import jdbc
from framework.factory.ClassFactory import ClassFactory as cf
import framework.utils.LoggerUtils as logger

class data_comparator(object):
    class athena_data(athena):
        def get_data(self, table_name):
            Query = f"select * from {table_name};"
            result_df = self.query_to_dataframe(Query)
            return result_df
        
    class jdbc_data(jdbc):
        def get_data(self, table_name):
            try:
                with self.get_connection() as conn:
                    query = f"SELECT * FROM {table_name}"
                    results = conn.execute(str(query)).fetchall()
                conn.close()
                return results
            except Exception as e:
                logger.error(f'Failure in getting the source data for {table_name}.')
                raise(e)
        
    def compare_dataframes(details):
        src_instance = cf.getEngineClass(details['Source'])
        tar_instance = cf.getEngineClass(details['Target'])

        df_source = src_instance.get_data(details['src_table_name'])
        df_target = tar_instance.get_data(details['tar_table_name'])

        if not df_source.index.equals(df_target.index):
            logger.warning("The DataFrames do not have the same index.")
        
        comparison = datacompy.Compare(
            df_source,
            df_target,
            join_columns='key',  
            df1_name=f'{details['table_name']}_source',
            df2_name=f'{details['table_name']}_target'
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