import framework.utils.LoggerUtils as logger
import boto3
import pandas as pd
import time

class athena(object):
    def __init__(self, database, s3_output):
        self.database = database
        self.s3_output = s3_output
        self.client = boto3.client('athena')

    def execute_query(self, query):
        response = self.client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': self.database},
            ResultConfiguration={'OutputLocation': self.s3_output}
        )
        return response['QueryExecutionId']
       
    def wait_for_query_to_complete(self, query_execution_id):
        while True:
            response_msg = self.client.get_query_execution(QueryExecutionId=query_execution_id)
            status = response_msg['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                return status
            time.sleep(1)


    def fetch_results(self, query_execution_id):
        results = self.client.get_query_results(QueryExecutionId=query_execution_id)
        return results

    def query_to_dataframe(self, query):
        query_execution_id = self.execute_query(query)
        status = self.wait_for_query_to_complete(query_execution_id)

        if status == 'SUCCEEDED':
            results = self.fetch_results(query_execution_id)
            rows = results['ResultSet']['Rows']
            columns = [col['VarCharValue'] for col in rows[0]['Data']]
            data = [
                [col.get('VarCharValue', '') for col in row['Data']]
                for row in rows[1:]  
            ]
            df = pd.DataFrame(data, columns=columns)
            return df
        else:
            raise Exception(f"Query failed with status: {status}")
        
    def get_total_count(self, table_name):
        Query = f"select count(*) from {table_name} as Total_Count"
        result_df = self.query_to_dataframe(Query)
        logger.info(f"{result_df}")
        return result_df.iat[0, 0]
    
    def get_distinct_pk_count(self, table_name, pk_list):
        if pk_list is None or len(pk_list) == 0:
            logger.warning(f"The pk_list provided is either None or have no elements. Getting the total count.")
            return self.get_total_count(table_name=table_name)
        else:
            pk_ls = ', '.join(map(str, pk_list))
            logger.info(f"The provided list of primary key is [{pk_ls}]")
            Query = f"select count(distinct({pk_ls})) from {table_name} as Distinct_PK_Count"
            result_df = self.query_to_dataframe(Query)
            logger.info(f"{result_df}")
        return result_df.iat[0, 0]
    
    def get_ddl(self, table_name):
        response = self.client.get_table_metadata(
            DatabaseName=self.database,
            TableName=table_name
        )        
        columns = response['Table']['StorageDescriptor']['Columns']
        column_dict = {col['Name']: col['Type'] for col in columns}
        logger.info(f"DDL for the table {table_name} is : \n {column_dict}")
        return column_dict

    def get_data(self, table_name):
        Query = f"select * from {table_name};"
        result_df = self.query_to_dataframe(Query)
        return result_df
