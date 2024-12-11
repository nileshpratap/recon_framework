import framework.utils.LoggerUtils as logger
import boto3
import time

class athena(object):
    def __init__(self, database, s3_output):
        self.database = database
        self.s3_output = s3_output
        self.client = boto3.client('athena', region_name = 'ap-south-1')

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

    # def query_to_dataframe(self, query):
    #     query_execution_id = self.execute_query(query)
    #     status = self.wait_for_query_to_complete(query_execution_id)

    #     if status == 'SUCCEEDED':
    #         results = self.fetch_results(query_execution_id)
    #         rows = results['ResultSet']['Rows']
    #         columns = [col['VarCharValue'] for col in rows[0]['Data']]
    #         data = [
    #             [col.get('VarCharValue', '') for col in row['Data']]
    #             for row in rows[1:]  
    #         ]
    #         # df = pd.DataFrame(data, columns=columns)
    #         return data
    #     else:
    #         raise Exception(f"Query failed with status: {status}")
        
    def getTotalCount(self, table_name):
        Query = f"select count(*) from {table_name} as Total_Count"
        # result_df = self.query_to_dataframe(Query)
        query_execution_id = self.execute_query(Query)
        status = self.wait_for_query_to_complete(query_execution_id)


        if status == 'SUCCEEDED':
            results = self.fetch_results(query_execution_id)
            rows = results['ResultSet']['Rows']
            total_count = int(rows[0]['Data']['VarCharValue'])
            # data = [
            #     [col.get('VarCharValue', '') for col in row['Data']]
            #     for row in rows[1:]  
            # ]
            # df = pd.DataFrame(data, columns=columns)
            return total_count
        else:
            raise Exception(f"Query failed with status: {status}")


    
    def getPKCount(self, table_name, pk_list):
        if pk_list is None or len(pk_list) == 0:
            logger.warning(f"The pk_list provided is either None or have no elements. Getting the total count.")
            return self.get_total_count(table_name=table_name)
        else:
            pk_ls = ', '.join(map(str, pk_list))
            logger.info(f"The provided list of primary key is [{pk_ls}]")
            Query = f"select count(distinct({pk_ls})) from {table_name} as Distinct_PK_Count"
            # result_df = self.query_to_dataframe(Query)
            query_execution_id = self.execute_query(Query)
            status = self.wait_for_query_to_complete(query_execution_id)

            if status == 'SUCCEEDED':
                results = self.fetch_results(query_execution_id)
                rows = results['ResultSet']['Rows']
                pk_count = int(rows[0]['Data']['VarCharValue'])
                # data = [
                #     [col.get('VarCharValue', '') for col in row['Data']]
                #     for row in rows[1:]  
                # ]
                # df = pd.DataFrame(data, columns=columns)
                return pk_count
            else:
                raise Exception(f"Query failed with status: {status}")

    
    def getDDL(self, table_name):
        response = self.client.get_table_metadata(
            DatabaseName=self.database,
            TableName=table_name
        )        
        columns = response['Table']['StorageDescriptor']['Columns']
        column_dict = {col['Name']: col['Type'] for col in columns}
        logger.info(f"DDL for the table {table_name} is : \n {column_dict}")
        return column_dict

    def getData(self, table_name):
        Query = f"select * from {table_name};"
        # result_df = self.query_to_dataframe(Query)
        query_execution_id = self.execute_query(Query)
        status = self.wait_for_query_to_complete(query_execution_id)
        if status == 'SUCCEEDED':
            results = self.fetch_results(query_execution_id)
            rows = results['ResultSet']['Rows']
            pk_count = int(rows[0]['Data']['VarCharValue'])
            metadata = response['ResultSet']['ResultSetMetadata']['ColumnInfo']

            # Create a list of dictionaries
            data = []
            for row in rows:
                row_data = dict(zip([col['Name'] for col in metadata], row['Data']))
                data.append(row_data)
            return data, None
        else:
            raise Exception(f"Query failed with status: {status}")
        
        
        



