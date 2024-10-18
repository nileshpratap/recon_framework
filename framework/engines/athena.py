import boto3
import pandas as pd
import time

class AthenaConnector:
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
            # Convert results to DataFrame
            rows = results['ResultSet']['Rows']
            columns = [col['VarCharValue'] for col in rows[0]['Data']]
            data = [
                [col.get('VarCharValue', '') for col in row['Data']]
                for row in rows[1:]  # Skip header row
            ]
            df = pd.DataFrame(data, columns=columns)
            return df
        else:
            raise Exception(f"Query failed with status: {status}")
