import framework.utils.LoggerUtils as logger
from sqlalchemy import create_engine
import boto3
import json

class jdbc(object):
    def __init__(self, secret_key):
        self.secret_key = secret_key
        self.connection_string = self.build_connection_string()

        # SQLAlchemy engine creation
        self.engine = create_engine(self.connection_string)

    def build_connection_string(self):
        # Initialize AWS Secrets Manager client
        client = boto3.client('secretsmanager')
        secret_value = client.get_secret_value(SecretId=self.secret_key)
        secret = json.loads(secret_value['SecretString'])

        # Build JDBC connection string
        engine = secret['engine']
        host = secret['host']
        port = secret['port']
        dbname = secret['dbname']
        username = secret['username']
        password = secret['password']

        connection_string = f"{engine}://{username}:{password}@{host}:{port}/{dbname}"
        return connection_string

    def get_connection(self):
        return self.engine.connect()

    def get_total_count(self, table_name):
        try:
            with self.get_connection() as conn:
                query = f"SELECT COUNT(*) FROM {table_name}"
                result = conn.execute(query)
                total_count = result.scalar()
            conn.close()
            return total_count
        except Exception as e:
            logger.error(e)

    def get_distinct_pk_count(self, table_name, pk_column):
        try:
            with self.get_connection() as conn:
                query = f"SELECT COUNT(DISTINCT {pk_column}) FROM {table_name}"
                result = conn.execute(query)
                distinct_count = result.scalar()
            conn.close()
            return distinct_count
        except Exception as e:
            logger.error(e)
            raise(e)

    def get_ddl(self, table_name):
        try:
            with self.get_connection() as conn:
                query = f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{table_name}';
                """  
                result = conn.execute(query)
                ddl = {row['column_name']: row['data_type'] for row in result.fetchall()}
                logger.info(f"DDL for the table {table_name} is : \n {ddl}")

            conn.close()
            return ddl
        except Exception as e:
            logger.error(f"Failure in getting the {table_name} ddl: {str(e)}")
            raise (e)