import framework.utils.LoggerUtils as logger
import sqlalchemy
import pandas as pd

class JDBCConnector:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.engine = create_engine(self.connection_string)

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
                query = f"SHOW CREATE TABLE {table_name}"  
                result = conn.execute(query)
                ddl = result.fetchone()[1]  
            conn.close()
            return ddl
        except Exception as e:
            logger.error(f'Failure in getting the {table_name} ddl')
            raise (e)

    def get_data(self, table_name):
        try:
            with self.get_connection() as conn:
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql(query, conn)
            conn.close()
            return df
        except Exception as e:
            logger.error(f'Failure in getting the source data for {table_name}.')
            raise(e)

