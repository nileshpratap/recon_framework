import snowflake.connector
from framework.utils.SecretUtils import SecretUtils as Sr
from framework.utils.ConfigUtils import ConfigUtils
from framework.utils.LoggerUtils import LoggerUtils as logger


class snowflake(object):
    def __init__(self, Config):
        self.Config = Config
        pass

    @staticmethod
    def getConnection(details):
        try:
            secret_details = Sr.getSecret(secret_name=details['secret_key'])
            if not isinstance(secret_details, dict):
                msg = f"Invalid Secret Found {details['secret_key']}"
                logger.error(msg)
                raise ValueError(msg)

            conn_details = {
                'user': secret_details['username'],
                'password': secret_details['password'],
                'account': details['account'],
                'warehouse': details['warehouse'],
                'database': details['DB'],
                'schema': details['schema'],
                'role': details['role']
            }
            # Establish a connection to Snowflake
            connection = snowflake.connector.connect(**conn_details)
            cursor = connection.cursor()
            return cursor, connection
        
        except snowflake.connector.Error as e:
            logger.error(f"Snowflake connection error: {e}")
            return None, None

        except ValueError as ve:
            logger.error(f"Value error: {ve}")
            return None, None
        
        except Exception as ex:
            logger.error(f"An unexpected error occurred in snowflake connection: {ex}")
            return None, None


    @staticmethod
    def getTotalCountandPKCount(details):
        try:
            schema = details['schema']
            table_name = details['name']
            pk_col = details['pk_col']
            cursor, connection = snowflake.getConnection(details)

            query = f"""SELECT COUNT(*) 
            FROM {schema}.{table_name};
            """
            cursor.execute(query)
            total_count = cursor.fetchall()

            query_for_pk = f"SELECT COUNT(distinct {pk_col}) FROM {schema}.{table_name}"
            cursor.execute(query_for_pk)
            distinct_pk_count = cursor.fetchall()

            return total_count, distinct_pk_count
        
        except Exception as ex:
            logger.error(f"An unexpected error occurred in Count Test: {ex}")
            return None
        finally:
            cursor.close()
            connection.close()

    @staticmethod
    def getDDL(details):
        try:
            schema = details['schema']
            table_name = details['name']
            db = details['DB']
            cursor, connection = snowflake.getConnection(details)

            query = f"""SELECT table_catalog AS database_name, table_schema, table_name, column_name, data_type, nullable
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = {table_name}
            and table_schema = {schema}
            and table_catalog = {db};
            """
            cursor.execute(query)
            ddl = cursor.fetchall()

            return ddl
        
        except Exception as ex:
            logger.error(f"An unexpected error occurred in DDL Test: {ex}")
            return None
        finally:
            cursor.close()
            connection.close()


    @staticmethod
    def func_check(details):
        try:
            schema = details['schema']
            table_name = details['name']
            watermark_column = details['watermark_column']
            query = details['query']
            cursor, connection = snowflake.getConnection(details)

            cursor.execute(query)
            result = cursor.fetchall()

            return result
        
        except Exception as ex:
            logger.error(f"An unexpected error occurred in Functional Check: {ex}")
            return None
        finally:
            cursor.close()
            connection.close()
    
    @staticmethod
    def getData(details):
        try:
            schema = details['schema']
            table_name = details['name']
            watermark_column = details['watermark_column']
            st_dt = details['st_dt']
            en_dt = details['en_dt']
            cursor, connection = snowflake.getConnection(details)

            query = f"""SELECT *
            FROM {schema}.{table_name}
            where {watermark_column} >= TO_TIMESTAMP_NTZ('{st_dt} 00:00:00')
            and {watermark_column} < TO_TIMESTAMP_NTZ('{en_dt} 00:00:00');
            """

            cursor.execute(query)
            result = cursor.fetchall()

            return result

        except Exception as ex:
            logger.error(f"An unexpected error occurred in Data Match Test:: {ex}")
            return None
        finally:
            cursor.close()
            connection.close()

