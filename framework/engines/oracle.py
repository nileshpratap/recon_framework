import cx_Oracle
import logging as logs
from framework.utils.SecretUtils import SecretUtils as Sr
from framework.utils.ConfigUtils import ConfigUtils
from framework.utils.LoggerUtils import LoggerUtils as logger


class oracle(object):
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

            username = secret_details['username']
            password = secret_details['password']
            host = details['host']
            port = details['port']
            service_name = details['service_name']
            
            # Check if all necessary details are provided
            # if not all([username, password, host, port, service_name]):
            #     raise ValueError("Missing connection details.")

            dsn_tns = cx_Oracle.makedsn(host, port, service_name=service_name)
            connection = cx_Oracle.connect(user=username, password=password, dsn=dsn_tns)
            cursor = connection.cursor()
            return cursor, connection
        
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            logger.error(f"Database error occurred: {error.message}")
            return None, None
        
        except ValueError as ve:
            logger.error(f"Value error: {ve}")
            return None, None
        
        except Exception as ex:
            logger.error(f"An unexpected error occurred: {ex}")
            return None, None
    
    @staticmethod
    def getTotalCountandPKCount(details):
        try:
            schema = details['schema']
            table_name = details['name']
            pk = details['primary_key']
            cursor, connection = oracle.getConnection(details)

            query = f"""SELECT COUNT(*) 
            FROM {schema}.{table_name};
            """
            cursor.execute(query)
            total_count = cursor.fetchall()

            query_for_pk = f"SELECT COUNT(distinct {pk}) FROM {schema}.{table_name}"
            cursor.execute(query_for_pk)
            distinct_pk_count = cursor.fetchall()

            return total_count, distinct_pk_count
        except cx_Oracle.Error as e:
            logger.error(f"Error executing Count Test: {e}")
            return None
        finally:
            cursor.close()
            connection.close()

    @staticmethod
    def getDDL(details):
        try:
            schema = details['schema']
            table_name = details['name']
            cursor, connection = oracle.getConnection(details)

            query = f"""SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
            FROM ALL_TAB_COLUMNS 
            WHERE TABLE_NAME={table_name} and OWNER={schema};
            """
            cursor.execute(query)
            ddl = cursor.fetchall()

            return ddl
        except cx_Oracle.Error as e:
            logger.error(f"Error executing DDL Test: {e}")
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
            cursor, connection = oracle.getConnection(details)

            # query = query

            cursor.execute(query)
            result = cursor.fetchall()

            return result
        except cx_Oracle.Error as e:
            logger.error(f"Error executing Functional Check: {e}")
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
            cursor, connection = oracle.getConnection(details)

            query = f"""SELECT *
            FROM {schema}.{table_name}
            where TRUNC({watermark_column}) >= TO_DATE({st_dt},'DD/MM/YY')
            and TRUNC({watermark_column}) < TO_DATE({en_dt},'DD/MM/YY');
            """
            
            cursor.execute(query)
            result = cursor.fetchall()

            return result
        except cx_Oracle.Error as e:
            logger.error(f"Error executing Data Match Test: {e}")
            return None
        finally:
            cursor.close()
            connection.close()



