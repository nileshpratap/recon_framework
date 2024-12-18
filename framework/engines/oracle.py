import cx_Oracle
import logging as logs
from framework.utils.SecretUtils import SecretUtils as Sr
from framework.utils.ConfigUtils import ConfigUtils
from framework.utils.LoggerUtils import LoggerUtils


class OracleClient(object):
    def __init__(self, Config):
        self.Config = Config
        pass

    @staticmethod
    def getConnection(details):
        logger = LoggerUtils.logger
        try:
            secret_details = Sr.getSecret(secret_name=details['secret_key'])
            if not isinstance(secret_details, dict):
                msg = f"Invalid Secret Found {details['secret_key']}"
                logger.error(msg)
                raise ValueError(msg)

            username = secret_details['username'] or details['username']
            password = secret_details['password'] or details['password']
            host = secret_details['host']
            port = secret_details['port']
            service_name = secret_details['dbname']

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
    def getTotalCount(details):
        logger = LoggerUtils.logger
        try:
            schema = details['schema']
            table_name = details['name']
            cursor, connection = OracleClient.getConnection(details)

            query = f"""SELECT COUNT(*) 
            FROM {schema}.{table_name};
            """
            cursor.execute(query)
            total_count = cursor.fetchone()[0]

            cursor.close()
            connection.close()

            return total_count
        except cx_Oracle.Error as e:
            logger.error(f"Error executing Count Test: {e}")
            return None
    
    
    @staticmethod
    def getPKCount(details):
        logger = LoggerUtils.logger
        try:
            schema = details['schema']
            table_name = details['name']
            primary_key = details.get('primary_key', None)
            cursor, connection = OracleClient.getConnection(details)

            if primary_key is not None:
                primary_key = ', '.join(map(str, primary_key))

            if primary_key is None or primary_key.strip() in ["*", ""]:
                logger.warning(f"The primary key provided is either None or *. Getting the count of distinct rows.")
                query_for_pk = f"SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {schema}.{table_name});"
            else:
                query_for_pk = f"SELECT COUNT(distinct {primary_key}) FROM {schema}.{table_name}"
            cursor.execute(query_for_pk)
            distinct_pk_count = cursor.fetchone()[0]

            
            cursor.close()
            connection.close()

            return distinct_pk_count
        except cx_Oracle.Error as e:
            logger.error(f"Error executing Count Test: {e}")
            return None
        except Exception as ex:
            logger.error(f"Error occurred in Count Test: {ex}")
            return None

    @staticmethod
    def getDDL(details):
        logger = LoggerUtils.logger
        try:
            schema = details['schema']
            table_name = details['name']
            cursor, connection = OracleClient.getConnection(details)

            query = f"""SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
            FROM ALL_TAB_COLUMNS 
            WHERE TABLE_NAME={table_name} and OWNER={schema};
            """
            result = cursor.execute(query)
            ddl = {row['column_name']: row['data_type'] for row in result.fetchall()}
            logger.info(f"DDL for the table {table_name} is : \n {ddl}")

            cursor.close()
            connection.close()

            return ddl
        except cx_Oracle.Error as e:
            logger.error(f"Error executing DDL Test: {e}")
            return None


    @staticmethod
    def fun_rcon(details):
        logger = LoggerUtils.logger
        try:
            schema = details['schema']
            table_name = details['name']
            watermark_column = details['watermark_column']
            query = details['query']
            cursor, connection = OracleClient.getConnection(details)

            # query = query

            cursor.execute(query)
            result = cursor.fetchall()

            
            cursor.close()
            connection.close()

            return result
        except cx_Oracle.Error as e:
            logger.error(f"Error executing Functional Check: {e}")
            return None
        
    @staticmethod
    def getData(details):
        logger = LoggerUtils.logger
        try:
            schema = details['schema']
            table_name = details['name']
            watermark_column = details.get('watermark_column', None)
            st_dt = details.get('st_dt', None)
            en_dt = details.get('en_dt', None)

            cursor, connection = OracleClient.getConnection(details)

            if watermark_column is None or st_dt is None or en_dt is None:
                logger.warning(f"Either watermark column or start/end date not passed, returning entire data.")
                query = f"""
                    SELECT *
                    FROM {schema}.{table_name};
                """
            else:
                query = f"""
                    SELECT *
                    FROM {schema}.{table_name}
                    where TRUNC({watermark_column}) >= TO_DATE({st_dt},'DD/MM/YY')
                    and TRUNC({watermark_column}) < TO_DATE({en_dt},'DD/MM/YY');
                """
            
            cursor.execute(query)
            result = cursor.fetchall()

            columns = [desc[0] for desc in cursor.description]

            cursor.close()
            connection.close()

            return result, columns
        except cx_Oracle.Error as e:
            logger.error(f"Error executing Data Match Test: {e}")
            return None



