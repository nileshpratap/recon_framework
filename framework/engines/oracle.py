import cx_Oracle
# from com.lumiq.framework.utils.AuditLogUtils import AuditLogUtils as AuditLogger
# from com.lumiq.framework.utils.JobUtils import JobUtils as Jb
import logging as logs
import pandas as pd
from framework.utils.SecretUtils import SecretUtils as Sr
from framework.utils.ConfigUtils import ConfigUtils

py4j_logger = logs.getLogger("py4j").setLevel(logs.INFO)
Config = ConfigUtils().getConfig()
if Config.has_section('Audit'):
    if Config.has_option('Audit','Audit.log.level'):
        log_level = Config.get('Audit','Audit.log.level')
        logs.basicConfig()
        logs.getLogger().setLevel(getattr(logs,log_level))

class oracle(object):
    def __init__(self, Config):
        self.Config = Config
        pass

    @staticmethod
    def get_Secret_Details(secret_key):
        secret_details = Sr.getSecret(secret_name=secret_key)
        if not isinstance(secret_details, dict):
            msg = f"Invalid Secret Found {secret_key}"
            logs.error(msg)
            raise ValueError(msg)
        else:
            return secret_details

    @staticmethod
    def getConnection(details):
        try:
            secret_details = oracle.get_Secret_Details(details['secret_key'])
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
            print(f"Database error occurred: {error.message}")
            return None, None
        
        except ValueError as ve:
            print(f"Value error: {ve}")
            return None, None
        
        except Exception as ex:
            print(f"An unexpected error occurred: {ex}")
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
            print(f"Error executing Count Test: {e}")
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
            print(f"Error executing DDL Test: {e}")
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
            print(f"Error executing Functional Check: {e}")
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
            print(f"Error executing Data Match Test: {e}")
            return None
        finally:
            cursor.close()
            connection.close()



