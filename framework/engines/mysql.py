import mysql.connector
from framework.utils.SecretUtils import SecretUtils as Sr
from framework.utils.ConfigUtils import ConfigUtils
from framework.utils.LoggerUtils import LoggerUtils


class MySQLClient:
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

            conn_details = {
                'user': secret_details['username'],
                'password': secret_details['password'],
                'host': details['host'],
                'database': details['db_name'],
                'port': details['port']
            }
            # Establish a connection to MySQL
            connection = mysql.connector.connect(**conn_details)
            cursor = connection.cursor(dictionary=True)
            return cursor, connection

        except mysql.connector.Error as e:
            logger.error(f"MySQL connection error: {e}")
            return None, None

        except ValueError as ve:
            logger.error(f"Value error: {ve}")
            return None, None

        except Exception as ex:
            logger.error(f"An unexpected error occurred in MySQL connection: {ex}")
            return None, None

    @staticmethod
    def getTotalCount(details):
        logger = LoggerUtils.logger
        try:
            schema = details['schema']
            table_name = details['name']
            cursor, connection = MySQLClient.getConnection(details)

            query = f"SELECT COUNT(*) FROM {schema}.{table_name}"
            cursor.execute(query)
            total_count = cursor.fetchone()[0]

            cursor.close()
            connection.close()

            return total_count

        except Exception as ex:
            logger.error(f"An unexpected error occurred in Count Test: {ex}")
            return None
    
    @staticmethod
    def getTotalCount(details):
        logger = LoggerUtils.logger
        try:
            schema = details['schema']
            table_name = details['name']
            primary_key = details.get('primary_key', None)
            cursor, connection = MySQLClient.getConnection(details)

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

        except Exception as ex:
            logger.error(f"An unexpected error occurred in Count Test: {ex}")
            return None

    @staticmethod
    def getDDL(details):
        logger = LoggerUtils.logger
        try:
            schema = details['schema']
            table_name = details['name']
            cursor, connection = MySQLClient.getConnection(details)

            query = f"""
            SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}';
            """
            cursor.execute(query)
            ddl = cursor.fetchall()

            cursor.close()
            connection.close()

            return ddl

        except Exception as ex:
            logger.error(f"An unexpected error occurred in DDL Test: {ex}")
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

            cursor, connection = MySQLClient.getConnection(details)

            query = f"""
                SELECT *
                FROM {schema}.{table_name}
                WHERE {watermark_column} >= '{st_dt}' 
                AND {watermark_column} < '{en_dt}';
            """

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
                    WHERE {watermark_column} >= '{st_dt}' 
                    AND {watermark_column} < '{en_dt}';
                """
            
            
            cursor.execute(query)
            result = cursor.fetchall()

            columns = [desc[0] for desc in cursor.description]

            cursor.close()
            connection.close()

            return result, columns
        except Exception as ex:
            logger.error(f"An unexpected error occurred in Data Match Test: {ex}")
            return None