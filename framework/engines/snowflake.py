import copy
import snowflake.connector
from framework.utils.SecretUtils import SecretUtils as Sr
from framework.utils.ConfigUtils import ConfigUtils
from framework.utils.LoggerUtils import LoggerUtils


class SnowflakeClient(object):
    def __init__(self, Config):
        self.Config = Config
        pass

    @staticmethod
    def getConnection(details):
        logger = LoggerUtils.logger
        try:
            # secret_details = None
            # if details.get('secret_key', None) is not None:
            #     secret_details = Sr.getSecret(secret_name=details['secret_key'])

            # if not isinstance(secret_details, dict):
            #     msg = f"Invalid Secret Found {details['secret_key']}"
            #     logger.error(msg)
            #     raise ValueError(msg)

            conn_details = {
                'user': details['username'],
                'password': details['password'],
                'account': details['account'],
                'warehouse': details['warehouse'],
                'database': details['db_name'],
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
    def getTotalCount(details):
        # return None
    
        logger = LoggerUtils.logger
        try:
            db = details['db_name']
            schema = details['schema']
            table_name = details['name']
            cursor, connection = SnowflakeClient.getConnection(details)

            query = f"""SELECT COUNT(*) FROM {schema}.{table_name};"""
            cursor.execute(query)
            total_count = cursor.fetchone()[0]

            cursor.close()
            connection.close()
            
            res = {
                "result": total_count,
                "TotalCount_query": query
            }


            return res
        
        except Exception as ex:
            logger.error(f"An unexpected error occurred in Count Test: {ex}")
            return -1
        

    @staticmethod
    def getIncrementalCount(details):
        logger = LoggerUtils.logger

        try:
            # Extract schema, table name, and condition from details
            db = details['db_name']
            schema = details['schema']
            table_name = details['name']
            condition = details.get("condition", None)

            if condition is None or condition.strip() == "":
                logger.warning("Condition not provided. Function will not calculate anything.")
                return None

            cursor, connection = SnowflakeClient.getConnection(details)

            try:
                query = f"""SELECT COUNT(1) FROM {schema}.{table_name} WHERE {condition};"""
                logger.info(f"Executing query: {query}")
                cursor.execute(query)

                incremental_count = cursor.fetchone()[0]

                res = {
                    "result": incremental_count,
                    "IncrementalCount_query": query
                }

                return res

            except Exception as query_ex:
                logger.error(f"An error occurred while executing the query: {query_ex}")
                return -1

            finally:
                # Ensure cursor and connection are closed
                cursor.close()
                connection.close()

        except KeyError as key_ex:
            logger.error(f"Missing required key in input: {key_ex}")
            return -1

        except Exception as ex:
            logger.error(f"An unexpected error occurred in Conditional Count: {ex}")
            return -1

    
    @staticmethod
    def getPKCount(details):
        # return None
    
        logger = LoggerUtils.logger
        try:
            db = details['db_name']
            schema = details['schema']
            table_name = details['name']
            primary_key = details.get('primary_key', None)
            cursor, connection = SnowflakeClient.getConnection(details)

            if primary_key is not None:
                primary_key = ', '.join(map(str, primary_key))

            if primary_key is None or primary_key.strip() in ["*", ""]:
                logger.warning(f"The primary key provided is either None or *. Getting the count of distinct rows.")
                query_for_pk = f"""SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {schema}.{table_name});"""
            else:
                query_for_pk = f"""
                SELECT COUNT(distinct {primary_key}) FROM {schema}.{table_name}
            """
            cursor.execute(query_for_pk)
            distinct_pk_count = cursor.fetchone()[0]

            res = {
                "result": distinct_pk_count,
                "PKCount_query": query_for_pk
            }

            cursor.close()
            connection.close()

            return res
        
        except Exception as ex:
            logger.error(f"Error occurred in Count Test: {ex}")
            return -1

    @staticmethod
    def getDDL(details):
        logger = LoggerUtils.logger
        try:
            db = details['db_name']
            schema = details['schema']
            table_name = details['name']
            cursor, connection = SnowflakeClient.getConnection(details)

            query = f"""
            SELECT 
            column_name, 
            data_type, 
            is_nullable,
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = '{table_name}'
            and table_schema = '{schema}'
            and table_catalog = '{db}'
            order by column_name;
            """
            cursor.execute(query)
            ddl = cursor.fetchall()

            res = {
                "result": ddl,
                "DDL_query": query
            }

            cursor.close()
            connection.close()

            return res
        
        except Exception as ex:
            logger.error(f"An unexpected error occurred in DDL Test: {ex}")
            return -1


    # @staticmethod
    # def fun_rcon(details):
    #     logger = LoggerUtils.logger
    #     results = []
        
    #     try:
    #         db = details['db_name']
    #         schema = details.get('schema')
    #         table_name = details.get('name')
    #         checks = details.get('checks')

    #         if not all([schema, table_name, checks]):
    #             logger.warning("Either schema, table name, checks are missing. This function will not check anything.")
    #             return None
            
    #         # Flatten the checks dictionary
    #         function_column_pairs = [
    #             {"function": function, "column": column.strip()}
    #             for function, columns in checks.items()
    #             for column in columns.split(',')
    #         ]
            
    #         if not function_column_pairs:
    #             raise ValueError("No function-column pairs found in the checks dictionary.")
    #             return -1
            
            
    #         cursor, connection = SnowflakeClient.getConnection(details)

    #         try:
    #             for pair in function_column_pairs:
    #                 function = pair['function']
    #                 column = pair['column']
                    
    #                 query = f"""
    #                     SELECT {function}("{column}") AS result
    #                     FROM {schema}.{table_name}
    #                 """
    #                 logger.info(f"Executing query: {query}")
    #                 cursor.execute(query)
    #                 results.append({"function": function, "column": column, "result": cursor.fetchall()})
    #         except Exception as query_ex:
    #             logger.error(f"An error occurred during query execution: {query_ex}")
    #             return -1
    #         finally:
    #             # Close cursor and connection
    #             cursor.close()
    #             connection.close()
        
    #     except Exception as ex:
    #         logger.error(f"An unexpected error occurred: {ex}")
    #         return -1

    #     return results



    # @staticmethod
    # def fun_rcon(details):
    #     logger = LoggerUtils.logger
    #     results = []

    #     try:
    #         # Extract input values
    #         schema = details.get("schema")
    #         table_name = details.get("name")
    #         fun_rcon = details.get("fun_rcon")

    #         if not all([schema, table_name, fun_rcon]):
    #             logger.warning("Either schema, table name, checks are missing. This function will not check anything.")
    #             return None
            
    #         # Extract global condition if present
    #         global_condition = fun_rcon.get("condition", None)

    #         # Flatten the fun_rcon dictionary
    #         function_column_pairs = []
    #         for function, value in fun_rcon.items():
    #             # Skip the global "condition" key
    #             if function == "condition":
    #                 continue
                
    #             if not isinstance(value, dict) or "columns" not in value:
    #                 raise ValueError(f"Invalid format for function '{function}' in checks dictionary.")
                
    #             columns = value["columns"]
    #             local_condition = value.get("condition", None)
    #             for column in columns.split(","):
    #                 function_column_pairs.append({
    #                     "function": function,
    #                     "column": column.strip(),
    #                     "condition": local_condition or global_condition
    #                 })

    #         if not function_column_pairs:
    #             raise ValueError("No function-column pairs found in the checks dictionary.")
    #             return -1

    #         # Get Snowflake connection
    #         cursor, connection = SnowflakeClient.getConnection(details)

    #         try:
    #             for pair in function_column_pairs:
    #                 function = pair["function"]
    #                 column = pair["column"]
    #                 condition = pair["condition"]

    #                 # Build the query with or without WHERE clause
    #                 if condition:
    #                     query = f"""
    #                         SELECT {function}("{column}") AS result
    #                         FROM {schema}.{table_name}
    #                         WHERE {condition}
    #                     """
    #                 else:
    #                     query = f"""
    #                         SELECT {function}("{column}") AS result
    #                         FROM {schema}.{table_name}
    #                     """

    #                 logger.info(f"Executing query: {query}")
    #                 try:
    #                     cursor.execute(query)
    #                     results.append({
    #                         "function": function,
    #                         "column": column,
    #                         "condition": condition,
    #                         "result": int(cursor.fetchone()[0])
    #                     })
    #                 except Exception as query_ex:
    #                     logger.error(f"An error occurred during query execution for {function}({column}): {query_ex}")
    #                     return -1
    #         finally:
    #             # Close cursor and connection
    #             cursor.close()
    #             connection.close()

    #     except ValueError as ve:
    #         logger.error(f"ValueError: {ve}")
    #         return -1
    #     except Exception as ex:
    #         logger.error(f"An unexpected error occurred: {ex}")
    #         return -1

    #     return results
    





    @staticmethod
    def fun_rcon(details):
        logger = LoggerUtils.logger
        results = []

        try:
            # Extract input values
            db = details['db_name']
            schema = details.get("schema")
            table_name = details.get("name")
            fun_rcon = details.get("fun_rcon")

            if not all([schema, table_name, fun_rcon]):
                logger.warning("Either schema, table name, checks are missing. This function will not check anything.")
                return None
            
            cursor, connection = SnowflakeClient.getConnection(details)
            

            for rcon_chk in fun_rcon:
                result = rcon_chk

                condition = rcon_chk.get("condition", None)
                condition = "" if condition is None or condition.strip() == "" else condition

                select_subpart_of_query = []
                select_subpart_of_query_for_unq_cnt_lst = []

                sub_chks_keys = {"SUM", "MIN", "MAX", "AVG", 'UNQ_CNT', 'COUNT'}
                sub_chks = {key: value for key, value in rcon_chk.items() if key.upper() in sub_chks_keys and value != [] and isinstance(value, list)}

           
                for fun, cols in sub_chks.items():
                    if fun.upper() in ("SUM", "MIN", "MAX", "AVG"):
                        for col in cols:
                            select_subpart_of_query.append(f'{fun.upper()}({col}) as "{fun.upper()}_of_{col}"')
                    elif fun.upper() == "UNQ_CNT":
                        for col in cols:
                            col_alias = col.replace(',', '__').replace(' ', '')
                            if isinstance(cols, list) != []:
                                select_subpart_of_query_for_unq_cnt_lst.append(f'{col} as "unq_cnt_of_{col_alias}"')
                
                select_subpart_of_query = ', '.join(select_subpart_of_query)
                
                agg_query = f"""SELECT {select_subpart_of_query} FROM {schema}.{table_name};"""
                
                unq_cnt_queries = []
                if select_subpart_of_query_for_unq_cnt_lst != []:
                    for item in select_subpart_of_query_for_unq_cnt_lst:
                        unq_cnt_queries.append(f"""SELECT DISTINCT {item} FROM {schema}.{table_name};""")

                queries = []
                queries.append(agg_query)
                queries.extend(unq_cnt_queries)

                print("queries to be executed: ", queries)

                if condition != "":
                    queries = [f"{query[:-1]} WHERE {condition};" for query in queries]

                result_dicts = {}                   
                for query in queries:
                    try:
                        logger.info("upcoming query for execution: ", query)
                        cursor.execute(query)
                        res = cursor.fetchone()
                        if res is None:
                            logger.warning(f"No data returned for query: {query}")
                            continue
                        result_dict = {column[0]: str(value) for column, value in zip(cursor.description, res)}
                        result_dicts.update(result_dict)
                    except Exception as ex:
                        logger.error(f"An unexpected error occurred in Data Match Test:: {ex}")
                        return -1

            
    
                result["qry"] = queries
                result["result"] = result_dicts

                results.append(result)
            

            # Close cursor and connection
            cursor.close()
            connection.close()

        except ValueError as ve:
            logger.error(f"ValueError: {ve}")
            return -1
        except Exception as ex:
            logger.error(f"An unexpected error occurred: {ex}")
            return -1

        return results


    @staticmethod
    def getData(details):
        return None
        logger = LoggerUtils.logger
        try:
            db = details['db_name']
            schema = details['schema']
            table_name = details['name']
            watermark_column = details.get('watermark_column', None)
            st_dt = details.get('st_dt', None)
            en_dt = details.get('en_dt', None)

            cursor, connection = SnowflakeClient.getConnection(details)

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
                    where {watermark_column} >= TO_TIMESTAMP_NTZ('{st_dt} 00:00:00')
                    and {watermark_column} < TO_TIMESTAMP_NTZ('{en_dt} 00:00:00');
                """
        
            cursor.execute(query)
            result = cursor.fetchone()[0]

            columns = [desc[0] for desc in cursor.description]
            print('columns list:', columns)
            print('result length:', len(result))

            cursor.close()
            connection.close()

            return result, columns

        except Exception as ex:
            logger.error(f"An unexpected error occurred in Data Match Test:: {ex}")
            return None


    @staticmethod
    def store_test_result(result):
        logger = LoggerUtils.logger
        try:
            print("result type: ", type(result))
            print("result: ", result)
            details = result["details"]
            test_result = result["test_result"]

            db = details['db_name']
            schema = details["schema"]
            table_name = details["name"]

            cursor, connection = SnowflakeClient.getConnection(details)

            columns = ', '.join(test_result.keys())
            values = tuple(test_result.values())
            values_str = ', '.join(['%s'] * len(values))
            print("columns: ", columns)
            print("values: ", values)
            insert_sql = f"INSERT INTO {schema}.{table_name} ({columns}) VALUES ({values_str})"
            print("insert_query: ", insert_sql)

            cursor.execute(insert_sql, values)
            
            cursor.close()
            connection.close()

            logger.info(f"Successfully inserted test result")

        except Exception as e:
            logger.error(f"Error inserting test result: {e}")


            