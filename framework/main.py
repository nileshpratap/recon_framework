from framework.engines.oracle import oracle
from framework.engines.snowflake import snowflake 
from framework.engines.jdbc import JDBCConnector 
from framework.engines.athena import AthenaConnector 


class test_handler(object):
    def __init__(self, Config):
        # self.Config = Config
        pass


    @staticmethod
    def call_test(test_name,details):
        engine = details['engine']

        test_map = {
            'count_and_pk_check': 'getTotalCountandPKCount',
            'ddl_check': 'getDDL',
            'functional_recon': 'func_check',
            'data_check': 'getData'
        }

        func_name = test_map.get(test_name)
        if not func_name:
            return None
        
        module = None
        if engine == 'oracle':
            module = oracle
        elif engine == 'snowflake':
            module = snowflake
        elif  engine == 'jdbc':
            module = JDBCConnector
        elif engine == 'athena':
            module = AthenaConnector

        # module = oracle if engine == 'oracle' else snowflake if engine == 'snowflake' else None
        if not module:
            return None
        
        return getattr(module, func_name)(details)
