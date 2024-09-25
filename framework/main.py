from framework.engines.oracle import oracle
from framework.engines.snowflake import snowflake 

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
        
        # module = oracle if engine == 'oracle' else snowflake if engine == 'snowflake' else None
        # if not module:
        #     return None
        
        # return getattr(module, func_name)(details)
