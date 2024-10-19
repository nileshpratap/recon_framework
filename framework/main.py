from framework.utils.LoggerUtils import LoggerUtils as logger
from framework.factory.ClassFactory import ClassFactory as cf 


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

        func_name = test_map.get(test_name) if test_map.get(test_name) else None
        module = cf.getEngineClass(engine)
        
        return getattr(module, func_name)(details)
