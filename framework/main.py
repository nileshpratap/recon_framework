from framework.utils.LoggerUtils import LoggerUtils
from framework.factory.ClassFactory import ClassFactory as cf 
# from framework.jobs.DataComparator import DataComparator


class test_handler(object):

    @staticmethod
    def call_test(func_name,details):
        logger = LoggerUtils.logger
        try:
            engine = details['engine']
            module = cf.getEngineClass(engine)
            # obj = module(details)
            return getattr(module, func_name)(details)    
        except Exception as ex:
            logger.error(f"An unexpected error occurred in main module while fetching and calling the test. {ex}")
            return None
    
    # @staticmethod 
    # def data_compare(details):
    #     response = DataComparator.compare_dataframes(details)
    #     return response
        