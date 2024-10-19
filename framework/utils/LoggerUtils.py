import logging
from framework.utils.ConfigUtils import ConfigUtils

'''
The various levels of logging:
2024-10-17 12:34:56 - DEBUG - This is a debug message. (level=10)
2024-10-17 12:34:56 - INFO - This is an info message. (level=20)
2024-10-17 12:34:56 - WARNING - This is a warning message. (level=30)
2024-10-17 12:34:56 - ERROR - This is an error message. (level=40)
2024-10-17 12:34:56 - CRITICAL - This is a critical message. (level=50)
'''

class LoggerUtils():
    def setup_logger(Job_Name = "Reconciliation_Job"):
        logger = logging.getLogger(Job_Name)
        #Setting up the logging level
        logger.setLevel('INFO')
        Config = ConfigUtils().getConfig()

        if Config.has_section('Audit'):
            if Config.has_option('Audit','Audit.log.level'):
                log_level = Config.get('Audit','Audit.log.level')
                logger.basicConfig()
                logger.setLevel(getattr(logger,log_level))
                for handler in logger.handlers:
                    handler.setLevel(getattr(logger,log_level))
                    
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger 

    logger = setup_logger()