import logging

class ParaLiteLog:
    
    @staticmethod
    def init(log_file, level):
        logging.basicConfig(filename=log_file,
                            level=level,
                            format='[%(asctime)s] %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S')
        logging.info("Logging Start")
        
    @staticmethod
    def debug(msg):
        logging.debug(msg)
            
    @staticmethod
    def info(msg):
        logging.info(msg)

    @staticmethod
    def warning(msg):
        logging.warning(msg)
        
    @staticmethod
    def error(msg):
        logging.error(msg)
        
    @staticmethod
    def critical(msg):
        logging.critical(msg)
        
        
