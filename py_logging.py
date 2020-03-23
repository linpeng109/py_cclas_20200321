import logging
import sys
from logging.handlers import RotatingFileHandler

import psutil as psutil
from py_config import ConfigFactory


class LoggerFactory():
    def __init__(self, config: ConfigFactory):
        self.cfg = config

    def getLogger(self):
        fileHandlerDict = dict(self.cfg.items('logger'))
        fileHandlerDict['maxBytes'] = int(fileHandlerDict['maxBytes'])
        fileHandlerDict['backupCount'] = int(fileHandlerDict['backupCount'])
        fileHandler = RotatingFileHandler(**fileHandlerDict)
        formatter = logging.Formatter(fmt="%(asctime)s %(name)s %(levelname)s %(message)s", datefmt="%Y%b%d-%H:%M:%S")
        fileHandler.setFormatter(formatter)
        logger = logging.getLogger()
        logger.addHandler(fileHandler)
        logger.addHandler(logging.StreamHandler(sys.stdout))
        logger.setLevel(logging.DEBUG)
        return logger


if __name__ == '__main__':
    config = ConfigFactory(config='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    logger.debug('Hello world!')
    for i in range(100):
        cpuper = psutil.cpu_percent()
        mem = psutil.virtual_memory()
        line = f'cpu:{cpuper}% mem:{mem} '
        logger.info(line)
