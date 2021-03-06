import multiprocessing
import os
import time
# from asyncio.subprocess import Process
from datetime import datetime

import winsound
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers.polling import PollingObserver as Observer

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_pandas_hby import HBParser
from py_pandas_jdy import JDYParser
from py_pandas_qty import QTYParser
from py_pandas_scn import SCNParser
from py_pandas_xjy import XJYParser
from py_path import Path


class WatchDogObServer():

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.jdyParser = JDYParser(config=config, logger=logger)
        self.scnParser = SCNParser(config=config, logger=logger)
        self.hbyParser = HBParser(config=config, logger=logger)
        self.qtyParser = QTYParser(config=config, logger=logger)
        self.xjyParser = XJYParser(config=config, logger=logger)

    def on_modified(self, event):
        # print(event)
        # result = self.parser.startProcess1(event)
        self.logger.debug(event)

    def on_any_event(self, event):
        self.logger.debug(event)

    # 当创建文件时触发
    def on_created(self, event):
        # 获取文件名
        filename = event.src_path
        # 发声
        if config.getboolean('watchdog', 'beep'):
            winsound.Beep(800, 500)
        # 解析数据文件
        if Path.filenameIsContains(filename, ['细菌氧化', '2020', 'xlsx']):
            xjyProcess = multiprocessing.Process(target=self.xjyWorker, args=(event,))
            xjyProcess.start()
        if Path.filenameIsContains(filename, ['生物氧化', '2020', 'xlsx']):
            qtyProcess = multiprocessing.Process(target=self.qtyWorker, args=(event,))
            qtyProcess.start()
            jdyProcess = multiprocessing.Process(target=self.jdyWorker, args=(event,))
            jdyProcess.start()
            scnProcess = multiprocessing.Process(target=self.scnWorker, args=(event,))
            scnProcess.start()
        if Path.filenameIsContains(filename, ['环保', '2020', 'xlsx']):
            hbyProcess = multiprocessing.Process(target=self.hbyWorker, args=(event,))
            hbyProcess.start()
        self.logger.debug(event)

    def scnWorker(self, event):
        filename = event.src_path
        sheet_name = 'SCN'
        method = 'SY001'

        scnDF = self.scnParser.getSCNDF(filename=filename, sheet_name=sheet_name)
        increamentDF = self.scnParser.getIncreamentDF(srcDF=scnDF, filename=filename, sheet_name=sheet_name)
        reports = self.scnParser.buildReport(dataframe=increamentDF, sheet_name=sheet_name, method=method,
                                             startEleNum=4)
        self.scnParser.outputReport(reports=reports)
        self.scnParser.reportFileHandle(filename=filename, sheet_name=sheet_name)

    def jdyWorker(self, event):
        filename = event.src_path
        sheet_name = 'JDY'
        method = 'SY001'

        jdyDF = self.jdyParser.getJDYDF(filename=filename, sheet_name=sheet_name)
        increamentDF = self.jdyParser.getIncreamentDF(srcDF=jdyDF, filename=filename, sheet_name=sheet_name)
        reports = self.jdyParser.buildReport(dataframe=increamentDF, sheet_name=sheet_name, method=method,
                                             startEleNum=4)
        self.jdyParser.outputReport(reports=reports)
        self.jdyParser.reportFileHandle(filename=filename, sheet_name=sheet_name)

    def hbyWorker(self, event):
        filename = event.src_path
        sheet_name = 'HBY'
        method = 'SY001'

        hbyDF = self.hbyParser.getHBYDF(filename=filename, sheet_name=sheet_name)
        increamentDF = self.hbyParser.getIncreamentDF(srcDF=hbyDF, filename=filename, sheet_name=sheet_name)
        reports = self.hbyParser.buildReport(dataframe=increamentDF, sheet_name=sheet_name, method=method,
                                             startEleNum=3)
        self.hbyParser.outputReport(reports=reports)
        self.hbyParser.reportFileHandle(filename=filename, sheet_name=sheet_name)

    def qtyWorker(self, event):
        filename = event.src_path
        sheet_name = 'QTY'
        method = 'SY001'

        qtyDF = self.qtyParser.getQTYDF(filename=filename, sheet_name=sheet_name)
        increamentDF = self.qtyParser.getIncreamentDF(srcDF=qtyDF, filename=filename, sheet_name=sheet_name)
        reports = self.qtyParser.buildReport(dataframe=increamentDF, sheet_name=sheet_name, method=method,
                                             startEleNum=4)
        self.qtyParser.outputReport(reports=reports)
        self.qtyParser.reportFileHandle(filename=filename, sheet_name=sheet_name)

    def xjyWorker(self, event):
        filename = event.src_path
        # 取得当前月份01、02……
        datem = datetime.today().month
        sheet_name = '%02d' % datem
        method = 'SY001'
        xjyDF = self.xjyParser.getXJYDF(filename=filename, sheet_name=sheet_name)
        increamentDF = self.xjyParser.getIncreamentDF(srcDF=xjyDF, filename=filename, sheet_name=sheet_name)
        reports = self.xjyParser.buildReport(dataframe=increamentDF, sheet_name=sheet_name, method=method,
                                             startEleNum=4)
        self.xjyParser.outputReport(reports=reports)
        self.xjyParser.reportFileHandle(filename=filename, sheet_name=sheet_name)

    def start(self):
        path = self.config.get('watchdog', 'path')
        patterns = self.config.get('watchdog', 'patterns').split(';')
        ignore_directories = self.config.getboolean('watchdog', 'ignore_directories')
        ignore_patterns = self.config.get('watchdog', 'ignore_patterns').split(';')
        case_sensitive = self.config.getboolean('watchdog', 'case_sensitive')
        recursive = self.config.getboolean('watchdog', 'recursive')

        event_handler = PatternMatchingEventHandler(patterns=patterns,
                                                    ignore_patterns=ignore_patterns,
                                                    ignore_directories=ignore_directories,
                                                    case_sensitive=case_sensitive)
        event_handler.on_created = self.on_created

        observer = Observer()
        observer.schedule(path=path, event_handler=event_handler, recursive=recursive)

        observer.start()

        self.logger.debug('WatchDog Observer for CCLAS is startting.....')
        self.logger.debug('patterns=%s' % patterns)
        self.logger.debug('path=%s' % path)
        self.logger.debug('delay=%s' % str(config.getfloat('watchdog', 'delay')))
        self.logger.debug('beep=%s' % str(config.getboolean('watchdog', 'beep')))
        try:
            while observer.is_alive():
                time.sleep(config.getfloat('watchdog', 'delay'))
        except KeyboardInterrupt:
            observer.stop()
            self.logger.debug('WatchDog Observer is stoped.')
        observer.join()


if __name__ == '__main__':
    if os.sys.platform.startswith('win'):
        multiprocessing.freeze_support()
    config = ConfigFactory(config='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    wObserver = WatchDogObServer(config=config, logger=logger)
    wObserver.start()
