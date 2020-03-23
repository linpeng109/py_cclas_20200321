import multiprocessing
import os
import time
import winsound

import tkinter.messagebox

from watchdog.events import PatternMatchingEventHandler
from watchdog.observers.polling import PollingObserver as Observer

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_pandas import DataFileParser


# from py_tkinter import AppUI


class WatchDogObServer():

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.parser = DataFileParser(config=config, logger=logger)

    def on_modified(self, event):
        # print(event)
        # result = self.parser.startProcess1(event)
        self.logger.debug(event)

    def on_any_event(self, event):
        self.logger.debug(event)

    def on_created(self, event):
        # print(event)
        # result = self.parser.startProcess1(event)
        if config.getboolean('watchdog', 'beep'):
            winsound.Beep(300, 500)
        # tkinter.messagebox.askyesnocancel('提示', '要执行此操作吗')

        self.logger.debug(event)

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
        # event_handler.on_modified = self.on_modified
        # event_handler.on_any_event = self.on_any_event

        observer = Observer()
        observer.schedule(path=path, event_handler=event_handler, recursive=recursive)

        observer.start()

        self.logger.debug('WatchDog Observer is startting.....')
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
