from configparser import ConfigParser
import configparser


class ConfigFactory():
    def __init__(self, config:str):
        self.config = config

    class _Configparser(ConfigParser):
        def optionxform(self, optionstr):
            return optionstr

    def getConfig(self):
        cfg = self._Configparser()
        cfg._interpolation = configparser.ExtendedInterpolation()
        cfg.read(filenames=self.config, encoding='utf8')
        return cfg


if __name__ == '__main__':
    cfg = ConfigFactory(config='py_uipath2.ini').getConfig()
    dic = dict(cfg.items('logger'))
    print(dic)
