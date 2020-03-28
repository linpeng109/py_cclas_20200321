import pandas as pd

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_pandas import Parser


class SCNParser(Parser):
    # 初始化
    def __init__(self, config: ConfigFactory, logger: LoggerFactory):
        super(SCNParser, self).__init__(config=config, logger=logger)

    def getSCNDF(self, filename: str):
        # 1.读取
        sheet_name = 'SCN'
        dict = {'sheet_name': sheet_name, 'header': None, }
        scnDF = pd.read_excel(io=filename, **dict)
        elementList = scnDF.iloc[0:1].values.tolist()[0]
        # 填充空缺值
        scnDF[0].fillna(method='ffill', inplace=True)
        scnDF[1].fillna(method='ffill', inplace=True)
        # 删除表头
        scnDF.drop(axis=0, index=[0, 1], inplace=True)
        # 日期时间格式
        scnDF[0] = scnDF[0].dt.strftime('%Y-%m-%d')
        scnDF[1] = scnDF[1].dt.strftime('%H:%M')
        # 修改列名
        scnDF.columns = elementList
        # 清除空行
        scnDF.dropna(axis=0, how='all', inplace=True)
        # 填充空数据
        scnDF.fillna('', inplace=True)
        # 重新建立索引
        scnDF.reset_index(drop=True, inplace=True)
        return scnDF


if __name__ == '__main__':
    config = ConfigFactory(config='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    scnParser = SCNParser(logger=logger, config=config)

    filename = 'e:/cclasdir/2020生物氧化表格2.xlsx'
    sheet_name = 'SCN'
    scnDF = scnParser.getSCNDF(filename=filename)

    increamentDF = scnParser.getIncreamentDF(srcDF=scnDF, filename=filename)
    logger.debug('===increamentDF===')
    logger.debug(increamentDF.dtypes)
    logger.debug(increamentDF)
    results = scnParser.buildJDYReport(dataframe=increamentDF)
    print(results)
