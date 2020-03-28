import pandas as pd

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_pandas import Parser


class QTYParser(Parser):
    # 初始化
    def __init__(self, config: ConfigFactory, logger: LoggerFactory):
        super(QTYParser, self).__init__(config=config, logger=logger)

    def getQTYDF(self, filename: str):
        # 1.读取
        sheet_name = 'QTY'
        dict = {'sheet_name': sheet_name, 'header': None, }
        qtyDF = pd.read_excel(io=filename, **dict)
        elementList = qtyDF.iloc[0].values.tolist()
        # 修改列名
        qtyDF.columns = elementList
        # 填充空缺值
        qtyDF['DATE'].fillna(method='ffill', inplace=True)
        # 删除表头
        qtyDF.drop(axis=0, index=[0, 1], inplace=True)
        # 日期时间格式
        qtyDF['DATE'] = pd.to_datetime(qtyDF['DATE'], format='%Y-%m-%d')
        qtyDF['DATE'] = qtyDF['DATE'].dt.strftime('%Y-%m-%d')
        qtyDF['TIME'] = pd.to_datetime(qtyDF['TIME'], format='%H:%M:%S')
        qtyDF['TIME'] = qtyDF['TIME'].dt.strftime('%H:%M')
        # 清除空行
        qtyDF.dropna(axis=0, how='all', inplace=True)
        # 填充空数据
        qtyDF.fillna('', inplace=True)
        # 重新建立索引
        qtyDF.reset_index(drop=True, inplace=True)
        return qtyDF


if __name__ == '__main__':
    config = ConfigFactory(config='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    qtyParser = QTYParser(logger=logger, config=config)

    filename = 'e:/cclasdir/2020生物氧化表格2.xlsx'
    sheet_name = 'QTY'
    qtyDF = qtyParser.getQTYDF(filename=filename)
    print('====qtyDF====')
    print(qtyDF.dtypes)
    print(qtyDF)

    increamentDF = qtyParser.getIncreamentDF(srcDF=qtyDF, filename=filename, sheet_name='QTY')
    logger.debug('===increamentDF===')
    logger.debug(increamentDF.dtypes)
    logger.debug(increamentDF)
    results = qtyParser.buildJDYReport(dataframe=increamentDF)
    print(results)
