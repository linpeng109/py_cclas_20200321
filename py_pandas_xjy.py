from datetime import datetime

import pandas as pd
from pandas import DataFrame

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_pandas import Parser


class XJYParser(Parser):

    def getXJYDF(self, filename: str, sheet_name: str):
        # 读取
        dict = {'sheet_name': sheet_name, 'header': None, }
        xjyDF = pd.read_excel(io=filename, **dict)
        print(xjyDF)
        # 检查列名是否重复或者空值
        elementList = xjyDF.iloc[0:1].values.tolist()[0]
        xjyDF.columns = elementList
        print('Element list is %s' % elementList)
        self.checkColumnsIsContainsDuplicateOrNan(dataFrame=xjyDF)
        xjyDF.drop(axis=0, index=[0, 1], inplace=True)
        xjyDF['DATE'].fillna(method='ffill', inplace=True)
        xjyDF['TIME'].fillna(method='ffill', inplace=True)

        xjyDF['DATE'] = pd.to_datetime(xjyDF['DATE'], format='%m/%d/%Y')
        xjyDF['DATE'] = xjyDF['DATE'].dt.strftime('%Y-%m-%d')
        xjyDF['TIME'] = pd.to_datetime(xjyDF['TIME'], format='%H:%M:%S')
        xjyDF['TIME'] = xjyDF['TIME'].dt.strftime('%H:%M:%S')
        print(xjyDF)
        xjyDF.dropna(axis=0, how='all', inplace=True)
        xjyDF.fillna('', inplace=True)
        xjyDF.reset_index(drop=True, inplace=True)
        return xjyDF


if __name__ == '__main__':
    config = ConfigFactory(config='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    xjyParser = XJYParser(logger=logger, config=config)

    filename = 'e:/cclasdir/2020细菌氧化V2.xlsx'
    sheet_name = '01'
    method = 'SY001'


    xjyDF = xjyParser.getXJYDF(filename=filename, sheet_name=sheet_name)
    increamentDF = xjyParser.getIncreamentDF(srcDF=xjyDF, filename=filename, sheet_name=sheet_name)
    reports = xjyParser.buildReport(dataframe=increamentDF, sheet_name=sheet_name, method=method, startEleNum=4)
    xjyParser.outputReport(reports=reports)
    xjyParser.reportFileHandle(filename=filename, sheet_name=sheet_name)
