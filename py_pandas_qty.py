from datetime import datetime

import pandas as pd
from pandas import DataFrame

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_pandas import Parser


class QTYParser(Parser):

    def getQTYDF(self, filename: str, sheet_name: str):
        dict = {'sheet_name': sheet_name, 'header': None, }
        qtyDF = pd.read_excel(io=filename, **dict)
        elementList = qtyDF.iloc[0].values.tolist()
        print(elementList)
        self.checkColumnsIsContainsDuplicateOrNan(qtyDF)
        qtyDF.columns = elementList
        qtyDF['DATE'].fillna(method='ffill', inplace=True)
        qtyDF.drop(axis=0, index=[0, 1], inplace=True)
        qtyDF['DATE'] = pd.to_datetime(qtyDF['DATE'], format='%Y-%m-%d')
        qtyDF['DATE'] = qtyDF['DATE'].dt.strftime('%Y-%m-%d')
        qtyDF['TIME'] = pd.to_datetime(qtyDF['TIME'], format='%H:%M:%S')
        qtyDF['TIME'] = qtyDF['TIME'].dt.strftime('%H:%M')
        qtyDF.dropna(axis=0, how='all', inplace=True)
        qtyDF.fillna('', inplace=True)
        qtyDF.reset_index(drop=True, inplace=True)
        return qtyDF


if __name__ == '__main__':
    config = ConfigFactory(config='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    qtyParser = QTYParser(logger=logger, config=config)

    filename = 'e:/cclasdir/2020生物氧化表格2.xlsx'
    sheet_name = 'QTY'
    method = 'SY001'

    qtyDF = qtyParser.getQTYDF(filename=filename, sheet_name=sheet_name)
    increamentDF = qtyParser.getIncreamentDF(srcDF=qtyDF, filename=filename, sheet_name=sheet_name)
    reports = qtyParser.buildReport(dataframe=increamentDF, sheet_name=sheet_name, method=method, startEleNum=4)
    qtyParser.outputReport(reports=reports)
    qtyParser.reportFileHandle(filename=filename, sheet_name=sheet_name)
