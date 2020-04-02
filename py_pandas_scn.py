import pandas as pd

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_pandas import Parser


class SCNParser(Parser):

    def getSCNDF(self, filename: str, sheet_name: str):
        dict = {'sheet_name': sheet_name, 'header': None, }
        scnDF = pd.read_excel(io=filename, **dict)
        elementList = scnDF.iloc[0:1].values.tolist()[0]
        scnDF.columns = elementList
        self.checkColumnsIsContainsDuplicateOrNan(dataFrame=scnDF)
        scnDF.drop(axis=0, index=[0, 1], inplace=True)
        scnDF['DATE'].fillna(method='ffill', inplace=True)
        scnDF['TIME'].fillna(method='ffill', inplace=True)
        # scnDF['DATE'] = pd.to_datetime(scnDF['DATE'], format='%Y/%m/%d')
        scnDF['DATE'] = scnDF['DATE'].dt.strftime('%Y-%m-%d')
        # scnDF['TIME'] = pd.to_datetime(scnDF['TIME'], format='%Y-%m-%d %H:%M:%S')
        scnDF['TIME'] = scnDF['TIME'].dt.strftime('%H:%M')
        scnDF.dropna(axis=0, how='all', inplace=True)
        scnDF.fillna('', inplace=True)
        scnDF.reset_index(drop=True, inplace=True)
        return scnDF


if __name__ == '__main__':
    config = ConfigFactory(config='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    scnParser = SCNParser(logger=logger, config=config)

    filename = 'e:/cclasdir/2020生物氧化表格2.xlsx'
    sheet_name = 'SCN'
    method = 'SY001'

    scnDF = scnParser.getSCNDF(filename=filename, sheet_name=sheet_name)
    increamentDF = scnParser.getIncreamentDF(srcDF=scnDF, filename=filename, sheet_name=sheet_name)
    reports = scnParser.buildReport(dataframe=increamentDF, sheet_name=sheet_name, method=method, startEleNum=4)
    scnParser.outputReport(reports=reports)
    scnParser.reportFileHandle(filename=filename, sheet_name=sheet_name)
