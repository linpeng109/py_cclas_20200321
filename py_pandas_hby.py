import uuid
from datetime import datetime

import pandas as pd
from pandas import DataFrame

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_pandas import Parser


class HBParser(Parser):

    def getHBYDF(self, filename: str, sheet_name: str):
        dict = {'sheet_name': sheet_name, 'header': None, }
        hbyDF = pd.read_excel(io=filename, **dict)
        elementList = hbyDF.iloc[0:1].values.tolist()[0]
        hbyDF.columns = elementList
        self.checkColumnsIsContainsDuplicateOrNan(hbyDF)
        hbyDF['DATE'].fillna(method='ffill', inplace=True)
        # hbDF['TIME'].fillna(method='ffill', inplace=True)
        hbyDF.drop(axis=0, index=[0, 1], inplace=True)
        hbyDF['DATE'] = pd.to_datetime(hbyDF['DATE'], format='%Y.%m.%d')
        hbyDF['DATE'] = hbyDF['DATE'].dt.strftime('%Y-%m-%d')
        hbyDF.dropna(axis=0, how='all', inplace=True)
        hbyDF.fillna('', inplace=True)
        hbyDF.reset_index(drop=True, inplace=True)
        return hbyDF

    # # 拼接
    # def buildReport(self, dataframe: DataFrame, sheet_name: str, method: str):
    #     reports = []
    #     for row in dataframe.itertuples():
    #         year_month = datetime.strftime(getattr(row, 'DATE'), '%y%m')
    #         month_day = datetime.strftime(getattr(row, 'DATE'), '%m%d')
    #         sample_id = str(getattr(row, 'SAMPLEID'))
    #         colsNum = len(dataframe.columns)
    #         not_null_cols_num = 0
    #         report = ''
    #         for j in range(3, colsNum):
    #             if (row[j + 1] != ''):
    #                 report = report + ('%-10s%-10.8s' % (dataframe.columns[j], row[j + 1]))
    #                 not_null_cols_num = not_null_cols_num + 1
    #         report = ('%-20s%-10s%-20s%02d%s' %
    #                   (sheet_name + year_month,
    #                    method,
    #                    month_day + '-' + sample_id,
    #                    not_null_cols_num,
    #                    report))
    #         reports.append(report)
    #     return reports

    # # 写出单行数据文件
    # def outputReport(self, filename: str, sheet_name: str, method: str):
    #     hbyDF = self.getHBYDF(filename=filename, sheet_name=sheet_name)
    #     increamentDF = self.getIncreamentDF(srcDF=hbyDF, filename=filename, sheet_name=sheet_name)
    #     reports = self.buildReport(dataframe=increamentDF, sheet_name=sheet_name, method=method)
    #     print('===reports===')
    #     for report in reports:
    #         filename = self.config.get('cclas', 'outputdir') + '/' + str(uuid.uuid4()).replace("-", "") + '.txt'
    #         with open(filename, 'w+') as file:
    #             file.write(report)
    #             file.close()
    #         self.logger.debug(report)


if __name__ == '__main__':
    # 初始化
    config = ConfigFactory(config='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    hbParser = HBParser(config=config, logger=logger)

    filename = 'e:/cclasdir/2020环保表格.xlsx'
    sheet_name = 'HBY'
    method = 'SY001'

    hbyDF = hbParser.getHBYDF(filename=filename, sheet_name=sheet_name)
    increamentDF = hbParser.getIncreamentDF(srcDF=hbyDF, filename=filename, sheet_name=sheet_name)
    reports = hbParser.buildReport(dataframe=increamentDF, sheet_name=sheet_name, method=method, startEleNum=3)
    hbParser.outputReport(reports=reports)
    hbParser.reportFileHandle(filename=filename, sheet_name=sheet_name)
