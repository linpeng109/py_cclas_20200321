from datetime import datetime

import pandas as pd
from pandas import DataFrame

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_path import Path


class Parser():
    # 初始化
    def __init__(self, config: ConfigFactory, logger: LoggerFactory):
        self.logger = logger
        self.config = config

    # 输入输出文件路径转换
    def filePathNameConverter(self, filename: str, sheet_name: str, prefix: str):
        new_path = self.config.get('json', 'outpath')
        Path.outpathIsExist(new_path)
        fileinfo = Path.splitFullPathFileName(filename)
        new_file_name = (
                new_path + fileinfo.get('sep') + prefix + '_' + '_' + fileinfo.get('main') + '.' + sheet_name)
        return new_file_name

    # 序列化
    def toSeries(self, dataFrame: DataFrame, filename: str):
        df = dataFrame.to_json(path_or_buf=filename, force_ascii=False)
        return df

    # 反序列化
    def fromSeries(self, filename: str):
        try:
            df = pd.read_json(path_or_buf=filename, encoding='gbk')
        except ValueError:
            df = DataFrame()
        return df

    # 检查列名是否重复或包含nan
    def checkColumnsIsContainsDuplicateOrNan(self, dataFrame: DataFrame):
        columnsList = dataFrame.columns.tolist()
        colDF = DataFrame(columnsList)
        isNull = colDF.isnull().sum().sum()
        isDuplicate = colDF.duplicated().sum().sum()
        if isNull > 0:
            raise TypeError('数据转换失败：化验元素项包含空值')
        if isDuplicate > 0:
            raise TypeError('数据转换失败：化验元素项包含重复值')

    # 获取比较不同
    def getIncreamentDF(self, srcDF: DataFrame, filename: str, sheet_name: str):

        ('===srcDF===')
        self.logger.debug(srcDF.dtypes)
        self.logger.debug(srcDF)

        self.checkColumnsIsContainsDuplicateOrNan(srcDF)

        newFile = self.filePathNameConverter(filename=filename, sheet_name=sheet_name, prefix='new')
        self.toSeries(dataFrame=srcDF, filename=newFile)
        newDF = self.fromSeries(filename=newFile)
        self.logger.debug('===newDF===')
        self.logger.debug(newDF.dtypes)
        self.logger.debug(newDF)

        oldFile = self.filePathNameConverter(filename=filename, sheet_name=sheet_name, prefix='old')
        # self.toSeries(srcDF, oldFile)
        oldDF = self.fromSeries(oldFile)
        self.logger.debug('===oldDF===')
        self.logger.debug(oldDF.dtypes)
        self.logger.debug(oldDF)

        tmp_df = DataFrame(columns=newDF.columns.tolist())
        tmp_df = pd.concat([tmp_df, oldDF]).drop_duplicates(keep=False).fillna('')
        tmp_file = self.filePathNameConverter(filename=filename, sheet_name=sheet_name, prefix='tmp')
        self.toSeries(dataFrame=tmp_df, filename=tmp_file)
        tmp_df = self.fromSeries(filename=tmp_file)
        self.logger.debug('===tmpDF===')
        self.logger.debug(tmp_df.dtypes)
        self.logger.debug(tmp_df)

        increamentDF = pd.concat([tmp_df, newDF, tmp_df]).drop_duplicates(keep=False)
        self.logger.debug('===increamentDF===')
        self.logger.debug(increamentDF.dtypes)
        self.logger.debug(increamentDF)

        return increamentDF

    # 拼接
    def buildJDYReport(self, dataframe: DataFrame):
        year_month = datetime.strftime(dataframe.loc[0, 'DATE'], '%y%m')
        month_day = datetime.strftime(dataframe.loc[0, 'DATE'], '%m%d')
        sheet_name = 'QTY'
        method = 'SY001'
        hour = dataframe.loc[0, 'TIME'].split(':')[0]
        sampleid = str(dataframe.loc[0, 'SAMPLEID'])
        print('sampleid=%s' % sampleid)
        colsNum = len(dataframe.columns)
        results = ('%-20s%-10s%-20s%02d' %
                   (sheet_name + year_month,
                    method,
                    month_day + '-' + hour + '-' + sampleid,
                    colsNum - 4))
        for i in range(4, colsNum):
            results = results + ('%-10s%-10s' % (dataframe.columns[i], dataframe.iloc[0, i]))

        return results
