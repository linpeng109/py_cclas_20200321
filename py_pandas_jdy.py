from datetime import datetime

import pandas as pd
from pandas import DataFrame

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_path import Path


class JDYParser():
    # 初始化
    def __init__(self, config: ConfigFactory, logger: LoggerFactory):
        self.logger = logger
        self.config = config

    # 输入输出文件路径转换
    def filePathNameConverter(self, filename, prefix: str):
        newpath = self.config.get('json', 'outpath')
        Path.outpathIsExist(newpath)
        fileinfo = Path.splitFullPathFileName(filename)
        newfilename = (newpath + fileinfo.get('sep') + prefix + '_' + fileinfo.get('main') + '.json')
        return newfilename

    # 解析 生物氧化表格 解毒流程样
    def getJdyDF(self, filename: str):
        # 1.读取
        sheet_name = 'JDY'
        dict = {'sheet_name': sheet_name, 'header': None, }
        jdlcyDf = pd.read_excel(io=filename, **dict)
        elementList = jdlcyDf.iloc[0:1].values.tolist()[0]
        # 填充空缺值
        jdlcyDf[0].fillna(method='ffill', inplace=True)
        jdlcyDf[1].fillna(method='ffill', inplace=True)
        # 删除表头
        jdlcyDf.drop(axis=0, index=[0, 1], inplace=True)
        # 日期时间格式
        jdlcyDf[0] = jdlcyDf[0].dt.strftime('%Y-%m-%d')
        # jdlcyDf[0].astype("object")
        jdlcyDf[1] = jdlcyDf[1].dt.strftime('%H:%M')
        # 修改列名
        jdlcyDf.columns = elementList
        # 清除空行
        jdlcyDf.dropna(axis=0, how='all', inplace=True)
        # 填充空数据
        jdlcyDf.fillna('', inplace=True)
        # 重新建立索引
        jdlcyDf.reset_index(drop=True, inplace=True)
        return jdlcyDf

    # 序列化
    def toSeries(self, dataFrame: DataFrame, jsonfilename: str):
        # df = dataFrame.to_csv(path_or_buf=jsonfilename, encoding='gbk', index=None)
        df = dataFrame.to_json(path_or_buf=jsonfilename, force_ascii=False)
        return df

    # 反序列化
    def fromSeries(self, jsonfilename: str):
        try:
            # dataframe = pd.read_csv(filepath_or_buffer=jsonfilename, encoding='gbk', na_filter=None)
            df = pd.read_json(path_or_buf=jsonfilename, encoding='gbk')
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
    def getIncreamentDF(self, filename: str):

        jdlcyDF = dataFileParser.getJdyDF(filename=filename)
        ('===jdlcyDF===')
        self.logger.debug(jdlcyDF.dtypes)
        self.logger.debug(jdlcyDF)

        dataFileParser.checkColumnsIsContainsDuplicateOrNan(jdlcyDF)

        newFile = dataFileParser.filePathNameConverter(filename=filename, prefix='new')
        dataFileParser.toSeries(dataFrame=jdlcyDF, jsonfilename=newFile)
        newDF = dataFileParser.fromSeries(jsonfilename=newFile)
        self.logger.debug('===newDF===')
        self.logger.debug(newDF.dtypes)
        self.logger.debug(newDF)

        oldFile = dataFileParser.filePathNameConverter(filename=filename, prefix='old')
        # dataFileParser.toSeries(jdlcyDF, oldFile)
        oldDF = dataFileParser.fromSeries(oldFile)
        self.logger.debug('===oldDF===')
        self.logger.debug(oldDF.dtypes)
        self.logger.debug(oldDF)

        tmpDF = DataFrame(columns=newDF.columns.tolist())
        tmpDF = pd.concat([tmpDF, oldDF]).drop_duplicates(keep=False).fillna('')
        tmpfile = dataFileParser.filePathNameConverter(filename=filename, prefix='tmp')
        dataFileParser.toSeries(dataFrame=tmpDF, jsonfilename=tmpfile)
        tmpDF = dataFileParser.fromSeries(jsonfilename=tmpfile)
        self.logger.debug('===tmpDF===')
        self.logger.debug(tmpDF.dtypes)
        self.logger.debug(tmpDF)

        increamentDF = pd.concat([tmpDF, newDF, tmpDF]).drop_duplicates(keep=False)
        self.logger.debug('===increamentDF===')
        self.logger.debug(increamentDF.dtypes)
        self.logger.debug(increamentDF)

        return increamentDF

    # 拼接
    def buildJDYReport(self, dataframe: DataFrame):
        year_month = datetime.strftime(dataframe.iloc[0, 0], '%y%m')
        month_day = datetime.strftime(dataframe.iloc[0, 0], '%m%d')
        sheet_name = 'JDY'
        method = 'SY001'
        hour = dataframe.iloc[0, 1].split(':')[0]
        sampleid = dataframe.iloc[0, 3]
        colsNum = len(dataframe.columns)
        results = ('%-20s%-10s%-20s%02d' %
                   (sheet_name + year_month,
                    method,
                    month_day + '-' + hour + '-' + sampleid,
                    colsNum - 4))
        for i in range(4, colsNum):
            results = results + ('%-10s%-10s' % (dataframe.columns[i], dataframe.iloc[0, i]))

        return results


if __name__ == '__main__':
    # 初始化
    config = ConfigFactory(config='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    dataFileParser = JDYParser(config=config, logger=logger)

    # 读取数据
    filename = 'e:/cclasdir/2020生物氧化表格2.xlsx'
    dataframe = dataFileParser.getIncreamentDF(filename=filename)
    results = dataFileParser.buildJDYReport(dataframe=dataframe)
    logger.debug(results)
