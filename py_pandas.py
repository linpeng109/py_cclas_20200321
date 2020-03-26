import pandas as pd
from pandas import DataFrame

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_path import Path

from collections import Counter
import numpy as np


class DataFileParser():
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
    def getSwyhjdlcyDF(self, filename: str):
        # 1.读取
        sheet_name = 'JDY'
        dict = {'sheet_name': sheet_name, 'header': None, }
        jdlcyDf = pd.read_excel(io=filename, **dict)
        # print('=====是否包含nan列名=====')
        # print(jdlcyDf.iloc[0:1].isnull().sum().sum() > 0)
        # print('=====是否包含重复列名=====')
        # print(jdlcyDf.iloc[0:1].isnull().sum().sum() > 0)
        # # 获取列名
        elementList = jdlcyDf.iloc[0:1].values.tolist()[0]
        # print(elementList)
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

    # 检测列名是否重复
    def colsIsDuplicate(self, dataFrame: DataFrame):
        columnList = dataFrame.columns.tolist()

        columnDict = dict(Counter(columnList))
        # key = 0
        for key, value in columnDict.items():
            if value > 1:
                return True
            if np.isnan(key):
                return True
        return True


# 获取比较不同
def getDiff(self, newDF: DataFrame, oldDF: DataFrame):
    difficut = pd.concat([newDF, oldDF, oldDF]).drop_duplicates(keep=False)
    difficut.fillna('', inplace=True)
    return difficut


if __name__ == '__main__':
    # 初始化
    config = ConfigFactory(config='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    dataFileParser = DataFileParser(config=config, logger=logger)

    # 读取数据
    filename = 'e:/cclasdir/2020生物氧化表格2.xlsx'
    sheet_name = 'JDY'

    jdlcyDF = dataFileParser.getSwyhjdlcyDF(filename=filename)
    print('\n===jdlcyDF===')

    columnsList = jdlcyDF.columns.tolist()
    colDF = DataFrame(columnsList)
    isNull = colDF.isnull().sum().sum()
    isDuplicate = colDF.duplicated().sum().sum()
    if isNull > 0:
        raise TypeError('数据转换失败：化验元素项包含空值')
    if isDuplicate > 0:
        raise TypeError('数据转换失败：化验元素项包含重复值')

    newFile = dataFileParser.filePathNameConverter(filename=filename, prefix='new')
    dataFileParser.toSeries(dataFrame=jdlcyDF, jsonfilename=newFile)
    newDF = dataFileParser.fromSeries(jsonfilename=newFile)
    print('\n===newDF===')
    print(newDF.dtypes)
    # print(newDF)

    oldFile = dataFileParser.filePathNameConverter(filename=filename, prefix='old')
    # dataFileParser.toSeries(jdlcyDF, oldFile)
    oldDF = dataFileParser.fromSeries(oldFile)
    # print('\n====oldDF=======')
    print(oldDF.dtypes)
    # print(oldDF)

    tmpDF = DataFrame(columns=newDF.columns.tolist())
    tmpDF = pd.concat([tmpDF, oldDF]).drop_duplicates(keep=False).fillna('')
    tmpfile = dataFileParser.filePathNameConverter(filename=filename, prefix='tmp')
    dataFileParser.toSeries(dataFrame=tmpDF, jsonfilename=tmpfile)
    tmpDF = dataFileParser.fromSeries(jsonfilename=tmpfile)
    # print('\n===tmpDF===')
    print(tmpDF.dtypes)
    # print(tmpDF)

    increamentDF = pd.concat([tmpDF, newDF, tmpDF]).drop_duplicates(keep=False)
    print('\n===increamentDF===')
    # print(increamentDF.dtypes)
    print(increamentDF)
