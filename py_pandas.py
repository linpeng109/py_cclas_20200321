import pandas as pd
from pandas import DataFrame

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_path import Path


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
        sheet_name = '解毒流程样'
        dict = {'sheet_name': sheet_name, 'header': None, }
        jdlcyDf = pd.read_excel(io=filename, **dict)
        elementList = jdlcyDf.iloc[0:1].values.tolist()

        # 2填充空缺值
        jdlcyDf[0].fillna(method='ffill', inplace=True)
        jdlcyDf[1].fillna(method='ffill', inplace=True)

        # 3删除表头
        jdlcyDf.drop(axis=0, index=[0, 1], inplace=True)

        # 4日期时间格式
        jdlcyDf[0] = jdlcyDf[0].dt.strftime('%y-%m-%d')
        jdlcyDf[1] = jdlcyDf[1].dt.strftime('%H:%M')

        # 5清除空行
        jdlcyDf.dropna(axis=0, how='all', inplace=True)
        # jdlcyDf.dropna(axis=1, how='all', inplace=True)
        # 6填充空数据
        # jdlcyDf.fillna('', inplace=True)
        # 7?重新命名列
        jdlcyDf.columns = elementList
        # 8?重新建立索引
        jdlcyDf.reset_index(drop=True, inplace=True)
        return jdlcyDf

    # 获取化验元素列表
    def getElementsDF(self, filename: str, sheet_name: str):
        # sheet_name = '解毒流程样'
        dict = {'sheet_name': sheet_name, 'header': None, }
        elementsDF = pd.read_excel(io=filename, **dict)
        # elements = elementsDF.iloc[0:1].values.tolist()[0]
        return elementsDF.iloc[0:1]

    # 序列化
    def toSeries(self, dataFrame: DataFrame, jsonfilename: str):
        return dataFrame.to_json(jsonfilename, orient='index', force_ascii=False)

    # 反序列化
    def fromSeries(self, jsonfilename: str):
        try:
            dataframe = pd.read_json(path_or_buf=jsonfilename, orient='index', encoding='gbk')
        except ValueError:
            dataframe = DataFrame()
        return dataframe

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
    filename = 'e:/cclasdir/2020生物氧化表格.xlsx'
    sheet_name = '解毒流程样'

    jdlcyElementDF = dataFileParser.getElementsDF(filename=filename, sheet_name=sheet_name)
    print('===jdlcyElementDF===')
    print(jdlcyElementDF)
    jdlcyDF = dataFileParser.getSwyhjdlcyDF(filename=filename)
    print('====jdlcyDF=======')
    print(jdlcyDF)

    oldFile = dataFileParser.filePathNameConverter(filename=filename, prefix='old')
    oldDF = dataFileParser.fromSeries(oldFile)
    oldDF = pd.concat([jdlcyElementDF, oldDF])

    increamentDF = DataFrame(jdlcyElementDF.values.tolist())
    increamentDF = pd.concat([oldDF, increamentDF])
    increamentDF = pd.concat([jdlcyDF, increamentDF]).drop_duplicates(keep=False)
    print('======increamentDF======')
    print(increamentDF)

#
# year = str(jdlcyDF.iloc[0, 0]).split('-')[0]
# mouth = str(jdlcyDF.iloc[0, 0]).split('-')[1]
# print('%-10s%-15s' % (year, mouth))

# 获取新数据的各个元素
# elementsDF = dataFileParser.getElementsDF(newDF)
# print('========elements==========')
# print(elementsDF.shape)
# print(elementsDF)
#
# # 序列化新数据
# newFile = dataFileParser.filePathNameConverter(filename=filename, prefix='new')
# dataFileParser.toSeries(jsonfilename=newFile, dataFrame=newDF)
# #
# # # 反序列化旧数据
# oldFile = dataFileParser.filePathNameConverter(filename=filename, prefix='old')
#
# # 扩充旧数据的列内容
# oldDF = dataFileParser.fromSeries(jsonfilename=oldFile)
# # print('====oldDF=======')
# # print(oldDF.shape)
# # print(oldDF)
#
# oldDF = pd.concat([elementsDF, oldDF])
# print('=====oldDF======')
# print(oldDF)
# #
# # 反序列化新数据
# newDF = dataFileParser.fromSeries(jsonfilename=newFile)
#
# # 数据比较获取增量
# result = dataFileParser.getDiff(newDF=newDF, oldDF=oldDF)
#
# # 如果存在增量
# # print(result.shape[0])
# if (result.shape[0]):
#     dataFileParser.toSeries(dataFrame=newDF, jsonfilename=oldFile)
# print('======增量========')
# print(result.shape)
# print(result)
