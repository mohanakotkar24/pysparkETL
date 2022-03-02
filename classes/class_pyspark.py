#!/usr/bin/python3
from copyreg import remove_extension
from distutils import filelist
import json, os, re, sys
from optparse import Option
from typing import Callable, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *

class SparkClass:
    
    def __init__(self, config) -> None:
        self.config = config
        self.debug_dir = "/tmp/spark"

    def sparkStart(self, kwargs: dict) -> SparkSession:
        MASTER = kwargs["spark_conf"]["master"]
        APP_NAME = kwargs["spark_conf"]["appname"]
        LOG_LEVEL = kwargs["log"]["level"]

        def createSession(master:Optional[str]="local[*]", app_name:Optional[str]="myapp") -> SparkSession:
            spark = SparkSession \
                        .builder \
                        .appName(app_name) \
                        .master(master) \
                        .getOrCreate()
            return spark

        def setLogging(spark: SparkSession, log_level:Optional[str]=None) -> None:
            spark.sparkContext.setLogLevel(log_level) if isinstance(log_level, str) else None

        def getSetting(spark: SparkSession):
            # Get spark setting
            print(f"\033[1;33m{spark}\033[0m")
            print(f"\033[96m{spark.sparkContext.getConf().getAll()}\033[0m")

        spark = createSession()
        # setLogging(spark, LOG_LEVEL)
        # getSetting(spark)
        return spark

    def openJson(self, filepath: str) -> dict:
        def openJson(filepath: str) -> dict:
            if isinstance(filepath, str) and os.path.exists(filepath):
                with open(filepath, "r") as f:
                    data = json.load(f)
                return data
        return openJson(filepath)
    
    def importData(self, spark: SparkSession, datapath: str, format:Optional[str]=None) -> DataFrame:

        def fileOrDirectory(datapath:str, format:Optional[str]=None) -> str:
            if isinstance(datapath, str) and os.path.exists(datapath):
                if os.path.isdir(datapath):
                    return openDirectory(datapath, format)
                elif os.path.isfile(datapath):
                    return openFile(datapath)
        
        # pathtype = fileOrDirectory(datapath)

        def openDirectory(datapath:str, format:Optional[str]=None) -> list:
            if isinstance(datapath, str) and os.path.exists(datapath):
                filelist = SparkClass(self.config).listDirectory(datapath, format)
                filetype = getUniqueFileExtensions(filelist)
                if filetype:
                    return SparkClass(self.config).createDataFrame(spark, filelist, filetype)

        def openFile(filepath:str):
            if isinstance(filepath, str) and os.path.exists(filepath):
                filelist = [filepath]
                filetype =  SparkClass(self.config).getFileExtension(filepath)
                return SparkClass(self.config).createDataFrame(spark, filelist, filetype)

        def getUniqueFileExtensions(filelist:list) -> list:
            if isinstance(filelist, list) and len(filelist)>0:
                exts = list(set([os.path.splitext(f)[1] for f in filelist]))
                return exts[0][1:] if len(exts)==1 else None
        
        return fileOrDirectory(datapath, format)

        # if pathtype == "Dir":
        #     return openDirectory(spark, datapath, format)
        # elif pathtype=="File":
        #     return openFile(SparkClass(self.config).getFileExtension, datapath)
        # else:
        #     None
        
    def getFileExtension(self, filepath:str) -> list:
        if isinstance(filepath, str) and os.path.exists(filepath):
            filename, file_extension = os.path.splitext(filepath)
            return file_extension[1:] if file_extension else None

    def listDirectory(self, directory: str, format:Optional[str]=None) -> list:
        
        def recursiveFilelist(directory):
            if os.path.exists(directory):
                fileList = []
                for dirpath, dirname, fname in os.walk(directory):
                    for f in fname:
                        fileList.append(f"{dirpath}/{f}")
                return fileList

        def filterFiles(fileList:list, format:str):
            if isinstance(format, str):
                return [x for x in fileList if re.search(format, x)]
            else:
                return fileList

        filelist = recursiveFilelist(directory)
        return filterFiles(filelist, format)

    def createDataFrame(self, spark: SparkSession, filelist: list, filetype: str) -> DataFrame:
        
        def dfFromCSV(filelist:list) -> DataFrame:
            # print(f"FileType: {filetype} \n filelist : {filelist}")
            if isinstance(filelist, list) and len(filelist)>0:
                df = (spark.read.format("csv")
                        .option("header","true")
                        .option("mode","DROPMALFORMED")
                        .load(filelist))
                return df

        def dfFromJSON(filelist:list) -> DataFrame:
            if isinstance(filelist, list) and len(filelist)>0:
                df = (spark.read.format("json")
                        .option("mode","PERMISSIVE")
                        .option("primitiveAsString","true")
                        .load(filelist))
                return df

        def makeDF(filelist: list, filetype: str) -> DataFrame:
            if filetype=="csv":
                return dfFromCSV(filelist)
            elif filetype=="json":
                return dfFromJSON(filelist)
            else:
                return None

        return makeDF(filelist, filetype)

    def debugDF(self, df: DataFrame, filename: str) -> None:
        # print(f"DataFrame: \n {df} \n FileName: {filename}")        
        def makeDirectory(directory:str) -> None:
            if not os.path.exists(directory):
                return os.makedirs(directory)

        def createFilepath(dir:str, filename: str) -> str:
            return f"{dir}/{filename}.json"

        def removeFile(filepath: str) -> None:
            if os.path.exists(filepath):
                os.remove(filepath)
        
        def dfToString(df: DataFrame) -> str:
            return df._jdf.schema().treeString()

        def createContent(df:DataFrame):
            content = {}
            content['count'] = df.count()
            content['schema'] = json.loads(df.schema.json())
            return json.dumps(content, sort_keys=True, indent=4, default = str, ensure_ascii=True)

        def createFile(filepath:str, content) -> None:
            with open(filepath,"a") as f:
                f.write(f"{content}\n")
            f.close()
        
        makeDirectory(self.debug_dir)
        filepath = createFilepath(self.debug_dir, filename)
        removeFile(filepath)
        # debugstr = dfToString(df)
        content = createContent(df)
        createFile(filepath, content)










        
        
        
        
        
        
        
        

