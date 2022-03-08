#!/usr/bin/python3
from copyreg import remove_extension
from distutils import filelist
from distutils.util import execute
from importlib.resources import path
import itertools
import json, os, re, sys
from optparse import Option
from typing import Callable, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *

class SparkClass:
    
    def __init__(self, config) -> None:
        self.config = config
        self.debug_dir = "/tmp/spark"
        self.config_path = (f"{self.debug_dir}/config",f"{self.debug_dir}/config/sparkSession.json")

    def sparkStart(self, kwargs: dict) -> SparkSession:
        MASTER = kwargs.get("spark_conf",{}).get("master","local[*]")
        APP_NAME = kwargs.get("spark_conf",{}).get("appname","myapp")
        CONFIG = kwargs.get("config")
        LOG_LEVEL = kwargs.get("log", {}).get("level")
        warehouse_loc = "spark-warehouse"

        def configDeltaLake(builder: SparkSession.Builder, config: dict):
            if isinstance(builder, SparkSession.Builder) and config.get("deltalake") == True:
                from delta import configure_spark_with_delta_pip
                builder = (builder
                        .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
                        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                        )
                return configure_spark_with_delta_pip(builder)
            else:
                return builder

        def createBuilder(master:str, app_name:str, config:dict) -> SparkSession.Builder:
            builder = (SparkSession 
                        .builder 
                        .appName(app_name) 
                        .master(master)
                        .config("spark.sql.warehouse.dir",warehouse_loc) 
                        .enableHiveSupport()
                    )
            return configDeltaLake(builder, config)

        def createSession(builder: SparkSession.Builder) -> SparkSession:
            if isinstance(builder, SparkSession.Builder):
                return builder.getOrCreate()
        
        def setLogging(spark: SparkSession, log_level:Optional[str]=None) -> None:
            spark.sparkContext.setLogLevel(log_level) if isinstance(log_level, str) else None

        def getSetting(spark: SparkSession, config_path:tuple):
            # Get spark setting
            # print(f"\033[1;33m{spark}\033[0m")
            print(f"\033[96m{spark.sparkContext.getConf().getAll()}\033[0m")
            c = {}
            c['spark.version'] = spark.version
            c['spark.SparkContext'] = spark.sparkContext.getConf().getAll()
            content = json.dumps(c, sort_keys=False, indent=4, default=str)
            SparkClass(self.config).debugCreateFile(paths=(config_path[0],config_path[1]),content=content)

        builder = createBuilder(MASTER, APP_NAME, CONFIG)
        spark = createSession(builder)
        setLogging(spark, LOG_LEVEL)
        getSetting(spark, self.config_path)
        return spark

    def openJson(self, filepath: str) -> dict:
        if isinstance(filepath, str) and os.path.exists(filepath):
            with open(filepath, "r") as f:
                data = json.load(f)
            return data
    
    def importData(self, spark: SparkSession, datapath: str, format:Optional[str]=None) -> DataFrame:

        def fileOrDirectory(datapath:str, format:Optional[str]=None) -> str:
            if isinstance(datapath, str) and os.path.exists(datapath):
                if os.path.isdir(datapath):
                    return openDirectory(datapath, format)
                elif os.path.isfile(datapath):
                    return openFile(datapath)
        
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

    def createTempTables(self,tupleDF:tuple):
        if isinstance(tupleDF,tuple) and len(tupleDF)==2:
            tupleDF[0].createOrReplaceTempView(tupleDF[1])
    
    def loadTables(self, spark: SparkSession, path: str, format: str) -> DataFrame:
        # listOfPaths - Spark, Path and Format
        # print(f"\033[98m{path}\033[0m")
        if os.path.exists(path):
            df = (spark.read.format(format)
                        .option("mergeSchema","true")
                        .load(path))
            return df
        
    def exportDF(self,tupleDF:tuple):
        # print(tupleDF[0],"->", tupleDF[1],"\n")
        # if isinstance(tupleDF,tuple) and len(tupleDF)==2 and self.config.get("export"):
        #     path = f"{self.config.get('export')}/{tupleDF[1]}"
        #     tupleDF[0].write.format("delta").mode("overwrite").save(path)
        
        def loopSession(sessionList: list, pattern: str) -> list:
            if isinstance(sessionList, list):
                result = [[x for x in linelist if matchPattern(x, pattern)] for linelist in sessionList]
                pkgs = set(list(itertools.chain.from_iterable(result)))
                # result = [[print(type(x),"->", x ,"->" ,pattern) for x in linelist] for linelist in sessionList]
                # print(f"\033[98m{pkgs}\033[0m")
                return True if len(pkgs) > 0 else False

        def matchPattern(item:str, pattern:str) -> str:
            if re.search(pattern, item):
                return item    
        
        def openSession(spark: SparkSession) -> list:
            return spark.sparkContext.getConf().getAll()
        
        def validateDependency(sessionList: list, pattern: str) -> bool:
            return loopSession(sessionList, pattern)
            # return print("Dependency Resolved") if val else print("Dependent Package Not Present")
            
        def writeExport(tupleDF:tuple) -> None:
            if isinstance(tupleDF, tuple) and len(tupleDF) > 0:
                # print(f"\033[98m{tupleDF}\033[0m")
                spark = tupleDF[0]
                df = tupleDF[1]
                settings = tupleDF[2]
                if validateDependency(openSession(spark), settings.get("format")) and isinstance(df, DataFrame):
                    if settings.get("format") == "delta":
                        SparkClass(self.config).exportDelta(spark, df, settings)
                    else: 
                        path = f"{self.config.get('export')}/{tupleDF[1]}"
                        df.write.format(settings.get("format")).mode("overwrite").save(path)
        return writeExport(tupleDF)

    def exportDelta(self, spark: SparkSession, df: DataFrame, settings: dict):
        from delta import DeltaTable
        
        def tableHistory(spark: SparkSession, df: DataFrame, settings: dict) -> None:
            """ Return information on table version """
            if DeltaTable.isDeltaTable(spark, settings.get('path')):
                dt = DeltaTable.forPath(spark, settings.get('path'))
                dt.history().show()

        def tableVacuum(spark: SparkSession, df: DataFrame, settings: dict) -> None:
            """ Remove previous table versions"""
            if DeltaTable.isDeltaTable(spark, settings.get('path')):
                dt = DeltaTable.forPath(spark, settings.get('path'))
                dt.vacuum(168)

        def debugSession(spark: SparkSession):
            """ Provides Spark Config information and store it in exportDelta.json file"""
            c = {}
            c['spark.version'] = spark.version
            c['spark.SparkContext'] = spark.sparkContext.getConf().getAll()
            content = json.dumps(c, sort_keys=False, indent=4, default=str)
            SparkClass(self.config).debugCreateFile(paths=(self.config_path[0],f"{self.config_path[0]}/exportDelta.json"),content=content)
    
        def tableNew(df: DataFrame, settings: dict) -> None:
            """ create a new table given DataFrame and config details"""
            return (df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(settings.get('path')))

        def tableMerge(spark: SparkSession, df: DataFrame, settings: dict) -> None:
            """ Merge the data with existing table - given DataFrame and existing table details along with config info"""
            if settings.get('key') == None:
                raise ValueError("Provide a key in your settings dict to merge tables") 
            
            if DeltaTable.isDeltaTable(spark, settings.get('path')):
                spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
                spark.sql("SET spark.databricks.delta.resolveMergeUpdateStructByName.enabled = false")
            
            debugSession(spark)
            dt = DeltaTable.forPath(spark, settings.get('path'))
            return (dt.alias("t")
                .merge(df.alias('s'),f"t.{settings.get('key')} = s.{settings.get('key')}")
                .whenNotMatchedInsertAll()
                .execute())
                
        def tableExists(spark: SparkSession, df: DataFrame, settings: dict) -> None:
            if DeltaTable.isDeltaTable(spark, settings.get('path')):
                print(f"\033[96m Table Merge : {settings.get('path')}\033[0m")
                tableMerge(spark, df, settings)
            else:
                print(f"\033[96m New Table : {settings.get('path')}\033[0m")
                tableNew(df, settings)
        
        tableExists(spark, df, settings)
        tableHistory(spark, df, settings)
        tableVacuum(spark, df, settings)

    def createHiveTables(self, tupleDF:tuple):
        if isinstance(tupleDF,tuple) and len(tupleDF)==2:
            tupleDF[0].write.mode("ignore").format("hive").saveAsTable(f"default.{tupleDF[1]}")

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
        
        def dfFromCSV(filelist:list, filetype:str) -> DataFrame:
            if filetype == "csv":
                df = (spark.read.format("csv")
                        .option("header","true")
                        .option("mode","DROPMALFORMED")
                        .load(filelist))
                return df

        def dfFromJSON(filelist:list, filetype:str) -> DataFrame:
            if filetype == "json":
                df = (spark.read.format("json")
                        .option("mode","PERMISSIVE")
                        .option("primitiveAsString","true")
                        .load(filelist))
                return df

        def loopFunctions(filelist: list, filetype: str) -> DataFrame:
            if isinstance(spark, SparkSession) and isinstance(filelist,list) and len(filelist)>0:
                functionList = [dfFromCSV, dfFromJSON]
                result = [f(filelist, filetype) for f in functionList]
                filtered = [ele for ele in result if ele != None]
                # print(f"\033[98m{filtered}\033[0m")
                # print(f"\033[98m{filtered}\033[0m")
                return filtered[0] if len(filtered) > 0 else None

        # def makeDF(filelist: list, filetype: str) -> DataFrame:
        #     if filetype=="csv":
        #         return dfFromCSV(filelist)
        #     elif filetype=="json":
        #         return dfFromJSON(filelist)
        #     else:
        #         return None

        return loopFunctions(filelist, filetype)
        # return makeDF(filelist, filetype)

    def debugDF(self, df: DataFrame, filename: str) -> None:
        # print(f"DataFrame: \n {df} \n FileName: {filename}")        
        def createFilepath(dir:str, filename: str) -> str:
            d = f"{dir}/dataframes"
            return d,f"{d}/{filename}.json"

        def createContent(df:DataFrame):
            content = {}
            content['count'] = df.count()
            content['schema'] = json.loads(df.schema.json())
            return json.dumps(content, sort_keys=True, indent=4, default = str, ensure_ascii=True)
        
        paths = createFilepath(self.debug_dir, filename)
        content = createContent(df)
        SparkClass(self.config).debugCreateFile(paths=paths, content=content)


    def debugCreateFile(self, content: dict, paths: tuple) -> None:
        # print(f"DataFrame: \n {df} \n FileName: {filename}")        
        def makeDirectory(directory:str) -> None:
            if not os.path.exists(directory):
                return os.makedirs(directory)

        def removeFile(filepath: str) -> None:
            if os.path.exists(filepath):
                os.remove(filepath)
        
        def createFile(filepath:str, content) -> None:
            with open(filepath,"a") as f:
                f.write(f"{content}\n")
            f.close()
        
        directory = paths[0]
        filepath = paths[1]

        makeDirectory(directory)
        removeFile(filepath)
        createFile(filepath, content)

    def debugTables(self, table) -> None:
        # print(f"DataFrame: \n {df} \n FileName: {filename}")        
        def createFilepath(dir:str, filename: str) -> str:
            d = f"{dir}/tables"
            return d,f"{d}/{filename}.json"

        def createContent(table):
            content = {}
            content['table'] = table._asdict()
            content['dir.table'] = dir(table)
            return json.dumps(content, sort_keys=True, indent=4, default = str, ensure_ascii=True)
        
        paths = createFilepath(self.debug_dir, table.name)
        content = createContent(table)
        SparkClass(self.config).debugCreateFile(paths=paths, content=content)

