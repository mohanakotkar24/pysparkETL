#!/usr/bin/python3
from fileinput import filename
import logging
import json, os, re, sys
from time import asctime
from typing import Callable, Optional
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
# from pyspark.sql.types import *
from pyspark.sql import DataFrame

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_FILE = f"{project_dir}/logs/job-{os.path.basename(__file__)}.log"
LOG_FORMAT = f"%(asctime)s - LINE:%(lineno)d - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format=LOG_FORMAT)
logger = logging.getLogger('py4j')

sys.path.insert(0, project_dir)
from classes import class_pyspark

def main(project_dir) -> None:
    """ Starts Spark Job"""
    conf = openConfig(f"{project_dir}/json/sales.json")
    spark = sparkStart(conf) 
    
    # Import Data
    transactionsDF = importData(spark, f"{project_dir}/test-data/sales/transactions", format=".json")
    customersDF = importData(spark, f"{project_dir}/test-data/sales/customers.csv")
    productsDF = importData(spark, f"{project_dir}/test-data/sales/products.csv")
    
    # Transform Data
    start = datetime.datetime.now()
    transformDF(spark, transactionsDF, customersDF, productsDF, f"{project_dir}/test-delta/sales")
    print(f"Time taken : {datetime.datetime.now() - start}")
    
    # Stopping Spark Session
    sparkStop(spark)   

def openConfig(filepath:str) -> dict:
    if isinstance(filepath, str) and os.path.exists(filepath):
        return class_pyspark.SparkClass(config={}).openJson(filepath)

def sparkStart(conf: dict) -> SparkSession:
    if isinstance(conf, dict):
        spark = class_pyspark.SparkClass(config={}).sparkStart(conf)
        return spark

def sparkStop(spark:SparkSession) -> None:
    spark.stop() if isinstance(spark, SparkSession) else None

def importData(spark: SparkSession, datapath: str, format:Optional[str]=None) -> DataFrame:
    if isinstance(spark, SparkSession):
        return class_pyspark.SparkClass(config={}).importData(spark, datapath, format)

def showMySchema(df: DataFrame, filename: str) -> None:
    if isinstance(df, DataFrame):
        class_pyspark.SparkClass(config={}).debugDF(df, filename)

def transformDF(spark: SparkSession, transactionsDF: DataFrame, customersDF: DataFrame, productsDF: DataFrame, path:str) -> DataFrame:
    
    # create temp tables in default spark sql 
    createTempTables(spark,[(cleanTransactions(transactionsDF),"transactionDF"),
                        (cleanCustomer(customersDF),"customerDF"),
                        (cleanProducts(productsDF),"productDF")])
    
    # Create table in deltalake
    exportResult([(spark, cleanTransactions(transactionsDF), {"format":"delta","path":f"{path}/transactions", "key":"date_of_purchase"}),
                    (spark, cleanCustomer(customersDF),{"format":"delta","path":f"{path}/customers","key":"customer_id"}),
                    (spark, cleanProducts(productsDF),{"format":"delta","path":f"{path}/products","key":"product_id"})])

    # load tables    
    l = loadDeltaTables([(spark, f"{path}/transactions","delta"),
                        (spark,f"{path}/customers","delta"),
                        (spark,f"{path}/products","delta")])

    listOfDF = list(zip(l,["transactions","customers","products"]))
    print(f"\033[96m Table Details \n:{listOfDF}\033[0m")
    createTempTables(spark, listOfDF)
    df = spark.sql("SELECT * FROM transactions")
    df.show()
    # createHiveTables(spark,[(cleanTransactions(transactionsDF),"transactionDF"),
    #                     (cleanCustomer(customersDF),"customerDF"),
    #                     (cleanProducts(productsDF),"productDF")])
    # pdf = cleanProducts(productsDF)
    # showMySchema(productsDF, "productsDF")

def cleanTransactions(df: DataFrame) -> DataFrame:
    if isinstance(df, DataFrame):
        df1 = df.withColumn("basket_explode", explode(col("basket"))).drop("basket")
        # df2 = df1.select(col("customer_id"),col("date_of_purchase"),col("basket_explode.*"))
        df2 = (df1.select(["customer_id","date_of_purchase","basket_explode.*"])
                .withColumn("date", col("date_of_purchase").cast("Date"))
                .withColumn("price",col("price").cast("Integer")))
        showMySchema(df2,"transactions")
        return df2

def cleanCustomer(df: DataFrame) -> DataFrame:
    if isinstance(df, DataFrame):
        df1 = df.withColumn("loyalty_score", col("loyalty_score").cast("Integer"))  
        showMySchema(df1,"customers")
        return df1

def cleanProducts(df: DataFrame) -> DataFrame:
    if isinstance(df, DataFrame):
        # df1 = df.withColumn("loyalty_score", col("loyalty_score").cast("Integer"))  
        showMySchema(df,"products")
        return df

def createTempTables(spark: SparkSession, listOfDF: list)-> None:
    # class_pyspark.SparkClass(config={}).createTempTables(listOfDF)
    # temp = [(lambda x: class_pyspark.SparkClass(config={}).createTempTables(x))(x) for x in listOfDF]
    temp = [class_pyspark.SparkClass(config={}).createTempTables(x) for x in listOfDF]
    tb = [class_pyspark.SparkClass(config={}).debugTables(x) for x in spark.catalog.listTables()]
    # print(spark.catalog.listTables())

def createHiveTables(spark: SparkSession, listOfDF:list) -> None: 
    # class_pyspark.SparkClass(config={}).createHiveTables(listOfDF)
    temp = [class_pyspark.SparkClass(config={}).createHiveTables(x) for x in listOfDF]
    print(spark.catalog.listTables())

def exportResult(listOfDF:list) -> None: 
    # temp = [class_pyspark.SparkClass(config={"export":"/tmp/spark/delta1"}).exportDF(x) for x in listOfDF]
    temp = [class_pyspark.SparkClass(config={}).exportDF(x) for x in listOfDF]
    # print(spark.catalog.listTables())

def loadDeltaTables(listOfPaths:list) -> list: 
    # print(f"\033[96m New Table : {listOfPaths}\033[0m")
    temp = [class_pyspark.SparkClass(config={}).loadTables(x[0],x[1],x[2]) for x in listOfPaths]
    return temp

if __name__ == "__main__":
    main(project_dir)