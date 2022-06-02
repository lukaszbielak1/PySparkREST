#!/usr/bin/env python3

from pyspark import SparkConf
from tenderApi.tender import Tender
from tenderApi.dbhelper import DBhelper
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType


def main():
    spark = SparkSession\
            .builder\
            .master('local[*]')\
            .config('spark.jars', '/Users//spark/jars/mssql-jdbc-10.2.1.jre8.jar')\
            .config('spark.executor.extraClassPath', '/Users//spark/jars/mssql-jdbc-10.2.1.jre8.jar')\
            .appName("PySpark")\
            .getOrCreate()

    

    t = Tender(spark)
    df = t.transform_data(max_pages = 30)
    print(df.count())
    #writer = DBhelper("","",".database.windows.net","")
    #writer.save_dataframe(df,"tender")
    

if __name__ == '__main__':    
    main()