from __future__ import print_function

import sys
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import \
    StructType, StructField, LongType, StringType, DateType
from datetime import datetime, date
import tempfile

# this demo shows how to create a parquet with partition
# also read https://spark.apache.org/docs/latest/sql-data-sources-parquet.html
def main(spark):
    sc = spark.sparkContext
    rdd = sc.textFile("file:///tmp/foo/data.gz")
    print(rdd.collect())
    

def run():    
    spark = SparkSession\
        .builder\
        .appName("Greet")\
        .getOrCreate()
    try:
        main(spark)
    finally:
        spark.stop()
