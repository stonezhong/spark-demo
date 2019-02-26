from __future__ import print_function

import sys
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import StructType, StructField, LongType, StringType
import datetime
import tempfile

# this demo shows how to create a parquet with partition
# also read https://spark.apache.org/docs/latest/sql-data-sources-parquet.html
def epoh_time(year, month, day):
    epoch = datetime.datetime.utcfromtimestamp(0)
    dt = datetime.datetime(year = year, month=month, day = day)
    return int((dt - epoch).total_seconds())

def main(spark):
    sc = spark.sparkContext
    sql_context = SQLContext(sc)

    base_dir = tempfile.mkdtemp()
    print("base_dir = {}".format(base_dir))

    schema = StructType([
        StructField("id",     LongType(),   True),
        StructField("name",   StringType(), True),
    ])

    # write to partition 1
    _time=epoh_time(2019, 1, 1)
    rdd = sc.parallelize([
        Row(id=1, name="foo"), 
        Row(id=2, name="bar"),
    ], 1)
    df = sql_context.createDataFrame(rdd, schema)
    df.write.mode('append').parquet("file://{}/_time={}/students.parquet".format(base_dir, _time))

    # write to partition 2
    _time=epoh_time(2019, 1, 2)
    rdd = sc.parallelize([
        Row(id=1, name="tar"), 
    ], 1)
    df = sql_context.createDataFrame(rdd, schema)
    df.write.mode('append').parquet("file://{}/_time={}/students.parquet".format(base_dir, _time))

    print("==================")
    print("base_dir: {}".format(base_dir))
    print("==================")

    # Now let's load parquet
    df = sql_context.read.parquet("file://{}/".format(base_dir))
    df.filter(df._time == epoh_time(2019, 1, 1)).show()

def run():    
    spark = SparkSession\
        .builder\
        .appName("Greet")\
        .getOrCreate()
    try:
        main(spark)
    finally:
        spark.stop()
