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
    sql_context = SQLContext(sc)

    base_dir = tempfile.mkdtemp()
    print("base_dir = {}".format(base_dir))

    schema = StructType([
        StructField("id",      LongType(),   True),
        StructField("gender",  StringType(), True),
        StructField("_time",   DateType(),   True),
        StructField("name",    StringType(), True),
    ])

    Person = Row("id", "gender", "_time", "name")

    rdd = sc.parallelize([
        Person(1, "male",   date(2019, 1, 1), "James Bond"),
        Person(2, "female", date(2019, 1, 1), "Emma Smith"),
        Person(3, "male",   date(2019, 1, 2), "Anakin Skywalker"),
    ], 1)
    df = sql_context.createDataFrame(rdd, schema)
    df\
        .write\
        .mode('append')\
        .partitionBy(["gender", "_time"])\
        .parquet("file://{}/students.parquet".format(base_dir))

    print("==================")
    print("base_dir: {}".format(base_dir))
    print("==================")

    # Now let's load parquet
    df = sql_context.read.parquet("file://{}/students.parquet".format(base_dir))
    df.printSchema()
    df.show()
    df.filter(df._time == date(2019, 1, 1)).show()

def run():    
    spark = SparkSession\
        .builder\
        .appName("Greet")\
        .getOrCreate()
    try:
        main(spark)
    finally:
        spark.stop()
