from __future__ import print_function

import sys
from pyspark.sql import SparkSession

class Cord(object):
    def __init__(self, x, y):
        self.x = x
        self.y = y
    
    # def __getstate__(self):
    #     return {}
    
    # def __setstate__(self, state):
    #     self.x = 0
    #     self.y = 0
    
def main(spark):
    sc = spark.sparkContext
    rdd = sc.parallelize([
        Cord(1, 2),
        Cord(3, 4)
    ])

    # rdd = rdd.filter(lambda t: t["x"] > 1)
    # rdd.map(lambda t: t.x)
    print("===============")
    for cord in rdd.collect():
        print(cord.x, cord.y)
    print("===============")

    print('Count =', rdd.count())


def run():    
    spark = SparkSession\
        .builder\
        .appName("Greet")\
        .getOrCreate()
    try:
        main(spark)
    finally:
        spark.stop()
