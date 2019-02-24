from __future__ import print_function

import sys
from pyspark.sql import SparkSession

class Cord(object):
    def __init__(self, x, y):
        self.x = x
        self.y = y

def main(spark):
    sc = spark.sparkContext
    rdd = sc.parallelize([
        {"x": 1, "y": 2},
        {"x": 3, "y": 4},
    ])

    # rdd = rdd.filter(lambda t: t["x"] > 1)
    rdd.map(lambda t: t.x)
    print("===============")
    print(rdd.collect())
    print("===============")

    print('Count =', rdd.count())


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("Greet")\
        .getOrCreate()
    main(spark)

    spark.stop()
