from __future__ import print_function

import sys
from pyspark.sql import SparkSession

from calculator import Calculator

def main(spark):
    calculator = Calculator()
    print("==========================")
    print("{} + {} = {}".
        format(
            1, 
            1, 
            calculator.add(1, 1)
        )
    )
    print("Done")
    print("==========================")

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("Greet")\
        .getOrCreate()
    main(spark)

    spark.stop()
