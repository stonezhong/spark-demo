# 我目前使用的Spark是2.4.0

# 在pyspark console中，预定义了两个变量
# sc   : SparkContext的实例
# spark: SparkSession的实例

# Python Spark SDK: https://spark.apache.org/docs/latest/api/python/index.html

# 从jsonl文件中读取rdd
# 每个json占用一行
# 参考sample_001.jsonl
df = spark.read.json("file:///tmp/sample_001.jsonl", multiLine=False)
# hdfs://localhost:9000/test/sample_01.jsonl
df = spark.read.json("hdfs://localhost:9000/test/sample_01.jsonl", multiLine=False)
df.show()
# 打印schema
df.printSchema()

# 访问row
# row['color'], row['location']['x'], ...

# df.schema: DataFrame的schema
# df.schema是一个StructType
# StructType初始化时，用一个StructField的array
# 例子: https://www.programcreek.com/python/example/104715/pyspark.sql.types.StructType
# https://spark.apache.org/docs/2.4.0/
# https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.types.StructType
# schema["foo"] ==> 访问字段foo 返回StructField
# schema[0]     ==> 访问字段 0
# StructField
# .name: 字段的名字
# https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.types.StructField
# 

from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
from pyspark.sql.types import LongType

def foo(x, y):
    return x + y

my_udf = udf(foo, LongType())
df.withColumn("sum", my_udf(df.x, df.y)).show()


df.filter(df.age > 28).show()
