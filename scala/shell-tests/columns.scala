// 列操作
val studentDF = Seq(
  (1, "Liu Bei"),
  (2, "Guan Yu"),
  (3, "Zhang Fei")
).toDF("id", "name")

// 选择一些列，并且可以改名
studentDF.select($"id".alias("id1"), $"name").show()

// 增加一个计算列
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField}

val schema = StructType(List(
  StructField("id2",  IntegerType),
  StructField("name", StringType)
))


studentDF.map(row => new GenericRowWithSchema((1L, "Hello"), schema)).show()



ret.show()

studentDF.show()         // 打印全部记录
studentDF.printSchema()  // 打印schema

// DataFrame就是Dataset[Row]
