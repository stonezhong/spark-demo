// 创建DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{
    StructType, StructField, StringType, IntegerType
}

// 通过createDataFrame来创建data frame，可以自己设置schema
val student_data = Seq(
  Row(1, "Liu Bei"),
  Row(2, "Guan Yu"),
  Row(3, "Zhang Fei")
)
val schema = StructType(List(
  StructField("id", IntegerType, true),
  StructField("name", StringType, true)
))

// 从RDD创建DataFrame
val studentDF = spark.createDataFrame(
  sc.parallelize(student_data),
  schema
)

studentDF.show()         // 打印全部记录
studentDF.printSchema()  // 打印schema
