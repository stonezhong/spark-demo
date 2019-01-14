// 增加一个计算列
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField}

val studentDF = Seq(
  (1, "Liu Bei"),
  (2, "Guan Yu"),
  (3, "Zhang Fei")
).toDF("id", "name")

// 选择一些列，并且可以改名
studentDF.select($"id".alias("id1"), $"name").show()

// 删除一些列
studentDF.drop("id").show()

// 重命名一个列
studentDF.drop("id").withColumnRenamed("name", "name1").show()
