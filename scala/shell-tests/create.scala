// 创建DataFrame
val studentDF = Seq(
  (1, "Liu Bei"),
  (2, "Guan Yu"),
  (3, "Zhang Fei")
).toDF("id", "name")

studentDF.show()         // 打印全部记录
studentDF.printSchema()  // 打印schema

// 通过createDataFrame来创建data frame，可以自己设置schema
val student_data = Seq(
  (1, "Liu Bei"),
  (2, "Guan Yu"),
  (3, "Zhang Fei")
)
val schema = StructType(List(
  StructField("id", IntegerType, true),
  StructField("name", StringType, true),
))
val studentDF = spark.createDataFrame(
  sc.parallelize(schema),
  schema
)

// DataFrame就是Dataset[Row]

case class Student(id:Long, name:String)
val studentDS = studentDF.as[Student]
studentDS.show()         // 打印全部记录
studentDS.printSchema()  // 打印schema
