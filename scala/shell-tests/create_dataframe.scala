val studentDF = Seq(
  (1, "Liu Bei"),
  (2, "Guan Yu"),
  (3, "Zhang Fei")
).toDF("id", "name")

studentDF.show()         // 打印全部记录
studentDF.printSchema()  // 打印schema

// DataFrame就是Dataset[Row]
