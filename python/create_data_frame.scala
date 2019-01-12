val studentDF = Seq(
  (1, "Liu Bei"),
  (2, "Guan Yu"),
  (3, "Zhang Fei")
).toDF("id", "name")

studentDF.show()
// 打印schema
studentDF.printSchema()