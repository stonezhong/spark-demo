// 增加一个计算列

val studentDF = Seq(
  (1, "Liu Bei", 27),
  (2, "Guan Yu", 26),
  (3, "Zhang Fei", 25)
).toDF("id", "name", "age")

// 选择一些列，并且可以改名
studentDF.
    map(
        row => (
            row.getInt(0), 
            row.getString(1),
            row.getAs("age").asInstanceOf[Int] - 1,
            row.getAs("age").asInstanceOf[Int]
        )
    ).
    withColumnRenamed("_1", "id").
    withColumnRenamed("_2", "name").
    withColumnRenamed("_3", "age_last_year").
    withColumnRenamed("_4", "age").
    show()


