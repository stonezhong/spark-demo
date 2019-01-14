val studentDF = Seq(
  (1, "Liu Bei", 80, "文官"),
  (2, "Guan Yu", 90, "武将"),
  (3, "Zhang Fei", 75, "武将")
).toDF("id", "name", "score", "type")

// 选择最低和最高分
val result = studentDF.
    groupBy("type").
    agg(
        min("score").alias("min_score"),
        max("score").alias("max_score")
    ).
    show()

// 参考 https://spark.apache.org/docs/1.5.2/api/java/org/apache/spark/sql/GroupedData.html
// 自己计算平均分
val result = studentDF.
    groupBy("type").
    agg(
        sum("score").alias("total_score"),
        count("*").alias("count"),
        min("score").alias("min_score")
    ).
    map(
        row => {
            (
                row.getString(0),
                row.getLong(1).asInstanceOf[Double] / row.getLong(2).asInstanceOf[Double],
                row.getLong(2)
            )
        }
    ).
    withColumnRenamed("_1", "type").
    withColumnRenamed("_2", "average_score").
    withColumnRenamed("_3", "min_score")

result.printSchema()
result.show()
