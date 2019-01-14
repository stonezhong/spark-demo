// 创建DataFrame
val studentDF = Seq(
  (1, "Liu Bei", 80, "文官"),
  (2, "Guan Yu", 90, "武将"),
  (3, "Zhang Fei", 75, "武将")
).toDF("id", "name", "score", "type")

// 找出武将中高于80分的
// type是保留字，所以避让
studentDF.filter(
    row => {
        val xType = row.getAs("type").asInstanceOf[String]
        val xScore = row.getAs("score").asInstanceOf[Int]
        xType == "武将" && xScore >= 80
    }
).
show()

