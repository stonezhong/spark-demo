// 增加一个计算列

val peopleDF = Seq(
  (1, "刘备"),
  (2, "关羽"),
  (3, "张飞"),
  (4, "曹操"),
  (5, "孙权")
).toDF("id", "name")

val kingdomDF = Seq(
    (1, "魏", 4),
    (2, "蜀", 1),
    (3, "吴", 5)
).toDF("id", "name", "kingID")

kingdomDF.join(
    peopleDF.
        withColumnRenamed("id", "people#id").
        withColumnRenamed("name", "people#name"),
    $"kingID" === $"people#id"
).
select(
    $"id",
    $"name",
    $"people#name".alias("king_name")
).
show()
