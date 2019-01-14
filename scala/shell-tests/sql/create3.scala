// 创建Dataset

case class Student(id:Long, name:String)
val studentDF = Seq(
  (1, "Liu Bei"),
  (2, "Guan Yu"),
  (3, "Zhang Fei")
).toDF("id", "name")
val studentDS = studentDF.as[Student]
studentDS.show()
