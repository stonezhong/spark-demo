val studentDF = Seq(
  (1, "Liu Bei", 80, "文官"),
  (2, "Guan Yu", 90, "武将"),
  (3, "Zhang Fei", 70, "武将")
).toDF("id", "name", "score", "type")

def reducer(summary: Row, current: Row) : Row = {
    return Row(
        current.getInt(0) + summary.getInt(0)
    )
}
// 获取总数，返回一个Row
val ret = studentDF.reduce(
    (summary, current) => {
        printf(
            "current = %d, summary = %d\n", 
            current.getInt(0), summary.getInt(0)
        )
        Row(current.getInt(0) + summary.getInt(0))
    }
)
printf("total = %d\n", ret.getInt(0))

// 获取总数，返回一个Row
val ret = studentDF.reduce(reducer _)
printf("total = %d\n", ret.getInt(0))
