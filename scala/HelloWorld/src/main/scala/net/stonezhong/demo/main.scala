package net.stonezhong.demo

import org.apache.spark.sql.SparkSession

import net.stonezhong.demo.models._

object HelloWorld {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .appName("My App")
            .getOrCreate()
        
        import spark.implicits._
        val studentDF = Seq(
            (1, "Liu Bei"),
            (2, "Guan Yu"),
            (3, "Zhang Fei")
        ).toDF("id", "name")

        studentDF.show()
        // 打印schema
        studentDF.printSchema()        
    }
}
