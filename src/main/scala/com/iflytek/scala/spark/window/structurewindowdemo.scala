package com.iflytek.scala.spark.window

import java.sql.Timestamp
import org.apache.spark.sql.SparkSession

object structurewindowdemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("BasicOperation")
      .getOrCreate()
    import spark.implicits._

    import org.apache.spark.sql.functions._
    val line = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9000)
      .option("includeTimestamp", true)
      .load() //给产生的数据自动添加时间戳
      .as[(String, Timestamp)]
      .flatMap {
        case (words, ts) => words.split(" ").map((_, ts))
      }
      .toDF("word", "ts")
      .groupBy(
        window($"ts", "4 minutes", "4 minutes"),
        $"word").count()

    line.writeStream
      .format("console")
      .outputMode("complete")
//      不省略的显示数据
      .option("truncate", false)
      .start()
      .awaitTermination()
  }
}
