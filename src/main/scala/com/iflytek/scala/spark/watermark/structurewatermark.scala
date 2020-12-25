package com.iflytek.scala.spark.watermark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

//window的设定无关数据本身，而是系统定义好了的。
//window是flink中划分数据一个基本单位，window的划分方式是固定的，默认会根据自然时间划分window，并且划分方式是前闭后开。

// Watermark = 最大EventTime-迟到阈值(delayThreshold)
// Watermark >= 窗口W: [2016-01-01 10:02:00, 2016-01-01 10:02:30)的结束时间
object structurewatermark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("BasicOperation")
      .getOrCreate()

    import spark.implicits._

    val streamingInputDF =
      spark
        .readStream // DataStreamReader
        .format("eventhubs") // DataStreamReader
        .load() // DataFrame

    // split lines by whitespaces and explode the array as rows of 'word'
    val df = streamingInputDF.select($"body".cast("string"))
      .withColumn("_tmp", split($"body", ";"))
      .select(
        $"_tmp".getItem(0).as("name"),
        $"_tmp".getItem(1).as("ptime")
      ).drop("_tmp")
      .withColumn("posttime", to_timestamp($"ptime", "yyyyMMdd HH:mm:ss"))
      .drop("ptime")
      .withWatermark("posttime", "15 minutes")
      .groupBy(
        window($"posttime", "5 minutes", "5 minutes"),
        $"name"
      )
      .count()
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

    df.awaitTermination()
  }

}
