package com.iflytek.scala.spark.join

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions.{col, from_json, lit,expr}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{SparkSession, functions}
import org.slf4j.LoggerFactory

/**
  * 注意
  * Stream-Stream Join是有状态的，通过状态，将无限的数据流的Join，拆分成有限的数据集Join。
  *
  * Stream-Stream Inner Join，水印和时间约束是可选的。未指定水印和时间约束时，记录将无限期存储在状态中; 在两侧设置水印和时间约束时，会相应地启用状态清除。
  *
  * Stream-Stream Left Join，水印和时间约束是必选的，即必须在右侧流上指定水印和时间约束。左侧流也可以指定水印和时间约束。
  *
  * Stream-Stream Right Join，水印和时间约束是必选的，即必须在左侧流上指定水印和时间约束。右侧流也可以指定水印和时间约束。
  *
  * Stream-Stream Full Join，不支持。
  *
  * 水印(Watermark)和时间约束(Time Constraint)的作用:
  *
  * A. 水印决定了数据可以延迟多久，以及数据何时会被删除，如:水印设置为30分钟，则超过30分钟的记录将被删除或忽略。
  *
  * B. 时间约束决定了与其相关的流的状态将保留多久的记录。
  *
  *
  * 从Spark 2.3开始，开始支持Stream-Stream Join，
  * 即Stream DataSet/DataFrame Join Stream DataSet/DataFrame。
  *
  */

/**
  * Summary:
  *   Stream-Stream Inner Join
  */
object StreamStreamJoin {

  lazy val logger = LoggerFactory.getLogger(StreamStreamJoin.getClass)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[3]").appName(this.getClass.getSimpleName.replace("$", "")).getOrCreate()
    import spark.implicits._

    // 注册UDF
    spark.udf.register("timezoneToTimestamp", timezoneToTimestamp _)

    //动态数据-浏览流
    val browseSchema = """{"type":"struct","fields":[{"name":"browse_time","type":"string","nullable":true},{"name":"browse_user","type":"string","nullable":true}]}"""
    val browseStreamDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "kafkademo1")
      .load()
      .select(from_json(col("value").cast("string"), DataType.fromJson(browseSchema)).as("value"))
      .select($"value.*")
      .withColumn("browse_timestamp", functions.callUDF("timezoneToTimestamp", functions.col("browse_time"),lit("yyyy-MM-dd HH:mm:ss"),lit("GMT+8")))
      .filter($"browse_timestamp".isNotNull && $"browse_user".isNotNull)
      .withWatermark("browse_timestamp", "10 seconds")

    //动态数据-点击流
    val clickSchema = """{"type":"struct","fields":[{"name":"click_time","type":"string","nullable":true},{"name":"click_user","type":"string","nullable":true}]}"""
    val clickStreamDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "kafkademo2")
      .load()
      .select(from_json(col("value").cast("string"), DataType.fromJson(clickSchema)).as("value"))
      .select($"value.*")
      .withColumn("click_timestamp", functions.callUDF("timezoneToTimestamp", functions.col("click_time"),lit("yyyy-MM-dd HH:mm:ss"),lit("GMT+8")))
      .filter($"click_timestamp".isNotNull && $"click_user".isNotNull)
      .withWatermark("click_timestamp", "30 seconds")

    // Inner Join
//    val joinedDF = browseStreamDF.join(
//      clickStreamDF,
//      expr("""
//        browse_user = click_user AND
//        click_timestamp >= browse_timestamp AND
//        click_timestamp <= browse_timestamp + interval 20 seconds
//        """)
//    )

//    Outer Join
//    val joinedDF = browseStreamDF.join(
//      clickStreamDF,
//      expr("""
//        browse_user = click_user AND
//        click_timestamp >= browse_timestamp AND
//        click_timestamp <= browse_timestamp + interval 20 seconds
//        """),
//      "left")

    browseStreamDF
      .writeStream
      .outputMode("append")
      .format("console")
      .start()
//
//    clickStreamDF
//      .writeStream
//      .outputMode("append")
//      .format("console")
//      .start()
//
//    joinedDF
//      .writeStream
//      .outputMode("append")
//      .trigger(Trigger.ProcessingTime("2 seconds"))
//      .format("console")
//      .start()

    spark.streams.awaitAnyTermination()

  }

  /**
    * 带时区的时间转换为Timestamp
    *
    * @param dateTime
    * @param dataTimeFormat
    * @param dataTimeZone
    * @return
    */
  def timezoneToTimestamp(dateTime: String, dataTimeFormat: String, dataTimeZone: String): Timestamp = {
    var output: Timestamp = null
    try {
      if (dateTime != null) {
        val format = DateTimeFormatter.ofPattern(dataTimeFormat)
        val eventTime = LocalDateTime.parse(dateTime, format).atZone(ZoneId.of(dataTimeZone));
        output = new Timestamp(eventTime.toInstant.toEpochMilli)
      }
    } catch {
      case ex: Exception => logger.error("时间转换异常..." + dateTime, ex)
    }
    output
  }
}
