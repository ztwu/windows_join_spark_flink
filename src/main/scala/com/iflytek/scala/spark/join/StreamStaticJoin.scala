package com.iflytek.scala.spark.join

import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
  * Stream-Static对Join的支持如下:
  * Left	Right	Inner	Left Outer	Right Outer	Full Outer
  * Stream	Static	Yes	Yes	No	No
  * Static	Stream	Yes	No	Yes	No
  *
  * Stream-Static Join是无状态的，流中的记录会与整个静态数据集进行匹配。
  *
  * 举个例子: Kafka Join 静态数据Mysql，会将查询下推到Mysql中，
  * 当Mysql的数据变化(如某列值发生改变)，此时可获取到新的值。
  * 同理Join CSV文件，每个微批Join，都会重新读取文件，然后通过Broadcast进行Join,
  * 因此，当文件内容变化时，也可获取到最新的值。
  */

/**
  * Summary:
  *   Stream-Static Join
  */
object StreamStaticJoin {

  lazy val logger = LoggerFactory.getLogger(StreamStaticJoin.getClass)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[3]").appName(this.getClass.getSimpleName.replace("$", "")).getOrCreate()
    import spark.implicits._

    //静态数据
    val staticDF: DataFrame = spark
      .read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://192.168.56.101:3306/test?characterEncoding=utf8&useSSL=false")
      .option("dbtable", "t_user_info")
      .option("user", "root")
      .option("password", "root")
      .load()

    //动态数据
    val kafkaJsonSchema= new StructType()
      .add("eventTime", StringType)
      .add("eventType", StringType)
      .add("userID", StringType)

    val streamDF: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "kafkademo2")
      .load()
//      .select(col("value").cast("string"))
      .select(from_json(col("value").cast("string"), kafkaJsonSchema).as("value"))
      .select($"value.*")

    // Stream-Static Join
    // Left Join
//     val joinedDF = streamDF.join(staticDF,streamDF("userID")===staticDF("userID"),"left")

    // Right Join 不支持
    // val joinedDF = streamDF.join(staticDF,streamDF("userID")===staticDF("userID"),"right")

    // Inner Join
    val joinedDF = streamDF.join(staticDF, streamDF("userID") === staticDF("userID"))

    val query = joinedDF
//    val query = streamDF
      .writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .format("console")
      .start()

    query.awaitTermination()

  }
}