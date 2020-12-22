package com.iflytek.scala.spark.join

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

case class Persons(id_person: Int, name: String, address: String)
case class Orders(id_order: Int, orderNum: Int, id_person: Int)

object DataframeJoin {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("rddjoin")
      .getOrCreate()

    val sqlContext = spark.sqlContext

    val personDataFrame = sqlContext.createDataFrame(List(Persons(1, "张三", "深圳"), Persons(2, "李四", "成都"), Persons(3, "王五", "厦门"), Persons(4, "朱六", "杭州")))
    val orderDataFrame = sqlContext.createDataFrame(List(Orders(1, 325, 2), Orders(2, 34, 3), Orders(3, 533, 1), Orders(4, 444, 1), Orders(5, 777, 11)))

    personDataFrame.join(orderDataFrame, personDataFrame("id_person") === orderDataFrame("id_person")).show()
    personDataFrame.join(orderDataFrame, personDataFrame("id_person") === orderDataFrame("id_person"), "inner").show()
    personDataFrame.join(orderDataFrame, personDataFrame("id_person") === orderDataFrame("id_person"), "left").show()
    personDataFrame.join(orderDataFrame, personDataFrame("id_person") === orderDataFrame("id_person"), "left_outer").show()
    personDataFrame.join(orderDataFrame, personDataFrame("id_person") === orderDataFrame("id_person"), "right").show()
    personDataFrame.join(orderDataFrame, personDataFrame("id_person") === orderDataFrame("id_person"), "right_outer").show()
    personDataFrame.join(orderDataFrame, personDataFrame("id_person") === orderDataFrame("id_person"), "full").show()
    personDataFrame.join(orderDataFrame, personDataFrame("id_person") === orderDataFrame("id_person"), "full_outer").show()
    personDataFrame.join(orderDataFrame, personDataFrame("id_person") === orderDataFrame("id_person"), "outer").show()
  }

}
