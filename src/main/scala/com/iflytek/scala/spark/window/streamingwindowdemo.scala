package com.iflytek.scala.spark.window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object streamingwindowdemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("TestCount")
    val ssc = new StreamingContext(conf,Seconds(1))
    ssc.checkpoint("datacheckpoint")

    val lines = ssc.socketTextStream("localhost",9999)
    val words = lines.flatMap (_.split(" "))

//    val windowCounts =  words.window(Seconds(30), Seconds(1))
//    windowCounts.print()

//    val windowCounts =  words.countByWindow(Seconds(3),Seconds(1))
//    windowCounts.print()

//    val windowCounts =  words.reduceByWindow(_+"-"+_,Seconds(3),Seconds(1))
//    windowCounts.print()

//    val pairs = words.map(x => (x, 1))
//    val windowCounts =  pairs.reduceByKeyAndWindow((a:Int,b:Int)=>(a+b),Seconds(3),Seconds(1))
//    windowCounts.print()

    val pairs = words.map(x => (x, 1))
    val windowCounts =  pairs.reduceByKeyAndWindow(
      (a:Int,b:Int)=>(a+b),
      (a:Int,b:Int)=>(a-b),
      Seconds(3),Seconds(1))
    windowCounts.print()

//    val windowCounts = words.countByValueAndWindow(Seconds(3),Seconds(1))
//    windowCounts.print()

    //通过start()启动消息采集和处理
    ssc.start()

    //启动完成后就不能再做其它操作
    //等待计算完成
    ssc.awaitTermination()
  }

}
