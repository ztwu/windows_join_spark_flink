package com.iflytek.scala.flink.window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object window01 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)
    val counts = text
      .flatMap {_.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map(x=>(x, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))  //定义一个5秒的翻滚窗口
      .sum(1)
    env.execute("3 Type of Double Stream Join")
  }

}
