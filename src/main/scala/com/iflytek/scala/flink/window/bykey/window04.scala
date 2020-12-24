package com.iflytek.scala.flink.window.bykey

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

object window04 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.socketTextStream("localhost",9000)
    //word map
    val values = source.flatMap(value => value.split("\\s+")).map(value => (value,1))
    val keyValue = values.keyBy(0)

//    一段时间内没有接收到数据就会生成新的窗口
    val countWindow = keyValue.window(EventTimeSessionWindows.withGap(Time.minutes(10)))

    countWindow.sum(1).name("count window").print()

    env.execute()
  }

}
