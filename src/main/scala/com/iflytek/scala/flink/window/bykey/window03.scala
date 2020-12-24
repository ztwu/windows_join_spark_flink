package com.iflytek.scala.flink.window.bykey

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object window03 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val source = env.socketTextStream("localhost",9000)
    //word map
    val values = source.flatMap(value => value.split("\\s+")).map(value => (value,1))
    val keyValue = values.keyBy(0)

//    到达指定数量的单词才进行统计
    val countWindow = keyValue.countWindow(5)

    countWindow.sum(1).name("count window").print()

    env.execute()
  }

}
