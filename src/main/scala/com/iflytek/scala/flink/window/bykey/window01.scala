package com.iflytek.scala.flink.window.bykey

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object window01 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val source = env.socketTextStream("localhost",9000)
    //word map
    val values = source.flatMap(value => value.split("\\s+")).map(value => (value,1))
    val keyValue = values.keyBy(0)

//      Tumbling Window(滚动窗口,一个窗口参数)使用
    val tumblingWindow = keyValue.window(TumblingEventTimeWindows.of(Time.seconds(15)))
//    val tumblingWindow = keyValue.timeWindow(Time.seconds(15))

    val countStream = tumblingWindow.sum(1).name("TumblingWindow").print()

    env.execute()
  }

}
