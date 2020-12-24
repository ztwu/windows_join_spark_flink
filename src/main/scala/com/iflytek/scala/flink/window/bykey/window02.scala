package com.iflytek.scala.flink.window.bykey

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object window02 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val source = env.socketTextStream("localhost",9000)
    //word map
    val values = source.flatMap(value => value.split("\\s+")).map(value => (value,1))
    val keyValue = values.keyBy(0)

//    ①窗口的大小
    //②在窗口中滑动的大小，但理论上讲滑动的大小不能超过窗口大小
    val slidingWindow = keyValue.window(
      SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5)))
//    val slidingWindow = keyValue.timeWindow(Time.seconds(15),Time.seconds(5))

    slidingWindow.sum(1).name("slidingwindow").print()

    env.execute()

  }

}
