package com.iflytek.scala.flink.window.bykey

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object windowfunction01 {

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

    slidingWindow
      .reduce(new MyReduceFunction)
//      .reduce { (v1, v2) => (v1._1, v1._2+v2._2) }
        .print()

    env.execute()

  }

}

class MyReduceFunction extends ReduceFunction[(String,Int)] {
  override def reduce(value1: (String,Int), value2: (String,Int)): (String,Int) = {
    (value1._1, value1._2+value2._2)
  }
}
