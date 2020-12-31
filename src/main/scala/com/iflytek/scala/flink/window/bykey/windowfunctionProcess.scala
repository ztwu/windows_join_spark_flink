package com.iflytek.scala.flink.window.bykey

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//ProcessWindowFunction获取一个Iterable，该Iterable包含窗口的所有元素，
// 以及一个Context对象，该对象可以访问时间和状态信息。
// 以及使其比其他窗口函数更具灵活性。这是以性能和资源消耗为代价，
// 因为不能增量聚合元素，而是需要在内部对其进行缓冲，直到认为该窗口已准备好进行处理为止。
object windowfunction04 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val source = env.socketTextStream("localhost",9000)
    //word map
    val values = source.flatMap(value => value.split("\\s+")).map(value => (value,1))
    val keyValue = values.keyBy(x=>x._1)

//    ①窗口的大小
    //②在窗口中滑动的大小，但理论上讲滑动的大小不能超过窗口大小
    val slidingWindow = keyValue.window(
      SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5)))
//    val slidingWindow = keyValue.timeWindow(Time.seconds(15),Time.seconds(5))

    slidingWindow
      .process(new UserDefineProcessWindowFunction)
      .print()

    env.execute()

  }

}

class UserDefineProcessWindowFunction extends ProcessWindowFunction[(String,Int),String,String,TimeWindow]{
  override def process(key: String,
                       context: Context,
                       elements: Iterable[(String, Int)],
                       out: Collector[String]): Unit = {
    out.collect(s"${key},${elements.toList.map(_._2).sum}")
  }
}

class UserDefineProcessWindowFunction2 extends ProcessWindowFunction[(String,Int),String,String,TimeWindow]{
  var windowStateDescriptor:ValueStateDescriptor[Int]=_
  var globalStateDescriptor:ValueStateDescriptor[Int]=_

  override def open(parameters: Configuration): Unit = {
    windowStateDescriptor=new ValueStateDescriptor[Int]("window",createTypeInformation[Int])
    globalStateDescriptor=new ValueStateDescriptor[Int]("global",createTypeInformation[Int])
  }

  override def process(key: String,
                       context: Context,
                       elements: Iterable[(String, Int)],
                       out: Collector[String]): Unit = {
    val sum = elements.toList.map(_._2).sum

    val windowState = context.windowState.getState(windowStateDescriptor)
    val globalSate = context.globalState.getState(globalStateDescriptor)

    windowState.update(sum+windowState.value())
    globalSate.update(sum+globalSate.value())

    out.collect(s"${key},window:${windowState.value()}\tglobal:${globalSate.value()}")
  }
}
