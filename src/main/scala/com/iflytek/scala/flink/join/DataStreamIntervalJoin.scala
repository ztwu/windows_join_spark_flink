package com.iflytek.scala.flink.join

import java.sql.Timestamp

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.{AscendingTimestampExtractor, BoundedOutOfOrdernessTimestampExtractor}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable

case class e1(id:String, name:String, dt:Timestamp)
case class e2(id:String, name:String, dt:Timestamp)

object DataStreamIntervalJoin {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 构造订单数据
    val ordersData = new mutable.MutableList[(String, String, Timestamp)]
    ordersData.+=(("001", "iphone", new Timestamp(1608713090000L)))
    ordersData.+=(("002", "mac", new Timestamp(1608713090000L)))
    ordersData.+=(("003", "book", new Timestamp(1608713090000L)))
    ordersData.+=(("004", "cup", new Timestamp(1608713090000L)))
    // 构造付款表
    val paymentData = new mutable.MutableList[(String, String, Timestamp)]
    paymentData.+=(("001", "alipay", new Timestamp(1608713090000L)))
    paymentData.+=(("002", "card", new Timestamp(1608713090000L)))
    paymentData.+=(("003", "card", new Timestamp(1608713090000L)))
    paymentData.+=(("004", "alipay", new Timestamp(1608713090000L)))
    val orders = env
      .fromCollection(ordersData)
      .map(a => e1(a._1,a._2,a._3))
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[e1]() {
        override def extractAscendingTimestamp(element:e1):Long = {
          return element.dt.getTime
        }
      })

    val ratesHistory = env
      .fromCollection(paymentData)
      .map(new MapFunction[(String,String,Timestamp),e2]{
        override def map(a: (String, String, Timestamp)): e2 = {
          println(a)
          e2(a._1,a._2,a._3)
        }
      })
//      .map(a => e2(a._1,a._2,a._3))

//    assignTimestampsAndWatermarks(assigner: AssignerWithPeriodicWatermarks[T]): DataStream[T]
//      此方法仅基于流元素创建水印，对于通过[[AssignerWithPunctuatedWatermarks＃extractTimestamp（Object，long）]]处理的每个元素，
      //调用[[AssignerWithPunctuatedWatermarks＃checkAndGetNextWatermark（）]]方法，如果返回的水印值大于以前的水印，会发出新的水印，
      //此方法可以完全控制水印的生成，但是要注意，每秒生成数百个水印会影响性能

//    assignTimestampsAndWatermarks(assigner: AssignerWithPeriodicWatermarks[T]): DataStream[T]
//    基于给定的水印生成器生成水印，即使没有新元素到达也会定期检查给定水印生成器的新水印，以指定允许延迟时间

//    assignTimestamps(extractor: TimestampExtractor[T]): DataStream[T]
//    此方法是数据流的快捷方式，其中已知元素时间戳在每个并行流中单调递增。在这种情况下，系统可以通过跟踪上升时间戳自动且完美地生成水印。
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[e2]() {
        def extractAscendingTimestamp(element:e2):Long = {
          return element.dt.getTime
        }
      })

    orders.keyBy(0)
      .intervalJoin(ratesHistory.keyBy(0))
      // 时间间隔,设定下界和上界
      // 下界: 10分钟前，上界: 当前EventTime时刻
      .between(Time.milliseconds(-1000), Time.milliseconds(0))
      // 不包含下界
      //.lowerBoundExclusive()
      // 不包含上界
      //.upperBoundExclusive()
      // 自定义ProcessJoinFunction 处理Join到的元素
      .process(new ProcessJoinFunction[e1, e2, String] {
        override def processElement(input1: e1,
                                    input2: e2,
                                    context: ProcessJoinFunction[e1, e2, String]#Context,
                                    out: Collector[String]): Unit = {

          println(e1)
          out.collect("input 1: " + input1.name + ", input 2: " + input2.name)

        }
      })
      .print();

    env.execute()
  }
}
class TimestampExtractor[T1, T2]
  extends BoundedOutOfOrdernessTimestampExtractor[(T1, T2, Timestamp)](Time.seconds(10)) {
  override def extractTimestamp(element: (T1, T2, Timestamp)): Long = {
    element._3.getTime
  }
}
