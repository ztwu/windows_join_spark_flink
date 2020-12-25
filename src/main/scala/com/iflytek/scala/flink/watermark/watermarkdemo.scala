package com.iflytek.scala.flink.watermark

import java.sql.Timestamp

import com.iflytek.scala.flink.join.e1
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.timestamps.{AscendingTimestampExtractor, BoundedOutOfOrdernessTimestampExtractor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

//window的设定无关数据本身，而是系统定义好了的。
//window是flink中划分数据一个基本单位，window的划分方式是固定的，默认会根据自然时间划分window，并且划分方式是前闭后开。
object watermarkdemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 构造订单数据
    val ordersData = new mutable.MutableList[(String, String, Timestamp)]
    ordersData.+=(("001", "iphone", new Timestamp(1608713090000L)))
    ordersData.+=(("002", "mac", new Timestamp(1608713090000L)))
    ordersData.+=(("001", "book", new Timestamp(1608713090000L)))
    ordersData.+=(("003", "cup", new Timestamp(1608713090000L)))

    val lateText = new OutputTag[e1]("late_data")
    val orders = env
      .fromCollection(ordersData)
      .map(a => e1(a._1,a._2,a._3))

      //    assignTimestamps(extractor: AscendingTimestampExtractor[T]): DataStream[T]
      //    此方法是数据流的快捷方式，其中已知元素时间戳在每个并行流中单调递增。在这种情况下，
      //    系统可以通过跟踪上升时间戳自动且完美地生成水印。
//      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[e1]() {
//        override def extractAscendingTimestamp(element:e1):Long = {
//          return element.dt.getTime
//        }
//      })


      //    assignTimestampsAndWatermarks(assigner: AssignerWithPeriodicWatermarks[T]): DataStream[T]
      //    基于给定的水印生成器生成水印，即使没有新元素到达也会定期检查给定水印生成器的新水印，
      //    以指定允许延迟时间
      // assign timestamp & watermarks periodically(定期生成水印)
//      BoundedOutOfOrdernessTimestampExtractor 基于事件最大时间戳
      //      new Watermark(currentMaxTimestamp - maxOutOfOrderness);
//      TimeLagWatermarkGenerator 基于系统时间
      //      new Watermark(System.currentTimeMillis() - maxTimeLag);
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[e1](Time.milliseconds(50)) {
        override def extractTimestamp(element: e1): Long = {
          element.dt.getTime
        }
      })

      //    assignTimestampsAndWatermarks(assigner: AssignerWithPeriodicWatermarks[T]): DataStream[T]
      //      此方法仅基于流元素创建水印，对于通过[[AssignerWithPunctuatedWatermarks＃extractTimestamp（Object，long）]]处理的每个元素，
      //调用[[AssignerWithPunctuatedWatermarks＃checkAndGetNextWatermark（）]]方法，
      // 如果返回的水印值大于以前的水印，会发出新的水印，
      //此方法可以完全控制水印的生成，但是要注意，每秒生成数百个水印会影响性能
//      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[e1]() {
//        // check extractTimestamp emitted watermark is non-null and large than previously
//        override def checkAndGetNextWatermark(lastElement: e1, extractedTimestamp: Long): Watermark = {
//          new Watermark(extractedTimestamp)
//        }
//        // generate next watermark
//        override def extractTimestamp(element: e1, previousElementTimestamp: Long): Long = {
//          val eventTime = element.dt.getTime
//          eventTime
//        }
//      })

      .keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .sideOutputLateData(lateText)
      .allowedLateness(Time.seconds(2)) // 允许延迟2s
      .apply(new WindowFunction[e1, String, String, TimeWindow]() {
        override def apply(key: String, window: TimeWindow,
                           input: Iterable[e1], out: Collector[String]): Unit = {
          println("key==",key)
          return out.collect("")
        }
      })

    orders.getSideOutput(lateText).map(x => {
      "延迟数据|name:" + x.name + "|datetime:" + x.dt
    })

    orders.print()

    env.execute()
  }
}
