package com.iflytek.scala.flink.hlchange

import java.util.Date
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem

object rowTocol {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val data = Array(("joinroom","001",10L,20L,10L,10L),("waitformic","001",10L,20L,10L,10L))
    val dataset = env.fromCollection(data)
    val tts= dataset
      .map(m=>{
        val actype=m._1
        var joinroom_uv=0L
        var waitformic_uv=0L
        var controlmic_uv=0L
        var exitroom_uv=0L
        if(actype.equals("joinroom"))joinroom_uv=m._3
        if(actype.equals("waitformic"))waitformic_uv=m._3
        if(actype.equals("controlmic"))controlmic_uv=m._3
        if(actype.equals("exitroom"))exitroom_uv=m._3

        (m._2,joinroom_uv,waitformic_uv,controlmic_uv,exitroom_uv)

      })
      .groupBy(0)
      .reduce(new ReduceFunction[Tuple5[String,Long,Long,Long,Long]]() {

        def reduce(value1: Tuple5[String,Long,Long,Long,Long],
                   value2: Tuple5[String,Long,Long,Long,Long]): Tuple5[String,Long,Long,Long,Long] = {
          return new Tuple5[String,Long,Long,Long,Long](value1._1, value1._2 + value2._2,value1._3+value2._3,value1._4+value2._4,value1._5+value2._5)
        }

      })
      .map(m1=>{
        val time: Date = new Date()
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
        val date = dateFormat.format(time)

        var jsonStr = "ktvroomid:"+m1._1+"_"+date+"&" // json格式开始
        jsonStr += "{"+
          "\"room_id\":\""+m1._1+
          "\",\"joinroom_uv\":\""+m1._2+
          "\",\"waitformic_uv\":\""+m1._3+
          "\",\"controlmic_uv\":\""+m1._4+
          "\",\"exitroom_uv\":\""+m1._5+
          "\"}"
        jsonStr
      })
    tts.print()
    tts.writeAsText("test.txt",FileSystem.WriteMode.OVERWRITE)

    env.execute()
  }

}
