package com.iflytek.scala.flink.join

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

object DatasetJoin {

  def main(args: Array[String]): Unit = {

    val env=ExecutionEnvironment.getExecutionEnvironment

    val data1=ListBuffer[(Int,String)]()
    data1.append((1,"ruoze"))
    data1.append((2,"jepson"))
    data1.append((3,"xingxing"))

    val data2=ListBuffer[(Int,String)]()
    data2.append((1,"beijing"))
    data2.append((2,"shanghai"))
    data2.append((4,"hangzhou"))

    val a=env.fromCollection(data1)
    val b=env.fromCollection(data2)

    // 内连接
    a.join(b)   // where 和 equalTo 方法必须要使用
      .where(0)  //左边的key，0表示第一列
      .equalTo(0)  //右边的key，0表示第一列
      .apply((a,b)=>{
        (a._1,a._2,b._2)
      })
      .print()

    println("---------------------------")

    //左连接
    a.leftOuterJoin(b)   // where 和 equalTo 方法必须要使用
      .where(0)  //左边的key，0表示第一列
      .equalTo(0)  //右边的key，0表示第一列
      .apply((a,b)=>{
        if (b == null){  //要判断如果右边没有匹配的，显示什么
          (a._1,a._2,"-")
        }else{
          (a._1,a._2,b._2)
        }
      })
      .print()

    println("---------------------------")
    //右连接
    a.rightOuterJoin(b)   // where 和 equalTo 方法必须要使用
      .where(0)  //左边的key，0表示第一列
      .equalTo(0)  //右边的key，0表示第一列
      .apply((a,b)=>{
        if (a == null){  //要判断如果左边没有匹配的，显示什么
          (b._1 ,"-",b._2)
        }else{
          (b._1,a._2,b._2)
        }
      })
      .print()

    println("---------------------------")
    //全外连接
    a.fullOuterJoin(b)
      .where(0)
      .equalTo(0)
      .apply((a, b) => {
        if(a == null){  //全连接 ，左边和右边 都要判读为null的情况
          (b._1, "-", b._2)
        }else if(b == null){
          (a._1, a._2, "-")
        }else{
          (b._1, a._2, b._2)
        }
      })
      .print()

    println("---------------------------")
    // 笛卡尔积
    val ds1=List("manlian","ashenna")
    val ds2=List("3","2","0")
    val left =env.fromCollection(ds1)
    val right=env.fromCollection(ds2)

    left.cross(right).print()

    println("---------------------------")

    // JoinHint 可选项：
    // OPTIMIZER_CHOOSES，由系统判断选择
    // BROADCAST_HASH_FIRST，第一个数据集构建哈希表并广播，由第二个表扫描。适用于第一个数据集较小的情况
    // BROADCAST_HASH_SECOND，适用于第二个数据集较小的情况
    // REPARTITION_HASH_FIRST，对两个数据同时进行分区，并从第一个输入构建哈希表。如果第一个输入小于第二个输入，则此策略很好。
    // REPARTITION_HASH_SECOND，适用于第二个输入小于第一个输入。
    // REPARTITION_SORT_MERGE，对两个数据同时进行分区，并对每个输入进行排序（除非数据已经分区或排序）。输入通过已排序输入的流合并来连接。如果已经对一个或两个输入进行过分区排序的情况，则此策略很好。
    // REPARTITION_HASH_FIRST 是系统使用的默认回退策略，如果不能进行大小估计，

    // 表示第二个数据集 input2 特别小
//    val result1 = input1.joinWithTiny(input2).where(0).equalTo(0)
    // 表示第二个数据集 input2 特别大
//    val result1 = input1.joinWithHuge(input2).where(0).equalTo(0)
    a.join(b, JoinHint.REPARTITION_HASH_FIRST)   // where 和 equalTo 方法必须要使用
      .where(0)  //左边的key，0表示第一列
      .equalTo(0){
      (a, b) => (a._1,a._2)
    } //JoinFunction
      .print()

  }

}
