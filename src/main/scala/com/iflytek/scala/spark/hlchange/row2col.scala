package com.iflytek.scala.spark.hlchange

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions._

object row2col {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[3]").getOrCreate()
    import spark.implicits._

    val df2 = spark.createDataFrame(List(
      ("2018-01","项目1",100),
      ("2018-01","项目2",200),
      ("2018-01","项目3",300),
      ("2018-02","项目1",1000),
      ("2018-02","项目2",2000),
      ("2018-03","项目x",999)
    )).toDF("年月","项目","收入")

    df2.createOrReplaceTempView("temp")

    /** pivot 多行转多列*/
    val sql = "select `年月`," +
      "max(case when `项目` = '项目1' then `收入` else 0 end) as `项目1`," +
      "max(case when `项目` = '项目2' then `收入` else 0 end) as `项目2`," +
      "max(case when `项目` = '项目3' then `收入` else 0 end) as `项目`," +
      "max(case when `项目` = '项目x' then `收入` else 0 end) as `项目x`" +
      "from temp group by `年月`"
    val df = spark.sql(sql)
    df.show()
  }

}
