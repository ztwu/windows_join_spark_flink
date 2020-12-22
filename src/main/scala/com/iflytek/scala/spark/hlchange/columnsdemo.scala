package com.iflytek.scala.spark.hlchange

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.functions._

object columnsdemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[3]").getOrCreate()
    import spark.implicits._

    /**DataFrame 数据格式:每个用户对每部电影的评分 userID 用户ID,movieID 电影ID,rating评分 */
    val df=spark.sparkContext.parallelize(Array(
      (15,399,2),
      (15,1401,5),
      (15,1608,4),
      (15,20,4),
      (18,100,3),
      (18,1401,3),
      (18,399,1)
    )).toDF("userID","movieID","rating")
    /** pivot 多行转多列*/
    val resultDF = df.groupBy($"userID").pivot("movieID").sum("rating").na.fill(-1)
    /**结果*/
    resultDF.show(false)

/*==============================================*/
    val arrayData = Seq(
      Row("张三",List("Java","Scala","C++")),
      Row("李四",List("Spark","Java","C++")),
      Row("王老五",List("C#","VB",""))
    )

    val arrayRDD = spark.sparkContext.parallelize(arrayData)

    // 创建DataFrame
    val arraySchema = new StructType()
      .add("name",StringType)
      .add("subjects",ArrayType(StringType))

    val arrayDF = spark.createDataFrame(arrayRDD, arraySchema)
    /** explode 多列转多行*/
    val arrayDFColumn = arrayDF.select($"name",explode($"subjects"))
    arrayDFColumn.show(false)

/*==============================================*/

    val df2 = spark.createDataFrame(List(
      ("2018-01","项目1",100),
      ("2018-01","项目2",200),
      ("2018-01","项目3",300),
      ("2018-02","项目1",1000),
      ("2018-02","项目2",2000),
      ("2018-03","项目x",999)
    )).toDF("年月","项目","收入")

    /** pivot 多行转多列*/
    val df_pivot = df2.groupBy("年月")
      .pivot("项目", List("项目1","项目2","项目3","项目x"))
      .agg(sum("收入")).na.fill(0)
    df_pivot.show()

    /** stack 多列转多行*/
    df_pivot.selectExpr("`年月`",
      "stack(4, '项目1', `项目1`,'项目2', `项目2`, '项目3', `项目3`, '项目x', `项目x`) as (`项目`,`收入`)")
      .filter("`收入` > 0 ")
      .orderBy("`年月`", "`项目`")
      .show()

/*==============================================*/
    /**多列转多行*/
    var columns = df_pivot.columns.toList.drop(1);
    var df_pivot_new = df_pivot
    for(column <- columns){
      //lit 增加常量（固定值）
      df_pivot_new = df_pivot_new.withColumn(s"$column",concat_ws(":", lit(s"$column") ,col(s"$column")))
    }
    df_pivot_new.withColumn("arrayvalues",
      concat_ws(",", col("项目1"),col("项目2"),
        col("项目3"),col("项目x")))
      .select($"年月",explode(split($"arrayvalues",",")))
      .withColumn("splitarrays",split($"col",":"))
      .withColumn("项目",$"splitarrays".getItem(0))
      .withColumn("收入",$"splitarrays".getItem(1))
      .filter($"收入">0)
      .orderBy($"年月",$"项目".desc)
      .show()

  }

}
