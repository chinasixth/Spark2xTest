package com.qinglianyun.spark.sparkml.basicstatistics

import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 17:34 2018/12/17
  * @ correlation是计算两个序列之间的关联性
  * 可以计算多个序列两两之间的相关性
  * 目前spark支持的相关方法是：pearson和spearman
  *
  * 输入n个m维的向量，输出1个min(n,m)*min(n,m)维的向量，
  * 如果调用了head方法，则返回的是一个min(n,m)*min(n,m)的本地矩阵
  * 可以根据矩阵的下标，来寻找某两列之间的关联性结果
  *
  **/
object CorrelationTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val data: Seq[linalg.Vector] = Seq(
      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
      Vectors.dense(4.0, 5.0, 0.0, 3.0),
      Vectors.dense(6.0, 7.0, 0.0, 8.0),
      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
    )

    import spark.implicits._

    // 将数据序列变成DataFrame
    val dataFrame: DataFrame = data.map(Tuple1.apply).toDF("feature")

    // 调用Correlation类中的corr方法
    // 默认调用的pearson方法
    val pearson: Row = org.apache.spark.ml.stat.Correlation.corr(dataFrame, "feature").head()
    println("pearson: \n" + pearson + "\n##########################")

    // 通过参数指定spearman
    val spearman: Row = org.apache.spark.ml.stat.Correlation.corr(dataFrame, "feature", "spearman").head()
    println("spearman: \n" + spearman + "\n##########################")

    val tow = Seq(
      Vectors.dense(2, 30, 2),
      Vectors.dense(4.0, 5.0, 4),
      Vectors.dense(6.0, 7.0, 6),
      Vectors.dense(8, 90, 8)
    )

    val towDF: DataFrame = tow.map(Tuple1.apply).toDF("two_col")

    //    val Row(coeff1: Matrix) = org.apache.spark.ml.stat.Correlation.corr(towDF, "two_col").head()
    //    println(s"$coeff1")

    val resultDF: DataFrame = org.apache.spark.ml.stat.Correlation.corr(towDF, "two_col")
    resultDF.show(false)

    spark.close()
  }
}
