package com.qinglianyun.spark.sparkml.basicstatistics

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 19:13 2018/12/17
  * @ 假设检验
  * 一般的问题是使用样本去推断总体
  * 但是又不能单纯的根据样本统计量去估计总体参数
  * 因为可能会出现一些随机性导致出现随机测量误差，所以要进行假设检验
  * 基本思想就是先对所需要比较的总体提出一个无差别的假设，然后通过样本数据去推断是否拒绝这一假设
  *
  * 帮助理解：https://www.douban.com/group/topic/116857671/
  */
object HypothesisTesting {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val data = Seq(
      (0.0, Vectors.dense(0.5, 10.0)),
      (0.0, Vectors.dense(1.5, 20.0)),
      (1.0, Vectors.dense(1.5, 30.0)),
      (0.0, Vectors.dense(3.5, 30.0)),
      (0.0, Vectors.dense(3.5, 40.0)),
      (1.0, Vectors.dense(3.5, 40.0))
    )

    import spark.implicits._

    val dataDF: DataFrame = data.toDF("label", "features")

    val row: Row = ChiSquareTest.test(dataDF, "features", "label").head()

    println(row)

    // P < 0.05为显著； P < 0.01为非常显著
    // 一般来说，假设检验主要看P值，当P值很大，表示两组数据的差别无明显意义
    println(s"pValues = ${row.getAs[Vector](0)}")

    // 自由度
    println(s"degreesOfFreedom ${row.getSeq[Int](1).mkString("[", ",", "]")}")

    // 检验统计量
    // 检验统计量的值越大，拒绝原假设的理由越充分
    println(s"statistics ${row.getAs[Vector](2)}")

    val df: DataFrame = ChiSquareTest.test(dataDF, "features", "label")
    println("=======================")
    df.show()

    spark.close()
  }

}
