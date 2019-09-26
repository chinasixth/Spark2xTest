package com.qinglianyun.spark.sparkml.basicstatistics

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 19:13 2018/12/17
  * @ 假设检验，主要是使用卡方假设检验
  * 一般的问题是使用样本去推断总体
  * 但是又不能单纯的根据样本统计量去估计总体参数
  * 因为可能会出现一些随机性导致出现随机测量误差，所以要进行假设检验
  * 基本思想就是先对所需要比较的总体提出一个无差别的假设，然后通过样本数据去推断是否拒绝这一假设
  *
  * 帮助理解：https://www.douban.com/group/topic/116857671/
  *
  * 须知：中心极限定理  正态分布
  *
  * 假设检验：
  * 零假设  备选假设
  * 在零假设成立的前提下，样本计算出的概率值：p值
  * 决策标准，零假设成立的前提下，计算出的概率小于决策标准，零假设成立：显著水平
  *
  * 流程：
  * 1.问题是什么？零假设和备选假设
  * 2.证据是什么？零假设成立时，得到样本平均值的概率
  * 3.判断标准是什么？计算出的概率到底处于什么范围内
  * 4.做出结论
  *
  * 卡方检测的目的：样本实际观测值和理论值的偏离程度，卡方越大，偏离程度越大
  * 若四格表资料四个格子的频数分别为a，b，c，d，则四格表资料卡方检验的卡方值=n(ad-bc)^2/(a+b)(c+d)(a+c)(b+d)，（或者使用拟合度公式）
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
    // 自由度v=（行数-1）（列数-1）=1
    println(s"degreesOfFreedom ${row.getSeq[Int](1).mkString("[", ",", "]")}")

    // 检验统计量
    // 检验统计量的值越大，拒绝原假设的理由越充分
    println(s"statistics ${row.getAs[Vector](2)}")

    val df: DataFrame = ChiSquareTest.test(dataDF, "features", "label")
    println("=======================")
    df.show(false)

    val testData = Seq(
      (0.0, Vectors.dense(0.5)),
      (2.0, Vectors.dense(0.5)),
      (1.0, Vectors.dense(2.5)),
      (0.0, Vectors.dense(0.5)),
      (0.0, Vectors.dense(0.5)),
      (1.0, Vectors.dense(2.5))
    )

    val testDF: DataFrame = testData.toDF("col_1", "col_2")

    val resultDF: DataFrame = ChiSquareTest.test(testDF, "col_2","col_1")

    resultDF.show(false)

    spark.close()
  }

}
