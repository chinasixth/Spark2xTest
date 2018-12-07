package com.qinglianyun.spark.sparkmllib

import org.apache.spark.ml.linalg.{Vectors, Vector}
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 17:00 2018/12/7
  * @ desc   ：假设检验在统计学中是一种强大的工具，用来确定一个结果是否具有统计学意义，这个结果是否偶然发生
  *            spark.ml目前支持皮尔逊卡方进行独立检测
  *            ChiSquareTest针对标签上的每个特性进行Pearson的独立性测试。
  *            对于每个特征，(特征、标签)对被转换为列联矩阵，为列联矩阵计算卡方统计量。所有标签和特征值必须是分类的。
  */
object HypothesisTest {
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
    val df: DataFrame = data.toDF("label", "features")
    df.select("features").show()
    val chi: Row = ChiSquareTest.test(df, "features", "label").head()
    println(chi)
    println(s"pValues = ${chi.getAs[Vector](0)}")
    println(s"degreesOfFreedom = ${chi.getSeq[Int](1).mkString("[",",","]")}")
    println(s"statistics ${chi.getAs[Vector](2)}")
  }

}
