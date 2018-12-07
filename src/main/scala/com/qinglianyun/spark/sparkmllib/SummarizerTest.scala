package com.qinglianyun.spark.sparkmllib

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 17:40 2018/12/7
  * @ desc   ：我们通过摘要器为Dataframe提供向量列摘要统计信息，
  * 可用的度量标准包括列的最大值、最小值、平均值、方差和非零的数量，以及总数
  */
object SummarizerTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.ml.stat.Summarizer._

    val data = Seq(
      (Vectors.dense(2.0, 3.0, 5.0), 1.0),
      (Vectors.dense(4.0, 6.0, 7.0), 2.0)
    )

    val df: DataFrame = data.toDF("features", "weight")

    val (meanVal, varianceVal) = df.select(metrics("mean", "variance")
      .summary($"features", $"weight").as("summary"))
      .select("summary.mean", "summary.variance")
      .as[(Vector, Vector)].first()

    println(s"with weight: mean = ${meanVal}, variance = ${varianceVal}")

    val (meanVal2, varianceVal2) = df.select(mean($"features"), variance($"features"))
      .as[(Vector, Vector)].first()

    println(s"without weight: mean = ${meanVal2}, sum = ${varianceVal2}")

    spark.close()

  }
}
