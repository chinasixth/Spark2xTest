package com.qinglianyun.spark.sparkml.featuretransformers

import org.apache.spark.ml.feature.{MaxAbsScaler, MaxAbsScalerModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 10:49 2019/1/17
  * @ desc: MaxAbsScaler转换Vector Row类型的数据集，通过除以每个特征的最大绝对值，
  * 将每个特征（每一列）重新缩放到[-1, 1]的范围内，它不会移动/居中数据，因此不会破坏任何稀疏性。
  *
  * 具体的计算方法就是：用每一个值除以当前特征的最大绝对值。
  * 也就是，除以每一列的绝对值的最大值
  *
  */
object MaxAbsScalerTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val dataset: DataFrame = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.1, -8.0)),
      (1, Vectors.dense(2.0, 1.0, -4.0)),
      (2, Vectors.dense(4.0, 10.0, 8.0))
    )).toDF("id", "features")
    // +---+--------------+
    //|id |features      |
    //+---+--------------+
    //|0  |[1.0,0.1,-8.0]|
    //|1  |[2.0,1.0,-4.0]|
    //|2  |[4.0,10.0,8.0]|
    //+---+--------------+

    val maxAbsScaler: MaxAbsScaler = new MaxAbsScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    val maxAbsScalerModel: MaxAbsScalerModel = maxAbsScaler.fit(dataset)

    val maxAbsScaled: DataFrame = maxAbsScalerModel.transform(dataset)

    maxAbsScaled.show(false)
    // +---+--------------+----------------+
    // |id |features      |scaledFeatures  |
    // +---+--------------+----------------+
    // |0  |[1.0,0.1,-8.0]|[0.25,0.01,-1.0]|
    // |1  |[2.0,1.0,-4.0]|[0.5,0.1,-0.5]  |
    // |2  |[4.0,10.0,8.0]|[1.0,1.0,1.0]   |
    // +---+--------------+----------------+

    spark.close()
  }

}
