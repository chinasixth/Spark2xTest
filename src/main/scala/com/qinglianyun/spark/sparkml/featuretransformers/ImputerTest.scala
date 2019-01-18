package com.qinglianyun.spark.sparkml.featuretransformers

import org.apache.spark.ml.feature.{Imputer, ImputerModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 16:49 2019/1/18
  * @ desc: Imputer是使用缺失值所在列的平均值或中值来补充数据集中的缺失值。输入列应该是double或float类型。
  * 目前，Imputer不支持分类特征，可能会为包含分类特征的列创建不正确的值。
  * Imputer可以通过setMissingValue()来输入除NaN之外的自定义值；也就是可以指定某些值也会被补充。
  */
object ImputerTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val dataset: DataFrame = spark.createDataFrame(Seq(
      (1.0, Double.NaN),
      (2.0, Double.NaN),
      (Double.NaN, 3.0),
      (4.0, 4.0),
      (5.0, 5.0)
    )).toDF("a", "b")
    // +---+---+
    // |a  |b  |
    // +---+---+
    // |1.0|NaN|
    // |2.0|NaN|
    // |NaN|3.0|
    // |4.0|4.0|
    // |5.0|5.0|
    // +---+---+

    val imputer: Imputer = new Imputer()
      .setInputCols(Array("a", "b"))
      .setOutputCols(Array("out_a", "out_b"))
      /*
      * Imputer的策略。目前只支持“mean”和“median”。
      * 如果是“mean”，则使用该特性的平均值替换缺失的值。
      * 如果“median”，则使用特征的近似中值替换缺失值。
      * 默认值: mean
      * */
      .setStrategy("mean")
      .setMissingValue(1.0) // 这样做就不包括NaN了

    val imputerModel: ImputerModel = imputer.fit(dataset)

    val imputered: DataFrame = imputerModel.transform(dataset)

    imputered.show(false)
    // +---+---+-----+-----+
    // |a  |b  |out_a|out_b|
    // +---+---+-----+-----+
    // |1.0|NaN|1.0  |4.0  |
    // |2.0|NaN|2.0  |4.0  |
    // |NaN|3.0|3.0  |3.0  |
    // |4.0|4.0|4.0  |4.0  |
    // |5.0|5.0|5.0  |5.0  |
    // +---+---+-----+-----+
    // setMissingValue(1.0)，NaN将不会被补充
    // +---+---+------------------+-----+
    // |a  |b  |out_a             |out_b|
    // +---+---+------------------+-----+
    // |1.0|NaN|3.6666666666666665|NaN  |
    // |2.0|NaN|2.0               |NaN  |
    // |NaN|3.0|NaN               |3.0  |
    // |4.0|4.0|4.0               |4.0  |
    // |5.0|5.0|5.0               |5.0  |
    // +---+---+------------------+-----+

    spark.close()
  }

}
