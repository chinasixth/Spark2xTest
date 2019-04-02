package com.qinglianyun.spark.sparkml.featureselectors

import org.apache.spark.ml.feature.{ChiSqSelector, ChiSqSelectorModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 17:11 2019/2/11
  * @ desc:
  * ChiSqSelector 就是使用了卡方独立性检验来决定哪些特征优秀应该被选择。
  * 也就是特征选择过程中，使用卡方检测，可以知道哪些特征可以被选择，一些价值小贡献小的特征不用被选择。
  *
  * ChiSqSelector表示卡方特征选择。它对具有分类特征的标记数据进行操作。ChiSqSelector使用独立卡方检验来决定选择哪些特性。它支持五种选择方法:numTopFeatures，percentile，fpr, fdr, fwe:
  * numTopFeatures根据卡方检验选择固定数量的顶部特征。这类似于生成具有最强大预测能力的特性。
  * 百分比与numTopFeatures类似，但选择所有特性的一部分，而不是一个固定的数字。
  * fpr选择所有p值低于阈值的特征，从而控制选择的假阳性率。
  * fdr使用benjamin - hochberg程序来选择错误发现率低于阈值的所有特征。
  * 我们选择所有p值低于阈值的特征。阈值按1/numFeatures进行缩放，从而控制选择的family-wise错误率。默认情况下，选择方法是numTopFeatures，顶部特性的默认数量设置为50。用户可以使用setSelectorType选择一个选择方法。
  *
  */
object ChiSqSelectorTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val dataset: DataFrame = spark.createDataFrame(Seq(
      (7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
      (8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
      (9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
    )).toDF("id", "features", "clicked")
    // 输入数据
    // +---+------------------+-------+
    // |id |features          |clicked|
    // +---+------------------+-------+
    // |7  |[0.0,0.0,18.0,1.0]|1.0    |
    // |8  |[0.0,1.0,12.0,0.0]|0.0    |
    // |9  |[1.0,0.0,15.0,0.1]|0.0    |
    // +---+------------------+-------+

    /*
    * 从特征中，选择一维对label col影响最大的.
    * */
    val chiSqSelector: ChiSqSelector = new ChiSqSelector()
      .setNumTopFeatures(1)
      .setFeaturesCol("features")
      .setLabelCol("clicked")
      .setOutputCol("selectedFeatures")

    val chiSqSelectorModel: ChiSqSelectorModel = chiSqSelector.fit(dataset)

    val chiSqSelectored: DataFrame = chiSqSelectorModel.transform(dataset)

    chiSqSelectored.show(false)

    spark.close()
  }

}
