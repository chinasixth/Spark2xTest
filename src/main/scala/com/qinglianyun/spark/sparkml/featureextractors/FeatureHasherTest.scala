package com.qinglianyun.spark.sparkml.featureextractors

import org.apache.spark.ml.feature.FeatureHasher
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 11:55 2019/1/8
  * @ desc:
  * 文本特征提取，将文本数据转化成特征向量的过程
  * 比较常用的文本特征表示法为词袋法。
  *
  *
  *
  *
  **/
object FeatureHasherTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val dataset: DataFrame = spark.createDataFrame(Seq(
      (2.2, true, "1", "foo"),
      (3.3, false, "2", "bar"),
      (4.4, false, "3", "baz"),
      (5.5, false, "4", "foo")
    )).toDF("real", "bool", "stringNum", "string")

    val hasher: FeatureHasher = new FeatureHasher()
      .setInputCols("real", "bool", "stringNum", "string")
      .setOutputCol("features")
      .setNumFeatures(20) // default: 2^18。建议使用2的幂

    val featurized: DataFrame = hasher.transform(dataset)

    // 是否截断长字符串。如果为真，将截断超过20个字符的字符串，并对所有单元格进行右对齐
    // 简单说就是，整张表都给你显示出来。
    featurized.show(false)

    // +----+-----+---------+------+--------------------------------------------------------+
    // |real|bool |stringNum|string|features                                                |
    // +----+-----+---------+------+--------------------------------------------------------+
    // |2.2 |true |1        |foo   |(262144,[174475,247670,257907,262126],[2.2,1.0,1.0,1.0])|
    // |3.3 |false|2        |bar   |(262144,[70644,89673,173866,174475],[1.0,1.0,1.0,3.3])  |
    // |4.4 |false|3        |baz   |(262144,[22406,70644,174475,187923],[1.0,1.0,4.4,1.0])  |
    // |5.5 |false|4        |foo   |(262144,[70644,101499,174475,257907],[1.0,1.0,5.5,1.0]) |
    // +----+-----+---------+------+--------------------------------------------------------+

    spark.stop()
  }

}
