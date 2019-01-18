package com.qinglianyun.spark.sparkml.featuretransformers

import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 19:03 2019/1/8
  * @ desc: 二值化就是将数值特征阙值化为二进制（0/1）特征的过程
  * 也就是根据阙值将数值型转换为二进制型，阙值可以设定，且处理的只能是数值型。
  * 大于阙值的特征值被二进制化为1.0；等于或小于阙值的值被二值化为0.0.
  * 支持Vector和Double类型的inputCol。
  *
  * 参考资料：
  * https://blog.csdn.net/u013019431/article/details/80009545
  */
object BinarizerTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val dataset: DataFrame = spark.createDataFrame(
      Array((0, 0.1), (1, 0.8), (2, 0.2))
    ).toDF("id", "sentence")

    val binarizer: Binarizer = new Binarizer()
      .setInputCol("sentence")
      .setOutputCol("binarizer")
      .setThreshold(0.5) // Default: 0.0

    val binarized: DataFrame = binarizer.transform(dataset)

    binarized.show(false)
    // +---+--------+---------+
    // |id |sentence|binarizer|
    // +---+--------+---------+
    // |0  |0.1     |0.0      |
    // |1  |0.8     |1.0      |
    // |2  |0.2     |0.0      |
    // +---+--------+---------+

    spark.close()
  }

}
