package com.qinglianyun.spark.sparkml.featuretransformers

import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 11:40 2019/1/17
  * @ desc: Bucketizer将一列连续的特征（也就是按列操作）转换为特征桶，特征桶由用户指定
  * 也就是说，将一列数，按照用户给定的范围划分成不同的类，比如淘宝购物者的年龄，将0-10划分一类，10-20划分一类。
  * 它接收一个参数：
  * splits: 用于将连续的特征映射到Bucket中指定的参数，n个桶有n+1个切片。
  * 每一个桶的大小是由切片x,y指定的，范围是[x, y)，最后一个桶包含y。
  * splits中的数值应该是严格递增的。负无穷和正无穷处的值应该被显示指定，以覆盖所有的双精度的值。否则，数值出现在splits指定的值之外，将会发生错误。
  *
  * 参考资料：https://blog.csdn.net/hadoop_spark_storm/article/details/53414094
  */
object BucketizerTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val dataset: DataFrame = spark.createDataFrame(
      Array(-999.9, -0.5, -0.3, 0.0, 0.2, 999.9).map(Tuple1.apply)
    ).toDF("features")
    // +--------+
    // |features|
    // +--------+
    // |-999.9  |
    // |-0.5    |
    // |-0.3    |
    // |0.0     |
    // |0.2     |
    // |999.9   |
    // +--------+

    // 定义一个splits
    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)

    val bucketizer: Bucketizer = new Bucketizer()
      .setInputCol("features")
      .setOutputCol("bucketedFeatures")
      .setSplits(splits)

    val bucketed: DataFrame = bucketizer.transform(dataset)

    bucketed.show(false)

    val splitsArray = Array(
      Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity),
      Array(Double.NegativeInfinity, -0.3, 0.0, 0.3, Double.PositiveInfinity)
    )

    val dataset2: DataFrame = spark.createDataFrame(
      Array(
        (-999.9, -999.9),
        (-0.5, -0.2),
        (-0.3, -0.1),
        (0.0, 0.0),
        (0.2, 0.4),
        (999.9, 999.9)
      )
    ).toDF("features1", "features2")

    // 如果有多列特征，那么每一列特征都要给定一个splits，用于分桶
    val bucketizer2: Bucketizer = new Bucketizer()
      .setInputCols(Array("features1", "features2"))
      .setOutputCols(Array("bucketedFeatures1", "bucketedFeatures2"))
      .setSplitsArray(splitsArray)

    val bucketed2: DataFrame = bucketizer2.transform(dataset2)

    bucketed2.show(false)
    // +---------+---------+-----------------+-----------------+
    // |features1|features2|bucketedFeatures1|bucketedFeatures2|
    // +---------+---------+-----------------+-----------------+
    // |-999.9   |-999.9   |0.0              |0.0              |
    // |-0.5     |-0.2     |1.0              |1.0              |
    // |-0.3     |-0.1     |1.0              |1.0              |
    // |0.0      |0.0      |2.0              |2.0              |
    // |0.2      |0.4      |2.0              |3.0              |
    // |999.9    |999.9    |3.0              |3.0              |
    // +---------+---------+-----------------+-----------------+

    spark.close()
  }

}
