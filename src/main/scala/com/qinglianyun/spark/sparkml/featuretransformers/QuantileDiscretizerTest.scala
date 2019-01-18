package com.qinglianyun.spark.sparkml.featuretransformers

import org.apache.spark.ml.feature.{Bucketizer, QuantileDiscretizer}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 14:55 2019/1/18
  * @ desc: QuantileDiscretizer获取具有连续特征的列，并输出具有分类特征的列。桶的数量由numbucket参数设置。
  * 如果输入的不同值太少，无法创建足够多的不同分位数，那么使用的桶的数量可能会小于这个值。
  * NaN值：在量子化拟合过程中，NaN值将从列表中移除。这将产生一个用于预测的Bucketizer 模型。
  * 。在转换过程中，Bucketizer在数据集中找到NaN值时会产生一个错误，但是用户也可以通过设置handleInvalid来选择是保留还是删除数据集中的NaN值。
  * 如果用户选择保留NaN值，则对其进行特殊处理并放入自己的bucket中，例如使用4个bucket，则将非NaN数据放入bucket中[0-3]，而将NaNs计数在一个特殊的bucket[4]中。
  *
  * 简单来说：作用和Bucketizer是一样的，将连续的特征进行分组，区别是，Bucketizer指定的是一个Array，这个直接指定桶数。
  * 且QuantileDiscrtizer训练出来的也是Bucketizer。
  *
  * 参考资料：cnblogs.com/Mustr/p/6060242.html
  */
object QuantileDiscretizerTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val dataset: DataFrame = spark.createDataFrame(Array(
      (0, 18.0),
      (1, 19.0),
      (2, 8.0),
      (3, 5.0),
      (4, 2.2)
    )).toDF("id", "hour")
    // +---+----+
    // |id |hour|
    // +---+----+
    // |0  |18.0|
    // |1  |19.0|
    // |2  |8.0 |
    // |3  |5.0 |
    // |4  |2.2 |
    // +---+----+

    val quantileDiscretizer: QuantileDiscretizer = new QuantileDiscretizer()
      .setInputCol("hour")
      .setOutputCol("result")
      .setNumBuckets(3) // default: 2。用于指定桶数

    val bucketizer: Bucketizer = quantileDiscretizer.fit(dataset)

    val bucketizered: DataFrame = bucketizer.transform(dataset)

    bucketizered.show(false)
    // +---+----+------+
    // |id |hour|result|
    // +---+----+------+
    // |0  |18.0|2.0   |
    // |1  |19.0|2.0   |
    // |2  |8.0 |1.0   |
    // |3  |5.0 |1.0   |
    // |4  |2.2 |0.0   |
    // +---+----+------+

    spark.close()
  }

}
