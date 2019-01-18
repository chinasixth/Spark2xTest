package com.qinglianyun.spark.sparkml.featuretransformers

import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 16:37 2019/1/17
  * @ desc: ElementwiseProduct使用元素级乘法，将每个输入向量与提供的"权重向量"相乘。
  * 即：通过标量乘法器缩放数据集的每一列。它表示输入向量v和变换向量w之间的哈达玛德乘积，从而得到一个结果向量
  *
  * 输入向量要和权重向量的特征数一样，否则报错。
  * 计算时就简单的将相同特征位置上的数乘一下就可以了。
  */
object ElementwiseProductTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val dataset: DataFrame = spark.createDataFrame(Seq(
      ("a", Vectors.dense(1.0, 2.0, 3.0)),
      ("b", Vectors.dense(4.0, 5.0, 6.0))
    )).toDF("id", "vector")
    // +---+-------------+
    //|id |features     |
    //+---+-------------+
    //|a  |[1.0,2.0,3.0]|
    //|b  |[4.0,5.0,6.0]|
    //+---+-------------+

    val transformingVec: linalg.Vector = Vectors.dense(0.0, 1.0, 2.0)

    val elementwiseProduct: ElementwiseProduct = new ElementwiseProduct()
      .setInputCol("vector")
      .setOutputCol("transformedVec")
      .setScalingVec(transformingVec)

    val transformedElementwiseProduct: DataFrame = elementwiseProduct.transform(dataset)

    transformedElementwiseProduct.show(false)
    // +---+-------------+--------------+
    // |id |vector       |transformedVec|
    // +---+-------------+--------------+
    // |a  |[1.0,2.0,3.0]|[0.0,2.0,6.0] |
    // |b  |[4.0,5.0,6.0]|[0.0,5.0,12.0]|
    // +---+-------------+--------------+



    spark.close()
  }

}
