package com.qinglianyun.spark.sparkml.featuretransformers

import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 11:08 2019/1/9
  * @ desc: 多项式扩展，将n维原始特征组合扩展到多项式空间的过程
  *
  * (x, y) degree(2)  (x, x*x, y, x*y, y*y)
  * (x, y) degree(3)  (x, x*x, x*x*x, y, x*y, x*x*y, y*y, x*y*y, y*y*y)
  *
  * 使用场景：
  * 当训练的模型欠拟合，且拿不到新的数据，只能进行扩维，即增加数据的维度
  *
  * 参考资料：https://blog.csdn.net/qq_28404829/article/details/82906059
  *         https://www.jianshu.com/p/bad47166a4c0
  */
object PolynomialExpansionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val data = Array(
      Vectors.dense(2.0, 1.0),
      Vectors.dense(0.0, 0.0),
      Vectors.dense(3.0, -1.0)
    )

    val dataset: DataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val polynomialExpansion: PolynomialExpansion = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")
      .setDegree(3)

    val polynomialExpansioned: DataFrame = polynomialExpansion.transform(dataset)

    polynomialExpansioned.show(false)
    // +----------+------------------------------------------+
    // |features  |polyFeatures                              |
    // +----------+------------------------------------------+
    // |[2.0,1.0] |[2.0,4.0,8.0,1.0,2.0,4.0,1.0,2.0,1.0]     |
    // |[0.0,0.0] |[0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]     |
    // |[3.0,-1.0]|[3.0,9.0,27.0,-1.0,-3.0,-9.0,1.0,3.0,-1.0]|
    // +----------+------------------------------------------+

    spark.close()
  }

}
