package com.qinglianyun.spark.sparkml.featuretransformers

import org.apache.spark.ml.feature.{StandardScaler, StandardScalerModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 17:08 2019/1/15
  * @ desc: StandardScaler转换向量行的数据集，将每一个特征（注意是特征）归一化，使其具有单位标准差或零均值。
  * StandardScaler处理的对象是每一列，也就是每一维特征。
  * 主要有两个参数可以设置：
  * withStd: 默认为true，将数据标准化到单位标准差
  * withMean: 默认是false，是否变换为0均值，这种方法将产生一个稠密输出，所以不适用于稀疏输入。
  *
  * StandardScaler是一种可以适用于数据集以生成StandardScalerModel的估计量;这相当于计算汇总统计信息。
  * 然后，该模型可以转换数据集中的向量列，使其具有单位标准差和/或零均值特征。
  *
  * 注意如果特征的标准差为零，则该特征在向量中返回的默认值为0.0。
  *
  * 参考资料：https://www.cnblogs.com/nucdy/p/7994542.html
  *
  * 与Normalizer的区别，Normalizer是对每一行向量进行归一化，规划后的范围是[-1, 1]
  * StandardScaler是对每一列进行归一化，没有指定范围。
  *
  */
object StandardScalerTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val dataset: DataFrame = spark.read
      .format("libsvm")
      .load("src/main/data/sample_libsvm_data.txt")

    val standardScaler: StandardScaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    val standardScalerModel: StandardScalerModel = standardScaler.fit(dataset)

    val result: DataFrame = standardScalerModel.transform(dataset)

    result.show(false)

    spark.close()
  }

}
