package com.qinglianyun.spark.sparkml.featuretransformers

import org.apache.spark.ml.feature.{MinMaxScaler, MinMaxScalerModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 10:12 2019/1/17
  * @ desc: MinMaxScaler用来转换Vector Row类型的数据集，将每个特征（每一列）重新缩放到特定的范围,通常是[0, 1]
  * 即：将向量数据按列归一化到[0, 1]之间
  * 它需要以下两个参数：
  * min: 默认是0.0。转换后的下界，被所有的特征共享
  * max: 默认是1.0。转换后的上届，被所有的特征共享
  *
  * MinMaxScaler计算数据集的摘要统计信息并生成一个MinMaxScalerModel。然后，模型可以单独转换么开一个特征，使其处于给定的范围内。
  *
  * 注意: 零值也有可能被转换成非零值，所以即使对于稀疏输入，转换器的输出也是DenseVector。
  */
object MinMaxScalerTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val dataset: DataFrame = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.1, -1.0)),
      (1, Vectors.dense(2.0, 1.1, 1.0)),
      (2, Vectors.dense(3.0, 10.1, 3.0))
    )).toDF("id", "features")
    // +---+--------------+
    // |id |features      |
    // +---+--------------+
    // |0  |[1.0,0.1,-1.0]|
    // |1  |[2.0,1.1,1.0] |
    // |2  |[3.0,10.1,3.0]|
    // +---+--------------+

    val minMaxScaler: MinMaxScaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    val minMaxScalerModel: MinMaxScalerModel = minMaxScaler.fit(dataset)

    val minMaxScaled: DataFrame = minMaxScalerModel.transform(dataset)

    minMaxScaled.show(false)
    // +---+--------------+--------------+
    // |id |features      |scaledFeatures|
    // +---+--------------+--------------+
    // |0  |[1.0,0.1,-1.0]|[0.0,0.0,0.0] |
    // |1  |[2.0,1.1,1.0] |[0.5,0.1,0.5] |
    // |2  |[3.0,10.1,3.0]|[1.0,1.0,1.0] |
    // +---+--------------+--------------+
    

    spark.close()
  }

}
