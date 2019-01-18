package com.qinglianyun.spark.sparkml.featuretransformers

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 10:36 2019/1/18
  * @ desc: VectorAssembler是一个转换器，它将给定的多个列转换成单个的向量列。
  * 它有助于将原始特征和由不同转换器生成的特征组合成单个特征向量，从而训练logistic回归和决策树等ML模型
  * VectorAssembler接收的数据类型有：所有数字类型、布尔类型、向量类型。
  * 在每一行中，输入列的值 将按照指定顺序连接到一个向量中。
  *
  * 简单说就是将多列合成单个向量列
  */
object VectorAssemblerTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val dataset: DataFrame = spark.createDataFrame(Seq(
      (0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0),
      (1, 19, 0.0, Vectors.dense(1.0, 5.0, 1.0), 2.0)
    )).toDF("id", "hour", "mobile", "userFeatures", "clicked")
    // +---+----+------+--------------+-------+
    // |id |hour|mobile|userFeatures  |clicked|
    // +---+----+------+--------------+-------+
    // |0  |18  |1.0   |[0.0,10.0,0.5]|1.0    |
    // |1  |19  |0.0   |[1.0,5.0,1.0] |2.0    |
    // +---+----+------+--------------+-------+

    val vectorAssembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("id", "hour", "mobile", "userFeatures", "clicked"))
      .setOutputCol("assemblerVec")

    val assemblered: DataFrame = vectorAssembler.transform(dataset)

    assemblered.show(false)
    // +---+----+------+--------------+-------+-------------------------------+
    // |id |hour|mobile|userFeatures  |clicked|assemblerVec                   |
    // +---+----+------+--------------+-------+-------------------------------+
    // |0  |18  |1.0   |[0.0,10.0,0.5]|1.0    |[0.0,18.0,1.0,0.0,10.0,0.5,1.0]|
    // |1  |19  |0.0   |[1.0,5.0,1.0] |2.0    |[1.0,19.0,0.0,1.0,5.0,1.0,2.0] |
    // +---+----+------+--------------+-------+-------------------------------+

    spark.close()
  }

}
