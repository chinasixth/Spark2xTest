package com.qinglianyun.spark.sparkml.featuretransformers

import org.apache.spark.ml.feature._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 11:22 2019/1/14
  * @ desc: 单热编码将分类特征(表示为标签索引)映射到二进制向量，其中最多只有一个单值，表示所有特征值集合中存在特定的特征值。
  * 这种编码允许期望连续特征(如逻辑回归)的算法使用分类特征。对于字符串类型输入数据，通常首先使用StringIndexer对分类特性进行编码。
  *
  * OneHotEncoderEstimator可以转换多个列，为每个输入列返回一个热编码的输出向量列。
  * 通常使用VectorAssembler将这些向量合并为单个特征向量
  *
  * OneHotEncoderEstimator支持handleInvalid参数来选择在转换数据期间如何处理无效输入。
  * 可用的选项包括' keep '(任何无效的输入都被分配给一个额外的分类索引)和' error '(抛出一个错误)。
  */
object OneHotEncoderEstimatorTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val dataset: DataFrame = spark.createDataFrame(
      Seq(
        (0.0, 1.0),
        (1.0, 0.0),
        (2.0, 1.0),
        (0.0, 2.0),
        (0.0, 1.0),
        (2.0, 0.0)
      )
    ).toDF("categoryIndex1", "categoryIndex2")

    val estimator = new OneHotEncoderEstimator()
      .setInputCols(Array("categoryIndex1", "categoryIndex2"))
      .setOutputCols(Array("categoryVec1", "categoryVec2"))
    // .setHandleInvalid("keep") // error

    val oneHotEncoderModel: OneHotEncoderModel = estimator.fit(dataset)

    val oneHotEncodered: DataFrame = oneHotEncoderModel.transform(dataset)

    oneHotEncodered.show(false)
    // setHandlerInvalid("error")
    // +--------------+--------------+-------------+-------------+
    // |categoryIndex1|categoryIndex2|categoryVec1 |categoryVec2 |
    // +--------------+--------------+-------------+-------------+
    // |0.0           |1.0           |(2,[0],[1.0])|(2,[1],[1.0])|
    // |1.0           |0.0           |(2,[1],[1.0])|(2,[0],[1.0])|
    // |2.0           |1.0           |(2,[],[])    |(2,[1],[1.0])|
    // |0.0           |2.0           |(2,[0],[1.0])|(2,[],[])    |
    // |0.0           |1.0           |(2,[0],[1.0])|(2,[1],[1.0])|
    // |2.0           |0.0           |(2,[],[])    |(2,[0],[1.0])|
    // +--------------+--------------+-------------+-------------+
    // setHandlerInvalid("keep")
    // +--------------+--------------+-------------+-------------+
    // |categoryIndex1|categoryIndex2|categoryVec1 |categoryVec2 |
    // +--------------+--------------+-------------+-------------+
    // |0.0           |1.0           |(3,[0],[1.0])|(3,[1],[1.0])|
    // |1.0           |0.0           |(3,[1],[1.0])|(3,[0],[1.0])|
    // |2.0           |1.0           |(3,[2],[1.0])|(3,[1],[1.0])|
    // |0.0           |2.0           |(3,[0],[1.0])|(3,[2],[1.0])|
    // |0.0           |1.0           |(3,[0],[1.0])|(3,[1],[1.0])|
    // |2.0           |0.0           |(3,[2],[1.0])|(3,[0],[1.0])|
    // +--------------+--------------+-------------+-------------+


    // 对于字符串类型的输入
    val dataset1: DataFrame = spark.createDataFrame(
      Seq("beijing", "shanghai", "guangzhou", "shenzhen", "hangzhou").map(Tuple1.apply)
    ).toDF("city")

    val stringIndexer: StringIndexer = new StringIndexer()
      .setInputCol("city")
      .setOutputCol("cityIndex")
      .setStringOrderType("alphabetAsc")

    val stringIndexerModel: StringIndexerModel = stringIndexer.fit(dataset1)

    val stringIndexed: DataFrame = stringIndexerModel.transform(dataset1)

    val oneHotEstimator = new OneHotEncoderEstimator()
      .setInputCols(Array("cityIndex"))
      .setOutputCols(Array("oneHotCityIndex"))
      .setHandleInvalid("keep")

    val oneHotModel = oneHotEstimator.fit(stringIndexed)

    val cityTransformed = oneHotModel.transform(stringIndexed)

    cityTransformed.show(false)
    // 得到的是一个稀疏向量，即SparseVector 
    // +---------+---------+---------------+
    // |city     |cityIndex|oneHotCityIndex|
    // +---------+---------+---------------+
    // |beijing  |0.0      |(5,[0],[1.0])  |
    // |shanghai |3.0      |(5,[3],[1.0])  |
    // |guangzhou|1.0      |(5,[1],[1.0])  |
    // |shenzhen |4.0      |(5,[4],[1.0])  |
    // |hangzhou |2.0      |(5,[2],[1.0])  |
    // +---------+---------+---------------+

    spark.close()
  }

}
