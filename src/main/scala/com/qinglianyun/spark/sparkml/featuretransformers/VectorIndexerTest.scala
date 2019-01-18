package com.qinglianyun.spark.sparkml.featuretransformers

import org.apache.spark.ml.feature.{VectorIndexer, VectorIndexerModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 16:13 2019/1/14
  * @ desc: VectorIndexer帮助索引向量数据集中的分类特征。它可以自动决定哪些特征是类别的，并将原始值转换为类别索引
  * VectorIndexer可以对向量数据集中的分类特征（列）建立索引。它可以自动决定哪些特征（列）是用来分类的，并将原始的数据转换成类别索引
  *
  * 具体来说，它执行以下操作
  * 1.获取输入类型为Vector的输入列和参数maxCategories
  * 2.基于不同值的数量决定哪些特征（列）应该用作分类，设置的maxCategories数目的特征用于分类
  * 3.为每个分类特征计算基于0的分类索引
  * 4.将原始特征值转换成索引
  *
  * 分类特征索引允许决策树和树集合等算法适当地处理分类特征，从而提高性能。
  *
  * 在下面的例子中，我们读取一个标记点的数据集，然后使用VectorIndexer来决定哪些特性应该被归类。
  * 我们将分类特征值转换为它们的索引。
  * 然后，这些转换后的数据可以传递给处理分类特征的DecisionTreeRegressor等算法。
  *
  * 主要作用：提高决策树或随机森林等ML方法的分类效果。
  * VectorIndexer是对数据集特征向量中的类别（离散值）特征（index categorical features categorical features ）进行编号。
  * 它能够自动判断那些特征是离散值型的特征，并对他们进行编号，
  * 具体做法是通过设置一个maxCategories，特征向量中某一个特征不重复取值个数小于maxCategories，则被重新编号为0～K（K<=maxCategories-1）。
  * 某一个特征不重复取值个数大于maxCategories，则该特征视为连续值，不会重新编号（不会发生任何改变）。
  *
  * sample_libsvm_data.txt数据说明：
  * label 1:value 2:value ......
  * label是标签的意思，1可以理解为向量中的第一维（列），对应的value是第一维的数值
  *
  * 通俗解释：
  * 列:值
  * 查看数据所有列的值，当某列的值的种类数（也就是count(distinct(值))）小于maxCategories，那么这一列就要作为特征列。
  *
  * 数据转换：
  * 输入的是向量，输出的是也是向量
  *
  * 参考：https://blog.csdn.net/shenxiaoming77/article/details/63715525
  */
object VectorIndexerTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    // 指定format("libsvm")，获取的数据有两列，默认列名为label、features
    // label就是标签
    // features是使用稀疏矩阵的形式表示的
    val data: DataFrame = spark.read.format("libsvm").load("src/main/data/sample_libsvm_data.txt")

    data.show(false)

    val vectorIndexer: VectorIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(10)

    val vectorIndexerModel: VectorIndexerModel = vectorIndexer.fit(data)

    val categoricalFeatures: Set[Int] = vectorIndexerModel.categoryMaps.keys.toSet
    // categoricalFeatures.size：有多少列被选择成为特征
    //
    println(s"Chose ${categoricalFeatures.size}  " +
      s"categorical features: ${categoricalFeatures.mkString(",")}")

    val vectorIndexed: DataFrame = vectorIndexerModel.transform(data)

    vectorIndexed.show(false)


    val dataFrame: DataFrame = spark.createDataFrame(
      Seq(
        Vectors.sparse(5, Array(1, 3, 4), Array(1.0, 3.0, 5.0)),
        Vectors.dense(2, 4, 6, 8, 10),
        Vectors.dense(1, 2, 5, 4, 10)
      ).map(Tuple1.apply)
    ).toDF("features")

    dataFrame.show(false)

    val vectorIndexer1 = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexed")
        .setMaxCategories(2)

    val vectorIndexerModel1 = vectorIndexer1.fit(dataFrame)

    val transformed = vectorIndexerModel1.transform(dataFrame)

    transformed.show(false)

    spark.close()
  }

}
