package com.qinglianyun.spark.sparkmllib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.regression
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 19:38 2018/12/7
  * @ MLLib提供了一系列基本数据类型以支持底层的机器学习算法。
  * 主要的数据类型包括：本地向量、标注点（Labeled Point）、本地矩阵、分布式矩阵等。
  * 单机模式存储的本地向量与矩阵，以及给予一个或多个RDD的分布式矩阵。其中本地向量与矩阵为公共接口提供简单数据模型，底层的线性代数操作有Breeze库和jblas库提供。
  * 标注点类型用来表示监督学习（Supervised Learning）中的一个训练样本
  *
  * 本地向量（Local Vector）
  * 本地向量存储在单机上，其拥有整形、从0开始的索引值以及浮点型的元素值。
  * MLLib提供了两种类型的本地向量，稠密向量DenseVector和稀疏向量SpareVector。
  * 稠密向量使用一个双精度浮点型数据来表示其中的每一维元素，而稀疏向量则是给予一个整形树荫数组和一个双精度浮点型的值数组（这是spark2.3之前可能）；最新的是使用类似Tuple2，第一个是整型索引，第二个值是双精度的值
  *
  * 标注点（Labeled Point）
  * 标注点时一种带有标签的本地向量，它可以是稠密或者是稀疏的。在MLlib中，标注点在监督学习算法中被使用。由于标签是用双精度浮点型来存储的，故标注点类型在回归（Regression）和分类（Classification）问题上均可以使用。
  * 例如，对于二分类问题来说，正样本的标签为1，负样本的标签为0；而对于大多数分类问题俩说，标签则应该是一个以0开始的索引序列：0，1，2...
  * 在实际的机器学习问题中，稀疏向量数据是非常常见的，MLlib提供了读取LIBSVM格式数据的支持，该格式被广泛应用于LIBSVM、LIBLINEAR等机器学习库。
  * 在该格式下，每一个带标注的样本点由一下格式表示：
  *   label index1:value1 index2:value2 index2:value3...
  * 需要注意的是:index是从“1”开始递增的。MLlib提供了相应的工具类。
  *
  * 说明：本次测试的是Spark-2.4.0的最新特性，是基于Spark-2.1.0的资料自行探索；最新的Spark MLlib使用的包是ml，导包时请注意。
  */
object BaseDataType {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    // 定义稠密向量
    val dense: linalg.Vector = Vectors.dense(2.0, 0.0, 8.0)
    println(s"稠密向量： ${dense}")
    // 定义稀疏向量
    // 方法一：
    val sparse1: linalg.Vector = Vectors.sparse(3, Seq((0, 2.0), (2, 8.0)))
    println(s"稀疏向量： $sparse1")
    // 方法二：
    val sparse2: linalg.Vector = Vectors.sparse(3, Array(0, 2), Array(2.0, 8.0))
    println(s"稀疏向量： $sparse2")

    // #################################################################################################################


    // 创建一个标签为1.0的稠密向量标注点（分类中可视为正样本）
    val denseLabel = LabeledPoint(1.0, Vectors.dense(2.0, 0.0, 8.0))
    println(s"标签为1.0的稠密向量：$denseLabel")

    // 创建一个标签为0.0的稀疏向量标注点（分类中可视为负样本）
    val sparseLabel = LabeledPoint(0.0, Vectors.sparse(3, Seq((0, 2.0), (2, 8.0))))
    println(s"标签为0.0的稀疏向量：$sparseLabel")

    // #################################################################################################################
    val examples: RDD[regression.LabeledPoint] = MLUtils.loadLibSVMFile(spark.sparkContext, "src/main/data/libsvm.txt")
    println(examples.collect.toBuffer)
    // ArrayBuffer((0.0,(8,[0,1,2,3,4,5,6,7],[1.1,2.2,3.3,4.4,5.5,6.6,7.7,8.8])), (1.0,(8,[0,1,2,3,4,5,6,7],[1.1,2.2,3.3,4.4,5.5,6.6,7.7,8.8])), (2.0,(8,[0,1,2,3,4,5,6,7],[1.1,2.2,3.3,4.4,5.5,6.6,7.7,8.8])))

    spark.close()
  }
}
