package com.qinglianyun.spark.sparkmllib

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.{Matrices, Matrix, Vectors}

import org.apache.spark.mllib.regression
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 19:38 2018/12/7
  * @ desc   ：主要测试Spark MLlib中的一些数据类型，以及对某些数据类型中数据结构的说明。
  *
  *
  * MLLib提供了一系列基本数据类型以支持底层的机器学习算法。
  * 主要的数据类型包括：本地向量、标注点（Labeled Point）、本地矩阵、分布式矩阵等。
  * 单机模式存储的本地向量与矩阵，以及给予一个或多个RDD的分布式矩阵。其中本地向量与矩阵为公共接口提供简单数据模型，底层的线性代数操作有Breeze库和jblas库提供。
  * 标注点类型用来表示监督学习（Supervised Learning）中的一个训练样本
  *
  * 本地向量（Local Vector）
  * 本地向量存储在单机上，其拥有整形、从0开始的索引值以及浮点型的元素值。
  * MLLib提供了两种类型的本地向量，稠密向量DenseVector和稀疏向量SpareVector。
  * 稠密向量使用一个双精度浮点型数据来表示其中的每一维元素，而稀疏向量则是给予一个整形树荫数组和一个双精度浮点型的值数组（这是spark2.3之前可能）；最新的是使用类似Tuple2，第一个是整型索引，第二个值是双精度的值
  * MLlib提供的工具类是：Vectors（工厂模式创建实例类0）
  *
  * 标注点（Labeled Point）
  * 标注点时一种带有标签的本地向量，它可以是稠密或者是稀疏的。在MLlib中，标注点在监督学习算法中被使用。由于标签是用双精度浮点型来存储的，故标注点类型在回归（Regression）和分类（Classification）问题上均可以使用。
  * 例如，对于二分类问题来说，正样本的标签为1，负样本的标签为0；而对于大多数分类问题俩说，标签则应该是一个以0开始的索引序列：0，1，2...
  * 在实际的机器学习问题中，稀疏向量数据是非常常见的，MLlib提供了读取LIBSVM格式数据的支持，该格式被广泛应用于LIBSVM、LIBLINEAR等机器学习库。
  * 在该格式下，每一个带标注的样本点由一下格式表示：
  * label index1:value1 index2:value2 index2:value3...
  * 需要注意的是:index是从“1”开始递增的。MLlib提供了相应的工具类。
  *
  * 本地矩阵（Local Matrix）
  * 本地矩阵具有整型的行索引值、列索引值和双精度的浮点型的元素值，它存储在单机上。
  * MLlib支持稠密矩阵（DenseMatrix）和稀疏矩阵（SparseMatrix）两种本地矩阵。
  * 稠密矩阵将数据存储在一个列优先的双精度型数组中；稀疏矩阵则将非零元素以列优先的csc模式进行存储。
  * MLlib提供的工具类是：Matrices
  *
  * 分布式矩阵（Distributed Matrix）
  * 分布式矩阵由长整形的行列索引值和双精度浮点型的元素值组成。它可以分布式的存储在一个或多个RDD上。
  * MLlib提供了三种分布式矩阵的存储方案：行矩阵（RowMatrix），索引行矩阵（IndexedRowMatrix），坐标矩阵（CoordinateMatrix）和分块矩阵

  *
  *
  * 说明：本次测试的是Spark-2.4.0的最新特性，是基于Spark-2.1.0的资料自行探索；最新的Spark MLlib使用的包是ml，导包时请注意。
  * 本地类型，存储于单台机器，如在分布式系统中使用，注意可能会出现的问题。
  */
object BaseDataType {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

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
    val examples: RDD[regression.LabeledPoint] = MLUtils.loadLibSVMFile(sc, "src/main/data/libsvm.txt")
    println(examples.collect.toBuffer)
    // ArrayBuffer((0.0,(8,[0,1,2,3,4,5,6,7],[1.1,2.2,3.3,4.4,5.5,6.6,7.7,8.8])), (1.0,(8,[0,1,2,3,4,5,6,7],[1.1,2.2,3.3,4.4,5.5,6.6,7.7,8.8])), (2.0,(8,[0,1,2,3,4,5,6,7],[1.1,2.2,3.3,4.4,5.5,6.6,7.7,8.8])))


    // #################################################################################################################
    // 创建一个三行两列的稠密矩阵（DenseMatrix）[ [1.0,2.0], [3.0,4.0], [5.0,6.0]]
    // 注意：这里的数组参数是列先序的
    val denseMatrix: Matrix = Matrices.dense(3, 2, Array(1, 3, 5, 2, 4, 6))
    println("三行两列的稠密矩阵：\n" + denseMatrix.toString())
    // 1.0  2.0
    // 3.0  4.0
    // 5.0  6.0

    // 创建一个三行两列的稀疏矩阵（SparseMatrix）[[9.0,0.0], [0.0,8.0], [0.0,6.0]]
    // 参数据说明（CSC模式）：
    // 前两个参数表示行数和列数
    // 第一个Array：表示列指针，即每一列元素开始的索引值
    // 第二个Array：表示行索引，即每一个个元素应该属于哪一行
    // 第三个Array：表示具体元素
    // 上方描述可能不是很清楚，现做以下说明和解释
    // 针对稀疏矩阵中的每一个元素，都可以使用三元组（colIndex，rowIndex，value）的形式表示，如9.0可表示为：（0，0，9）
    // 将所有的元素都这么表示，显然是可以用来存储一个稀疏矩阵的。但是，还可以做进一步压缩
    // 首先将行索引和列索引分离，将非零元素从0开始按序排列。然而，我们没有必要存储每一个元素的列索引；
    // 多个元素可能在同一列中，这是非常显示且常见的，那么我们只需要记录，在同一列中所有元素的第一个元素的列索引即可。
    // 然后根据行索引，就可以确定这些元素的具体位置。
    // 所以，第一个Array中的元素，就是存储的就是，将非零元素排序后，每一列第一个元素的索引；第二个Array存储的就是每一个元素的行索引；第三个Array存储的就是每一个具体的元素值。
    val sparseMatrix: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))
    println("三行两列的稀疏矩阵：\n" + sparseMatrix)
    // 创建一个六行六列的稀疏矩阵
    val ssMatrix: Matrix = Matrices.sparse(6, 6, Array(0, 1, 3, 6, 9, 11, 14), Array(0, 0, 1, 0, 1, 2, 0, 2, 3, 2, 3, 2, 4, 5), Array(11, 12, 22, 13, 23, 33, 14, 34, 44, 35, 45, 35, 56, 66))
    println("六行六列的稀疏矩阵：\n" + ssMatrix)


    // #################################################################################################################

    sc.stop()
    spark.close()
  }
}
