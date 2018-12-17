package com.qinglianyun.spark.sparkmllib

import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg.{Matrix, Vectors}
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 14:42 2018/12/10
  * @ desc   ：分布式矩阵的测试
  *
  * （一）行矩阵（RowMatrix）
  * 行矩阵RowMatrix是最基础的分布式矩阵类型。每行是一个向量，行索引无实际意义（即无法直接使用）。
  * 数据存储在一个由行组成的RDD中，其中每一行都使用一个本地向量来进行存储。
  * 由于行是通过本地向量来实现的，故列数（行的维度）被限制在普通整型（integer）的范围内。
  * 在实际使用时，由于本计处理本地向量的存储和通信代价，行维度更是需要被控制在一个更小的范围之内。
  * RowMatrix可以通过一个RDD[Vector]的实例来创建。
  * 在获得RowMatrix实例后，可以通过其自带的computeColumnSummaryStatistics（）方法获取该矩阵的一些统计摘要信息，
  * 并可以对其进行QR分解、SVD分解和PCA分解，这一部分内容将在特征降维的章节详细解说，此处不再叙述。
  * 统计摘要信息部分代码如下方所示。
  *
  * （二）索引行矩阵（IndexedRowMatrix）
  * 索引行矩阵和行矩阵类似，但它的每一行都带有一个有意义的行索引值，这个索引值可以被用来识别不同行，或是进行诸如join之类的操作。
  * 其数据存储在一个由IndexedRow组成的RDD里，即每一行都是一个带长整形索引的本地向量。
  * 与RowMatrix类似，IndexedRowMatrix的实例可以通过RDD[IndexedRow]实例来创建。
  *
  * （三）坐标矩阵（Coordinate Matrix）
  * 坐标矩阵CoordinateMatrix是一个基于矩阵项构成的RDD的分布式矩阵。每一个矩阵项都是一个三元组：（行索引，列索引，value）。
  * 坐标矩阵一般在矩阵的两个维度都很大，且矩阵非常稀疏的时候使用。
  * CoordinateMatrix实例可通过RDD[MatrixEntry]实例来创建，其中每一个矩阵项都是一个(rowIndex,colIndex,elem)的三元组。
  * 坐标矩阵还可以通过transpose()方法对矩阵进行专职曹祖，并可以通过自带的toIndexedRowMatrix()方法转换成索引行矩阵IndexedRowMatrix。
  * 也可以转换成块矩阵，行矩阵。
  *
  * （四）分块矩阵（Block Matrix）
  * 分块矩阵是基于矩阵块MatrixBlock构成的分布式矩阵，其中每一个矩阵块MatrixBlock都是一个元组((Int, Int), Matrix)，
  * 其中(Int, Int)是块的索引，而Matrix则是在对应位置的子矩阵（sub-matrix），其尺寸由rowsPerBlock和colsPerBlock决定，默认值是1024.
  * 分块矩阵支持和另一个分块矩阵进行假发操作和乘法操作，并提供了一个支持方法validate()来确认分块矩阵是否创建成功。
  * 分块矩阵可由索引矩阵IndexedRowMatrix或坐标矩阵CoordinateMatrix调用toBlockMatrix()方法来进行转换，
  * 该方法将矩阵划分成尺寸默认为1024*1024的分块，可以在调用toBlockMatrix(rowsPerBlock, colsPerBlock)方法时传入参数来调整分块的尺寸。
  * 以矩阵 1  0  0  0为例，先利用矩阵项MatrixEntry将其构造成坐标矩阵，再转化成分块矩阵，然后进行操作。
  * 0  1  0  0
  * -1 2  1  0
  * 1  1  0  1
  * 分块矩阵BlockMatrix将矩阵分成一系列矩阵块，底层是由矩阵块构成的RDD来进行数据存储。
  * 需要指出，用来生成分布式矩阵的底层RDD必须是已经确定的，因为矩阵的尺寸将被存储下来，所以使用未确定的RDD将会导致错误。
  * 而且，不同类型的分布式矩阵之间的转换需要进行一个全局的shuffle操作，非常耗费资源。
  * 所以，根据数据本身的性质和应用需求来选取恰当的分布式矩阵存储类型时非常重要的。
  *
  *
  * 说明：RowMatrix在mllib包中，所需要的类型也是mllib中的Vector等。而ml包中的类型还不熟悉，所以就使用老版本的mllib进行测试
  */
object DistributedMatrix {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // #################################################################################################################
    // 创建两个本地稠密向量dv1,dv2
    val dv1: linalg.Vector = Vectors.dense(1, 2, 3)
    val dv2: linalg.Vector = Vectors.dense(2, 3, 4)
    // 使用两个本地向量创建一个RDD[Vector]
    val rows: RDD[linalg.Vector] = sc.parallelize(Array(dv1, dv2))
    // 通过RDD[Matrix]创建RowMatrix
    val matrix: RowMatrix = new RowMatrix(rows)
    // 打印行矩阵的行数和列数
    val numRows: Long = matrix.numRows()
    val numCols: Long = matrix.numCols()
    println(s"numRows: $numRows \nnumCols: $numCols")
    // 循环打印行矩阵数据
    // rows是将RowMatrix变成RDD[Vector]
    matrix.rows.foreach(println)
    // #################################################################################################################

    // 通过computeColumnSummaryStatistics()方法获取统计摘要类的实例
    val summary: MultivariateStatisticalSummary = matrix.computeColumnSummaryStatistics()
    // 获取矩阵的行数
    val rowsCount: Long = summary.count
    println(s"rowsCount: $rowsCount")
    // 最大向量
    val maxVector: linalg.Vector = summary.max
    println(s"maxVector: $maxVector")
    // 最小向量
    val minVector: linalg.Vector = summary.min
    println(s"minVector: $minVector")
    // 平均向量
    val meanVector: linalg.Vector = summary.mean
    println(s"meanVector: $meanVector")
    // L1范数向量
    val normL1Vector: linalg.Vector = summary.normL1
    println(s"normL1Vector: $normL1Vector")
    // #################################################################################################################

    // 通过本地向量dv1，dv2创建对应的IndexedRow
    // 在创建时可以给定行的索引值，如，这里给dv1的向量赋索引值1，dv2赋索引值2
    val indexRow1: IndexedRow = IndexedRow(1, dv1)
    val indexRow2: IndexedRow = IndexedRow(2, dv2)

    // 通过IndexedRow创建RDD[IndexedRow]
    val indexRows: RDD[IndexedRow] = sc.parallelize(Array(indexRow1, indexRow2))
    // 通过RDD[IndexedRow]创建一个索引行矩阵
    val indexRowMatrix = new IndexedRowMatrix(indexRows)
    // 打印相关信息
    // 为什么行数和列数的值是一样的？
    // 行数和列数：
    // numRows: 3
    // numCols: 3
    println(s"行数和列数：\n" +
      s"numRows: ${indexRowMatrix.numRows()}\n" +
      s"numCols: ${indexRowMatrix.numCols()}")
    indexRowMatrix.rows.foreach(println)
    // #################################################################################################################

    // 创建两个矩阵项ent1和ent2，每一个矩阵项都是由索引值和元素值构成的三元组
    val ent1: MatrixEntry = new MatrixEntry(0, 1, 0.5)
    val ent2: MatrixEntry = new MatrixEntry(2, 2, 1.8)
    // 创建RDD[MatrixEntry]
    val entries: RDD[MatrixEntry] = sc.parallelize(Array(ent1, ent2))
    // 通过RDD[MatrixEntry]创建一个坐标矩阵
    val coordinateMatrix = new CoordinateMatrix(entries)
    // 打印
    println(s"打印行数和列数：\n" +
      s"numRows: ${coordinateMatrix.numRows()}\n" +
      s"numCols: ${coordinateMatrix.numCols()}")
    coordinateMatrix.entries.foreach(println)
    // #################################################################################################################

    // 创建8个矩阵项
    val entry1 = new MatrixEntry(0, 0, 1)
    val entry2 = new MatrixEntry(1, 1, 1)
    val entry3 = new MatrixEntry(2, 0, -1)
    val entry4 = new MatrixEntry(2, 1, 2)
    val entry5 = new MatrixEntry(2, 2, 1)
    val entry6 = new MatrixEntry(3, 0, 1)
    val entry7 = new MatrixEntry(3, 1, 1)
    val entry8 = new MatrixEntry(3, 3, 1)
    // 创建RDD[MatrixEntry]
    val ents: RDD[MatrixEntry] = sc.parallelize(Array(entry1, entry2, entry3, entry4, entry5, entry6, entry7, entry8))
    // 通过RDD[MatrixEntry]创建坐标矩阵
    val coorMatrix = new CoordinateMatrix(ents)
    // 将坐标矩阵转换成2x2的分块矩阵并存储，尺寸通过参数传入
    val mat: BlockMatrix = coorMatrix.toBlockMatrix(2, 2).cache()
    // 判断分块矩阵是否创建成功。
    mat.validate()
    // 可以先通过toLocalMatrix转换成本地矩阵，查看其分块情况
    val localMatrix: Matrix = mat.toLocalMatrix()
    println("分块矩阵： \n" + localMatrix)
    // 查看分块情况
    println(s"分块情况：\n" +
      s"numColBlocks: ${mat.numColBlocks}\n" +
      s"numRowBlocks: ${mat.numRowBlocks}")
    // 计算矩阵和其转置矩阵的乘积（积矩阵）
    val ata: BlockMatrix = mat.transpose.multiply(mat)
    println(ata.toLocalMatrix())

    sc.stop()
    // close内部调用了stop方法
    spark.close()
    // stop方法内部调用了sparkContext方法
    // spark.stop()
  }
}
