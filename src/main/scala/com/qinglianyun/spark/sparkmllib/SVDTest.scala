package com.qinglianyun.spark.sparkmllib

import org.apache.spark.mllib.feature.{PCA, PCAModel}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 9:35 2018/12/12
  * @ desc   : 奇异值分解（SVD）
  *
  * 奇异值分解是MLlib机器学习库提供的两个常用的降维f方法之一
  * 还有一个是主成分分析（PCA）
  *
  *
  *
  *
  */
object SVDTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val svdData: RDD[String] = sc.textFile("src/main/data/svd.txt")

    val svdVector: RDD[linalg.Vector] = svdData.map(_.split(" ").map(_.toDouble)).map(line => Vectors.dense(line))

    val svdMatrix = new RowMatrix(svdVector)
    svdMatrix.rows.foreach(println)

    // 保留前三个奇异值
    // 参数说明：
    // 第二个参数默认是false，也就是不计算U成员，如果设置为true，就会就算U，计算结果中的U也就不为null
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = svdMatrix.computeSVD(3, false)

    // V、s、U拿到SVD分解后的右奇异矩阵、奇异值向量、左奇异矩阵
    // 因为限定了取前三个奇异值，所以就有三个奇异值从大到小排列
    // 右奇异矩阵的每一列都代表了对应的右奇异矩阵。
    println("右奇异矩阵：\n" + svd.V)
    println("奇异值向量：\n" + svd.s)
    println("左奇异向量：\n" + svd.U)
    // 右奇异矩阵：
    // -0.32908987300830383  0.6309429972945555    0.16077051991193514
    // -0.2208243332000108   -0.1315794105679425   -0.2368641953308101
    // -0.35540818799208057  0.39958899365222394   -0.147099615168733
    // -0.37221718676772064  0.2541945113699779    -0.25918656625268804
    // -0.3499773046239524   -0.24670052066546988  -0.34607608172732196
    // -0.21080978995485605  0.036424486072344636  0.7867152486535043
    // -0.38111806017302313  -0.1925222521055529   -0.09403561250768909
    // -0.32751631238613577  -0.3056795887065441   0.09922623079118417
    // -0.3982876638452927   -0.40941282445850646  0.26805622896042314
    // 奇异值向量：
    // [28.741265581939565,10.847941223452608,7.089519467626695]
    // 左奇异向量：
    // null

    // 以下三行代码是异想天开测试
    val projectedSVD: RowMatrix = svdMatrix.multiply(svd.V)
    println("完成SVD转换以后：")
    projectedSVD.rows.foreach(println)
    // #################################################################################################################

    // 第一种PCA变换

    val pcaVector: RDD[linalg.Vector] = svdData.map(_.split(" ").map(_.toDouble)).map(line => Vectors.dense(line))
    // 原矩阵是由4个样本，9个特征的数据集组成的
    val pcaMatrix = new RowMatrix(pcaVector)
    // 保留三个主要成分
    // 主成分矩阵是一个（9， 3）的矩阵，其中每一列代表一个主成分（新坐标轴），每一行代表原有的一个特征
    // 也就是说，将原来的九维的空间投影到一个三维空间中。
    val pca: Matrix = pcaMatrix.computePrincipalComponents(3)
    println("pca: \n" + pca)
    // 使用矩阵乘法来完成对原矩阵PCA转换
    val projected: RowMatrix = pcaMatrix.multiply(pca)
    println(s"完成PCA转换以后：")
    // 原有的（4，9）矩阵变换成（4，3）矩阵
    projected.rows.foreach(println)
    // 完成PCA转换以后：
    // [-1.2537294080109986,-10.15675264890709,-4.8697886049036025]
    // [12.247647483894383,-2.725468189870252,-5.568954759405281]
    // [2.8762985358626505,-2.2654415718974685,1.428630138613534]
    // [12.284448024169402,-12.510510992280857,-0.16048149283293078]
    // #################################################################################################################

    // 第二种PCA转换，“模型式PCA变换实现”
    // 该方法特别适用于原始数据是LabeledPoint类型的情况，只需要取出LabeledPoint中的feature成员，对其做PCA操作后，再放回，
    // 即可在不影响原有标签情况下进行PCA转换

    // 创建LabeledPoint，第一个标签为0，其他的为1
    val pcaLabel: RDD[LabeledPoint] = sc.textFile("src/main/data/svd.txt").map(_.split(" ").map(_.toDouble))
      .map(line => {
        LabeledPoint(if (line(0) > 1) 1.toDouble else 0.toDouble, Vectors.dense(line))
      })
    println("LabeledPoint: ")
    pcaLabel.foreach(println)

    // 开始进行pca转换
    // 创建一个pca对象。在构造器中给定主成分个数为3，并调用其fit方法来生成一个PCAModel类的对象pca,该对象保存了对应的主成分矩阵
    val pCAModel: PCAModel = new PCA(3).fit(pcaLabel.map(_.features))
    // 对于LabeledPoint类型的数据来说，可使用map算子对每一条数据进行处理，将features成员替换成PCA变换后的特征即可
    val projectPCA2: RDD[LabeledPoint] = pcaLabel.map(p => p.copy(features = pCAModel.transform(p.features)))
    println("projectPCA2: ")
    projectPCA2.foreach(println)
    // projectPCA2:
    // (0.0,[12.247647483894383,-2.725468189870252,-5.568954759405281])
    // (1.0,[12.284448024169402,-12.510510992280857,-0.16048149283293078])
    // (1.0,[-1.2537294080109986,-10.15675264890709,-4.8697886049036025])
    // (1.0,[2.8762985358626505,-2.2654415718974685,1.428630138613534])

   spark.close()

  }
}
