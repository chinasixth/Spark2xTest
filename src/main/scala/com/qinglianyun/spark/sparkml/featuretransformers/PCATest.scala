package com.qinglianyun.spark.sparkml.featuretransformers

import org.apache.spark.ml.feature.{PCA, PCAModel}
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 19:50 2019/1/8
  * @ desc: PCA 主成分分析法，是一种使用最广泛的数据压缩方法。
  *
  * 降维是对数据高纬度特征的一种预处理方法。降维是将高纬度的数据保留下最重要的一些特征，去除噪声和不重要的特征，从而实现提升数据处理速度的目的。
  * 在实际生产和应用中，降维在一定信息损失范围内，可以为我们节省大量的时间和成本。
  * 降维优点：
  * 使数据更易使用
  * 降低算法的计算开销
  * 去除噪声
  * 使得结果容易理解
  *
  * PCA思想：数据从原来的坐标系转换到新的坐标系，由数据本身决定。
  * 转换坐标系时，以方差最大的方向为坐标轴方向，选择方差是因为方差能给出数据最重要的信息
  * 第一个坐标轴选择的是原始数据中方差最大的方向，
  * 第二个坐标轴选择的是与第一个坐标轴正交且方差次大的方向，重复该过程，重复次数为原始数据的特征维数。
  * 通过这种方式获得的新的坐标系，可以发现，大部分方差都包含在前面的几个坐标轴中，后面的坐标轴所含的方差几乎为0，
  * 于是我们可以忽略余下的坐标轴，只保留恰免得几个含有绝大部分防擦和的坐标轴。
  * 事实上，这样做也就相当于只保留包含绝大部分方差的维度特征，而忽略方差几乎为0的特征维度，也就实现了对数据特征的降维处理。
  *
  * 如何计算？
  * 通过计算数据矩阵的协方差矩阵，然后得到协方差矩阵的特征值以及特征向量，选择特征值最大（也就是包含方差最大）的N个特征所对应的特征向量组成的矩阵，
  * 我们就可以将数据矩阵转换到新的空间当中，实现数据特征的降维（N维）
  *
  * 参考资料：https://www.cnblogs.com/zy230530/p/7074215.html
  *
  */
object PCATest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val data: Array[linalg.Vector] = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))), //稀疏向量
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0), // 稠密向量
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )

    val dataset: DataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val pca: PCA = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(3) // 主成分的数量。也就是要降到几维

    val pCAModel: PCAModel = pca.fit(dataset)

    val pCATransformed: DataFrame = pCAModel.transform(dataset)

    pCATransformed.show(false)
    // +---------------------+-----------------------------------------------------------+
    // |features             |pcaFeatures                                                |
    // +---------------------+-----------------------------------------------------------+
    // |(5,[1,3],[1.0,7.0])  |[1.6485728230883807,-4.013282700516296,-5.524543751369388] |
    // |[2.0,0.0,3.0,4.0,5.0]|[-4.645104331781534,-1.1167972663619026,-5.524543751369387]|
    // |[4.0,0.0,0.0,6.0,7.0]|[-6.428880535676489,-5.337951427775355,-5.524543751369389] |
    // +---------------------+-----------------------------------------------------------+

    spark.close()
  }

}
