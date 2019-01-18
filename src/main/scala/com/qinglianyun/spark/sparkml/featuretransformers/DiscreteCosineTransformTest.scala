package com.qinglianyun.spark.sparkml.featuretransformers

import org.apache.spark.ml.feature.DCT
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 14:19 2019/1/9
  * @ desc: 离散余弦变换，主要用于（可能）信号和图像的有损数据的压缩
  * DCT和FFT变换都属于变换压缩方法，变换压缩的一个特点是将从前密度均匀的信息分布变换为密度不同的信息分布。
  * 在图像中，低频部分的信息要大于高频部分的信息量，尽管低频部分的数据量要比高频部分的数据量要小的多。
  * 例如，删除占50%存储空间的高频部分，信息量的损失可能还不到50%
  */
object DiscreteCosineTransformTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val data = Seq(
      Vectors.dense(0.0, 1.0, -2.0, 3.0),
      Vectors.dense(-1.0, 2.0, 4.0, -7.0),
      Vectors.dense(14.0, -2.0, -5.0, 1.0)
    )

    val dataset: DataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val dct: DCT = new DCT()
      .setInputCol("features")
      .setOutputCol("dctFeatures")
      .setInverse(false) // 指示是执行逆DCT (true)还是正向DCT (false)。 default: false

    val dcted: DataFrame = dct.transform(dataset)

    dcted.show(false)


    spark.close()
  }

}
