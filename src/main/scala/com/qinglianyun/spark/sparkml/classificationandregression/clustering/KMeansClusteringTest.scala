package com.qinglianyun.spark.sparkml.classificationandregression.clustering

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @
  * 轮廓系数定义为：
  *
  * s(o) = [b(o) -a(o)]/max{a(o), b(o)}
  *
  * 轮廓系数的值在-1和1之间。
  *
  * a(o)的值反映o所属的簇的紧凑性。该值越小，簇越紧凑。
  *
  * b(o)的值捕获o与其他簇的分离程度。b(o)的值越大，o与其他簇越分离。
  *
  * 当o的轮廓系数值接近1时，包含o的簇是紧凑的，并且o远离其他簇，这是一种可取的情况。
  *
  * 当轮廓系数的值为负时，这意味在期望情况下，o距离其他簇的对象比距离与自己同在簇的对象更近，许多情况下，这很糟糕，应当避免。
  */
object KMeansClusteringTest {

  /**
    * 官网示例
    *
    * @param spark
    */
  def kMeansClustering(spark: SparkSession) = {

    val data: DataFrame = spark
      .read
      .format("libsvm")
      .load("src/main/data/sample_kmeans_data.txt")

    val kMeans: KMeans = new KMeans()
      .setK(2) // 聚类个数，最终结果可能小于这个值
      .setSeed(1L)

    val model: KMeansModel = kMeans.fit(data)

    val result: DataFrame = model.transform(data)

    result.show()

    val evaluator = new ClusteringEvaluator()

    val silhouette: Double = evaluator.evaluate(result)

    println(s"Silhouette with squared euclidean distance = $silhouette")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)


  }

  /**
    * 鸢尾花数据测试
    *
    * @param spark
    */
  def kMeansClusteringIris(spark: SparkSession) = {

    val data: DataFrame = spark
      .read
      .format("csv")
      .load("src/main/data/iris.data")

    import spark.implicits._
    val dataDF: DataFrame = data.map { x =>
      (x.getString(0).toDouble, x.getString(1).toDouble, x.getString(2).toDouble, x.getString(3).toDouble, x.getString(4))
    }.toDF("a", "b", "c", "d", "strLabel")

    val assemblerDF: DataFrame = new VectorAssembler()
      .setInputCols(Array("a", "b", "c", "d"))
      .setOutputCol("features")
      .transform(dataDF)

    val Array(trainingData, testData) = assemblerDF.randomSplit(Array(0.7, 0.3))

    val kMeans: KMeans = new KMeans()
      .setK(3)
      .setSeed(1L)

    val model: KMeansModel = kMeans.fit(trainingData)

    val result: DataFrame = model.transform(testData)

    result.show(150)

    val evaluator = new ClusteringEvaluator()

    val silhouette: Double = evaluator.evaluate(result) // 轮廓系数[-1, 1]:对于D中的每个对象o,计算o与o所属的簇内其他对象之间的平均距离a(o)

    println(s"Silhouette with squared euclidean distance = $silhouette")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    //    kMeansClustering(spark)

    kMeansClusteringIris(spark)

    spark.close()

  }

}
