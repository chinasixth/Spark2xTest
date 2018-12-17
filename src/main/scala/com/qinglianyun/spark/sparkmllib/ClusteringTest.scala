package com.qinglianyun.spark.sparkmllib

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 19:18 2018/12/12
  * @ 
  */
object ClusteringTest {
  def main(args: Array[String]): Unit = {
    // 模板代码
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // 读取数据
    val originalData: RDD[String] = sc.textFile("src/main/data/iris.data")
    val trainingData: RDD[linalg.Vector] = originalData.map(_.split(",")).map(x =>
      Vectors.dense(x(0).toDouble, x(1).toDouble, x(2).toDouble, x(3).toDouble)
    ).cache()
    // 调用KMeans.train训练模型，指定k值为3，最大迭代100次，运行次数为5
    // * @param k Number of clusters to create.
    // * @param maxIterations Maximum number of iterations allowed.
    // * @param runs This param has no effect since Spark 2.0.0.
    // * @param initializationMode The initialization algorithm. This can either be "random" or
    // *                           "k-means||". (default: "k-means||")
    val model: KMeansModel = KMeans.train(trainingData, 3, 100, 5, "k-means||")

    // 查看所有聚类中心的情况
    println("所有聚类中心的情况：")
    model.clusterCenters.foreach(println)
    // 确定每个样本所属聚类
    trainingData.collect.foreach(sample => {
      val predictedCluster: Int = model.predict(sample)
      println(sample.toString + " belongs to cluster " + predictedCluster)
    })

    // 计算集合内误差平方和
    val wssse: Double = model.computeCost(trainingData)
    println("wssse: " + wssse)

    spark.close()
  }

}
