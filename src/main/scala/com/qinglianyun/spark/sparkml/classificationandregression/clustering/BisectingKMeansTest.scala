package com.qinglianyun.spark.sparkml.classificationandregression.clustering

import org.apache.spark.ml.clustering.{BisectingKMeans, BisectingKMeansModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 二分k-means比普通k-means快，
  * 但是会产生一个不同的聚类
  */
object BisectingKMeansTest {

  /**
    * 官网实例
    *
    * @param spark
    */
  def bisectionKMeans(spark: SparkSession) = {

    val data = spark
      .read
      .format("libsvm")
      .load("src/main/data/sample_kmeans_data.txt")

    val bkm: BisectingKMeans = new BisectingKMeans()
      .setK(2)
      .setMaxIter(10)

    val model: BisectingKMeansModel = bkm.fit(data)

    val result: DataFrame = model.transform(data)
    result.show(150, false)

    // 计算输入点与其对应的聚类中心之间距离的平方和
    val cost = model.computeCost(data)
    println(s"Within Set Sum of Squared Errors = $cost")

    // 聚类中心点
    println("Cluster Centers: ")
    val centers = model.clusterCenters
    centers.foreach(println)


  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    bisectionKMeans(spark)

    spark.close()
  }

}
