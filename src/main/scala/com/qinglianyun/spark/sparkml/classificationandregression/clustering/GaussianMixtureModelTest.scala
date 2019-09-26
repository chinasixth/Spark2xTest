package com.qinglianyun.spark.sparkml.classificationandregression.clustering

import org.apache.spark.ml.clustering.{GaussianMixture, GaussianMixtureModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object GaussianMixtureModelTest {

  /**
    * 官网实例
    *
    * @param spark
    */
  def gaussianMixtureModel(spark: SparkSession) = {

    val data = spark
      .read
      .format("libsvm")
      .load("src/main/data/sample_kmeans_data.txt")

    val gmm: GaussianMixture = new GaussianMixture()
      .setK(2)

    val model: GaussianMixtureModel = gmm.fit(data)

    val result: DataFrame = model.transform(data)

    result.show(false)

    for (i <- 0 until model.getK) {
      println(s"Gaussian $i:\nweight=${model.weights(i)}\n" +
        s"mu=${model.gaussians(i).mean}\nsigma=\n${model.gaussians(i).cov}\n")
    }

  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    gaussianMixtureModel(spark)

    spark.close()
  }

}
