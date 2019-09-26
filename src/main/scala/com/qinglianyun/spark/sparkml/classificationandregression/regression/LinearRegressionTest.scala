package com.qinglianyun.spark.sparkml.classificationandregression.regression

import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object LinearRegressionTest {

  /**
    * 线性回归
    *
    * @param spark
    */
  def linearRegression(spark: SparkSession) = {

    val data: DataFrame = spark
      .read
      .format("libsvm")
      .load("src/main/data/sample_multiclass_classification_data.txt")


    val lr: LinearRegression = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val model: LinearRegressionModel = lr.fit(data)

    val result: DataFrame = model.transform(data)

    result.show(false)


    val trainingSummary = model.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show() // 残差(标签-预测值)
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}") // 均方根误差(计算预测值与实际值差值的平方值的均值)
    println(s"r2: ${trainingSummary.r2}") // 决定系数

  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    linearRegression(spark)

    spark.close()
  }

}
