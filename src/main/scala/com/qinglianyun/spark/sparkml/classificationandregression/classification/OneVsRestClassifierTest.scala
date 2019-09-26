package com.qinglianyun.spark.sparkml.classificationandregression.classification

import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest, OneVsRestModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object OneVsRestClassifierTest {

  /**
    * 官网实例
    * One Vs Rest 是一个多分类器
    *
    * @param spark
    */
  def oneVsRestClassifier(spark: SparkSession) = {

    val data: DataFrame = spark
      .read
      .format("libsvm")
      .load("src/main/data/sample_multiclass_classification_data.txt")

    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    val lr: LogisticRegression = new LogisticRegression()
      .setMaxIter(10)
      .setTol(1E-6) // 迭代算法的收敛性参数
      .setFitIntercept(true) // 用于截距项判断

    val ovr: OneVsRest = new OneVsRest()
      .setClassifier(lr)

    val model: OneVsRestModel = ovr.fit(trainingData)

    val result: DataFrame = model.transform(testData)

    println(s"result count: ${result.count()}")
    result.show(false)

    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy: Double = evaluator.evaluate(result)
    println(s"accuracy: $accuracy")


  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    oneVsRestClassifier(spark)

    spark.close()
  }

}
