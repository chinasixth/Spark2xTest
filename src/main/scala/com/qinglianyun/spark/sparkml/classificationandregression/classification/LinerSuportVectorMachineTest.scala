package com.qinglianyun.spark.sparkml.classificationandregression.classification

import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object LinerSuportVectorMachineTest {

  /**
    * 线性支持向量机算法
    * spark mllib中的算法目前支持二分类
    *
    * @param spark
    */
  def linerSupportVectorMachine(spark: SparkSession) = {

    val data: DataFrame = spark
      .read
      .format("libsvm")
      .load("src/main/data/sample_libsvm_data.txt")

    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    val lsvc: LinearSVC = new LinearSVC()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(10)
      .setRegParam(0.1) // 正则化参数

    val model: LinearSVCModel = lsvc.fit(trainingData)

    val result: DataFrame = model.transform(testData)

    println(s"rows: ${result.count()}")
    result.show()

    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy: Double = evaluator.evaluate(result)

    println(s"accuracy: $accuracy")

  }

  /**
    * 鸢尾花数据测试
    * 测试内容:  是否支持多分类
    * 验证结果： 不支持多分类
    *
    * @param spark
    */
  def linerSupportVectorMachineIris(spark: SparkSession) = {
    import spark.implicits._

    val data: DataFrame = spark
      .read
      .format("csv")
      .load("src/main/data/iris.data")

    val dataDF: DataFrame = data.map { x =>
      (x.getString(0).toDouble, x.getString(1).toDouble, x.getString(2).toDouble, x.getString(3).toDouble, x.getString(4))
    }.toDF("a", "b", "c", "d", "label")

    val assemblerDF: DataFrame = new VectorAssembler()
      .setInputCols(Array("a", "b", "c", "d"))
      .setOutputCol("features")
      .setHandleInvalid("error")
      .transform(dataDF)

    val labelIndexer: StringIndexerModel = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(assemblerDF)

    val Array(trainingData, testData) = assemblerDF.randomSplit(Array(0.7, 0.3))

    val lsvc: LinearSVC = new LinearSVC()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features")
      .setMaxIter(10)
      .setRegParam(0.1)

    val labelConverter: IndexToString = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val model: PipelineModel = new Pipeline()
      .setStages(Array(labelIndexer, lsvc, labelConverter))
      .fit(trainingData)

    val result: DataFrame = model.transform(testData)

    println(s"rows: ${result.count()}")
    result.show(false)

    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
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

    //    linerSupportVectorMachine(spark)
    linerSupportVectorMachineIris(spark)


    spark.close()
  }

}
