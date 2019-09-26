package com.qinglianyun.spark.sparkml.classificationandregression.classification

import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object NaiveBayesTest {

  /**
    * 朴树贝叶斯
    * 多分类
    * spark mllib 支持 多项朴素贝叶斯和伯努利朴素贝叶斯
    *
    * @param spark
    */
  def navieBayes(spark: SparkSession) = {

    val data: DataFrame = spark
      .read
      .format("libsvm")
      .load("src/main/data/sample_multiclass_classification_data.txt")

    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    val model: NaiveBayesModel = new NaiveBayes()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .fit(trainingData)

    val result: DataFrame = model.transform(testData)
    println(s"result count: ${result.count()}")
    result.show()

    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy: Double = evaluator.evaluate(result)

    println(s"accuracy: $accuracy")


  }

  /**
    * 普通鸢尾花数据测试
    *
    * @param spark
    */
  def navieBayesIris(spark: SparkSession) = {

    val data: DataFrame = spark
      .read
      .format("csv")
      .load("src/main/data/iris.data")

    import spark.implicits._
    val dataDF: DataFrame = data.map { x =>
      (x.getString(0).toDouble, x.getString(1).toDouble, x.getString(2).toDouble, x.getString(3).toDouble, x.getString(4))
    }.toDF("a", "b", "c", "d", "label")

    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("a", "b", "c", "d"))
      .setOutputCol("features")
      .setHandleInvalid("error")

    val assemblerDF: DataFrame = assembler.transform(dataDF)

    val Array(trainingData, testData) = assemblerDF.randomSplit(Array(0.7, 0.3))

    val labelIndexer: StringIndexerModel = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .setHandleInvalid("error")
      .fit(assemblerDF)

    val featuresIndexer: VectorIndexerModel = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(assemblerDF)

    val nbs: NaiveBayes = new NaiveBayes()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features")

    val labelConverter: IndexToString = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val model: PipelineModel = new Pipeline()
      .setStages(Array(labelIndexer, featuresIndexer, nbs, labelConverter))
      .fit(trainingData)

    val result: DataFrame = model.transform(testData)

    println(s"result count: ${result.count()}")
    //    result.show()

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


    for (i <- 0 to 100) {
      //      navieBayes(spark)
      navieBayesIris(spark)
    }

    spark.close()
  }

}
