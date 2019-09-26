package com.qinglianyun.spark.sparkml.classificationandregression.classification

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ Random Forest 集合了许多决策树，避免过度拟合
  */
object RandomForestClassifierTest {

  /**
    * 官网例子
    *
    * @param spark
    */
  def randomForestClassifier(spark: SparkSession) = {

    val data: DataFrame = spark
      .read
      .format("libsvm")
      .load("src/main/data/sample_libsvm_data.txt")

    val labelIndexer: StringIndexerModel = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)

    val featureIndexer: VectorIndexerModel = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)

    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    val rf: RandomForestClassifier = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)

    val labelConverter: IndexToString = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val pipeline: Pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    val model: PipelineModel = pipeline.fit(trainingData)

    val result: DataFrame = model.transform(testData)

    println(s"rows: ${result.count()}")
    result.show()

    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy: Double = evaluator.evaluate(result)

    println(s"accuracy: $accuracy")


  }

  /**
    * 鸢尾花数据测试
    *
    * @param spark
    */
  def randomForestClassifierIris(spark: SparkSession) = {

    val data: DataFrame = spark
      .read
      .format("text")
      .load("src/main/data/iris.data")

    import spark.implicits._
    val dataDF: DataFrame = data.map { x =>
      val line: String = x.getString(0)

      val splits: Array[String] = line.split(",")

      (splits(0).toDouble, splits(1).toDouble, splits(2).toDouble, splits(3).toDouble, splits(4))
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

    val featureIndexer: VectorIndexerModel = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(assemblerDF)

    val Array(trainingData, testData) = assemblerDF.randomSplit(Array(0.7, 0.3))

    val rf: RandomForestClassifier = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)

    val labelConverter: IndexToString = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val model: PipelineModel = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))
      .fit(trainingData)

    val result: DataFrame = model.transform(testData)

    println(s"rows: ${result.count()}")
    result.show()

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

    //    randomForestClassifier(spark)

    randomForestClassifierIris(spark)

    spark.close()
  }

}
