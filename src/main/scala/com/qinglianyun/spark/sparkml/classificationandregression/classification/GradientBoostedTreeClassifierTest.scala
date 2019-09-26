package com.qinglianyun.spark.sparkml.classificationandregression.classification

import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
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
object GradientBoostedTreeClassifierTest {

  /**
    * 官网例子
    * 梯度下降树分类: 当前只支持二分类
    *
    * @param spark
    */
  def gradientBoostedTreeClassifier(spark: SparkSession) = {

    val data: DataFrame = spark
      .read
      .format("libsvm")
      .load("src/main/data/sample_libsvm_data.txt")

    val labelIndexer: StringIndexerModel = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .setHandleInvalid("error")
      .fit(data)

    val featureIndexer: VectorIndexerModel = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(10)
      .fit(data)

    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    val gbt: GBTClassifier = new GBTClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(10)
      .setFeatureSubsetStrategy("auto")

    val labelConverter: IndexToString = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val model: PipelineModel = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))
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

    /*
    * 打印模型信息
    * */
    val gbtModel: GBTClassificationModel = model.stages(2).asInstanceOf[GBTClassificationModel]
    println(s"Learned classification GBT model:\n ${gbtModel.toDebugString}")
  }

  /**
    * 鸢尾花数据测试
    * 梯度回归只支持二分类，所以现有数据会出错
    *
    * @param spark
    */
  def gradientBoostedTreeClassifierIris(spark: SparkSession) = {

    val data: DataFrame = spark
      .read
      .format("csv")
      .load("src/main/data/iris.data")

    import spark.implicits._
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
      .setHandleInvalid("error")
      .fit(assemblerDF)

    val featureIndexer: VectorIndexerModel = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(assemblerDF)

    val Array(trainingData, testData) = assemblerDF.randomSplit(Array(0.7, 0.3))

    val gbt: GBTClassifier = new GBTClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(10)
      .setFeatureSubsetStrategy("auto")

    val labelConverter: IndexToString = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val model: PipelineModel = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))
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

    val gbtModel: GBTClassificationModel = model.stages(2).asInstanceOf[GBTClassificationModel]
    println(s"Learned classification GBT model:\n ${gbtModel.toDebugString}")


  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    //    gradientBoostedTreeClassifier(spark)

    gradientBoostedTreeClassifierIris(spark)

    spark.close()
  }

}
