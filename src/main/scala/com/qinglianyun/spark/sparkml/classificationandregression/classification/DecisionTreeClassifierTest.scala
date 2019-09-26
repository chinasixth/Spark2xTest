package com.qinglianyun.spark.sparkml.classificationandregression.classification

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @ Author ：
  * @ Company: qinglian cloud
  * @ Date   ：Created in 
  * @ 
  */
object DecisionTreeClassifierTest {

  /**
    * 官网例子
    *
    * @param spark
    */
  def decisionTreeClassifier(spark: SparkSession) = {

    val data: DataFrame = spark
      .read
      .format("libsvm")
      .load("src/main/data/sample_libsvm_data.txt")

    /*
    * 将原始标签转换成索引标签
    * 没有手动设置原始标签和索引标签的映射关系数据集数据集
    * */
    val labelIndexer: StringIndexerModel = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)

    /*
    * VectorIndexer能够帮助索引向量数据集中的分类特征，即：向量中哪些列是可以用来作为分类的
    * categories用来限制分类列的个数，默认值：20
    * */
    val featureIndexer: VectorIndexerModel = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // 向量中最多有多少维作为分类特征
      .fit(data)

    /*
    * 划分数据集为训练集和测试集
    * 一般以训练集居多
    * 常用比例有  6:4  7:3  8:2  9:1
    * 如果两个数值加起来不等于1，会进行标准化
    * */
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    /*
    * 训练决策树模型
    * 输入：标签列、特征列
    * 输出：rawPrediction、probability、prediction
    * */
    val dt: DecisionTreeClassifier = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")

    /*
    * 将索引标签转换成原始的string
    * 因为没有手动设置数据集规定原始标签和索引标签的映射关系
    * 所以要设置原始标签
    * */
    val labelConverter: IndexToString = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    /*
    * 创建一个pipeline
    * 用来管理机器学习中一系列算法以及数据流的处理
    * */
    val pipeline: Pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

    val model: PipelineModel = pipeline.fit(trainingData)

    val predictions: DataFrame = model.transform(testData)

    //    predictions.select("predictedLabel", "label", "features")
    //      .show(150)

    //    predictions.show()

    /*
    * 创建评估器，查看模型的基本信息
    * 如：准确度
    * */
    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy: Double = evaluator.evaluate(predictions)
    //    println(s"Test Error = ${1.0 - accuracy}")
    println(s"accuracy: ${accuracy}")

  }

  /**
    * 鸢尾花数据测试
    *
    * @param spark
    */
  def decisionTreeClassifierIris(spark: SparkSession) = {

    val data: DataFrame = spark
      .read
      .format("text")
      .load("src/main/data/iris.data")

    /*
    * 数据类型处理
    * spark读取进来的数据类型是string（默认）
    * 算法中需要的数值型
    * */
    import spark.implicits._
    val dataDF: DataFrame = data.map((x: Row) => {

      val line: String = x.getString(0)
      val splits: Array[String] = line.split(",")

      (splits(0).toDouble, splits(1).toDouble, splits(2).toDouble, splits(3).toDouble, splits(4))
    }).toDF("a", "b", "c", "d", "label")

    val assemblerDF: DataFrame = new VectorAssembler()
      .setInputCols(Array("a", "b", "c", "d"))
      .setOutputCol("features")
      .setHandleInvalid("error") // 当遇到Null或NaN时如何处理
      .transform(dataDF)

    val indexerLabel: StringIndexerModel = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(assemblerDF)

    val indexerFeatures: VectorIndexerModel = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setHandleInvalid("error")
      .setMaxCategories(4)
      .fit(assemblerDF)

    val Array(trainingData, testData) = assemblerDF.randomSplit(Array(0.6, 0.4))

    val df: DecisionTreeClassifier = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")

    val labelConverter: IndexToString = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(indexerLabel.labels)

    val pipeline: Pipeline = new Pipeline()
      .setStages(Array(indexerLabel, indexerFeatures, df, labelConverter))

    val model: PipelineModel = pipeline.fit(trainingData)

    /*
    * 模拟生产数据，
    * 数据中只有特征数据，
    * pipeline中的操作，对训练数据和测试数据同样有效
    * 模型可以保存，下次使用的时候直接根据存储路径读取模型就可以
    * */
    val test: DataFrame = testData.select("features")

    val testResult: DataFrame = model.transform(test)

    println("testResult: ")
    testResult.show(false)

    val result: DataFrame = model.transform(testData)

    println(result.count())

    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("f1")

    //    val accuracy: Double = evaluator.evaluate(result)
    //
    //    println(s"accuracy: ${accuracy}")

    evaluator.setMetricName("f1") // 默认是f1

    val f1: Double = evaluator.evaluate(result)

    println(s"f1: $f1")

  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()


    for (i <- 1 to 100) {
      //      decisionTreeClassifier(spark)
      decisionTreeClassifierIris(spark)
    }

    spark.close()
  }

}
