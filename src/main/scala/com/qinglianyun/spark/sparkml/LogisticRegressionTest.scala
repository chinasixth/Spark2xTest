package com.qinglianyun.spark.sparkml

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.{Pipeline, PipelineModel, Transformer}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 16:04 2018/12/13
  * @
  */
object LogisticRegressionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val sqlContext = spark.sqlContext
    val sc = spark.sparkContext

    //    val obverations: RDD[linalg.Vector] = sc.textFile("src/main/data/iris.data")
    //      .map(_.split(","))
    //      .map(x => Vectors.dense(x(2).toDouble, x(3).toDouble))
    //
    //    // 获取一些概要信息，比如最大值、最小值、均值等
    //    val summary: MultivariateStatisticalSummary = Statistics.colStats(obverations)
    //    println("max: " + summary.max)
    //    println("min: " + summary.min)
    //    println("variance: " + summary.variance)
    //    println("mean" + summary.mean)
    //    println("numNonzeros" + summary.numNonzeros)

    import spark.implicits._

    // 1.读取数据
    val data: DataFrame = sc.textFile("src/main/data/iris.data")
      .map(_.split(","))
      .map(x => Iris(Vectors.dense(x(2).toDouble, x(3).toDouble), x(4)))
      .toDF()

    // 查看一下数据
    data.show()

    // 2.构建ML的pipeline
    val labelIndexer: StringIndexerModel = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)

    val vectorIndexer: VectorIndexerModel = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .fit(data)

    // 构建测试集和数据集
    val Array(training, test) = data.randomSplit(Array(0.7, 0.3), 3L)

    // 设置logistic参数
    val logisticRegression: LogisticRegression = new LogisticRegression()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    // println("LogisticRegression parameters:\n" + logisticRegression.explainParams() + "\n")

    // 设置一个labelConvert，目的是把预测的类别重新转化成字符型
    val indexToString: IndexToString = new IndexToString()
      .setInputCol("prediction") // prediction是预测结果自动生成的两个列之一
      .setOutputCol("predictionLabel")
      .setLabels(labelIndexer.labels)

    // 构建pipeline，设置stage，然后调用fit来训练模型
    val pipeline: Pipeline = new Pipeline()
      .setStages(Array(labelIndexer, vectorIndexer, logisticRegression, indexToString))

    // 3.训练模型
    val model: PipelineModel = pipeline.fit(training)

    // 使用训练好的模型对测试集进行验证
    val predictions: DataFrame = model.transform(test)

    // 查看一下预测结果
    // rawPrediction   probability   prediction是模型自己创建的
    predictions.select("indexedLabel", "probability", "prediction").foreach(println(_))

    // 4.模型评估
    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")

    // 打印一下准确性：
    println(evaluator.evaluate(predictions))

    // 通过model来获取训练好的模型
    // 返回的结果应该就是输入stage中参数对象，顺序应该也是不变的
    val stages: Array[Transformer] = model.stages
    val lrModel: LogisticRegressionModel = stages(2).asInstanceOf[LogisticRegressionModel]

    println("Coefficients: " + lrModel.coefficientMatrix + "Intercept: " + lrModel.interceptVector +
      "numClasses: " + lrModel.numClasses + "numFeatures: " + lrModel.numFeatures)

    spark.close()

  }

  case class Iris(features: Vector, label: String)

}
