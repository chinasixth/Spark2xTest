package com.qinglianyun.spark.sparkml

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 19:03 2018/12/13
  * @
  */
object DecisionTreeTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val sqlContext = spark.sqlContext
    val sc = spark.sparkContext

    import spark.implicits._

    // 读取数据，只要鸢尾花花瓣的数据，也就是第三列和第四列作为features
    val dataFrame: DataFrame = sc.textFile("src/main/data/iris.data")
      .map(_.split(","))
      .map(x => Iris(Vectors.dense(x(2).toDouble, x(3).toDouble), x(4)))
      .toDF()

    // 构建Pipeline
    // 将字符标签进行索引
    val stringIndexerModel: StringIndexerModel = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexerLabel")
      .fit(dataFrame)

    // 选取所有特征中的某些特征进行索引
    val vectorIndexerModel: VectorIndexerModel = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexerFeatures")
      .fit(dataFrame)

    // 划分数据集
    val Array(training, test) = dataFrame.randomSplit(Array(0.7, 0.3))

    // 创建模型。
    val decisionTreeClassifier: DecisionTreeClassifier = new DecisionTreeClassifier()
      .setLabelCol("indexerLabel")
      .setFeaturesCol("indexerFeatures")
      .setMaxDepth(5)
      .setImpurity("gini")

    // 将训练得到的索引标签，转换成字符标签
    val indexToString: IndexToString = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictionLabel")
      .setLabels(stringIndexerModel.labels)

    // 组织成Pipeline
    val pipeline: Pipeline = new Pipeline()
      .setStages(Array(stringIndexerModel, vectorIndexerModel, decisionTreeClassifier, indexToString))

    // 训练模型
    val model: PipelineModel = pipeline.fit(training)

    // 将模型应用到测试集
    val frame: DataFrame = model.transform(test)
    frame.show()
    frame.select("indexerLabel", "probability", "prediction").show()

    // 模型评估
    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexerLabel")
      .setPredictionCol("prediction")
    println(evaluator.evaluate(frame))

    // 获得训练好的模型
    val classifier: DecisionTreeClassificationModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]

    println(classifier.toDebugString)

    // 使用模型去分析真实的数据
    val d: Double = classifier.predict(Vectors.dense(1.4, 0.2))

    println(d)

    spark.close()
  }

  case class Iris(features: Vector, label: String)

}
