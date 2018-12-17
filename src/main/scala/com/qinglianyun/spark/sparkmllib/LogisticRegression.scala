package com.qinglianyun.spark.sparkmllib

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 11:40 2018/12/12
  * @ desc   ：逻辑回归模型是一种经典的分类算法，属于对数线性模型
  *
  * 输入数据是LabeledPoint[label, features]类型的
  * label是标签，是开发人员自己提前分析好的
  * features是特征数据，也就是一个样本，这个样本属于什么标签，或者根据这个样本能得到什么样的标签。
  *
  *  机器学习：
  *  1.读取数据
  *    将数据整理成模型需要的类型
  *  2.划分数据集
  *    将数据集划分为训练集和测试集
  *  3.构建模型
  *  4.训练模型
  *  5.模型测试
  *  6.模型评估
  */
object LogisticRegression {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val originalData: RDD[String] = sc.textFile("src/main/data/iris.data")
    // 数据集中包含标签列和特征列，所以需要使用LabeledPoint来存储
    val labeledPointRDD: RDD[LabeledPoint] = originalData.map(_.split(","))
      .map(x => {
        LabeledPoint(
          if (x(4) == "Iris-setosa")
            0.toDouble
          else if (x(4) == "Iris-versicolor")
            1.toDouble
          else
            2.toDouble
          ,
          Vectors.dense(x(0).toDouble, x(1).toDouble, x(2).toDouble, x(3).toDouble)
        )
      })
    // 查看一下数据
    println("原始数据：")
    labeledPointRDD.foreach(println)

    // 构建模型
    // 首先对数据集进行划分，选择60%作为训练集，40%作为测试集
    // 第二个参数是随机种子。
    val trainAndTest: Array[RDD[LabeledPoint]] = labeledPointRDD.randomSplit(Array(0.8, 0.2), 11L)
    // 将训练集缓存
    val training: RDD[LabeledPoint] = trainAndTest(0).cache()
    // 测试集
    val test: RDD[LabeledPoint] = trainAndTest(1)

    // 选择模型，此处选择logistic模型，用set的方法设置参数，比如设置分类的数目，此处可以实现多分类模型
    // 选择BFGS是求解非线性优化问题的方法
    val model: LogisticRegressionModel = new LogisticRegressionWithLBFGS()
      .setNumClasses(3)
      .run(training)
    println(s"model: \n${model}")

    // 调用predict方法对测试数据进行预测，把结果保存在MulticlassMetrics中，
    val predictionAndLabel: RDD[(Double, Double)] = test.map {
      case LabeledPoint(label, features) => {
        val prediction: Double = model.predict(features)
        // 前面的是真正预测值，后面的是理论值
        (prediction, label)
      }
    }
    println("打印predictionAndLabel：")
    predictionAndLabel.foreach(println)

    // 模型评估。也就是将mo模型预测的准确度打印出来
    val metrics: MulticlassMetrics = new MulticlassMetrics(predictionAndLabel)
    val precision: Double = metrics.accuracy
    println(s"precision: \n${precision}")

    spark.close()
  }

}
