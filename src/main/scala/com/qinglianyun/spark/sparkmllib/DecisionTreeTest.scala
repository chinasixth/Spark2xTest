package com.qinglianyun.spark.sparkmllib

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 14:25 2018/12/12
  * @ desc   : 决策树是一种基本的分类与回归的方法，这里主要测试用于分类的决策树。
  *
  * 决策树x学习通常包括三个步骤：
  * 特征选择、决策树的生成和决策树的剪枝
  *
  **/
object DecisionTreeTest {
  def main(args: Array[String]): Unit = {
    // 模板代码
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // 读取数据
    val originalData: RDD[String] = sc.textFile("src/main/data/iris.data")
    // 同样是使用LabeledPoint存储特征标签列和特征列
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
    // 打印一下数据
    println("labeledPointRDD: ")
    labeledPointRDD.foreachPartition((it: Iterator[LabeledPoint]) => {
      while (it.hasNext) {
        val label: LabeledPoint = it.next()
        println(label)
      }
    })

    // 划分数据集
    val trainingAndTest: Array[RDD[LabeledPoint]] = labeledPointRDD.randomSplit(Array(0.8, 0.2), 11L)
    val training: RDD[LabeledPoint] = trainingAndTest(0).cache()
    val test: RDD[LabeledPoint] = trainingAndTest(1)

    // 构建模型。调用决策树的trainClassifier方法构建决策树模型，设置参数，比如：分类数、信息增益的选择、树的最大深度等
    // 分类数
    val numClasses = 3
    // 地图存储分类特征的多样性。一个entry（n到k）表明特征n是分类的，k个类别从0索引：{0，1，...，k-1}。
    val categoricalFeaturesInfo: Map[Int, Int] = Map[Int, Int]()
    // 用于信息增益计算的标准。
    // 支持值：“gini”（推荐）或“entropy”。
    val impurity = "gini"
    // 决策树的最大深度
    val maxDepth = 5
    // 用于分割特征的最大箱数。（建议值：32）
    val maxBins = 32
    val model: DecisionTreeModel = DecisionTree.trainClassifier(training, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)
    val predictionAndLabel: RDD[(Double, Double)] = test.map {
      case LabeledPoint(label, features) => {
        val prediction: Double = model.predict(features)
        (prediction, label)
      }
    }
    // 打印模型结构
    // 使用的是toDebugString，并且不能使用括号
    println("Learned classification tree model: \n" + model.toDebugString)

    // 模型评估
    val metrics = new MulticlassMetrics(predictionAndLabel)
    val accuracy: Double = metrics.accuracy
    println(s"accuracy: \n" + accuracy)

    val testErr: Double = predictionAndLabel.filter(x => x._1 != x._2).count().toDouble / predictionAndLabel.count()
    println("手动计算：")
    println(1 - testErr)

    spark.close()

  }

}
