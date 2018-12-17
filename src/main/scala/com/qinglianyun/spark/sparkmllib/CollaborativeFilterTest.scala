package com.qinglianyun.spark.sparkmllib

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 16:19 2018/12/12
  * @ 协同过滤算法：
  * 输入的数据：用户id、商品id、用户对商品的打分
  */
object CollaborativeFilterTest {
  def main(args: Array[String]): Unit = {
    // 模板代码
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // 读取数据
    val originalData: RDD[String] = sc.textFile("src/main/data/als/test.txt")
    // 将数据转化成模型需要的形式
    val ratings: RDD[Rating] = originalData.map(_.split(","))
      .map {
        case Array(user, item, rate) => {
          Rating(user.toInt, item.toInt, rate.toDouble)
        }
      }
    // 打印一下需要输入的数据
    ratings.foreach(println)

    // 划分数据集
    val trainingAndTest: Array[RDD[Rating]] = ratings.randomSplit(Array(0.6, 0.4), 11L)
    val Array(training, test) = Array(trainingAndTest(0), trainingAndTest(1))

    // 构建模型
    // 指定参数
    // 要使用的特征数量，也就是潜在因素的数量
    val rank = 10
    // ALS的迭代次数
    val numIterations = 10
    val model: MatrixFactorizationModel = ALS.train(training, rank, numIterations, 0.01)

    // 在 MLlib 中的实现有如下的参数:
    //
    // numBlocks 是用于并行化计算的分块个数 (设置为-1，为自动配置)。
    // rank 是模型中隐语义因子的个数。
    // iterations 是迭代的次数。
    // lambda 是ALS的正则化参数。
    // implicitPrefs 决定了是用显性反馈ALS的版本还是用适用隐性反馈数据集的版本。
    // alpha 是一个针对于隐性反馈 ALS 版本的参数，这个参数决定了偏好行为强度的基准。
    // 可以调整这些参数，不断优化结果，使均方差变小。比如：iterations越多，lambda较小，均方差会较小，推荐结果较优。
    // 上面的例子中调用了 ALS.train(ratings, rank, numIterations, 0.01) 。
    //
    // 我们还可以设置其他参数，调用方式如下：
    // val model = new ALS()
    //   .setRank(params.rank)
    //   .setIterations(params.numIterations)
    //   .setLambda(params.lambda)
    //   .setImplicitPrefs(params.implicitPrefs)
    //   .setUserBlocks(params.numUserBlocks)
    //   .setProductBlocks(params.numProductBlocks)
    //   .run(training)

    // 使用模型进行预测
    // 进行test的数据和training的数据格式不一样
    val testUserProduct: RDD[(Int, Int)] = test.map {
      case Rating(user, product, rate) => {
        (user, product)
      }
    }
    // 打印一下测试集：
    println("testUserProduct")
    testUserProduct.foreach(println)

    // 开始测试
    val predictions: RDD[((Int, Int), Double)] = model.predict(testUserProduct).map {
      case Rating(user, product, rate) => {
        ((user, product), rate)
      }
    }
    // 打印一下预测集
    println("predictions: ")
    predictions.foreach(println)

    // 将真实评分结果和测试评分结果进行合并
    val rateAndPre: RDD[((Int, Int), (Double, Double))] = test.map {
      case Rating(user, product, rate) => {
        ((user, product), rate)
      }
    }.join(predictions)
    println("rateAndPre: ")
    rateAndPre.foreach(println)

    // 打印出平均方差值
    // 均方差值越小越好
    val MSE: Double = rateAndPre.map {
      case ((user, product), (r1, r2)) => {
        val err = (r1 - r2)
        err * err
      }
    }.mean()
    println("Mean Squared Error: \n" + MSE)

    spark.close()
  }

}
