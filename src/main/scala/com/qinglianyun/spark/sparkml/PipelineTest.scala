package com.qinglianyun.spark.sparkml

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 20:25 2018/12/12
  * @ 
  */
object PipelineTest {
  def main(args: Array[String]): Unit = {
    // 模板代码
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local")
      .getOrCreate()

    val sqlContext = spark.sqlContext

    // 创建DataFrame
    val training: DataFrame = sqlContext.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop reduce", 0.0)
    )).toDF("id", "text", "label")

    // 定义Pipeline中的各个工作流阶段PipelineStage，包括转换器和评估器
    // 具体包含tokenizer， hashingTF和lr三个步骤
    val tokenizer: Tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("word")

    val hashingTF: HashingTF = new HashingTF()
      .setNumFeatures(1000) // 设置哈希表的桶数
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")

    val lr: LogisticRegression = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.01)  // 设置正则化参数

    // 组织PipelineStages创建一个Pipeline
    val pipeline: Pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

    // 现在构建的Pipeline本质上是一个Estimator，在它的fit方法运行之后，它将产生一个PipelineModel，它是一个Transformer
    val model: PipelineModel = pipeline.fit(training)

    val test: DataFrame = sqlContext.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "spark a"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    model.transform(test)
        .select("id","text","probability", "prediction")
        .collect()
        .foreach{
          case Row(id: Long, text: String, prob: Vector, prediction: Double ) => {
            println(s"($id, $text) --> prob=$prob, prediction=$prediction")
          }
        }
    // 数据说明：
    // prediction是预测的结果，
    // probability中，第一个值是预测成0.0的概率，第二个值是预测成1.0的结果
    // (4, spark i j k) --> prob=[0.5406433544851448,0.4593566455148551], prediction=0.0
    // (5, l m n) --> prob=[0.9334382627383266,0.0665617372616733], prediction=0.0
    // (6, spark a) --> prob=[0.15041430048068416,0.8495856995193158], prediction=1.0
    // (7, apache hadoop) --> prob=[0.9768636139518305,0.023136386048169477], prediction=0.0

    spark.close()
  }

}
