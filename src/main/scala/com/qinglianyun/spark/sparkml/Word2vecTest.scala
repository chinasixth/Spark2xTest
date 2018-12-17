package com.qinglianyun.spark.sparkml

import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 14:06 2018/12/13
  * @ 
  */
object Word2vecTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val sqlContext = spark.sqlContext

    val documentDF: DataFrame = sqlContext.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text") // 注意apply后面不能加上小括号

    // 创建一个Word2vec并设置相应的参数，此处设置特征向量为3
    val word2Vec: Word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)

    // 读入训练集，调用fit方法生成一个Word2VctModel
    val model: Word2VecModel = word2Vec.fit(documentDF)

    // 利用Word2VecModel模型将文档转成特征向量
    val result: DataFrame = model.transform(documentDF)
    result.select("result")
      .take(3)
      .foreach(println(_))

    // 得到的特征向量可以应用到相关的机器学习算法中。

    spark.close()
  }

}
